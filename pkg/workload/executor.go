// Copyright 2026 Justin Santa Barbara
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package workload

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// executor issues etcd RPCs shaped like kube-apiserver's storage layer and
// records their outcome. It is stateful: like the apiserver, it remembers the
// mod_revision of every key it wrote so that updates and deletes can be
// conditional on it.
type executor struct {
	cfg   *Config
	kv    clientv3.KV
	lease clientv3.Lease
	state *stateMap

	mu    sync.RWMutex
	stats *Stats

	// compaction state, as kept by apiserver's compactor
	compactMu      sync.Mutex
	compactVersion int64
	compactRev     int64
}

var errNotFound = errors.New("key not found")

func (e *executor) setStats(s *Stats) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stats = s
}

func (e *executor) getStats() *Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *executor) observe(op string, start time.Time, err error) error {
	stats := e.getStats()
	st := stats.Op(op)
	st.Count.Add(1)
	st.Latency.Observe(time.Since(start))
	if err != nil {
		st.Errors.Add(1)
		stats.recordError(op, err)
	}
	return err
}

func (e *executor) conflict(op string) {
	e.getStats().Op(op).Conflicts.Add(1)
}

// create is apiserver's Create: Txn(If mod_revision(key)==0 Then Put Else Get).
func (e *executor) create(ctx context.Context, op, key string, value []byte, leaseID clientv3.LeaseID) error {
	e.state.beginWrite(key)
	start := time.Now()
	var opts []clientv3.OpOption
	if leaseID != 0 {
		opts = append(opts, clientv3.WithLease(leaseID))
	}
	resp, err := e.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value), opts...)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return e.observe(op, start, err)
	}
	if !resp.Succeeded {
		// AlreadyExists; adopt the existing revision so later updates work.
		e.conflict(op)
		if kvs := resp.Responses[0].GetResponseRange().GetKvs(); len(kvs) > 0 {
			e.state.endWrite(key, kvs[0].ModRevision)
		}
		return e.observe(op, start, fmt.Errorf("create %s: already exists", key))
	}
	e.state.endWrite(key, resp.Header.Revision)
	return e.observe(op, start, nil)
}

// update is apiserver's GuaranteedUpdate: Txn(If mod_revision(key)==rev Then
// Put Else Get); on a conflict it re-reads the revision and retries once.
func (e *executor) update(ctx context.Context, op, key string, value []byte) error {
	return e.updateWithLease(ctx, op, key, value, 0)
}

// updateWithLease is update with the value attached to leaseID (if non-zero).
func (e *executor) updateWithLease(ctx context.Context, op, key string, value []byte, leaseID clientv3.LeaseID) error {
	return e.updateOrCreate(ctx, op, key, value, leaseID, false)
}

// upsertWithLease is updateWithLease that creates the key if it no longer
// exists — GuaranteedUpdate with ignoreNotFound, as the masterlease
// reconciler uses, since a lease-attached key vanishes when its lease lapses.
func (e *executor) upsertWithLease(ctx context.Context, op, key string, value []byte, leaseID clientv3.LeaseID) error {
	return e.updateOrCreate(ctx, op, key, value, leaseID, true)
}

func (e *executor) updateOrCreate(ctx context.Context, op, key string, value []byte, leaseID clientv3.LeaseID, createIfMissing bool) error {
	rev := e.state.beginWrite(key)
	start := time.Now()
	var opts []clientv3.OpOption
	if leaseID != 0 {
		opts = append(opts, clientv3.WithLease(leaseID))
	}
	for attempt := 0; ; attempt++ {
		resp, err := e.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
			Then(clientv3.OpPut(key, string(value), opts...)).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			return e.observe(op, start, err)
		}
		if resp.Succeeded {
			e.state.endWrite(key, resp.Header.Revision)
			return e.observe(op, start, nil)
		}
		e.conflict(op)
		kvs := resp.Responses[0].GetResponseRange().GetKvs()
		if len(kvs) == 0 {
			e.state.delete(key)
			if createIfMissing {
				e.observe(op, start, nil)
				return e.create(ctx, op, key, value, leaseID)
			}
			return e.observe(op, start, fmt.Errorf("update %s: %w", key, errNotFound))
		}
		rev = kvs[0].ModRevision
		if attempt >= 1 {
			e.state.endWrite(key, rev)
			return e.observe(op, start, fmt.Errorf("update %s: conflict after retry", key))
		}
	}
}

// delete is apiserver's Delete: a Get to learn the current state, then
// Txn(If mod_revision(key)==rev Then DeleteRange Else Get).
func (e *executor) delete(ctx context.Context, op, key string) error {
	start := time.Now()
	getResp, err := e.kv.Get(ctx, key)
	if err != nil {
		return e.observe(op, start, err)
	}
	if len(getResp.Kvs) == 0 {
		e.state.delete(key)
		return e.observe(op, start, fmt.Errorf("delete %s: %w", key, errNotFound))
	}
	rev := getResp.Kvs[0].ModRevision
	e.state.beginWrite(key)
	for attempt := 0; ; attempt++ {
		resp, err := e.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
			Then(clientv3.OpDelete(key)).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			return e.observe(op, start, err)
		}
		if resp.Succeeded {
			e.state.delete(key)
			return e.observe(op, start, nil)
		}
		e.conflict(op)
		kvs := resp.Responses[0].GetResponseRange().GetKvs()
		if len(kvs) == 0 {
			e.state.delete(key)
			return e.observe(op, start, nil)
		}
		rev = kvs[0].ModRevision
		if attempt >= 1 {
			return e.observe(op, start, fmt.Errorf("delete %s: conflict after retry", key))
		}
	}
}

// get is a plain single-key Range. The apiserver uses it on the bare resource
// key (/registry/pods) to learn the current revision for a consistent read
// from its watch cache, and on /registry/health as its etcd health probe.
func (e *executor) get(ctx context.Context, op, key string) error {
	start := time.Now()
	_, err := e.kv.Get(ctx, key)
	return e.observe(op, start, err)
}

// listAll is an unpaginated list of a small prefix, as the masterlease
// reconciler does every 10s.
func (e *executor) listAll(ctx context.Context, op, prefix string) error {
	start := time.Now()
	_, err := e.kv.Get(ctx, prefix, clientv3.WithRange(prefixEnd(prefix)))
	return e.observe(op, start, err)
}

// list is a full paginated list of prefix at a consistent revision, as a
// reflector's initial list does. It returns the number of keys seen.
func (e *executor) list(ctx context.Context, op, prefix string, pageSize int64) (int, error) {
	start := time.Now()
	end := prefixEnd(prefix)
	key := prefix
	var rev int64
	n := 0
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithLimit(pageSize)}
		if rev != 0 {
			opts = append(opts, clientv3.WithRev(rev))
		}
		resp, err := e.kv.Get(ctx, key, opts...)
		if err != nil {
			return n, e.observe(op, start, err)
		}
		if rev == 0 {
			rev = resp.Header.Revision
		}
		n += len(resp.Kvs)
		if !resp.More || len(resp.Kvs) == 0 {
			return n, e.observe(op, start, nil)
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
}

// count is apiserver's Count: a count-only range over the resource prefix.
func (e *executor) count(ctx context.Context, op, prefix string) error {
	start := time.Now()
	_, err := e.kv.Get(ctx, prefix, clientv3.WithRange(prefixEnd(prefix)), clientv3.WithCountOnly())
	return e.observe(op, start, err)
}

// leaseGrant grants a lease with the given TTL.
func (e *executor) leaseGrant(ctx context.Context, op string, ttl time.Duration) (clientv3.LeaseID, error) {
	start := time.Now()
	resp, err := e.lease.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, e.observe(op, start, err)
	}
	return resp.ID, e.observe(op, start, nil)
}

// compact is one round of apiserver's compactor: bump compact_rev_key under a
// version guard, then compact to the revision recorded by the previous round.
func (e *executor) compact(ctx context.Context, op string) error {
	e.compactMu.Lock()
	defer e.compactMu.Unlock()
	start := time.Now()

	resp, err := e.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(compactRevKey), "=", e.compactVersion)).
		Then(clientv3.OpPut(compactRevKey, strconv.FormatInt(e.compactRev, 10))).
		Else(clientv3.OpGet(compactRevKey)).
		Commit()
	if err != nil {
		return e.observe(op, start, err)
	}
	if !resp.Succeeded {
		// Another apiserver compacted; adopt its state.
		e.conflict(op)
		if kvs := resp.Responses[0].GetResponseRange().GetKvs(); len(kvs) > 0 {
			e.compactVersion = kvs[0].Version
			e.compactRev, _ = strconv.ParseInt(string(kvs[0].Value), 10, 64)
		}
		return e.observe(op, start, nil)
	}
	e.compactVersion++
	prev := e.compactRev
	e.compactRev = resp.Header.Revision
	if prev == 0 {
		return e.observe(op, start, nil)
	}
	_, err = e.kv.Compact(ctx, prev)
	return e.observe(op, start, err)
}

// leaseCache reuses a lease across many objects, as apiserver's etcd3
// leaseManager does for Events: a lease is reused for up to reuseDuration or
// maxObjects, whichever comes first.
type leaseCache struct {
	mu            sync.Mutex
	ttl           time.Duration
	reuseDuration time.Duration
	maxObjects    int

	id       clientv3.LeaseID
	reuseEnd time.Time
	attached int
}

func newLeaseCache(ttl time.Duration) *leaseCache {
	return &leaseCache{ttl: ttl, reuseDuration: time.Minute, maxObjects: 1000}
}

func (l *leaseCache) get(ctx context.Context, e *executor) (clientv3.LeaseID, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if l.id != 0 && now.Before(l.reuseEnd) && l.attached < l.maxObjects {
		l.attached++
		return l.id, nil
	}
	// apiserver adds a little to the TTL so that reused leases still cover
	// the full TTL for the last object attached.
	id, err := e.leaseGrant(ctx, OpLeaseGrant, l.ttl+l.reuseDuration)
	if err != nil {
		return 0, err
	}
	l.id = id
	l.reuseEnd = now.Add(l.reuseDuration)
	l.attached = 1
	return id, nil
}
