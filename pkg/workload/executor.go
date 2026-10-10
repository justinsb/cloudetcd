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
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// executor issues etcd RPCs shaped like kube-apiserver's storage layer and
// records their outcome. It is stateful: like the apiserver, it remembers the
// mod_revision of every key it wrote so that updates and deletes can be
// conditional on it. When a Checker is attached, every response is also
// checked against the oracle.
type executor struct {
	cfg   *Config
	kv    clientv3.KV
	lease clientv3.Lease
	state *stateMap
	// check is the correctness oracle, or nil to only measure.
	check *Checker

	mu    sync.RWMutex
	stats *Stats

	// compaction state, as kept by apiserver's compactor
	compactMu      sync.Mutex
	compactVersion int64
	compactRev     int64
	// lastCompacted is the revision the most recent Compact RPC asked for;
	// the server's compaction point is at or below it.
	lastCompacted atomic.Int64
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

// observe records an op's outcome and returns err, for callers that return it.
func (e *executor) observe(op string, start time.Time, err error) error {
	e.record(op, start, err)
	return err
}

// record records an op's outcome.
func (e *executor) record(op string, start time.Time, err error) {
	stats := e.getStats()
	st := stats.Op(op)
	st.Count.Add(1)
	st.Latency.Observe(time.Since(start))
	if err != nil {
		st.Errors.Add(1)
		stats.recordError(op, err)
	}
}

func (e *executor) conflict(op string) {
	e.getStats().Op(op).Conflicts.Add(1)
}

// stamp prepares a value for writing: with a checker attached it gets the
// nonce trailer the oracle matches reads against.
func (e *executor) stamp(value []byte) ([]byte, uint64) {
	if e.check == nil {
		return value, 0
	}
	return e.check.stamp(value)
}

// begin returns the revision floor for a request about to be issued.
func (e *executor) begin() int64 {
	if e.check == nil {
		return 0
	}
	return e.check.begin()
}

// header checks a response header against the floor.
func (e *executor) header(hdr *etcdserverpb.ResponseHeader, floor int64, op string) {
	if e.check != nil {
		e.check.header(hdr, floor, op)
	}
}

// writeBegin/writeEnd bracket a write for the oracle's in-flight accounting.
func (e *executor) writeBegin(key string) {
	if e.check != nil {
		e.check.writeBegin(key)
	}
}

func (e *executor) writeEnd(key string, ok bool) {
	if e.check != nil {
		e.check.writeEnd(key, ok)
	}
}

// create is apiserver's Create: Txn(If mod_revision(key)==0 Then Put Else Get).
func (e *executor) create(ctx context.Context, op, key string, value []byte, leaseID clientv3.LeaseID) error {
	e.state.beginWrite(key)
	e.writeBegin(key)
	value, nonce := e.stamp(value)
	start := time.Now()
	floor := e.begin()
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
		e.writeEnd(key, false)
		return e.observe(op, start, err)
	}
	e.header(resp.Header, floor, op)
	if !resp.Succeeded {
		// AlreadyExists; adopt the existing revision so later updates work.
		e.conflict(op)
		if kvs := resp.Responses[0].GetResponseRange().GetKvs(); len(kvs) > 0 {
			e.state.endWrite(key, kvs[0].ModRevision)
		}
		if e.check != nil {
			e.check.conflict(key, 0, (*etcdserverpb.TxnResponse)(resp), op)
		}
		e.writeEnd(key, true)
		return e.observe(op, start, fmt.Errorf("create %s: already exists", key))
	}
	e.state.endWrite(key, resp.Header.Revision)
	if e.check != nil {
		e.check.createOK(key, resp.Header.Revision, nonce, int64(leaseID), op)
	}
	e.writeEnd(key, true)
	return e.observe(op, start, nil)
}

// update is apiserver's GuaranteedUpdate: Txn(If mod_revision(key)==rev Then
// Put Else Get); on a conflict it re-reads the revision and retries, up to
// Config.UpdateRetries times.
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
	return e.updateFrom(ctx, op, key, rev, value, leaseID, createIfMissing, false)
}

// contendedUpdate is GuaranteedUpdate as a second writer does it: read the
// current mod_revision, then write conditionally on it, retrying on
// conflict. It is what a controller or the scheduler does to an object a
// kubelet also writes, and it does not update the owner's remembered
// revision, so the owner sees the conflict the apiserver would.
func (e *executor) contendedUpdate(ctx context.Context, op, key string, value []byte) error {
	start := time.Now()
	floor := e.begin()
	getResp, err := e.kv.Get(ctx, key)
	if err != nil {
		return e.observe(op, start, err)
	}
	e.header(getResp.Header, floor, op)
	if e.check != nil {
		e.check.rangeResult(key, (*etcdserverpb.RangeResponse)(getResp), op)
	}
	if len(getResp.Kvs) == 0 {
		// The object is gone (the pod was deleted under us): the apiserver
		// returns NotFound to its client, which is not a failure here.
		return e.observe(op, start, nil)
	}
	return e.updateFrom(ctx, op, key, getResp.Kvs[0].ModRevision, value, 0, false, true)
}

// updateFrom issues the conditional put loop from a known revision. If
// contended is set the owner's remembered revision is left alone.
func (e *executor) updateFrom(ctx context.Context, op, key string, rev int64, value []byte, leaseID clientv3.LeaseID, createIfMissing, contended bool) error {
	e.writeBegin(key)
	value, nonce := e.stamp(value)
	start := time.Now()
	var opts []clientv3.OpOption
	if leaseID != 0 {
		opts = append(opts, clientv3.WithLease(leaseID))
	}
	remember := func(rev int64) {
		if !contended {
			e.state.endWrite(key, rev)
		}
	}
	for attempt := 0; ; attempt++ {
		floor := e.begin()
		resp, err := e.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
			Then(clientv3.OpPut(key, string(value), opts...)).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			e.writeEnd(key, false)
			return e.observe(op, start, err)
		}
		e.header(resp.Header, floor, op)
		if resp.Succeeded {
			remember(resp.Header.Revision)
			if e.check != nil {
				e.check.put(key, resp.Header.Revision, nonce, int64(leaseID), op)
			}
			e.writeEnd(key, true)
			return e.observe(op, start, nil)
		}
		e.conflict(op)
		if e.check != nil {
			e.check.conflict(key, rev, (*etcdserverpb.TxnResponse)(resp), op)
		}
		kvs := resp.Responses[0].GetResponseRange().GetKvs()
		if len(kvs) == 0 {
			e.writeEnd(key, true)
			if !contended {
				e.state.delete(key)
			}
			if createIfMissing {
				e.record(op, start, nil)
				return e.create(ctx, op, key, value[:len(value)-e.stampSize()], leaseID)
			}
			if contended {
				return e.observe(op, start, nil) // deleted under us: NotFound to the client
			}
			return e.observe(op, start, fmt.Errorf("update %s: %w", key, errNotFound))
		}
		rev = kvs[0].ModRevision
		if attempt >= e.cfg.UpdateRetries {
			remember(rev)
			e.writeEnd(key, true)
			return e.observe(op, start, fmt.Errorf("update %s: conflict after %d retries", key, attempt))
		}
	}
}

// stampSize is the length of the trailer stamp adds.
func (e *executor) stampSize() int {
	if e.check == nil {
		return 0
	}
	return nonceSize
}

// delete is apiserver's Delete: a Get to learn the current state, then
// Txn(If mod_revision(key)==rev Then DeleteRange Else Get).
func (e *executor) delete(ctx context.Context, op, key string) error {
	start := time.Now()
	floor := e.begin()
	getResp, err := e.kv.Get(ctx, key)
	if err != nil {
		return e.observe(op, start, err)
	}
	e.header(getResp.Header, floor, op)
	if e.check != nil {
		e.check.rangeResult(key, (*etcdserverpb.RangeResponse)(getResp), op)
	}
	if len(getResp.Kvs) == 0 {
		e.state.delete(key)
		return e.observe(op, start, fmt.Errorf("delete %s: %w", key, errNotFound))
	}
	rev := getResp.Kvs[0].ModRevision
	e.state.beginWrite(key)
	e.writeBegin(key)
	for attempt := 0; ; attempt++ {
		floor := e.begin()
		resp, err := e.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
			Then(clientv3.OpDelete(key)).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			e.writeEnd(key, false)
			return e.observe(op, start, err)
		}
		e.header(resp.Header, floor, op)
		if resp.Succeeded {
			e.state.delete(key)
			if e.check != nil {
				e.check.del(key, resp.Header.Revision, op)
			}
			e.writeEnd(key, true)
			return e.observe(op, start, nil)
		}
		e.conflict(op)
		if e.check != nil {
			e.check.conflict(key, rev, (*etcdserverpb.TxnResponse)(resp), op)
		}
		kvs := resp.Responses[0].GetResponseRange().GetKvs()
		if len(kvs) == 0 {
			e.state.delete(key)
			e.writeEnd(key, true)
			return e.observe(op, start, nil)
		}
		rev = kvs[0].ModRevision
		if attempt >= e.cfg.UpdateRetries {
			e.writeEnd(key, true)
			return e.observe(op, start, fmt.Errorf("delete %s: conflict after %d retries", key, attempt))
		}
	}
}

// get is a plain single-key Range. The apiserver uses it on the bare resource
// key (/registry/pods) to learn the current revision for a consistent read
// from its watch cache, and on /registry/health as its etcd health probe.
func (e *executor) get(ctx context.Context, op, key string) error {
	start := time.Now()
	floor := e.begin()
	resp, err := e.kv.Get(ctx, key)
	if err != nil {
		return e.observe(op, start, err)
	}
	e.header(resp.Header, floor, op)
	if e.check != nil {
		e.check.rangeResult(key, (*etcdserverpb.RangeResponse)(resp), op)
	}
	return e.observe(op, start, nil)
}

// listAll is an unpaginated list of a small prefix, as the masterlease
// reconciler does every 10s.
func (e *executor) listAll(ctx context.Context, op, prefix string) error {
	start := time.Now()
	floor := e.begin()
	resp, err := e.kv.Get(ctx, prefix, clientv3.WithRange(prefixEnd(prefix)))
	if err != nil {
		return e.observe(op, start, err)
	}
	e.header(resp.Header, floor, op)
	if e.check != nil {
		l := e.check.list(prefix, op)
		l.page((*etcdserverpb.RangeResponse)(resp))
	}
	return e.observe(op, start, nil)
}

// list is a full paginated list of prefix at a consistent revision, as a
// reflector's initial list does. It returns the number of keys seen. With
// complete set, the oracle also checks that nothing live was left out
// (which costs a pass over the oracle's keys).
func (e *executor) list(ctx context.Context, op, prefix string, pageSize int64, complete bool) (int, error) {
	start := time.Now()
	end := prefixEnd(prefix)
	key := prefix
	var rev int64
	n := 0
	var l *listCheck
	if e.check != nil {
		l = e.check.list(prefix, op)
	}
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithLimit(pageSize)}
		if rev != 0 {
			opts = append(opts, clientv3.WithRev(rev))
		}
		floor := e.begin()
		resp, err := e.kv.Get(ctx, key, opts...)
		if err != nil {
			return n, e.observe(op, start, err)
		}
		e.header(resp.Header, floor, op)
		if rev == 0 {
			rev = resp.Header.Revision
		}
		if l != nil {
			l.page((*etcdserverpb.RangeResponse)(resp))
		}
		n += len(resp.Kvs)
		if !resp.More || len(resp.Kvs) == 0 {
			if l != nil && complete {
				l.complete()
			}
			return n, e.observe(op, start, nil)
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
}

// count is apiserver's Count: a count-only range over the resource prefix.
func (e *executor) count(ctx context.Context, op, prefix string) error {
	start := time.Now()
	floor := e.begin()
	resp, err := e.kv.Get(ctx, prefix, clientv3.WithRange(prefixEnd(prefix)), clientv3.WithCountOnly())
	if err != nil {
		return e.observe(op, start, err)
	}
	e.header(resp.Header, floor, op)
	return e.observe(op, start, nil)
}

// leaseGrant grants a lease with the given TTL.
func (e *executor) leaseGrant(ctx context.Context, op string, ttl time.Duration) (clientv3.LeaseID, error) {
	start := time.Now()
	resp, err := e.lease.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, e.observe(op, start, err)
	}
	if e.check != nil {
		e.check.lease(int64(resp.ID), time.Duration(resp.TTL)*time.Second, start)
	}
	return resp.ID, e.observe(op, start, nil)
}

// compact is one round of apiserver's compactor: bump compact_rev_key under a
// version guard, then compact to the revision recorded by the previous round.
func (e *executor) compact(ctx context.Context, op string) error {
	e.compactMu.Lock()
	defer e.compactMu.Unlock()
	start := time.Now()

	floor := e.begin()
	resp, err := e.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(compactRevKey), "=", e.compactVersion)).
		Then(clientv3.OpPut(compactRevKey, strconv.FormatInt(e.compactRev, 10))).
		Else(clientv3.OpGet(compactRevKey)).
		Commit()
	if err != nil {
		return e.observe(op, start, err)
	}
	e.header(resp.Header, floor, op)
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
	if err == nil {
		e.lastCompacted.Store(prev)
	}
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

func newLeaseCache(ttl, reuse time.Duration) *leaseCache {
	return &leaseCache{ttl: ttl, reuseDuration: reuse, maxObjects: 1000}
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
