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
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Checker is the correctness oracle for a run: a client-side history of
// every write the workload made, against which every response the server
// gives is checked. The workload is the only client, so the oracle knows the
// complete history of every key and can tell a wrong answer from a
// surprising one.
//
// Every value written carries a trailer with a nonce unique to that write
// (cloudetcd never inspects values, so this changes nothing on the server),
// which is how a value read back is matched to the write that produced it.
//
// What is checked, in terms of what kube-apiserver relies on:
//
//   - Conditional writes: a Txn whose compare on mod_revision succeeds is the
//     only writer at that revision (revisions are never assigned twice); a
//     compare that fails returns a current mod_revision that is not the one
//     compared against; a create cannot succeed over a key the oracle knows
//     to be live; a key cannot vanish (a conflict's Else-Range finding
//     nothing) unless it was deleted or its lease expired.
//   - Reads: a key read at revision R has the value, version, create_revision
//     and lease of the oracle's latest write at or below R; a write
//     acknowledged at revision w <= R is never missing from a read at R
//     (stale read); a revision the oracle has never seen is a phantom.
//   - Real-time order: a response's header revision is never below a
//     revision observed in any response received before the request was
//     issued.
//   - Watches: events on a prefix arrive in strictly increasing revision
//     order, each matching the oracle's write at that revision with prev_kv
//     matching the previous version; no event arrives for a revision a
//     progress notification already covered; every write under the prefix
//     in the watched range is delivered exactly once.
//   - Lists: keys are strictly ascending across pages at the list revision,
//     and a full list has exactly the oracle's live keys.
//   - Leases: a key is deleted by the server only when its current lease has
//     had time to expire; a key re-attached to a fresh lease survives the
//     old lease's expiry (what the apiserver's masterlease relies on).
//
// Checks that cannot be settled at once (a revision the oracle has not seen
// yet while a write to that key is in flight) are parked and settled in
// Drain, once the in-flight writes have resolved.
type Checker struct {
	shards [checkShards]checkShard
	revs   [checkShards]revShard

	leaseMu sync.Mutex
	leases  map[int64]leaseInfo

	// floor is the highest header revision seen in any response.
	floor atomic.Int64
	nonce atomic.Uint64

	vmu        sync.Mutex
	violations []Violation
	dropped    int64
	pending    []func() *Violation
	// stop is called with the first violation, so a run can be cut short.
	stop func(v Violation)

	// MaxViolations caps the violations kept; beyond it only a count is.
	MaxViolations int
	// Clock is for tests.
	now func() time.Time
}

const checkShards = 256

type checkShard struct {
	mu   sync.Mutex
	keys map[string]*keyHistory
}

type revShard struct {
	mu sync.Mutex
	m  map[int64]string // revision -> key written at it
}

type leaseInfo struct {
	granted time.Time // when the grant was issued (so the earliest expiry is granted+ttl)
	ttl     time.Duration
}

// version is one state of a key in the oracle's history.
type version struct {
	rev     int64
	nonce   uint64 // 0 for a delete
	deleted bool
	lease   int64
	// expired marks a delete the server made (lease expiry) rather than one
	// the workload issued.
	expired bool
	// version and createRev are what etcd would report for this state:
	// version counts puts since the key was last created, createRev is the
	// revision of that create.
	version   int64
	createRev int64
}

// keyHistory is everything the oracle knows about one key.
type keyHistory struct {
	versions []version // ascending by rev
	// inflight is the number of writes issued whose outcome is not known yet.
	inflight int
	// uncertain is the number of writes whose outcome will never be known
	// (the RPC failed after possibly committing); while non-zero, unknown
	// revisions of this key are adopted rather than reported.
	uncertain int
	// gone means the server removed the key at a revision the oracle does
	// not know (a lease expired, seen through a read rather than a watch).
	gone bool
}

// Violation is one correctness failure.
type Violation struct {
	// Kind is a short stable name for the class of failure.
	Kind string
	// Where is the op or watch that observed it.
	Where string
	Key   string
	Rev   int64
	// Detail explains what was expected and what was seen.
	Detail string
	At     time.Time
}

func (v Violation) Error() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s (%s)", v.Kind, v.Where)
	if v.Key != "" {
		fmt.Fprintf(&sb, " key %s", v.Key)
	}
	if v.Rev != 0 {
		fmt.Fprintf(&sb, " rev %d", v.Rev)
	}
	fmt.Fprintf(&sb, ": %s", v.Detail)
	return sb.String()
}

// Violation kinds.
const (
	ViolationStaleRead      = "stale-read"
	ViolationPhantomRev     = "phantom-revision"
	ViolationWrongValue     = "wrong-value"
	ViolationWrongVersion   = "wrong-version"
	ViolationWrongLease     = "wrong-lease"
	ViolationLostKey        = "lost-key"
	ViolationDeletedRead    = "deleted-key-read"
	ViolationDuplicateRev   = "duplicate-revision"
	ViolationCreateOverLive = "create-over-live-key"
	ViolationBadConflict    = "bad-conflict"
	ViolationRevBackwards   = "revision-went-backwards"
	ViolationWatchOrder     = "watch-out-of-order"
	ViolationWatchRange     = "watch-outside-range"
	ViolationWatchProgress  = "watch-event-after-progress"
	ViolationWatchPrevKV    = "watch-prev-kv"
	ViolationWatchMissed    = "watch-missed-event"
	ViolationWatchDelete    = "watch-unexpected-delete"
	ViolationWatchBehind    = "watch-did-not-catch-up"
	ViolationListOrder      = "list-out-of-order"
	ViolationListMissing    = "list-missing-key"
	ViolationUnexpectedKey  = "unexpected-key"
)

// NewChecker creates an empty oracle.
func NewChecker() *Checker {
	c := &Checker{
		leases:        map[int64]leaseInfo{},
		MaxViolations: 64,
		now:           time.Now,
	}
	for i := range c.shards {
		c.shards[i].keys = map[string]*keyHistory{}
	}
	for i := range c.revs {
		c.revs[i].m = map[int64]string{}
	}
	return c
}

// OnViolation sets a function called with the first violation recorded (and
// any later one, up to MaxViolations), for example to cancel the run.
func (c *Checker) OnViolation(fn func(Violation)) {
	c.vmu.Lock()
	defer c.vmu.Unlock()
	c.stop = fn
}

func (c *Checker) shard(key string) *checkShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &c.shards[h.Sum32()%checkShards]
}

// history returns the key's history, creating it, with its shard locked.
func (c *Checker) history(key string) (*keyHistory, func()) {
	sh := c.shard(key)
	sh.mu.Lock()
	h := sh.keys[key]
	if h == nil {
		h = &keyHistory{}
		sh.keys[key] = h
	}
	return h, sh.mu.Unlock
}

func (c *Checker) report(v Violation) {
	v.At = c.now()
	c.vmu.Lock()
	if len(c.violations) >= c.MaxViolations {
		c.dropped++
		c.vmu.Unlock()
		return
	}
	c.violations = append(c.violations, v)
	stop := c.stop
	c.vmu.Unlock()
	if stop != nil {
		stop(v)
	}
}

func (c *Checker) park(fn func() *Violation) {
	c.vmu.Lock()
	defer c.vmu.Unlock()
	c.pending = append(c.pending, fn)
}

// Violations returns the violations recorded so far.
func (c *Checker) Violations() []Violation {
	c.vmu.Lock()
	defer c.vmu.Unlock()
	return append([]Violation(nil), c.violations...)
}

// ViolationCount returns how many violations were detected, including any
// beyond MaxViolations that were not kept.
func (c *Checker) ViolationCount() int64 {
	c.vmu.Lock()
	defer c.vmu.Unlock()
	return int64(len(c.violations)) + c.dropped
}

// Pending returns the number of checks waiting for Drain.
func (c *Checker) Pending() int {
	c.vmu.Lock()
	defer c.vmu.Unlock()
	return len(c.pending)
}

// Drain settles the parked checks. It is meant to be called when no write is
// in flight, at the end of a phase; a check that still cannot be settled
// then is a violation.
func (c *Checker) Drain() {
	c.vmu.Lock()
	pending := c.pending
	c.pending = nil
	c.vmu.Unlock()
	for _, fn := range pending {
		if v := fn(); v != nil {
			c.report(*v)
		}
	}
}

// ---- values

// nonceSize is the trailer appended to every value written.
const nonceSize = 8

// stamp returns value with a fresh nonce appended, and the nonce.
func (c *Checker) stamp(value []byte) ([]byte, uint64) {
	n := c.nonce.Add(1)
	out := make([]byte, len(value)+nonceSize)
	copy(out, value)
	binary.BigEndian.PutUint64(out[len(value):], n)
	return out, n
}

// nonceOf extracts the nonce from a stamped value (0 if too short).
func nonceOf(value []byte) uint64 {
	if len(value) < nonceSize {
		return 0
	}
	return binary.BigEndian.Uint64(value[len(value)-nonceSize:])
}

// ---- revisions

// begin marks the start of a request and returns the revision floor every
// response to it must respect.
func (c *Checker) begin() int64 { return c.floor.Load() }

// header checks a response header against the floor and raises the floor.
func (c *Checker) header(hdr *etcdserverpb.ResponseHeader, floor int64, where string) {
	if hdr == nil || hdr.Revision == 0 {
		return
	}
	rev := hdr.Revision
	if rev < floor {
		c.report(Violation{Kind: ViolationRevBackwards, Where: where, Rev: rev,
			Detail: fmt.Sprintf("response revision %d is below revision %d observed before the request was issued", rev, floor)})
	}
	for {
		cur := c.floor.Load()
		if rev <= cur || c.floor.CompareAndSwap(cur, rev) {
			return
		}
	}
}

// Floor returns the highest revision observed so far.
func (c *Checker) Floor() int64 { return c.floor.Load() }

// ---- writes

// writeBegin marks a write to key as in flight.
func (c *Checker) writeBegin(key string) {
	h, unlock := c.history(key)
	h.inflight++
	unlock()
}

// writeEnd marks an in-flight write as resolved: either its outcome is known
// (ok) or it failed in a way that leaves the server's state unknown.
func (c *Checker) writeEnd(key string, ok bool) {
	h, unlock := c.history(key)
	h.inflight--
	if !ok {
		h.uncertain++
	}
	unlock()
}

// recordRev notes which key was written at rev; a second claim on the same
// revision is a violation.
func (c *Checker) recordRev(key string, rev int64, where string) {
	sh := &c.revs[uint64(rev)%checkShards]
	sh.mu.Lock()
	prev, dup := sh.m[rev]
	if !dup {
		sh.m[rev] = key
	}
	sh.mu.Unlock()
	if dup && prev != key {
		c.report(Violation{Kind: ViolationDuplicateRev, Where: where, Key: key, Rev: rev,
			Detail: fmt.Sprintf("revision %d was already assigned to a write of %s", rev, prev)})
	}
}

// put records a successful put of key at rev.
func (c *Checker) put(key string, rev int64, nonce uint64, lease int64, where string) {
	c.recordRev(key, rev, where)
	h, unlock := c.history(key)
	dup := h.insert(version{rev: rev, nonce: nonce, lease: lease})
	unlock()
	if dup {
		c.report(Violation{Kind: ViolationDuplicateRev, Where: where, Key: key, Rev: rev,
			Detail: "the key already has a recorded write at this revision"})
	}
}

// del records a successful delete of key at rev.
func (c *Checker) del(key string, rev int64, where string) {
	c.recordRev(key, rev, where)
	h, unlock := c.history(key)
	dup := h.insert(version{rev: rev, deleted: true})
	unlock()
	if dup {
		c.report(Violation{Kind: ViolationDuplicateRev, Where: where, Key: key, Rev: rev,
			Detail: "the key already has a recorded write at this revision"})
	}
}

// insert adds v to the history in revision order and recomputes the etcd
// version/create_revision of everything from it on. It returns true if a
// write at that revision was already recorded.
func (h *keyHistory) insert(v version) bool {
	i := sort.Search(len(h.versions), func(i int) bool { return h.versions[i].rev >= v.rev })
	if i < len(h.versions) && h.versions[i].rev == v.rev {
		return true
	}
	h.versions = append(h.versions, version{})
	copy(h.versions[i+1:], h.versions[i:])
	h.versions[i] = v
	for j := i; j < len(h.versions); j++ {
		cur := &h.versions[j]
		if cur.deleted {
			cur.version, cur.createRev = 0, 0
			continue
		}
		if j == 0 || h.versions[j-1].deleted {
			cur.version, cur.createRev = 1, cur.rev
		} else {
			cur.version, cur.createRev = h.versions[j-1].version+1, h.versions[j-1].createRev
		}
	}
	// A write we issued, or a server delete at a known revision, settles
	// what "gone" stood for.
	if i == len(h.versions)-1 {
		h.gone = false
	}
	return false
}

// latestAt returns the last version at or below rev.
func (h *keyHistory) latestAt(rev int64) (version, bool) {
	i := sort.Search(len(h.versions), func(i int) bool { return h.versions[i].rev > rev })
	if i == 0 {
		return version{}, false
	}
	return h.versions[i-1], true
}

// at returns the version at exactly rev.
func (h *keyHistory) at(rev int64) (version, bool) {
	i := sort.Search(len(h.versions), func(i int) bool { return h.versions[i].rev >= rev })
	if i < len(h.versions) && h.versions[i].rev == rev {
		return h.versions[i], true
	}
	return version{}, false
}

// live reports whether the key is live at rev according to the oracle (a
// gone key is not).
func (h *keyHistory) live(rev int64) (version, bool) {
	v, ok := h.latestAt(rev)
	if !ok || v.deleted || (h.gone && rev >= v.rev) {
		return v, false
	}
	return v, true
}

// ---- leases

// lease records a grant issued at start with ttl.
func (c *Checker) lease(id int64, ttl time.Duration, start time.Time) {
	c.leaseMu.Lock()
	defer c.leaseMu.Unlock()
	c.leases[id] = leaseInfo{granted: start, ttl: ttl}
}

// leaseMayHaveExpired reports whether lease id could have expired by now.
// Unknown leases (including 0, no lease) cannot have.
func (c *Checker) leaseMayHaveExpired(id int64, now time.Time) bool {
	if id == 0 {
		return false
	}
	c.leaseMu.Lock()
	info, ok := c.leases[id]
	c.leaseMu.Unlock()
	if !ok {
		return false
	}
	return !now.Before(info.granted.Add(info.ttl))
}

// ---- reads

// kv checks a key-value the server returned from a read at readRev (for a
// watch event, the event's own revision; for a prev_kv, one below it).
func (c *Checker) kv(kv *mvccpb.KeyValue, readRev int64, where string) {
	if v := c.checkKV(kv, readRev, where, false); v != nil {
		c.report(*v)
	}
}

// checkKV is kv; if final is false and the answer depends on an in-flight
// write, the check is parked instead and nil returned.
func (c *Checker) checkKV(kv *mvccpb.KeyValue, readRev int64, where string, final bool) *Violation {
	key := string(kv.Key)
	h, unlock := c.history(key)
	defer unlock()
	latest, haveLatest := h.latestAt(readRev)
	if haveLatest && latest.rev > kv.ModRevision {
		return &Violation{Kind: ViolationStaleRead, Where: where, Key: key, Rev: kv.ModRevision,
			Detail: fmt.Sprintf("read at revision %d returned mod_revision %d, but a write at revision %d had already been acknowledged", readRev, kv.ModRevision, latest.rev)}
	}
	v, known := h.at(kv.ModRevision)
	if !known {
		if h.uncertain > 0 {
			// A write whose RPC failed may have committed; this is it.
			h.insert(version{rev: kv.ModRevision, nonce: nonceOf(kv.Value), lease: kv.Lease})
			return nil
		}
		if !final && h.inflight > 0 {
			c.park(func() *Violation { return c.checkKV(kv, readRev, where, true) })
			return nil
		}
		return &Violation{Kind: ViolationPhantomRev, Where: where, Key: key, Rev: kv.ModRevision,
			Detail: fmt.Sprintf("mod_revision %d was never assigned to a write of this key (latest known: %s)", kv.ModRevision, describe(h))}
	}
	if v.deleted {
		return &Violation{Kind: ViolationDeletedRead, Where: where, Key: key, Rev: kv.ModRevision,
			Detail: fmt.Sprintf("the key was deleted at revision %d, yet a read returned a value at that mod_revision", v.rev)}
	}
	if got := nonceOf(kv.Value); got != v.nonce {
		return &Violation{Kind: ViolationWrongValue, Where: where, Key: key, Rev: kv.ModRevision,
			Detail: fmt.Sprintf("value nonce %d, want %d (the value written at this revision)", got, v.nonce)}
	}
	if kv.Version != v.version || kv.CreateRevision != v.createRev {
		return &Violation{Kind: ViolationWrongVersion, Where: where, Key: key, Rev: kv.ModRevision,
			Detail: fmt.Sprintf("version %d create_revision %d, want version %d create_revision %d", kv.Version, kv.CreateRevision, v.version, v.createRev)}
	}
	if kv.Lease != v.lease {
		return &Violation{Kind: ViolationWrongLease, Where: where, Key: key, Rev: kv.ModRevision,
			Detail: fmt.Sprintf("lease %d, want %d", kv.Lease, v.lease)}
	}
	return nil
}

// absent checks a read at readRev that found no value for key.
func (c *Checker) absent(key string, readRev int64, where string) {
	if v := c.checkAbsent(key, readRev, where, false); v != nil {
		c.report(*v)
	}
}

func (c *Checker) checkAbsent(key string, readRev int64, where string, final bool) *Violation {
	now := c.now()
	h, unlock := c.history(key)
	defer unlock()
	v, live := h.live(readRev)
	if !live {
		return nil
	}
	if h.uncertain > 0 {
		h.gone = true
		return nil
	}
	if c.leaseMayHaveExpired(v.lease, now) {
		h.gone = true
		return nil
	}
	if !final && h.inflight > 0 {
		c.park(func() *Violation { return c.checkAbsent(key, readRev, where, true) })
		return nil
	}
	return &Violation{Kind: ViolationLostKey, Where: where, Key: key, Rev: readRev,
		Detail: fmt.Sprintf("read at revision %d found no value, but the key was written at revision %d (version %d, lease %d) and never deleted", readRev, v.rev, v.version, v.lease)}
}

// rangeResult checks a single-key Range response.
func (c *Checker) rangeResult(key string, resp *etcdserverpb.RangeResponse, where string) {
	if len(resp.Kvs) == 0 {
		c.absent(key, resp.Header.Revision, where)
		return
	}
	c.kv(resp.Kvs[0], resp.Header.Revision, where)
}

// ---- conditional writes

// createOK records a create (compare mod_revision==0) that succeeded.
func (c *Checker) createOK(key string, rev int64, nonce uint64, lease int64, where string) {
	now := c.now()
	h, unlock := c.history(key)
	v, live := h.live(rev - 1)
	lost := live && h.uncertain == 0 && !c.leaseMayHaveExpired(v.lease, now)
	h.gone = false // whatever the server had, it now has this
	unlock()
	if lost {
		c.report(Violation{Kind: ViolationCreateOverLive, Where: where, Key: key, Rev: rev,
			Detail: fmt.Sprintf("create succeeded at revision %d, but the key was live since revision %d (version %d) and never deleted: that write was lost", rev, v.rev, v.version)})
	}
	c.put(key, rev, nonce, lease, where)
}

// conflict checks the Else branch of a conditional write on key whose
// compare on mod_revision==compareRev failed. kvs is what the Else Range
// returned.
func (c *Checker) conflict(key string, compareRev int64, resp *etcdserverpb.TxnResponse, where string) {
	hdrRev := resp.Header.Revision
	var kvs []*mvccpb.KeyValue
	if len(resp.Responses) > 0 {
		kvs = resp.Responses[0].GetResponseRange().GetKvs()
	}
	if len(kvs) == 0 {
		if compareRev == 0 {
			c.report(Violation{Kind: ViolationBadConflict, Where: where, Key: key, Rev: hdrRev,
				Detail: "create failed its mod_revision==0 compare, yet the Else Range found no key"})
			return
		}
		c.absent(key, hdrRev, where)
		return
	}
	kv := kvs[0]
	if kv.ModRevision == compareRev {
		c.report(Violation{Kind: ViolationBadConflict, Where: where, Key: key, Rev: hdrRev,
			Detail: fmt.Sprintf("compare mod_revision==%d failed, yet the Else Range returned mod_revision %d", compareRev, kv.ModRevision)})
		return
	}
	c.kv(kv, hdrRev, where)
}

// ---- watches

// watchCheck verifies one watch stream on a prefix.
type watchCheck struct {
	c      *Checker
	prefix string
	where  string

	mu          sync.Mutex
	fromRev     int64 // the watch starts after this revision
	lastRev     int64 // last event revision seen
	progressRev int64 // highest progress notification
	seen        []int64
}

// Watch starts checking a watch on prefix that was opened from fromRev+1.
func (c *Checker) Watch(label, prefix string, fromRev int64) *watchCheck {
	return &watchCheck{c: c, prefix: prefix, where: "watch/" + label, fromRev: fromRev, lastRev: fromRev}
}

// progress records a progress notification at rev.
func (w *watchCheck) progress(rev int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if rev > w.progressRev {
		w.progressRev = rev
	}
}

// reached returns the revision the watch is known to have delivered through.
func (w *watchCheck) reached() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return max(w.lastRev, w.progressRev)
}

// event checks one event.
func (w *watchCheck) event(ev *clientv3.Event, at time.Time) {
	c := w.c
	key := string(ev.Kv.Key)
	rev := ev.Kv.ModRevision

	w.mu.Lock()
	if !strings.HasPrefix(key, w.prefix) {
		w.mu.Unlock()
		c.report(Violation{Kind: ViolationWatchRange, Where: w.where, Key: key, Rev: rev,
			Detail: fmt.Sprintf("event for a key outside the watched prefix %s", w.prefix)})
		return
	}
	if rev <= w.lastRev {
		last := w.lastRev
		w.mu.Unlock()
		c.report(Violation{Kind: ViolationWatchOrder, Where: w.where, Key: key, Rev: rev,
			Detail: fmt.Sprintf("event revision %d after revision %d (watch started after %d)", rev, last, w.fromRev)})
		return
	}
	if rev <= w.progressRev {
		p := w.progressRev
		w.mu.Unlock()
		c.report(Violation{Kind: ViolationWatchProgress, Where: w.where, Key: key, Rev: rev,
			Detail: fmt.Sprintf("event at revision %d arrived after a progress notification at revision %d claimed everything through it had been delivered", rev, p)})
		w.mu.Lock()
	}
	w.lastRev = rev
	w.seen = append(w.seen, rev)
	w.mu.Unlock()

	switch ev.Type {
	case mvccpb.PUT:
		c.kv(ev.Kv, rev, w.where)
	case mvccpb.DELETE:
		if v := c.checkWatchDelete(key, rev, at, w.where, false); v != nil {
			c.report(*v)
		}
	}
	if v := c.checkPrevKV(ev, w.where, false); v != nil {
		c.report(*v)
	}
}

// checkWatchDelete checks a delete event the watch delivered at rev.
func (c *Checker) checkWatchDelete(key string, rev int64, at time.Time, where string, final bool) *Violation {
	h, unlock := c.history(key)
	defer unlock()
	if v, ok := h.at(rev); ok {
		if !v.deleted {
			return &Violation{Kind: ViolationWatchDelete, Where: where, Key: key, Rev: rev,
				Detail: fmt.Sprintf("DELETE event at revision %d, but the key was put at that revision", rev)}
		}
		return nil
	}
	prev, wasLive := h.live(rev - 1)
	if h.uncertain > 0 || (wasLive && c.leaseMayHaveExpired(prev.lease, at)) {
		// The server expired the lease: that is the delete.
		h.insert(version{rev: rev, deleted: true, expired: true})
		return nil
	}
	if !final && h.inflight > 0 {
		c.park(func() *Violation { return c.checkWatchDelete(key, rev, at, where, true) })
		return nil
	}
	if !wasLive {
		return &Violation{Kind: ViolationWatchDelete, Where: where, Key: key, Rev: rev,
			Detail: fmt.Sprintf("DELETE event at revision %d for a key that was not live (%s)", rev, describe(h))}
	}
	return &Violation{Kind: ViolationWatchDelete, Where: where, Key: key, Rev: rev,
		Detail: fmt.Sprintf("DELETE event at revision %d that the workload did not issue; the key's lease %d has not expired (%s)", rev, prev.lease, describe(h))}
}

// checkPrevKV checks an event's prev_kv against the previous version.
func (c *Checker) checkPrevKV(ev *clientv3.Event, where string, final bool) *Violation {
	key := string(ev.Kv.Key)
	rev := ev.Kv.ModRevision
	if ev.PrevKv != nil {
		if string(ev.PrevKv.Key) != key {
			return &Violation{Kind: ViolationWatchPrevKV, Where: where, Key: key, Rev: rev,
				Detail: fmt.Sprintf("prev_kv is for key %s", ev.PrevKv.Key)}
		}
		if v := c.checkKV(ev.PrevKv, rev-1, where+"/prev_kv", final); v != nil {
			v.Kind = ViolationWatchPrevKV
			return v
		}
		return nil
	}
	h, unlock := c.history(key)
	defer unlock()
	prev, wasLive := h.live(rev - 1)
	if !wasLive || h.uncertain > 0 {
		return nil
	}
	if !final && h.inflight > 0 {
		c.park(func() *Violation { return c.checkPrevKV(ev, where, true) })
		return nil
	}
	return &Violation{Kind: ViolationWatchPrevKV, Where: where, Key: key, Rev: rev,
		Detail: fmt.Sprintf("event has no prev_kv, but the key was live at revision %d (version %d)", prev.rev, prev.version)}
}

// finish checks that every write under the prefix in (fromRev, endRev] was
// delivered, given that the watch has reached endRev. It returns the number
// of events seen in the range.
func (w *watchCheck) finish(endRev int64) int {
	c := w.c
	w.mu.Lock()
	reached := max(w.lastRev, w.progressRev)
	seen := w.seen
	w.mu.Unlock()
	if reached < endRev {
		c.report(Violation{Kind: ViolationWatchBehind, Where: w.where, Rev: endRev,
			Detail: fmt.Sprintf("the watch only reached revision %d when revision %d had been written", reached, endRev)})
		endRev = reached
	}
	n := 0
	for _, rev := range seen {
		if rev > w.fromRev && rev <= endRev {
			n++
		}
	}
	c.forEachKey(w.prefix, func(key string, h *keyHistory) {
		for _, v := range h.versions {
			if v.rev <= w.fromRev || v.rev > endRev {
				continue
			}
			i := sort.Search(len(seen), func(i int) bool { return seen[i] >= v.rev })
			if i < len(seen) && seen[i] == v.rev {
				continue
			}
			kind := "PUT"
			if v.deleted {
				kind = "DELETE"
			}
			c.report(Violation{Kind: ViolationWatchMissed, Where: w.where, Key: key, Rev: v.rev,
				Detail: fmt.Sprintf("%s at revision %d was acknowledged but never delivered on the watch (which reached revision %d)", kind, v.rev, reached)})
		}
	})
	return n
}

// forEachKey calls fn for every key with the prefix, with its shard locked.
func (c *Checker) forEachKey(prefix string, fn func(key string, h *keyHistory)) {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		for key, h := range sh.keys {
			if strings.HasPrefix(key, prefix) {
				fn(key, h)
			}
		}
		sh.mu.Unlock()
	}
}

// ---- lists

// listCheck verifies one paginated list at a fixed revision.
type listCheck struct {
	c       *Checker
	prefix  string
	where   string
	rev     int64
	lastKey string
	keys    map[string]struct{}
	n       int
}

// list starts checking a list of prefix.
func (c *Checker) list(prefix, where string) *listCheck {
	return &listCheck{c: c, prefix: prefix, where: where, keys: map[string]struct{}{}}
}

// page checks one page of results.
func (l *listCheck) page(resp *etcdserverpb.RangeResponse) {
	if l.rev == 0 {
		l.rev = resp.Header.Revision
	}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if l.n > 0 && key <= l.lastKey {
			l.c.report(Violation{Kind: ViolationListOrder, Where: l.where, Key: key, Rev: l.rev,
				Detail: fmt.Sprintf("key follows %s in a list at revision %d", l.lastKey, l.rev)})
		}
		if !strings.HasPrefix(key, l.prefix) {
			l.c.report(Violation{Kind: ViolationUnexpectedKey, Where: l.where, Key: key, Rev: l.rev,
				Detail: fmt.Sprintf("key outside the listed prefix %s", l.prefix)})
		}
		l.lastKey = key
		l.keys[key] = struct{}{}
		l.n++
		l.c.kv(kv, l.rev, l.where)
	}
}

// complete checks that the full list had every key the oracle has live at
// the list's revision. Keys whose lease may have expired are not required.
func (l *listCheck) complete() {
	now := l.c.now()
	l.c.forEachKey(l.prefix, func(key string, h *keyHistory) {
		if _, ok := l.keys[key]; ok {
			return
		}
		v, live := h.live(l.rev)
		if !live || h.uncertain > 0 || h.inflight > 0 || l.c.leaseMayHaveExpired(v.lease, now) {
			return
		}
		l.c.report(Violation{Kind: ViolationListMissing, Where: l.where, Key: key, Rev: l.rev,
			Detail: fmt.Sprintf("list at revision %d did not return the key, which was written at revision %d and never deleted", l.rev, v.rev)})
	})
}

// VerifyState lists everything under prefix at the current revision and
// checks it against the oracle: every key is as last written, nothing is
// missing and nothing extra is there. It is meant for a quiesced server (no
// writes in flight), and settles parked checks first.
func (c *Checker) VerifyState(ctx context.Context, kv clientv3.KV, prefix string, pageSize int64) (keys int, err error) {
	c.Drain()
	l := c.list(prefix, "verify-state")
	end := prefixEnd(prefix)
	key := prefix
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithLimit(pageSize)}
		if l.rev != 0 {
			opts = append(opts, clientv3.WithRev(l.rev))
		}
		floor := c.begin()
		resp, err := kv.Get(ctx, key, opts...)
		if err != nil {
			return l.n, fmt.Errorf("listing %s: %w", prefix, err)
		}
		c.header(resp.Header, floor, "verify-state")
		l.page((*etcdserverpb.RangeResponse)(resp))
		if !resp.More || len(resp.Kvs) == 0 {
			break
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	l.complete()
	c.Drain()
	return l.n, nil
}

// VerifyHistory replays the history of prefix from a fresh watch opened
// after fromRev and checks it is exactly the oracle's: every write in
// (fromRev, toRev] delivered once, in order, with the right prev_kv. If the
// server has compacted past fromRev the replay starts at the compacted
// revision instead, which is returned.
func (c *Checker) VerifyHistory(ctx context.Context, client *clientv3.Client, label, prefix string, fromRev, toRev int64) (startedFrom int64, events int, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	watcher := clientv3.NewWatcher(client)
	defer watcher.Close()
	for {
		w := c.Watch(label, prefix, fromRev)
		wch := watcher.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(fromRev+1), clientv3.WithPrevKV())
		compacted, err := w.drain(ctx, watcher, wch, toRev, 10*time.Second)
		if err != nil {
			return fromRev, 0, err
		}
		if compacted != 0 {
			if compacted <= fromRev {
				return fromRev, 0, fmt.Errorf("watch from %d reported compaction at %d", fromRev+1, compacted)
			}
			fromRev = compacted
			continue
		}
		c.Drain()
		return fromRev, w.finish(toRev), nil
	}
}

// drain consumes wch until the watch has reached toRev (by events or a
// progress notification, which it requests twice a second), then for a
// short grace period to catch anything delivered late. It returns the
// compacted revision if the server cancelled the watch as compacted.
func (w *watchCheck) drain(ctx context.Context, watcher clientv3.Watcher, wch clientv3.WatchChan, toRev int64, timeout time.Duration) (compacted int64, err error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	progress := time.NewTicker(500 * time.Millisecond)
	defer progress.Stop()
	var grace <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-deadline.C:
			return 0, fmt.Errorf("watch stalled at revision %d before reaching %d", w.reached(), toRev)
		case <-grace:
			return 0, nil
		case <-progress.C:
			if grace == nil && w.reached() < toRev {
				_ = watcher.RequestProgress(ctx)
			}
		case wr, ok := <-wch:
			if !ok {
				if grace != nil {
					return 0, nil
				}
				return 0, fmt.Errorf("watch closed before reaching revision %d (reached %d)", toRev, w.reached())
			}
			if wr.CompactRevision != 0 {
				return wr.CompactRevision, nil
			}
			if err := wr.Err(); err != nil {
				return 0, fmt.Errorf("watch: %w", err)
			}
			now := w.c.now()
			for _, ev := range wr.Events {
				w.event(ev, now)
			}
			if wr.IsProgressNotify() {
				w.progress(wr.Header.Revision)
			}
			if grace == nil && w.reached() >= toRev {
				grace = time.After(200 * time.Millisecond)
			}
		}
	}
}

// ---- diagnostics

// describe summarizes a key's history in one line.
func describe(h *keyHistory) string {
	if len(h.versions) == 0 {
		return "no writes recorded"
	}
	var sb strings.Builder
	start := 0
	if len(h.versions) > 6 {
		fmt.Fprintf(&sb, "%d earlier versions, ", len(h.versions)-6)
		start = len(h.versions) - 6
	}
	for i, v := range h.versions[start:] {
		if i > 0 {
			sb.WriteString(", ")
		}
		switch {
		case v.expired:
			fmt.Fprintf(&sb, "expired@%d", v.rev)
		case v.deleted:
			fmt.Fprintf(&sb, "del@%d", v.rev)
		default:
			fmt.Fprintf(&sb, "put@%d(v%d", v.rev, v.version)
			if v.lease != 0 {
				fmt.Fprintf(&sb, " lease %x", v.lease)
			}
			sb.WriteString(")")
		}
	}
	if h.inflight > 0 {
		fmt.Fprintf(&sb, "; %d in flight", h.inflight)
	}
	if h.uncertain > 0 {
		fmt.Fprintf(&sb, "; %d uncertain", h.uncertain)
	}
	if h.gone {
		sb.WriteString("; gone (server-side delete at unknown revision)")
	}
	return sb.String()
}

// Dump returns the oracle's full history of key, for diagnosing a violation.
func (c *Checker) Dump(key string) string {
	h, unlock := c.history(key)
	defer unlock()
	var sb strings.Builder
	fmt.Fprintf(&sb, "history of %s (%d versions", key, len(h.versions))
	if h.inflight > 0 {
		fmt.Fprintf(&sb, ", %d in flight", h.inflight)
	}
	if h.uncertain > 0 {
		fmt.Fprintf(&sb, ", %d uncertain", h.uncertain)
	}
	if h.gone {
		sb.WriteString(", gone")
	}
	sb.WriteString("):\n")
	for _, v := range h.versions {
		switch {
		case v.expired:
			fmt.Fprintf(&sb, "  rev %d  DELETE (lease expiry, observed)\n", v.rev)
		case v.deleted:
			fmt.Fprintf(&sb, "  rev %d  DELETE\n", v.rev)
		default:
			fmt.Fprintf(&sb, "  rev %d  PUT nonce %d version %d create_revision %d lease %x\n", v.rev, v.nonce, v.version, v.createRev, v.lease)
		}
	}
	return sb.String()
}

// LiveKeys returns the number of keys the oracle believes are live, and how
// many of those are on a lease that may have expired (which the server may
// therefore have deleted).
func (c *Checker) LiveKeys() (live, maybeExpired int) {
	now := c.now()
	c.forEachKey("", func(_ string, h *keyHistory) {
		v, ok := h.live(int64(^uint64(0) >> 1))
		if !ok {
			return
		}
		live++
		if c.leaseMayHaveExpired(v.lease, now) {
			maybeExpired++
		}
	})
	return live, maybeExpired
}
