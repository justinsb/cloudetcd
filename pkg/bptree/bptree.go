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

// Package bptree implements the in-memory key index: for each key, the list
// of log revisions at which it was written, ordered by key so that ranges can
// be scanned.
//
// The index is a radix (prefix) tree. Each node holds entries sorted by their
// prefix; sibling entries never share a first byte, so a node has at most 256
// entries and is searched by binary search on that byte. An entry carries the
// revisions of the key that ends exactly at it (if any) and a child node for
// the keys that continue past it (if any). Inserting a key that shares only
// part of an entry's prefix splits the entry at the common prefix. Kubernetes
// keys share long prefixes (/registry/pods/<ns>/...), so the tree stores each
// prefix once, and keys under one prefix are a few small nodes deep rather
// than one long list.
//
// The tree is safe for concurrent readers with a single writer serialized by
// its own lock; memorystorage additionally serializes writes against reads.
package bptree

import (
	"bytes"
	"fmt"
	"slices"
	"sort"
	"sync"

	"justinsb.com/cloudetcd/pkg/persistence"
)

type Revision = persistence.Revision

// BPTree indexes keys to the revisions at which they were written. The zero
// value is an empty index.
type BPTree struct {
	mu   sync.RWMutex
	root node
	// emptyKey is the entry for the empty key, which has no first byte to
	// place it in a node. Its prefix is nil.
	emptyKey nodeEntry
	// keys is the number of distinct keys in the index.
	keys int
}

// node holds entries sorted by prefix; no two entries share a first byte.
type node struct {
	entries []nodeEntry
}

// nodeEntry is one edge of the tree: prefix is the key bytes it covers
// (relative to its parent), revisions those of the key ending exactly here
// (nil if no key does), and child the node for keys continuing past it (nil
// if none do).
type nodeEntry struct {
	prefix    []byte
	revisions []Revision
	child     *node
	// tombstone is whether the latest revision is a delete. Only the latest
	// one needs marking: Compact removes a key whose latest write at or
	// below the compaction point is a delete, and otherwise keeps an
	// earlier delete like any other revision (the log keeps the delete
	// record for as long as the index refers to it), so a put, delete, put
	// sequence needs nothing more than the latest flag.
	tombstone bool
}

// compact trims the entry's revisions for Compact, and reports how many it
// dropped and whether the key is gone.
func (e *nodeEntry) compact(through Revision) (dropped int, gone bool) {
	if e.revisions == nil {
		return 0, false
	}
	kept, dropped, gone := compactRevisions(e.revisions, e.tombstone, through)
	if gone {
		e.revisions = nil
		e.tombstone = false
	} else {
		e.revisions = kept
	}
	return dropped, gone
}

// Dump prints the index, for debugging.
func (t *BPTree) Dump() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.emptyKey.revisions != nil {
		fmt.Printf("key: (empty), revisions: %v\n", t.emptyKey.revisions)
	}
	t.root.dump(nil)
}

func (n *node) dump(path []byte) {
	for i := range n.entries {
		e := &n.entries[i]
		key := append(path, e.prefix...)
		if e.revisions != nil {
			fmt.Printf("key: %s, revisions: %v\n", key, e.revisions)
		}
		if e.child != nil {
			e.child.dump(key)
		}
	}
}

// Len returns the number of keys in the index.
func (t *BPTree) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.keys
}

// find returns the index of the entry whose prefix starts with b, or the
// index at which such an entry would be inserted, and whether it exists.
func (n *node) find(b byte) (int, bool) {
	i := sort.Search(len(n.entries), func(i int) bool { return n.entries[i].prefix[0] >= b })
	return i, i < len(n.entries) && n.entries[i].prefix[0] == b
}

func commonPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// AddRevision records that key was written at revision: a put, or a delete
// if tombstone is set. Revisions for a key are expected to be added in
// increasing order.
func (t *BPTree) AddRevision(key []byte, revision Revision, tombstone bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(key) == 0 {
		e := &t.emptyKey
		if e.revisions == nil {
			t.keys++
		}
		e.revisions = append(e.revisions, revision)
		e.tombstone = tombstone
		return
	}
	if t.root.addRevision(key, revision, tombstone) {
		t.keys++
	}
}

// Compact discards history below through: each key keeps its revisions >=
// through and the latest one below, which is all a read at or after through
// can observe. A key whose latest revision is a delete at or below through is
// removed. It returns the number of keys removed and revisions dropped.
// It locks the tree for the whole walk; the server uses CompactFrom.
func (t *BPTree) Compact(through Revision) (keysRemoved, revisionsDropped int) {
	_, keysRemoved, revisionsDropped = t.CompactFrom(nil, through, 0)
	return keysRemoved, revisionsDropped
}

// CompactFrom is Compact a chunk at a time: it prunes up to maxKeys keys in
// key order (all of them if maxKeys <= 0), starting after start (from the
// beginning if start is nil), and returns where to continue — nil when the
// walk is done — with the counts. Callers loop over it, so the tree is
// locked only a chunk at a time while reads and writes interleave. That is
// sound because pruning only ever shrinks a key's revisions: revisions at
// or above through, and everything written between chunks, are kept, and
// reads pick each key's latest revision at or below their snapshot, which
// pruning never touches for the snapshots still readable after Compact.
func (t *BPTree) CompactFrom(start []byte, through Revision, maxKeys int) (next []byte, keysRemoved, revisionsDropped int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := &compactState{through: through, remaining: maxKeys}
	if maxKeys <= 0 {
		s.remaining = int(^uint(0) >> 1)
	}
	var walkStart []byte
	if start == nil {
		if t.emptyKey.revisions != nil {
			dropped, gone := t.emptyKey.compact(through)
			s.dropped += dropped
			if gone {
				s.removed++
			}
			s.remaining--
		}
		if s.remaining == 0 {
			// Resume strictly after the empty key: at the first real key.
			s.next = []byte{}
		}
	} else {
		// Resume strictly after start: with byte-string keys, that is at or
		// after start plus a zero byte.
		walkStart = append(bytes.Clone(start), 0)
	}
	if s.next == nil {
		t.root.compactWalk(make([]byte, 0, 256), walkStart, s)
	}
	t.keys -= s.removed
	return s.next, s.removed, s.dropped
}

// compactState carries one CompactFrom chunk through the walk.
type compactState struct {
	through   Revision
	remaining int
	removed   int
	dropped   int
	// next is the last key pruned when the chunk ran out, nil while the
	// walk should continue.
	next []byte
}

// compactRevisions trims one key's revisions for Compact.
func compactRevisions(revisions []Revision, tombstone bool, through Revision) (kept []Revision, dropped int, gone bool) {
	latest := revisions[len(revisions)-1]
	if tombstone && latest <= through {
		return nil, len(revisions), true
	}
	// Revisions are ascending: keep from the last one <= through onward.
	i := sort.Search(len(revisions), func(i int) bool { return revisions[i] > through })
	if i > 0 {
		i--
	}
	if i == 0 {
		return revisions, 0, false
	}
	return append(revisions[:0], revisions[i:]...), i, false
}

// compactWalk prunes the keys below this node in key order, skipping those
// below start (relative to this node; nil once passed), stopping when the
// chunk is used up. It returns false to unwind the walk, with state.next
// set. The start handling mirrors (*node).list.
func (n *node) compactWalk(path []byte, start []byte, s *compactState) bool {
	i := 0
	if len(start) > 0 {
		i, _ = n.find(start[0])
	}
	for ; i < len(n.entries); i++ {
		e := &n.entries[i]
		childStart := []byte(nil)
		pruneOwn := true
		if len(start) > 0 {
			m := min(len(e.prefix), len(start))
			switch c := bytes.Compare(e.prefix[:m], start[:m]); {
			case c < 0:
				// The whole subtree is below start.
				continue
			case c == 0 && len(e.prefix) < len(start):
				// start continues below this entry: the entry's own key is
				// below start, and only part of its subtree qualifies.
				pruneOwn = false
				childStart = start[len(e.prefix):]
			default:
				// e.prefix >= start: everything from here on qualifies.
			}
			// Later siblings are entirely above start.
			start = nil
		}
		key := append(path, e.prefix...)
		if pruneOwn && e.revisions != nil {
			dropped, gone := e.compact(s.through)
			s.dropped += dropped
			if gone {
				s.removed++
			}
			if s.remaining--; s.remaining == 0 {
				s.next = bytes.Clone(key)
				return false
			}
		}
		if e.child != nil {
			if !e.child.compactWalk(key, childStart, s) {
				return false
			}
		}
	}
	return true
}

// addRevision adds revision for the key that continues with key from this
// node. It returns true if the key is new.
func (n *node) addRevision(key []byte, revision Revision, tombstone bool) bool {
	pos, found := n.find(key[0])
	if !found {
		// No entry shares a first byte with the key: a new leaf edge. Clone
		// the key; callers pass buffers owned by request messages.
		n.entries = slices.Insert(n.entries, pos, nodeEntry{prefix: bytes.Clone(key), revisions: []Revision{revision}, tombstone: tombstone})
		return true
	}

	e := &n.entries[pos]
	common := commonPrefixLen(e.prefix, key)
	if common == len(e.prefix) {
		if common == len(key) {
			// The key ends exactly at this entry.
			isNew := e.revisions == nil
			e.revisions = append(e.revisions, revision)
			e.tombstone = tombstone
			return isNew
		}
		// The entry's prefix is a prefix of the key: continue below it.
		if e.child == nil {
			e.child = &node{}
		}
		return e.child.addRevision(key[common:], revision, tombstone)
	}

	// The key diverges inside the entry's prefix: split the entry at the
	// common prefix. The old entry's remainder becomes the sole child of the
	// new intermediate entry, keeping its revisions and subtree.
	child := &node{entries: []nodeEntry{{
		prefix:    e.prefix[common:],
		revisions: e.revisions,
		child:     e.child,
		tombstone: e.tombstone,
	}}}
	e.prefix = e.prefix[:common:common]
	e.revisions = nil
	e.tombstone = false
	e.child = child
	if common == len(key) {
		// The key ends exactly at the split point.
		e.revisions = []Revision{revision}
		e.tombstone = tombstone
		return true
	}
	// The key's remainder differs from the old remainder in its first byte,
	// so it becomes a second edge of the new child.
	return child.addRevision(key[common:], revision, tombstone)
}

// GetLatestRevisionByKey returns the latest revision at which key was
// written that is <= atRevision, and whether there is one.
func (t *BPTree) GetLatestRevisionByKey(key []byte, atRevision Revision) (Revision, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var revisions []Revision
	if len(key) == 0 {
		revisions = t.emptyKey.revisions
	} else {
		revisions = t.root.lookup(key)
	}
	// Revisions are ascending, so scan back from the newest.
	for i := len(revisions) - 1; i >= 0; i-- {
		if revisions[i] <= atRevision {
			return revisions[i], true
		}
	}
	return 0, false
}

// lookup returns the revisions of the key that continues with key from this
// node, or nil.
func (n *node) lookup(key []byte) []Revision {
	for {
		pos, found := n.find(key[0])
		if !found {
			return nil
		}
		e := &n.entries[pos]
		if !bytes.HasPrefix(key, e.prefix) {
			return nil
		}
		if len(key) == len(e.prefix) {
			return e.revisions
		}
		if e.child == nil {
			return nil
		}
		key = key[len(e.prefix):]
		n = e.child
	}
}

// ListRevisionsByKeyRange calls callback for every key >= startKey, in key
// order, with all revisions at which the key was written (the caller filters
// by revision), until callback returns false. The key passed to the callback
// is only valid for the duration of the call. atRevision is accepted for
// interface compatibility; callers filter revisions themselves.
func (t *BPTree) ListRevisionsByKeyRange(startKey []byte, atRevision Revision, callback func(key []byte, revisions []Revision) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(startKey) == 0 && t.emptyKey.revisions != nil {
		if !callback(nil, t.emptyKey.revisions) {
			return
		}
	}
	path := make([]byte, 0, 256)
	t.root.list(path, startKey, callback)
}

// list visits the keys below this node that are >= start (relative to this
// node; nil once the start has been passed and everything qualifies), in
// order. path is the key prefix leading to this node.
func (n *node) list(path []byte, start []byte, callback func(key []byte, revisions []Revision) bool) bool {
	// Skip entries that sort entirely before start: with distinct first
	// bytes, that is every entry whose first byte is below start's.
	i := 0
	if len(start) > 0 {
		i, _ = n.find(start[0])
	}
	for ; i < len(n.entries); i++ {
		e := &n.entries[i]
		childStart := []byte(nil)
		emitOwn := true
		if len(start) > 0 {
			m := min(len(e.prefix), len(start))
			switch c := bytes.Compare(e.prefix[:m], start[:m]); {
			case c < 0:
				// The whole subtree is below start.
				continue
			case c == 0 && len(e.prefix) < len(start):
				// start continues below this entry: the entry's own key
				// is below start, and only part of its subtree qualifies.
				emitOwn = false
				childStart = start[len(e.prefix):]
			default:
				// e.prefix >= start: everything from here on qualifies.
			}
			// Later siblings are entirely above start.
			start = nil
		}
		key := append(path, e.prefix...)
		if emitOwn && e.revisions != nil {
			if !callback(key, e.revisions) {
				return false
			}
		}
		if e.child != nil {
			if !e.child.list(key, childStart, callback) {
				return false
			}
		}
	}
	return true
}
