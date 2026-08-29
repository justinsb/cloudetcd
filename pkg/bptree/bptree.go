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
	// emptyKey holds the revisions of the empty key, which has no first byte
	// to place it in a node.
	emptyKey []Revision
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
}

// Dump prints the index, for debugging.
func (t *BPTree) Dump() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.emptyKey != nil {
		fmt.Printf("key: (empty), revisions: %v\n", t.emptyKey)
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

// AddRevision records that key was written at revision. Revisions for a key
// are expected to be added in increasing order.
func (t *BPTree) AddRevision(key []byte, revision Revision) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(key) == 0 {
		if t.emptyKey == nil {
			t.keys++
		}
		t.emptyKey = append(t.emptyKey, revision)
		return
	}
	if t.root.addRevision(key, revision) {
		t.keys++
	}
}

// addRevision adds revision for the key that continues with key from this
// node. It returns true if the key is new.
func (n *node) addRevision(key []byte, revision Revision) bool {
	pos, found := n.find(key[0])
	if !found {
		// No entry shares a first byte with the key: a new leaf edge. Clone
		// the key; callers pass buffers owned by request messages.
		n.entries = slices.Insert(n.entries, pos, nodeEntry{prefix: bytes.Clone(key), revisions: []Revision{revision}})
		return true
	}

	e := &n.entries[pos]
	common := commonPrefixLen(e.prefix, key)
	if common == len(e.prefix) {
		if common == len(key) {
			// The key ends exactly at this entry.
			isNew := e.revisions == nil
			e.revisions = append(e.revisions, revision)
			return isNew
		}
		// The entry's prefix is a prefix of the key: continue below it.
		if e.child == nil {
			e.child = &node{}
		}
		return e.child.addRevision(key[common:], revision)
	}

	// The key diverges inside the entry's prefix: split the entry at the
	// common prefix. The old entry's remainder becomes the sole child of the
	// new intermediate entry, keeping its revisions and subtree.
	child := &node{entries: []nodeEntry{{
		prefix:    e.prefix[common:],
		revisions: e.revisions,
		child:     e.child,
	}}}
	e.prefix = e.prefix[:common:common]
	e.revisions = nil
	e.child = child
	if common == len(key) {
		// The key ends exactly at the split point.
		e.revisions = []Revision{revision}
		return true
	}
	// The key's remainder differs from the old remainder in its first byte,
	// so it becomes a second edge of the new child.
	return child.addRevision(key[common:], revision)
}

// GetLatestRevisionByKey returns the latest revision at which key was
// written that is <= atRevision, and whether there is one.
func (t *BPTree) GetLatestRevisionByKey(key []byte, atRevision Revision) (Revision, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var revisions []Revision
	if len(key) == 0 {
		revisions = t.emptyKey
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
	if len(startKey) == 0 && t.emptyKey != nil {
		if !callback(nil, t.emptyKey) {
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
