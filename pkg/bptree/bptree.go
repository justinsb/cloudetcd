// Copyright 2026 Google LLC
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

// Package bptree implements an in-memory B+ tree.
//
// This B+ tree is used as an index for key-based lookups. It does not store
// values directly, but rather a list of 64-bit revision numbers for each key.
//
// The implementation is designed for in-memory use, and as such does not have
// strictly balanced pages. It also supports multiple revision numbers for each key.
package bptree

import (
	"bytes"
	"fmt"
	"sync"

	"justinsb.com/cloudetcd/pkg/persistence"
)

// TODO: Reduce and support splitting
const maxKeys = 25600

// BPTree is a B+ tree implementation. It contains a pointer to the root node
// and a read-write mutex for concurrent access.
type BPTree struct {
	// revision atomic.Uint64
	root node
}

type Revision = persistence.Revision

// Dump dumps the B+ tree to the console.
func (t *BPTree) Dump() {
	t.root.dump()
}

func (n *node) dump() {
	for _, e := range n.entries {
		fmt.Printf("prefix: %s, child: %v, revisions: %v\n", e.prefix, e.child != nil, e.revisions)
	}
}

// // GetCurrentRevision returns the current revision of the B+ tree.
// func (t *BPTree) GetCurrentRevision() Revision {
// 	v := t.revision.Load()
// 	return Revision(v)
// }

// AddRevision adds a new revision to a key. If the key does not exist, it is created.
//
// The algorithm is as follows:
//  1. Traverse the tree to find the leaf node where the key should be inserted.
//  2. If the key already exists, append the new revision to the existing list of revisions.
//  3. If the key does not exist, insert the key and the new revision into the leaf node.
//  4. If the leaf node is full, split it into two nodes and promote the middle key to the parent node.
//     This splitting process may propagate up to the root of the tree.
func (t *BPTree) AddRevision(key []byte, revision Revision) {
	t.root.addRevision(key, revision)

	// for {
	// 	oldRevision := t.revision.Load()
	// 	if revision <= Revision(oldRevision) {
	// 		break
	// 	}
	// 	if t.revision.CompareAndSwap(oldRevision, uint64(revision)) {
	// 		break
	// 	}
	// }
}

// GetLatestRevisionByKey returns the latest revision for a key that is less than or equal to the given timestamp.
//
// The algorithm is as follows:
// 1. Traverse the tree to find the leaf node containing the key.
// 2. If the key is found, iterate through its revisions and return the latest revision that is less than or equal to the given timestamp.
// 3. If the key is not found, return 0 and false.
func (t *BPTree) GetLatestRevisionByKey(key []byte, atRevision Revision) (Revision, bool) {
	return t.root.getLatestRevisionByKey(key, atRevision)
}

// listRevisionsByKeyRange calls the callback for each key in the given range with its revisions.
//
// The algorithm is as follows:
// 1. Traverse the tree to find the leaf node where the startKey is located.
// 2. Iterate through the leaf nodes until the endKey is reached.
// 3. For each key in the range, find all revisions that are less than or equal to the given timestamp and call the callback with the key and revisions.
func (t *BPTree) ListRevisionsByKeyRange(startKey []byte, atRevision Revision, callback func(key []byte, revisions []Revision) bool) {
	t.root.listRevisionsByKeyRange(startKey, atRevision, callback)
}

// node represents a node in the B+ tree. It contains a slice of keys, a slice
// of values (which are slices of 64-bit integers), and a slice of child nodes.
type node struct {
	mutex sync.RWMutex

	entries []nodeEntry
}

type nodeEntry struct {
	prefix    []byte
	child     *node
	revisions []Revision
}

// split splits a full node into two.
// When a node becomes full (i.e., it contains the maximum number of keys), it is split into two nodes.
// The middle key is promoted to the parent node, and the remaining keys are divided between the two new nodes.
// This process ensures that the tree remains balanced.
func (n *node) split(parent *node, i int) {
	panic("not implemented")
	// newChild := &node{}
	// mid := len(n.keys) / 2
	// parent.keys = append(parent.keys, nil)
	// copy(parent.keys[i+1:], parent.keys[i:])
	// parent.keys[i] = n.keys[mid]

	// newChild.keys = append(newChild.keys, n.keys[mid+1:]...)
	// n.keys = n.keys[:mid]

	// if len(n.revisions) > 0 {
	// 	newChild.revisions = append(newChild.revisions, n.revisions[mid+1:]...)
	// 	n.revisions = n.revisions[:mid]
	// }

	// if len(n.children) > 0 {
	// 	newChild.children = append(newChild.children, n.children[mid+1:]...)
	// 	n.children = n.children[:mid+1]
	// }

	// parent.children = append(parent.children, nil)
	// copy(parent.children[i+1:], parent.children[i:])
	// parent.children[i+1] = newChild
}

func (n *node) addRevision(remainingKey []byte, revision Revision) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	pos, match := n.findEntry(remainingKey)
	if match {
		e := &n.entries[pos]
		if len(e.prefix) == len(remainingKey) {
			e.revisions = append(e.revisions, revision)
		} else {
			if e.child == nil {
				e.child = &node{}
			}
			e.child.addRevision(remainingKey[len(e.prefix):], revision)
		}
		return
	}

	// We need to insert a new entry

	// TODO: Split if we feel there are "too many" entries

	newEntries := make([]nodeEntry, len(n.entries)+1)
	copy(newEntries, n.entries[:pos])
	newEntries[pos] = nodeEntry{prefix: remainingKey, revisions: []Revision{revision}}
	copy(newEntries[pos+1:], n.entries[pos:])
	n.entries = newEntries
}

func (n *node) getLatestRevisionByKey(remainingKey []byte, atRevision Revision) (Revision, bool) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	pos, match := n.findEntry(remainingKey)
	if !match {
		return 0, false
	}

	e := &n.entries[pos]

	revisions := e.revisions
	if len(revisions) > 0 && bytes.Equal(e.prefix, remainingKey) {
		latest := Revision(0)
		found := false
		for _, r := range revisions {
			if r <= atRevision {
				if r > latest {
					latest = r
				}
				found = true
			}
		}
		return latest, found
	}

	if e.child != nil {
		return e.child.getLatestRevisionByKey(remainingKey, atRevision)
	}

	return 0, false
}

// findEntry finds the index for the matching entry, or the insertion point if not found.
func (n *node) findEntry(prefixRemaining []byte) (int, bool) {
	i := 0

	for ; i < len(n.entries); i++ {
		e := &n.entries[i]

		if e.child != nil {
			if bytes.HasPrefix(prefixRemaining, e.prefix) {
				return i, true
			}
		}

		cmp := bytes.Compare(e.prefix, prefixRemaining)
		if cmp > 0 {
			return i, false
		}

		if cmp == 0 {
			return i, true
		}
	}

	return i, false
}

func (n *node) listRevisionsByKeyRange(fromPrefixRemaining []byte, atRevision Revision, callback func(key []byte, revisions []Revision) bool) bool {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	// pos, match := n.findSupremumHoldingLock(fromPrefixRemaining)
	// i := pos
	// if !match && i > 0 {
	// 	i--
	// }

	isMatch := false
	i := 0
	for ; i < len(n.entries); i++ {
		e := &n.entries[i]

		minLen := min(len(e.prefix), len(fromPrefixRemaining))

		cmp := bytes.Compare(e.prefix[:minLen], fromPrefixRemaining[:minLen])
		if cmp > 0 {
			break
		}

		if cmp == 0 {
			isMatch = true
			break
		}
	}

	if isMatch {
		e := &n.entries[i]
		if len(e.revisions) > 0 && bytes.Compare(e.prefix, fromPrefixRemaining) >= 0 {
			if !callback(e.prefix, e.revisions) {
				return false
			}
		}

		if e.child != nil {
			if !e.child.listRevisionsByKeyRange(fromPrefixRemaining[len(e.prefix):], atRevision, callback) {
				return false
			}
		}
		i++
	}

	for ; i < len(n.entries); i++ {
		e := &n.entries[i]
		if len(e.revisions) > 0 {
			if !callback(e.prefix, e.revisions) {
				return false
			}
		}
		if e.child != nil {
			if bytes.HasPrefix(fromPrefixRemaining, e.prefix) {
				if !e.child.listRevisionsByKeyRange(fromPrefixRemaining[len(e.prefix):], atRevision, callback) {
					return false
				}
			}
		}
	}
	return true
}
