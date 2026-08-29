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
// It is a thin layer over github.com/google/btree (the B-tree kube-apiserver's
// own watch cache uses). The tree itself is not safe for concurrent writes;
// callers serialize writes against reads (memorystorage does so with its
// storage lock).
package bptree

import (
	"bytes"
	"fmt"

	"github.com/google/btree"

	"justinsb.com/cloudetcd/pkg/persistence"
)

type Revision = persistence.Revision

// btreeDegree is the B-tree's branching factor: nodes hold up to 2*degree-1
// entries, so keys stay in a handful of contiguous slices and lookups are a
// few cache lines per level.
const btreeDegree = 64

// BPTree indexes keys to the revisions at which they were written. The zero
// value is an empty index.
type BPTree struct {
	tree *btree.BTreeG[*entry]
}

// entry is one key and every revision at which it was written, ascending.
type entry struct {
	key       []byte
	revisions []Revision
}

func lessEntry(a, b *entry) bool {
	return bytes.Compare(a.key, b.key) < 0
}

func (t *BPTree) init() {
	if t.tree == nil {
		t.tree = btree.NewG(btreeDegree, lessEntry)
	}
}

// Dump prints the index, for debugging.
func (t *BPTree) Dump() {
	if t.tree == nil {
		return
	}
	t.tree.Ascend(func(e *entry) bool {
		fmt.Printf("key: %s, revisions: %v\n", e.key, e.revisions)
		return true
	})
}

// Len returns the number of keys in the index.
func (t *BPTree) Len() int {
	if t.tree == nil {
		return 0
	}
	return t.tree.Len()
}

// AddRevision records that key was written at revision. Revisions for a key
// are expected to be added in increasing order.
func (t *BPTree) AddRevision(key []byte, revision Revision) {
	t.init()
	if e, ok := t.tree.Get(&entry{key: key}); ok {
		e.revisions = append(e.revisions, revision)
		return
	}
	// Copy the key: callers pass buffers owned by request messages.
	t.tree.ReplaceOrInsert(&entry{key: bytes.Clone(key), revisions: []Revision{revision}})
}

// GetLatestRevisionByKey returns the latest revision at which key was
// written that is <= atRevision, and whether there is one.
func (t *BPTree) GetLatestRevisionByKey(key []byte, atRevision Revision) (Revision, bool) {
	if t.tree == nil {
		return 0, false
	}
	e, ok := t.tree.Get(&entry{key: key})
	if !ok {
		return 0, false
	}
	// Revisions are ascending, so scan back from the newest.
	for i := len(e.revisions) - 1; i >= 0; i-- {
		if e.revisions[i] <= atRevision {
			return e.revisions[i], true
		}
	}
	return 0, false
}

// ListRevisionsByKeyRange calls callback for every key >= startKey, in key
// order, with all revisions at which the key was written (the caller filters
// by revision), until callback returns false. atRevision is accepted for
// interface compatibility; callers filter revisions themselves.
func (t *BPTree) ListRevisionsByKeyRange(startKey []byte, atRevision Revision, callback func(key []byte, revisions []Revision) bool) {
	if t.tree == nil {
		return
	}
	t.tree.AscendGreaterOrEqual(&entry{key: startKey}, func(e *entry) bool {
		return callback(e.key, e.revisions)
	})
}
