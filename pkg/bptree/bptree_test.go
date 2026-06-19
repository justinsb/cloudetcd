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

package bptree

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// func TestBPTree_AddRevision(t *testing.T) {
// 	tree := New()
// 	tree.AddRevision([]byte("key1"), 1)
// 	tree.AddRevision([]byte("key2"), 2)
// 	tree.AddRevision([]byte("key1"), 3)

// 	if len(tree.root.keys) != 2 {
// 		t.Errorf("expected 2 keys, got %d", len(tree.root.keys))
// 	}
// 	if string(tree.root.keys[0]) != "key1" {
// 		t.Errorf("expected key1, got %s", string(tree.root.keys[0]))
// 	}
// 	if string(tree.root.keys[1]) != "key2" {
// 		t.Errorf("expected key2, got %s", string(tree.root.keys[1]))
// 	}
// 	if len(tree.root.revisions[0]) != 2 {
// 		t.Errorf("expected 2 values for key1, got %d", len(tree.root.revisions[0]))
// 	}
// 	if tree.root.revisions[0][0] != 1 {
// 		t.Errorf("expected 1, got %d", tree.root.revisions[0][0])
// 	}
// 	if tree.root.revisions[0][1] != 3 {
// 		t.Errorf("expected 3, got %d", tree.root.revisions[0][1])
// 	}
// 	if len(tree.root.revisions[1]) != 1 {
// 		t.Errorf("expected 1 value for key2, got %d", len(tree.root.revisions[1]))
// 	}
// 	if tree.root.revisions[1][0] != 2 {
// 		t.Errorf("expected 2, got %d", tree.root.revisions[1][0])
// 	}
// }

func TestBPTree_GetLatestRevisionByKey(t *testing.T) {
	var tree BPTree
	tree.AddRevision([]byte("key1"), 1)
	tree.AddRevision([]byte("key3"), 2)
	tree.AddRevision([]byte("key5"), 3)
	tree.AddRevision([]byte("key1"), 4)
	tree.AddRevision([]byte("key3"), 5)

	scenarios := []struct {
		key          []byte
		atRevision   Revision
		wantRevision Revision
		wantOk       bool
	}{
		{[]byte("key1"), 4, 4, true},
		{[]byte("key1"), 5, 4, true},
		{[]byte("key1"), 0, 0, false},

		{[]byte("key"), 10, 0, false},
		{[]byte("key1"), 10, 4, true},
		{[]byte("key2"), 10, 0, false},
		{[]byte("key3"), 10, 5, true},
		{[]byte("key4"), 10, 0, false},
		{[]byte("key5"), 10, 3, true},
		{[]byte("key6"), 10, 0, false},
	}
	for _, scenario := range scenarios {
		t.Run(fmt.Sprintf("%s-revision-%d", string(scenario.key), scenario.atRevision), func(t *testing.T) {
			rev, ok := tree.GetLatestRevisionByKey(scenario.key, scenario.atRevision)
			if ok != scenario.wantOk {
				if scenario.wantOk {
					t.Errorf("expected to find key %s", string(scenario.key))
				} else {
					t.Errorf("expected to not find key %s", string(scenario.key))
				}
			}
			if rev != scenario.wantRevision {
				t.Errorf("expected revision %d, got %d", scenario.wantRevision, rev)
			}
		})
	}
}

func TestBPTree_ListRevisionsByKeyRange(t *testing.T) {
	var tree BPTree
	tree.AddRevision([]byte("key1"), 1)
	tree.AddRevision([]byte("key3"), 2)
	tree.AddRevision([]byte("key5"), 3)
	tree.AddRevision([]byte("key1"), 4)
	tree.AddRevision([]byte("key3"), 5)

	{
		got := make(map[string][]Revision)
		tree.ListRevisionsByKeyRange([]byte("key"), 3, func(key []byte, revisions []Revision) bool {
			got[string(key)] = revisions
			return true
		})
		want := map[string][]Revision{
			"key1": {1, 4},
			"key3": {2, 5},
			"key5": {3},
		}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("results mismatch for key (-want +got):\n%s", diff)
		}
	}

	{
		got := make(map[string][]Revision)
		tree.ListRevisionsByKeyRange([]byte("key3"), 3, func(key []byte, revisions []Revision) bool {
			got[string(key)] = revisions
			return true
		})
		want := map[string][]Revision{
			"key3": {2, 5},
			"key5": {3},
		}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("results mismatch for key3 (-want +got):\n%s", diff)
		}
	}

	{
		got := make(map[string][]Revision)
		tree.ListRevisionsByKeyRange([]byte("key5b"), 3, func(key []byte, revisions []Revision) bool {
			got[string(key)] = revisions
			return true
		})
		want := map[string][]Revision{}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("results mismatch for key5b (-want +got):\n%s", diff)
		}
	}
}

// func TestBPTree_Split(t *testing.T) {
// 	tree := New()
// 	for i := 0; i < maxKeys; i++ {
// 		key := []byte{byte(i)}
// 		tree.AddRevision(key, int64(i))
// 	}
// 	if len(tree.root.keys) != maxKeys {
// 		t.Errorf("expected %d keys, got %d", maxKeys, len(tree.root.keys))
// 	}

// 	tree.AddRevision([]byte{byte(maxKeys)}, int64(maxKeys))

// 	if len(tree.root.keys) != 1 {
// 		t.Errorf("expected 1 key in root, got %d", len(tree.root.keys))
// 	}
// 	if len(tree.root.children) != 2 {
// 		t.Errorf("expected 2 children in root, got %d", len(tree.root.children))
// 	}
// 	if len(tree.root.children[0].keys) != maxKeys/2 {
// 		t.Errorf("expected %d keys in left child, got %d", maxKeys/2, len(tree.root.children[0].keys))
// 	}
// 	if len(tree.root.children[1].keys) != maxKeys/2 {
// 		t.Errorf("expected %d keys in right child, got %d", maxKeys/2, len(tree.root.children[1].keys))
// 	}
// }
