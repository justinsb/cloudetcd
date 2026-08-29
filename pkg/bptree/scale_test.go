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
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// TestBPTree_ManyKeys inserts keys in random order and checks ordering,
// lookups, range scans and revision history at a size where a linear
// structure would be visibly slow.
func TestBPTree_ManyKeys(t *testing.T) {
	const n = 200_000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("/registry/pods/ns-%03d/pod-%06d", i%100, i))
	}
	perm := rand.New(rand.NewSource(1)).Perm(n)

	var tree BPTree
	for rev, i := range perm {
		tree.AddRevision(keys[i], Revision(rev+1))
	}
	// A second write to every key, at a later revision.
	for rev, i := range perm {
		tree.AddRevision(keys[i], Revision(n+rev+1))
	}
	if tree.Len() != n {
		t.Fatalf("Len = %d, want %d", tree.Len(), n)
	}

	// Point lookups at "now" see the second write; at revision n, the first.
	for rev, i := range perm {
		if got, ok := tree.GetLatestRevisionByKey(keys[i], Revision(2*n)); !ok || got != Revision(n+rev+1) {
			t.Fatalf("latest of %s = %d,%v; want %d", keys[i], got, ok, n+rev+1)
		}
		if got, ok := tree.GetLatestRevisionByKey(keys[i], Revision(n)); !ok || got != Revision(rev+1) {
			t.Fatalf("latest of %s at %d = %d,%v; want %d", keys[i], n, got, ok, rev+1)
		}
	}
	if _, ok := tree.GetLatestRevisionByKey([]byte("/registry/pods/ns-000/pod-000000"), 0); ok {
		t.Errorf("found a revision <= 0")
	}
	if _, ok := tree.GetLatestRevisionByKey([]byte("/registry/nope"), Revision(2*n)); ok {
		t.Errorf("found a key that was never written")
	}

	// A range scan over one namespace's prefix returns its keys in order,
	// each with both revisions, and stops at the callback's request.
	prefix := []byte("/registry/pods/ns-042/")
	var seen [][]byte
	tree.ListRevisionsByKeyRange(prefix, Revision(2*n), func(key []byte, revisions []Revision) bool {
		if !bytes.HasPrefix(key, prefix) {
			return false
		}
		if len(revisions) != 2 || revisions[0] >= revisions[1] {
			t.Errorf("%s: revisions %v", key, revisions)
		}
		seen = append(seen, key)
		return true
	})
	if len(seen) != n/100 {
		t.Fatalf("range scan saw %d keys, want %d", len(seen), n/100)
	}
	for i := 1; i < len(seen); i++ {
		if bytes.Compare(seen[i-1], seen[i]) >= 0 {
			t.Fatalf("range scan out of order: %s before %s", seen[i-1], seen[i])
		}
	}
}

func BenchmarkAddRevision(b *testing.B) {
	var tree BPTree
	for i := 0; i < b.N; i++ {
		tree.AddRevision([]byte(fmt.Sprintf("/registry/minions/node-%08d", i)), Revision(i+1))
	}
}
