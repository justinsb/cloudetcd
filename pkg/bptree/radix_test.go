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
	"sort"
	"testing"
)

// reference is a sorted map used to check the tree.
type reference struct {
	revs map[string][]Revision
}

func (r *reference) add(key string, rev Revision) {
	if r.revs == nil {
		r.revs = map[string][]Revision{}
	}
	r.revs[key] = append(r.revs[key], rev)
}

func (r *reference) keysFrom(start string) []string {
	var keys []string
	for k := range r.revs {
		if k >= start {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// checkAgainst verifies every lookup and every range scan the reference can
// answer.
func checkAgainst(t *testing.T, tree *BPTree, ref *reference, starts []string) {
	t.Helper()
	if tree.Len() != len(ref.revs) {
		t.Fatalf("Len = %d, want %d", tree.Len(), len(ref.revs))
	}
	for k, revs := range ref.revs {
		latest := revs[len(revs)-1]
		if got, ok := tree.GetLatestRevisionByKey([]byte(k), latest); !ok || got != latest {
			t.Fatalf("latest(%q) = %d,%v want %d", k, got, ok, latest)
		}
		if got, ok := tree.GetLatestRevisionByKey([]byte(k), revs[0]); !ok || got != revs[0] {
			t.Fatalf("latest(%q at %d) = %d,%v want %d", k, revs[0], got, ok, revs[0])
		}
		if _, ok := tree.GetLatestRevisionByKey([]byte(k), revs[0]-1); ok {
			t.Fatalf("latest(%q at %d) found something", k, revs[0]-1)
		}
	}
	for _, start := range starts {
		var got []string
		tree.ListRevisionsByKeyRange([]byte(start), 1<<62, func(key []byte, revisions []Revision) bool {
			got = append(got, string(key))
			if want := ref.revs[string(key)]; fmt.Sprint(want) != fmt.Sprint(revisions) {
				t.Fatalf("scan from %q: key %q revisions %v want %v", start, key, revisions, want)
			}
			return true
		})
		if want := ref.keysFrom(start); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("scan from %q:\n got %q\nwant %q", start, got, want)
		}
	}
}

// TestRadixSplitting inserts keys that share prefixes in every combination
// that forces a split, in several orders, and checks lookups and scans.
func TestRadixSplitting(t *testing.T) {
	keys := []string{"a", "ab", "abc", "abd", "abcd", "b", "ba", "", "/registry/pods/ns/p1", "/registry/pods/ns/p2", "/registry/pod", "/registry/pods", "/registry"}
	orders := [][]string{keys, reverse(keys), shuffled(keys, 7), shuffled(keys, 11)}
	starts := append([]string{"", "a", "aa", "ab", "abb", "abc", "abcc", "abd", "abz", "b", "bb", "/", "/registry/", "/registry/pods/", "/registry/pods/ns/p1", "/registry/pods/ns/p15", "z"}, keys...)
	for _, order := range orders {
		var tree BPTree
		var ref reference
		for i, k := range order {
			tree.AddRevision([]byte(k), Revision(i+1), false)
			ref.add(k, Revision(i+1))
		}
		// Second revisions, in a different order.
		for i, k := range reverse(order) {
			tree.AddRevision([]byte(k), Revision(100+i), false)
			ref.add(k, Revision(100+i))
		}
		checkAgainst(t, &tree, &ref, starts)
	}
}

// TestRadixRandom is a property test against the reference with random keys
// drawn from a small alphabet, so that prefixes collide constantly.
func TestRadixRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	var tree BPTree
	var ref reference
	var starts []string
	for i := 0; i < 5000; i++ {
		n := rng.Intn(6)
		b := make([]byte, n)
		for j := range b {
			b[j] = "abc/"[rng.Intn(4)]
		}
		k := string(b)
		tree.AddRevision([]byte(k), Revision(i+1), false)
		ref.add(k, Revision(i+1))
		if i%100 == 0 {
			starts = append(starts, k, k+"a", k[:len(k)/2])
		}
	}
	checkAgainst(t, &tree, &ref, starts)
}

// TestKeysAreNotAliased checks that the tree does not retain the caller's
// key buffer.
func TestKeysAreNotAliased(t *testing.T) {
	var tree BPTree
	buf := []byte("/registry/minions/node-1")
	tree.AddRevision(buf, 1, false)
	copy(buf, "/registry/minions/node-2")
	if _, ok := tree.GetLatestRevisionByKey([]byte("/registry/minions/node-1"), 1); !ok {
		t.Fatal("key changed after the caller reused its buffer")
	}
	var got []string
	tree.ListRevisionsByKeyRange(nil, 1, func(key []byte, _ []Revision) bool {
		got = append(got, string(key))
		return true
	})
	if len(got) != 1 || got[0] != "/registry/minions/node-1" {
		t.Fatalf("scan = %q", got)
	}
}

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
		tree.AddRevision(keys[i], Revision(rev+1), false)
	}
	for rev, i := range perm {
		tree.AddRevision(keys[i], Revision(n+rev+1), false)
	}
	if tree.Len() != n {
		t.Fatalf("Len = %d, want %d", tree.Len(), n)
	}
	for rev, i := range perm {
		if got, ok := tree.GetLatestRevisionByKey(keys[i], Revision(2*n)); !ok || got != Revision(n+rev+1) {
			t.Fatalf("latest of %s = %d,%v; want %d", keys[i], got, ok, n+rev+1)
		}
		if got, ok := tree.GetLatestRevisionByKey(keys[i], Revision(n)); !ok || got != Revision(rev+1) {
			t.Fatalf("latest of %s at %d = %d,%v; want %d", keys[i], n, got, ok, rev+1)
		}
	}
	prefix := []byte("/registry/pods/ns-042/")
	var seen [][]byte
	tree.ListRevisionsByKeyRange(prefix, Revision(2*n), func(key []byte, revisions []Revision) bool {
		if !bytes.HasPrefix(key, prefix) {
			return false
		}
		if len(revisions) != 2 || revisions[0] >= revisions[1] {
			t.Errorf("%s: revisions %v", key, revisions)
		}
		seen = append(seen, bytes.Clone(key))
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
		tree.AddRevision([]byte(fmt.Sprintf("/registry/minions/node-%08d", i)), Revision(i+1), false)
	}
}

func BenchmarkGetLatestRevisionByKey(b *testing.B) {
	var tree BPTree
	const n = 200_000
	for i := 0; i < n; i++ {
		tree.AddRevision([]byte(fmt.Sprintf("/registry/minions/node-%08d", i)), Revision(i+1), false)
	}
	keys := make([][]byte, 1024)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("/registry/minions/node-%08d", (i*7919)%n))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.GetLatestRevisionByKey(keys[i%len(keys)], Revision(n))
	}
}

func reverse(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}

func shuffled(s []string, seed int64) []string {
	out := append([]string(nil), s...)
	rand.New(rand.NewSource(seed)).Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// TestCompact checks that compaction keeps what a read at or after the
// compaction revision can observe, drops the rest, and removes keys whose
// latest revision is a delete below it.
func TestCompact(t *testing.T) {
	var tree BPTree
	tree.AddRevision([]byte("a"), 1, false)
	tree.AddRevision([]byte("a"), 5, false)
	tree.AddRevision([]byte("a"), 9, false)
	tree.AddRevision([]byte("b"), 2, false)
	tree.AddRevision([]byte("b"), 6, true)
	tree.AddRevision([]byte("c"), 3, false)
	tree.AddRevision([]byte("c"), 12, true)
	tree.AddRevision([]byte("d"), 4, false)
	tree.AddRevision([]byte("d"), 11, false)

	removed, dropped := tree.Compact(8)
	// b is gone (2 revisions); a drops 1; c and d keep their latest below 8.
	if removed != 1 || dropped != 3 {
		t.Fatalf("Compact(8) removed %d keys, dropped %d revisions; want 1, 3", removed, dropped)
	}
	if tree.Len() != 3 {
		t.Fatalf("Len = %d, want 3", tree.Len())
	}
	if got, ok := tree.GetLatestRevisionByKey([]byte("a"), 8); !ok || got != 5 {
		t.Errorf("a at 8 = %d,%v want 5", got, ok)
	}
	if _, ok := tree.GetLatestRevisionByKey([]byte("a"), 1); ok {
		t.Errorf("a at 1 should be gone (compacted)")
	}
	if _, ok := tree.GetLatestRevisionByKey([]byte("b"), 100); ok {
		t.Errorf("b was deleted below the compaction revision and should be gone")
	}
	if got, ok := tree.GetLatestRevisionByKey([]byte("c"), 100); !ok || got != 12 {
		t.Errorf("c = %d,%v want tombstone at 12", got, ok)
	}
	var keys []string
	tree.ListRevisionsByKeyRange(nil, 100, func(key []byte, revisions []Revision) bool {
		keys = append(keys, string(key)+fmt.Sprint(revisions))
		return true
	})
	if fmt.Sprint(keys) != "[a[5 9] c[3 12] d[4 11]]" {
		t.Errorf("after compaction: %v", keys)
	}
}

// TestCompactFrom checks that chunked compaction ends where one-pass
// compaction does, even with the smallest chunks and with writes landing
// between chunks.
func TestCompactFrom(t *testing.T) {
	build := func() *BPTree {
		var tree BPTree
		tree.AddRevision(nil, 1, false)
		tree.AddRevision(nil, 9, false)
		for k := 0; k < 50; k++ {
			key := []byte(fmt.Sprintf("/reg/thing/%02d", k))
			for r := 0; r < 4; r++ {
				tree.AddRevision(key, Revision(1+k+50*r), false)
			}
			if k%5 == 0 {
				tree.AddRevision(key, Revision(300+k), true)
			}
		}
		return &tree
	}
	dump := func(tree *BPTree) string {
		var out []string
		tree.ListRevisionsByKeyRange(nil, 1000, func(key []byte, revisions []Revision) bool {
			out = append(out, fmt.Sprintf("%s%v", key, revisions))
			return true
		})
		return fmt.Sprintf("len=%d %v", tree.Len(), out)
	}
	const through = Revision(150)

	want := build()
	wantRemoved, wantDropped := want.Compact(through)

	got := build()
	var removed, dropped int
	var chunks int
	cursor, first := []byte(nil), true
	for first || cursor != nil {
		var k, d int
		cursor, k, d = got.CompactFrom(cursor, through, 1)
		removed += k
		dropped += d
		chunks++
		first = false
	}
	if removed != wantRemoved || dropped != wantDropped {
		t.Fatalf("chunked compaction removed %d/dropped %d, one-pass %d/%d", removed, dropped, wantRemoved, wantDropped)
	}
	if dump(got) != dump(want) {
		t.Fatalf("chunked compaction differs:\n chunked %s\n one-pass %s", dump(got), dump(want))
	}
	if chunks < 40 {
		t.Fatalf("chunk size 1 finished in %d chunks; the walk is not chunking", chunks)
	}

	// Writes between chunks: new revisions (above through) survive, both on
	// existing keys and on keys the walk has already passed.
	live := build()
	cursor, first = nil, true
	for first || cursor != nil {
		cursor, _, _ = live.CompactFrom(cursor, through, 7)
		live.AddRevision([]byte("/reg/thing/00"), Revision(400+len(cursor)), false)
		live.AddRevision([]byte(fmt.Sprintf("/new/%02d", len(cursor))), 401, false)
		first = false
	}
	if rev, ok := live.GetLatestRevisionByKey([]byte("/reg/thing/13"), 1000); !ok || rev != Revision(1+13+150) {
		t.Fatalf("existing key after interleaved compaction: %d, %v", rev, ok)
	}
	if _, ok := live.GetLatestRevisionByKey([]byte("/new/00"), 1000); !ok {
		t.Fatalf("key written between chunks is missing")
	}
	if rev, ok := live.GetLatestRevisionByKey([]byte("/reg/thing/00"), 399); !ok || rev != Revision(300) {
		t.Fatalf("tombstone above through: %d, %v (want kept at 300)", rev, ok)
	}
}
