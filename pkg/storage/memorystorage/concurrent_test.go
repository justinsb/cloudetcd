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

package memorystorage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"justinsb.com/cloudetcd/pkg/persistence/filesystemlog"
	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
	"justinsb.com/cloudetcd/pkg/storage"
)

// TestConcurrentConditionalUpdatesSerialize races N compare-and-put
// transactions on one key that all observed the same mod_revision. Exactly
// one must succeed; the rest must fail their compare (Succeeded=false, with
// the Else Range showing the winner's value) rather than error or overwrite.
func TestConcurrentConditionalUpdatesSerialize(t *testing.T) {
	ctx := t.Context()
	store, err := NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("/registry/leases/kube-node-lease/node-1")
	putResp, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v0")})
	if err != nil {
		t.Fatal(err)
	}
	rev := putResp.Header.Revision

	const racers = 32
	var wg sync.WaitGroup
	results := make([]*etcdserverpb.TxnResponse, racers)
	errs := make([]error, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = store.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Key:         key,
					Target:      etcdserverpb.Compare_MOD,
					Result:      etcdserverpb.Compare_EQUAL,
					TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: rev},
				}},
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte(fmt.Sprintf("racer-%d", i))}}}},
				Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
					RequestRange: &etcdserverpb.RangeRequest{Key: key}}}},
			})
		}(i)
	}
	wg.Wait()

	winners := 0
	var winnerValue string
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("racer %d: %v", i, errs[i])
		}
		if results[i].Succeeded {
			winners++
			winnerValue = fmt.Sprintf("racer-%d", i)
		}
	}
	if winners != 1 {
		t.Fatalf("%d racers succeeded, want exactly 1", winners)
	}
	for i, r := range results {
		if r.Succeeded {
			continue
		}
		kvs := r.Responses[0].GetResponseRange().Kvs
		if len(kvs) != 1 || string(kvs[0].Value) != winnerValue {
			t.Errorf("racer %d: else-branch read %q, want the winner's %q", i, kvs, winnerValue)
		}
	}
	got, err := store.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Kvs[0].Value) != winnerValue || got.Kvs[0].ModRevision != rev+1 || got.Kvs[0].Version != 2 {
		t.Errorf("final state %v, want value %q at mod_revision %d version 2", got.Kvs[0], winnerValue, rev+1)
	}
}

// TestConcurrentWritesToDistinctKeys races puts on distinct keys. All must
// succeed, be assigned distinct contiguous revisions, and be readable at the
// revision each response reported, with the log and index agreeing.
func TestConcurrentWritesToDistinctKeys(t *testing.T) {
	ctx := t.Context()
	store, err := NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatal(err)
	}
	base, err := store.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}

	const writers = 64
	revs := make([]int64, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("/registry/minions/node-%03d", i)
			// Create, then a conditional update on the revision we were told.
			resp, err := store.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{Key: []byte(key), Target: etcdserverpb.Compare_MOD, Result: etcdserverpb.Compare_EQUAL,
					TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0}}},
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("a")}}}},
			})
			if err != nil || !resp.Succeeded {
				t.Errorf("create %s: succeeded=%v err=%v", key, resp.GetSucceeded(), err)
				return
			}
			resp, err = store.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{Key: []byte(key), Target: etcdserverpb.Compare_MOD, Result: etcdserverpb.Compare_EQUAL,
					TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: resp.Header.Revision}}},
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("b")}}}},
			})
			if err != nil || !resp.Succeeded {
				t.Errorf("update %s: succeeded=%v err=%v", key, resp.GetSucceeded(), err)
				return
			}
			revs[i] = resp.Header.Revision
			// Our write must be visible to a read at the revision we were told.
			got, err := store.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(key), Revision: resp.Header.Revision})
			if err != nil || len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "b" || got.Kvs[0].Version != 2 {
				t.Errorf("read-your-write %s at %d: %v err=%v", key, resp.Header.Revision, got.GetKvs(), err)
			}
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	// 2 writes per writer, contiguous after base, no duplicates.
	current, err := store.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if int(current-base) != 2*writers {
		t.Errorf("revision advanced by %d, want %d", current-base, 2*writers)
	}
	sort.Slice(revs, func(i, j int) bool { return revs[i] < revs[j] })
	for i := 1; i < len(revs); i++ {
		if revs[i] == revs[i-1] {
			t.Errorf("duplicate revision %d", revs[i])
		}
	}

	// The index agrees with the log: every key is present with version 2 at
	// its own mod_revision, and a list at the head sees all of them.
	list, err := store.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/minions/"), RangeEnd: []byte("/registry/minions0")})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Kvs) != writers {
		t.Fatalf("list saw %d keys, want %d", len(list.Kvs), writers)
	}
	for _, kv := range list.Kvs {
		if kv.Version != 2 || string(kv.Value) != "b" {
			t.Errorf("%s: version %d value %q", kv.Key, kv.Version, kv.Value)
		}
	}
}

// TestStorageCompact drives compaction through the storage: overwritten and
// deleted keys, an open watcher that must not be cut off, then the log is
// reopened and the state is what it was.
func TestStorageCompact(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	newStore := func() *MemoryStorage {
		t.Helper()
		lg, err := filesystemlog.NewFilesystemLogWithOptions(dir, filesystemlog.Options{RotateBytes: 4000, CacheBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		store, err := NewMemoryStorage(lg)
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := newStore()

	for round := 0; round < 20; round++ {
		for k := 0; k < 10; k++ {
			key := fmt.Sprintf("/k/%02d", k)
			if _, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(fmt.Sprintf("v%d", round))}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for k := 0; k < 10; k += 2 {
		if _, err := store.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte(fmt.Sprintf("/k/%02d", k))}); err != nil {
			t.Fatal(err)
		}
	}
	// A watcher that has delivered up to some revision holds the compaction
	// floor at that revision.
	watchFrom, _ := store.GetCurrentRevision(ctx)
	head := watchFrom
	compactedTo, err := store.Compact(ctx, head)
	if err != nil {
		t.Fatal(err)
	}
	if compactedTo != head {
		t.Fatalf("compacted to %d, want %d", compactedTo, head)
	}

	expect := func(store *MemoryStorage) {
		t.Helper()
		list, err := store.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k/"), RangeEnd: []byte("/k0")})
		if err != nil {
			t.Fatal(err)
		}
		if len(list.Kvs) != 5 {
			t.Fatalf("list has %d keys, want 5 (odd ones)", len(list.Kvs))
		}
		for _, kv := range list.Kvs {
			if string(kv.Value) != "v19" || kv.Version != 20 {
				t.Fatalf("%s = %s (version %d), want v19 (version 20)", kv.Key, kv.Value, kv.Version)
			}
		}
		for k := 0; k < 10; k += 2 {
			resp, err := store.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(fmt.Sprintf("/k/%02d", k))})
			if err != nil || len(resp.Kvs) != 0 {
				t.Fatalf("deleted key %02d: %v %v", k, resp.GetKvs(), err)
			}
		}
	}
	expect(store)
	store.WaitForCompaction()
	if _, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/k/01"), Value: []byte("after")}); err != nil {
		t.Fatal(err)
	}
	store.GracefulStop()
	store.log.Close()

	store2 := newStore()
	defer store2.log.Close()
	resp, err := store2.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k/01")})
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "after" || resp.Kvs[0].Version != 21 {
		t.Fatalf("after reopen, /k/01 = %v (%v)", resp.GetKvs(), err)
	}
	list, _ := store2.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k/"), RangeEnd: []byte("/k0")})
	if len(list.Kvs) != 5 {
		t.Fatalf("after reopen, list has %d keys, want 5", len(list.Kvs))
	}
}

// TestCompactKeepsStateAtPoint checks that reads at the compaction point see
// the same state after the log is compacted as before. Two histories matter:
// a key written again above the point (its write at the point must survive,
// though it is no longer the key's latest), and a key deleted below the
// point and recreated above it (the delete must survive, so that the key
// reads as absent at the point rather than failing).
func TestCompactKeepsStateAtPoint(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	open := func() *MemoryStorage {
		t.Helper()
		lg, err := filesystemlog.NewFilesystemLogWithOptions(dir, filesystemlog.Options{RotateBytes: 2000, CacheBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		store, err := NewMemoryStorage(lg)
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := open()

	var point Revision
	for round := 0; round < 12; round++ {
		for k := 0; k < 10; k++ {
			key := []byte(fmt.Sprintf("/k/%02d", k))
			if k%3 == 0 && round >= 4 && round < 8 {
				// Deleted in round 4, absent through round 7, recreated in
				// round 8.
				if round == 4 {
					if _, err := store.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: key}); err != nil {
						t.Fatal(err)
					}
				}
				continue
			}
			if _, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte(fmt.Sprintf("v%d", round))}); err != nil {
				t.Fatal(err)
			}
		}
		if round == 6 {
			point, _ = store.GetCurrentRevision(ctx)
		}
	}

	snapshot := func(store *MemoryStorage, at Revision) string {
		t.Helper()
		list, err := store.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k/"), RangeEnd: []byte("/k0"), Revision: int64(at)})
		if err != nil {
			t.Fatalf("list at %d: %v", at, err)
		}
		var s []string
		for _, kv := range list.Kvs {
			s = append(s, fmt.Sprintf("%s=%s@%d", kv.Key, kv.Value, kv.ModRevision))
		}
		return fmt.Sprint(s)
	}
	head, _ := store.GetCurrentRevision(ctx)
	atPoint, atHead := snapshot(store, point), snapshot(store, head)
	if !strings.Contains(atPoint, "/k/01=v6@") || strings.Contains(atPoint, "/k/00=") {
		t.Fatalf("unexpected state at the point: %s", atPoint)
	}

	if compacted, err := store.Compact(ctx, point); err != nil || compacted != point {
		t.Fatalf("Compact = %d, %v; want %d", compacted, err, point)
	}
	store.WaitForCompaction()
	if got := snapshot(store, point); got != atPoint {
		t.Fatalf("state at the compaction point changed:\n before %s\n after  %s", atPoint, got)
	}
	if got := snapshot(store, head); got != atHead {
		t.Fatalf("state at head changed:\n before %s\n after  %s", atHead, got)
	}

	// The compacted files are what a restart rebuilds from.
	store.GracefulStop()
	store.log.Close()
	store = open()
	defer store.log.Close()
	if got := snapshot(store, point); got != atPoint {
		t.Fatalf("state at the compaction point after restart:\n before %s\n after  %s", atPoint, got)
	}
	if got := snapshot(store, head); got != atHead {
		t.Fatalf("state at head after restart:\n before %s\n after  %s", atHead, got)
	}
}

// TestCompactedReadsAndWatches checks the compaction-point semantics at the
// storage API: reads at or above the point (and no further than head) work,
// reads below it are ErrCompacted, reads beyond head are ErrFutureRev, and a
// watch may only start above the point (a refused watch reports the point).
func TestCompactedReadsAndWatches(t *testing.T) {
	ctx := t.Context()
	lg, err := filesystemlog.NewFilesystemLogWithOptions(t.TempDir(), filesystemlog.Options{CacheBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewMemoryStorage(lg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.log.Close()

	var putRev []Revision
	for i := 0; i < 10; i++ {
		resp, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/k"), Value: []byte(fmt.Sprintf("v%d", i))})
		if err != nil {
			t.Fatal(err)
		}
		putRev = append(putRev, Revision(resp.Header.Revision))
	}
	point := putRev[5]
	if compacted, err := store.Compact(ctx, point); err != nil || compacted != point {
		t.Fatalf("Compact = %d, %v; want %d", compacted, err, point)
	}

	if resp, err := store.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k"), Revision: int64(point)}); err != nil || string(resp.Kvs[0].Value) != "v5" {
		t.Fatalf("read at the compaction point: %v, %v", resp.GetKvs(), err)
	}
	if _, err := store.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k"), Revision: int64(point - 1)}); !errors.Is(err, storage.ErrCompacted) {
		t.Fatalf("read below the compaction point: got %v, want ErrCompacted", err)
	}
	if _, err := store.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("/"), RangeEnd: []byte("0"), Revision: int64(point - 1)}); !errors.Is(err, storage.ErrCompacted) {
		t.Fatalf("list below the compaction point: got %v, want ErrCompacted", err)
	}
	if _, err := store.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k"), Revision: 99}); !errors.Is(err, storage.ErrFutureRev) {
		t.Fatalf("read beyond head: got %v, want ErrFutureRev", err)
	}

	cb := func(*etcdserverpb.WatchResponse) error { return nil }
	if _, rev, err := store.Watch(ctx, &etcdserverpb.WatchCreateRequest{WatchId: 1, Key: []byte("/k"), StartRevision: int64(point)}, cb); !errors.Is(err, storage.ErrCompacted) || rev != point {
		t.Fatalf("watch from the compaction point: got rev %d, %v; want ErrCompacted with rev %d", rev, err, point)
	}
	w, _, err := store.Watch(ctx, &etcdserverpb.WatchCreateRequest{WatchId: 2, Key: []byte("/k"), StartRevision: int64(point + 1)}, cb)
	if err != nil {
		t.Fatalf("watch from above the compaction point: %v", err)
	}
	w.Close()
}

// TestCompactedGateSurvivesRestart checks that the reads-below-the-point
// gate is rebuilt from the log on startup: the compaction point is not
// stored anywhere else, and before the log reported its floor a restarted
// server answered reads below the point from the pruned index (absent keys)
// instead of refusing them.
func TestCompactedGateSurvivesRestart(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	opts := filesystemlog.Options{RotateBytes: 2000}
	store := openStore(t, dir, opts)
	defer func() { store.log.Close() }()

	var putRev []Revision
	for i := 0; i < 200; i++ {
		resp, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(fmt.Sprintf("/k/%02d", i%10)), Value: []byte(fmt.Sprintf("v%d", i))})
		if err != nil {
			t.Fatal(err)
		}
		putRev = append(putRev, Revision(resp.Header.Revision))
	}
	point := putRev[150]
	if compacted, err := store.Compact(ctx, point); err != nil || compacted != point {
		t.Fatalf("Compact = %d, %v; want %d", compacted, err, point)
	}
	store.WaitForCompaction()

	store = restart(t, store, dir, opts)
	if _, err := store.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k/01"), Revision: int64(putRev[50])}); !errors.Is(err, storage.ErrCompacted) {
		t.Fatalf("read below the point after restart: got %v, want ErrCompacted", err)
	}
	cb := func(*etcdserverpb.WatchResponse) error { return nil }
	if _, _, err := store.Watch(ctx, &etcdserverpb.WatchCreateRequest{WatchId: 1, Key: []byte("/k/01"), StartRevision: int64(putRev[50])}, cb); !errors.Is(err, storage.ErrCompacted) {
		t.Fatalf("watch below the point after restart: got %v, want ErrCompacted", err)
	}
	if resp, err := store.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k/01"), Revision: int64(point)}); err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("read at the point after restart: %v, %v", resp.GetKvs(), err)
	}
}
