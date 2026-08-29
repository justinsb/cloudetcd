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
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
)

// TestConcurrentConditionalUpdatesSerialize races N compare-and-put
// transactions on one key that all observed the same mod_revision. Exactly
// one must succeed; the rest must fail their compare (Succeeded=false, with
// the Else Range showing the winner's value) rather than error or overwrite.
func TestConcurrentConditionalUpdatesSerialize(t *testing.T) {
	ctx := t.Context()
	// A commit latency makes the batching window wide enough that the racing
	// transactions land in the same batch, which is the case that matters.
	store, err := NewMemoryStorage(memorylog.New(memorylog.WithCommitLatency(20 * time.Millisecond)))
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
	store, err := NewMemoryStorage(memorylog.New(memorylog.WithCommitLatency(20 * time.Millisecond)))
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
