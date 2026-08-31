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
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"justinsb.com/cloudetcd/pkg/persistence/filesystemlog"
	"justinsb.com/cloudetcd/pkg/storage"
)

func openStore(t *testing.T, dir string, opts filesystemlog.Options) *MemoryStorage {
	t.Helper()
	if opts.CacheBytes == 0 {
		opts.CacheBytes = 1
	}
	lg, err := filesystemlog.NewFilesystemLogWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewMemoryStorage(lg)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func restart(t *testing.T, store *MemoryStorage, dir string, opts filesystemlog.Options) *MemoryStorage {
	t.Helper()
	store.GracefulStop()
	if err := store.log.Close(); err != nil {
		t.Fatal(err)
	}
	return openStore(t, dir, opts)
}

func hasKey(t *testing.T, store *MemoryStorage, key string) bool {
	t.Helper()
	resp, err := store.Get(t.Context(), &etcdserverpb.RangeRequest{Key: []byte(key)})
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Kvs) > 0
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestLeasePersistence checks that leases survive a restart: the grant is a
// log record, so a replayed lease has its granted TTL (refreshed, as etcd
// does), its keys stay attached — and above all stay alive: before leases
// were persisted, a restart gave leased keys an already-expired placeholder
// lease and promptly deleted them. A revoke is persisted the same way.
func TestLeasePersistence(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	opts := filesystemlog.Options{}
	store := openStore(t, dir, opts)
	defer func() { store.log.Close() }()

	grant, err := store.LeaseManager().LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/leased"), Value: []byte("v"), Lease: grant.ID}); err != nil {
		t.Fatal(err)
	}

	store = restart(t, store, dir, opts)

	lm := store.LeaseManager()
	if !lm.HasLease(grant.ID) {
		t.Fatal("lease did not survive the restart")
	}
	ttl, err := lm.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID})
	if err != nil {
		t.Fatal(err)
	}
	if ttl.GrantedTTL != 300 || ttl.TTL <= 0 || ttl.TTL > 300 {
		t.Fatalf("lease after restart: TTL %d of %d granted", ttl.TTL, ttl.GrantedTTL)
	}

	// Run the expiry loop: the lease is far from expiring, so the key must
	// stay (the placeholder-lease bug deleted it here).
	runCtx, stopRun := context.WithCancel(ctx)
	go lm.Run(runCtx)
	time.Sleep(1500 * time.Millisecond)
	if !hasKey(t, store, "/leased") {
		t.Fatal("leased key was deleted after a restart")
	}

	// Revoke deletes the keys and survives a restart too.
	if _, err := lm.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID}); err != nil {
		t.Fatal(err)
	}
	if lm.HasLease(grant.ID) {
		t.Fatal("lease still present after revoke")
	}
	eventually(t, "revoked lease's key to be deleted", 10*time.Second, func() bool { return !hasKey(t, store, "/leased") })
	stopRun()

	store = restart(t, store, dir, opts)
	if store.LeaseManager().HasLease(grant.ID) {
		t.Fatal("revoked lease came back after a restart")
	}
	if hasKey(t, store, "/leased") {
		t.Fatal("revoked lease's key came back after a restart")
	}
}

// TestLeaseExpiryPersisted checks that a lease granted before a restart
// still expires after it, and that the expiry is itself persisted: after a
// further restart the lease stays gone without the expiry loop running.
func TestLeaseExpiryPersisted(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	opts := filesystemlog.Options{}
	store := openStore(t, dir, opts)
	defer func() { store.log.Close() }()

	grant, err := store.LeaseManager().LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/expiring"), Value: []byte("v"), Lease: grant.ID}); err != nil {
		t.Fatal(err)
	}

	store = restart(t, store, dir, opts)
	lm := store.LeaseManager()
	if !lm.HasLease(grant.ID) {
		t.Fatal("lease did not survive the restart")
	}

	runCtx, stopRun := context.WithCancel(ctx)
	go lm.Run(runCtx)
	eventually(t, "lease to expire", 15*time.Second, func() bool { return !lm.HasLease(grant.ID) })
	eventually(t, "expired lease's key to be deleted", 10*time.Second, func() bool { return !hasKey(t, store, "/expiring") })
	stopRun()

	store = restart(t, store, dir, opts)
	if store.LeaseManager().HasLease(grant.ID) {
		t.Fatal("expired lease came back after a restart")
	}
}

// TestLeaseCompaction checks that compaction keeps a live lease's grant
// record (the lease survives a restart from the compacted log) and lets a
// revoked lease's records go.
func TestLeaseCompaction(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	opts := filesystemlog.Options{RotateBytes: 2000}
	store := openStore(t, dir, opts)
	defer func() { store.log.Close() }()

	grant, err := store.LeaseManager().LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/leased"), Value: []byte("v"), Lease: grant.ID}); err != nil {
		t.Fatal(err)
	}
	fill := func() {
		t.Helper()
		for i := 0; i < 100; i++ {
			if _, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(fmt.Sprintf("/fill/%02d", i%10)), Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
				t.Fatal(err)
			}
		}
	}
	fill()
	compact := func() {
		t.Helper()
		head, _ := store.GetCurrentRevision(ctx)
		if _, err := store.Compact(ctx, head); err != nil {
			t.Fatal(err)
		}
		store.WaitForCompaction()
	}
	compact()

	store = restart(t, store, dir, opts)
	lm := store.LeaseManager()
	if !lm.HasLease(grant.ID) {
		t.Fatal("lease did not survive compaction and restart")
	}
	if ttl, err := lm.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID}); err != nil || ttl.GrantedTTL != 300 {
		t.Fatalf("lease TTL after compaction and restart: %v, %v", ttl.GetGrantedTTL(), err)
	}
	if !hasKey(t, store, "/leased") {
		t.Fatal("leased key did not survive compaction and restart")
	}

	// Revoke, let the keys go, compact again: the lease must stay gone.
	runCtx, stopRun := context.WithCancel(ctx)
	go lm.Run(runCtx)
	if _, err := lm.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revoked lease's key to be deleted", 10*time.Second, func() bool { return !hasKey(t, store, "/leased") })
	stopRun()
	fill()
	compact()

	store = restart(t, store, dir, opts)
	if store.LeaseManager().HasLease(grant.ID) {
		t.Fatal("revoked lease survived compaction and restart")
	}
}

// TestInternalKeysHidden checks that the internal lease records are not
// visible to clients: not in List results over the whole keyspace, and not
// in Watch events.
func TestInternalKeysHidden(t *testing.T) {
	ctx := t.Context()
	store := openStore(t, t.TempDir(), filesystemlog.Options{})
	defer func() { store.log.Close() }()

	var mu = make(chan struct{}, 1)
	var seen [][]byte
	cb := func(resp *etcdserverpb.WatchResponse) error {
		mu <- struct{}{}
		defer func() { <-mu }()
		for _, ev := range resp.Events {
			seen = append(seen, append([]byte(nil), ev.Kv.Key...))
		}
		return nil
	}
	w, _, err := store.Watch(ctx, &etcdserverpb.WatchCreateRequest{WatchId: 7, Key: []byte{0}, RangeEnd: []byte{0}, StartRevision: 1}, cb)
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go w.Run(watchCtx)

	grant, err := store.LeaseManager().LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/user"), Value: []byte("v"), Lease: grant.ID}); err != nil {
		t.Fatal(err)
	}

	list, err := store.List(ctx, &etcdserverpb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range list.Kvs {
		if storage.IsInternalKey(kv.Key) {
			t.Fatalf("List returned internal key %q", kv.Key)
		}
	}
	if len(list.Kvs) != 1 || !bytes.Equal(list.Kvs[0].Key, []byte("/user")) {
		t.Fatalf("List = %v, want just /user", list.Kvs)
	}

	eventually(t, "watch to deliver the user key", 5*time.Second, func() bool {
		mu <- struct{}{}
		defer func() { <-mu }()
		for _, k := range seen {
			if bytes.Equal(k, []byte("/user")) {
				return true
			}
		}
		return false
	})
	mu <- struct{}{}
	defer func() { <-mu }()
	for _, k := range seen {
		if storage.IsInternalKey(k) {
			t.Fatalf("watch delivered internal key %q", k)
		}
	}
	w.Close()
}
