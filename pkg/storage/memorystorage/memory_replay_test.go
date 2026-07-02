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
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"justinsb.com/cloudetcd/pkg/persistence/filesystemlog"
	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
)

func TestMemoryStorageLogReplay(t *testing.T) {
	// Create a memory log for testing
	log := memorylog.New()

	// Create storage with the log
	storage, err := NewMemoryStorage(log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Add some test data
	ctx := t.Context()

	// Put some keys
	if _, err := storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key1"), Value: []byte("value1")}); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	if _, err = storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key2"), Value: []byte("value2")}); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}

	// Update key1
	if _, err = storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key1"), Value: []byte("value1-updated")}); err != nil {
		t.Fatalf("Failed to update key1: %v", err)
	}

	// Delete key2
	delResp, err := storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("key2")})
	if err != nil {
		t.Fatalf("Failed to delete key2: %v", err)
	}
	rev4 := delResp.Header.Revision

	// Verify current state
	kv1Resp, err := storage.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("key1")})
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	kv1 := kv1Resp.Kvs[0]
	if string(kv1.Value) != "value1-updated" {
		t.Errorf("Expected key1 value to be 'value1-updated', got '%s'", string(kv1.Value))
	}

	// key2 should be deleted
	kvResp2, err := storage.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("key2")})
	assertNotFound(t, kvResp2, err)

	// Now create a new storage instance with the same log to test replay
	newStorage, err := NewMemoryStorage(log)
	if err != nil {
		t.Fatalf("Failed to create new storage: %v", err)
	}

	// Verify that the state was replayed correctly
	newKv1Resp, err := newStorage.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("key1")})
	if err != nil {
		t.Fatalf("Failed to get key1 after replay: %v", err)
	}
	newKv1 := newKv1Resp.Kvs[0]
	if string(newKv1.Value) != "value1-updated" {
		t.Errorf("After replay: expected key1 value to be 'value1-updated', got '%s'", string(newKv1.Value))
	}

	// key2 should still be deleted
	kvResp2, err = newStorage.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("key2")})
	assertNotFound(t, kvResp2, err)

	// Verify revision numbers
	logRevision, err := newStorage.log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if logRevision != Revision(rev4) {
		t.Errorf("Expected revision to be %d after replay, got %d", rev4, logRevision)
	}

	// Test list functionality after replay
	keysResp, err := newStorage.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("aaa"), RangeEnd: []byte("zzz")})
	if err != nil {
		t.Fatalf("Failed to list keys after replay: %v", err)
	}
	keys := keysResp.Kvs
	// Should only have key1 (key2 is deleted)
	if len(keys) != 1 {
		t.Errorf("Expected 1 key after replay, got %d: %+v", len(keys), keys)
	}
	if string(keys[0].Key) != "key1" {
		t.Errorf("Expected key1 to be the only key, got %s", string(keys[0].Key))
	}
}

func TestMemoryStorageLogReplayEmpty(t *testing.T) {
	ctx := t.Context()

	// Test replay with an empty log
	log := memorylog.New()
	storage, err := NewMemoryStorage(log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Should not crash and should have revision 0
	logRevision, err := storage.log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if logRevision != 1 {
		t.Errorf("Expected revision 1 for empty log, got %d", logRevision)
	}

	// List should return empty
	keysResp, err := storage.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("aaa"), RangeEnd: []byte("zzz")})
	if err != nil {
		t.Fatalf("Failed to list keys: %v", err)
	}
	keys := keysResp.Kvs
	if len(keys) != 0 {
		t.Errorf("Expected empty list for empty log, got %d keys", len(keys))
	}
}

func TestMemoryStorageForceReplay(t *testing.T) {
	// Create a memory log for testing
	log := memorylog.New()

	// Create storage with the log
	storage, err := NewMemoryStorage(log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Add some test data
	ctx := t.Context()
	storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key1"), Value: []byte("value1")})
	storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key2"), Value: []byte("value2")})

	// Verify initial state
	keysResp, err := storage.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("aaa"), RangeEnd: []byte("zzz")})
	if err != nil {
		t.Fatalf("Failed to list keys: %v", err)
	}
	keys := keysResp.Kvs
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys initially, got %d", len(keys))
	}

	// Force replay
	err = storage.ForceReplayLog(ctx)
	if err != nil {
		t.Fatalf("Failed to force replay log: %v", err)
	}

	// Verify state is restored
	keysResp, err = storage.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("aaa"), RangeEnd: []byte("zzz")})
	if err != nil {
		t.Fatalf("Failed to list keys after force replay: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys after force replay, got %d", len(keys))
	}
}

func TestMemoryStorageFilesystemLogReplay(t *testing.T) {
	// Create a temporary directory for the log
	logDir := filepath.Join(os.TempDir(), "cloudetcd-test-fs-log-replay")
	defer os.RemoveAll(logDir) // Clean up after we're done

	// Step 1: Create a filesystem log
	fsLog, err := filesystemlog.NewFilesystemLog(logDir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}

	// Step 2: Create storage with the filesystem log
	storage1, err := NewMemoryStorage(fsLog)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Step 3: Add some data
	ctx := t.Context()

	resp1, err := storage1.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("app/config"), Value: []byte("production")})
	if err != nil {
		t.Fatalf("Failed to put app/config: %v", err)
	}
	if getRevision(t, resp1) != 2 {
		t.Errorf("Expected revision 2, got %d", getRevision(t, resp1))
	}

	resp2, err := storage1.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("app/version"), Value: []byte("1.0.0")})
	if err != nil {
		t.Fatalf("Failed to put app/version: %v", err)
	}
	if getRevision(t, resp2) != 3 {
		t.Errorf("Expected revision 3, got %d", getRevision(t, resp2))
	}

	resp3, err := storage1.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("app/config"), Value: []byte("staging")})
	if err != nil {
		t.Fatalf("Failed to update app/config: %v", err)
	}
	if getRevision(t, resp3) != 4 {
		t.Errorf("Expected revision 4, got %d", getRevision(t, resp3))
	}

	delResp, err := storage1.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("app/version")})
	if err != nil {
		t.Fatalf("Failed to delete app/version: %v", err)
	}
	rev4 := delResp.Header.Revision
	if rev4 != 5 {
		t.Errorf("Expected revision 5, got %d", rev4)
	}

	// Step 4: Verify current state
	keysResp, err := storage1.List(ctx, &etcdserverpb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
	if err != nil {
		t.Fatalf("Failed to list keys: %v", err)
	}
	keys := keysResp.Kvs
	if len(keys) != 1 {
		t.Errorf("Expected 1 key, got %d", len(keys))
	}
	if string(keys[0].Key) != "app/config" {
		t.Errorf("Expected key 'app/config', got '%s'", string(keys[0].Key))
	}
	if string(keys[0].Value) != "staging" {
		t.Errorf("Expected value 'staging', got '%s'", string(keys[0].Value))
	}

	// Step 5: Create a new storage instance with the same log (simulating restart)
	storage2, err := NewMemoryStorage(fsLog)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Step 6: Verify that the state was replayed correctly
	keysResp, err = storage2.List(ctx, &etcdserverpb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
	if err != nil {
		t.Fatalf("Failed to list keys in storage2: %v", err)
	}
	keys = keysResp.Kvs
	if len(keys) != 1 {
		t.Errorf("After replay: expected 1 key, got %d", len(keys))
	}
	if string(keys[0].Key) != "app/config" {
		t.Errorf("After replay: expected key 'app/config', got '%s'", string(keys[0].Key))
	}
	if string(keys[0].Value) != "staging" {
		t.Errorf("After replay: expected value 'staging', got '%s'", string(keys[0].Value))
	}

	// Step 7: Verify revision numbers match
	logRevision1, err := storage1.log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	logRevision2, err := storage2.log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if logRevision1 != logRevision2 {
		t.Errorf("Revisions don't match: storage1=%d, storage2=%d", logRevision1, logRevision2)
	}
	if logRevision1 != Revision(rev4) {
		t.Errorf("Expected revision %d, got %d", rev4, logRevision1)
	}

	// Step 8: Test that we can still write to the replayed storage
	resp5, err := storage2.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("app/status"), Value: []byte("running")})
	if err != nil {
		t.Fatalf("Failed to put app/status: %v", err)
	}
	if getRevision(t, resp5) != 6 {
		t.Errorf("Expected revision 6, got %d", getRevision(t, resp5))
	}

	// Step 9: Verify the new data is visible in storage2 but not storage1
	keysResp, err = storage2.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("aaa"), RangeEnd: []byte("zzz")})
	if err != nil {
		t.Fatalf("Failed to list keys in storage2: %v", err)
	}
	keys = keysResp.Kvs
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys in storage2, got %d", len(keys))
	}

	// storage1 should still only have the original key
	keysResp, err = storage1.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("aaa"), RangeEnd: []byte("zzz")})
	if err != nil {
		t.Fatalf("Failed to list keys in storage1: %v", err)
	}
	keys = keysResp.Kvs
	if len(keys) != 1 {
		t.Errorf("Expected 1 key in storage1, got %d", len(keys))
	}

	// Test that deleted key is still deleted after replay
	kvResp2, err := storage2.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("app/version")})
	assertNotFound(t, kvResp2, err)
}
