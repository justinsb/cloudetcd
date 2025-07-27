package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"justinsb.com/cloudetcd/pkg/persistence"
)

func TestMemoryStorageLogReplay(t *testing.T) {
	// Create a memory log for testing
	log := persistence.NewMemoryLog()

	// Create storage with the log
	storage, err := NewMemoryStorage(log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Add some test data
	ctx := context.Background()

	// Put some keys
	if _, err := storage.Put(ctx, []byte("key1"), []byte("value1"), 0); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	if _, err = storage.Put(ctx, []byte("key2"), []byte("value2"), 0); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}

	// Update key1
	if _, err = storage.Put(ctx, []byte("key1"), []byte("value1-updated"), 0); err != nil {
		t.Fatalf("Failed to update key1: %v", err)
	}

	// Delete key2
	rev4, err := storage.Delete(ctx, []byte("key2"))
	if err != nil {
		t.Fatalf("Failed to delete key2: %v", err)
	}

	// Verify current state
	kv1, err := storage.Get(ctx, []byte("key1"), 0)
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if string(kv1.Value) != "value1-updated" {
		t.Errorf("Expected key1 value to be 'value1-updated', got '%s'", string(kv1.Value))
	}

	// key2 should be deleted
	_, err = storage.Get(ctx, []byte("key2"), 0)
	if err == nil {
		t.Error("Expected key2 to be deleted")
	}

	// Now create a new storage instance with the same log to test replay
	newStorage, err := NewMemoryStorage(log)
	if err != nil {
		t.Fatalf("Failed to create new storage: %v", err)
	}

	// Verify that the state was replayed correctly
	newKv1, err := newStorage.Get(ctx, []byte("key1"), 0)
	if err != nil {
		t.Fatalf("Failed to get key1 after replay: %v", err)
	}
	if string(newKv1.Value) != "value1-updated" {
		t.Errorf("After replay: expected key1 value to be 'value1-updated', got '%s'", string(newKv1.Value))
	}

	// key2 should still be deleted
	_, err = newStorage.Get(ctx, []byte("key2"), 0)
	if err == nil {
		t.Error("After replay: expected key2 to be deleted")
	}

	// Verify revision numbers
	if newStorage.GetCurrentRevision() != Revision(rev4) {
		t.Errorf("Expected revision to be %d after replay, got %d", rev4, newStorage.GetCurrentRevision())
	}

	// Test list functionality after replay
	keys, err := newStorage.List(ctx, []byte(""), []byte(""), 0)
	if err != nil {
		t.Fatalf("Failed to list keys after replay: %v", err)
	}

	// Should only have key1 (key2 is deleted)
	if len(keys) != 1 {
		t.Errorf("Expected 1 key after replay, got %d", len(keys))
	}
	if string(keys[0].Key) != "key1" {
		t.Errorf("Expected key1 to be the only key, got %s", string(keys[0].Key))
	}
}

func TestMemoryStorageLogReplayEmpty(t *testing.T) {
	// Test replay with an empty log
	log := persistence.NewMemoryLog()
	storage, err := NewMemoryStorage(log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Should not crash and should have revision 0
	if storage.GetCurrentRevision() != 0 {
		t.Errorf("Expected revision 0 for empty log, got %d", storage.GetCurrentRevision())
	}

	// List should return empty
	keys, err := storage.List(context.Background(), []byte(""), []byte(""), 0)
	if err != nil {
		t.Fatalf("Failed to list keys: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("Expected empty list for empty log, got %d keys", len(keys))
	}
}

func TestMemoryStorageForceReplay(t *testing.T) {
	// Create a memory log for testing
	log := persistence.NewMemoryLog()

	// Create storage with the log
	storage, err := NewMemoryStorage(log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Add some test data
	ctx := context.Background()
	storage.Put(ctx, []byte("key1"), []byte("value1"), 0)
	storage.Put(ctx, []byte("key2"), []byte("value2"), 0)

	// Verify initial state
	keys, err := storage.List(ctx, []byte(""), []byte(""), 0)
	if err != nil {
		t.Fatalf("Failed to list keys: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys initially, got %d", len(keys))
	}

	// Force replay
	err = storage.ForceReplayLog(ctx)
	if err != nil {
		t.Fatalf("Failed to force replay log: %v", err)
	}

	// Verify state is restored
	keys, err = storage.List(ctx, []byte(""), []byte(""), 0)
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
	fsLog, err := persistence.NewFilesystemLog(logDir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}

	// Step 2: Create storage with the filesystem log
	storage1, err := NewMemoryStorage(fsLog)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Step 3: Add some data
	ctx := context.Background()

	rev1, err := storage1.Put(ctx, []byte("app/config"), []byte("production"), 0)
	if err != nil {
		t.Fatalf("Failed to put app/config: %v", err)
	}
	if rev1 != 1 {
		t.Errorf("Expected revision 1, got %d", rev1)
	}

	rev2, err := storage1.Put(ctx, []byte("app/version"), []byte("1.0.0"), 0)
	if err != nil {
		t.Fatalf("Failed to put app/version: %v", err)
	}
	if rev2 != 2 {
		t.Errorf("Expected revision 2, got %d", rev2)
	}

	rev3, err := storage1.Put(ctx, []byte("app/config"), []byte("staging"), 0)
	if err != nil {
		t.Fatalf("Failed to update app/config: %v", err)
	}
	if rev3 != 3 {
		t.Errorf("Expected revision 3, got %d", rev3)
	}

	rev4, err := storage1.Delete(ctx, []byte("app/version"))
	if err != nil {
		t.Fatalf("Failed to delete app/version: %v", err)
	}
	if rev4 != 4 {
		t.Errorf("Expected revision 4, got %d", rev4)
	}

	// Step 4: Verify current state
	keys, err := storage1.List(ctx, []byte(""), []byte(""), 0)
	if err != nil {
		t.Fatalf("Failed to list keys: %v", err)
	}
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
	keys, err = storage2.List(ctx, []byte(""), []byte(""), 0)
	if err != nil {
		t.Fatalf("Failed to list keys in storage2: %v", err)
	}
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
	if storage1.GetCurrentRevision() != storage2.GetCurrentRevision() {
		t.Errorf("Revisions don't match: storage1=%d, storage2=%d",
			storage1.GetCurrentRevision(), storage2.GetCurrentRevision())
	}
	if storage1.GetCurrentRevision() != Revision(rev4) {
		t.Errorf("Expected revision %d, got %d", rev4, storage1.GetCurrentRevision())
	}

	// Step 8: Test that we can still write to the replayed storage
	rev5, err := storage2.Put(ctx, []byte("app/status"), []byte("running"), 0)
	if err != nil {
		t.Fatalf("Failed to put app/status: %v", err)
	}
	if rev5 != 5 {
		t.Errorf("Expected revision 5, got %d", rev5)
	}

	// Step 9: Verify the new data is visible in storage2 but not storage1
	keys, err = storage2.List(ctx, []byte(""), []byte(""), 0)
	if err != nil {
		t.Fatalf("Failed to list keys in storage2: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys in storage2, got %d", len(keys))
	}

	// storage1 should still only have the original key
	keys, err = storage1.List(ctx, []byte(""), []byte(""), 0)
	if err != nil {
		t.Fatalf("Failed to list keys in storage1: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("Expected 1 key in storage1, got %d", len(keys))
	}

	// Test that deleted key is still deleted after replay
	_, err = storage2.Get(ctx, []byte("app/version"), 0)
	if err == nil {
		t.Error("Expected app/version to be deleted after replay")
	}
}
