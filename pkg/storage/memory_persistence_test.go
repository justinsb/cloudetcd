package storage

import (
	"context"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/persistence"
)

func TestMemoryStorage_WithPersistence(t *testing.T) {
	ctx := context.Background()

	// Create a memory log
	memoryLog := persistence.NewMemoryLog()

	// Create store with the log
	store, err := NewMemoryStorage(memoryLog)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	t.Log("=== Testing Cloud etcd with Persistence Integration ===")

	// Put some values
	t.Log("1. Putting key-value pairs...")
	rev1, err := store.Put(ctx, []byte("key1"), []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	if rev1 != 1 {
		t.Errorf("Expected revision 1, got %d", rev1)
	}
	t.Logf("   Put key1=value1 at revision %d", rev1)

	rev2, err := store.Put(ctx, []byte("key2"), []byte("value2"), 0)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	if rev2 != 2 {
		t.Errorf("Expected revision 2, got %d", rev2)
	}
	t.Logf("   Put key2=value2 at revision %d", rev2)

	rev3, err := store.Put(ctx, []byte("key1"), []byte("updated-value1"), 0)
	if err != nil {
		t.Fatalf("Failed to update key1: %v", err)
	}
	if rev3 != 3 {
		t.Errorf("Expected revision 3, got %d", rev3)
	}
	t.Logf("   Updated key1=updated-value1 at revision %d", rev3)

	// Delete a key
	t.Log("2. Deleting a key...")
	rev4, err := store.Delete(ctx, []byte("key2"))
	if err != nil {
		t.Fatalf("Failed to delete key2: %v", err)
	}
	if rev4 != 4 {
		t.Errorf("Expected revision 4, got %d", rev4)
	}
	t.Logf("   Deleted key2 at revision %d", rev4)

	// Read from the log
	t.Log("3. Reading from persistence log...")
	records, err := memoryLog.Read(ctx, 1, 10)
	if err != nil {
		t.Fatalf("Failed to read from log: %v", err)
	}

	if len(records) != 4 {
		t.Errorf("Expected 4 log records, got %d", len(records))
	}

	t.Logf("   Found %d log records:", len(records))
	for _, record := range records {
		t.Logf("   - Revision %d: %s key=%s value=%s",
			record.Revision,
			record.Operation,
			string(record.Key),
			string(record.Value))
	}

	// Verify log records
	expectedRecords := []struct {
		revision  Revision
		operation mvccpb.Event_EventType
		key       string
		value     string
	}{
		{1, mvccpb.PUT, "key1", "value1"},
		{2, mvccpb.PUT, "key2", "value2"},
		{3, mvccpb.PUT, "key1", "updated-value1"},
		{4, mvccpb.DELETE, "key2", ""},
	}

	for i, expected := range expectedRecords {
		if i >= len(records) {
			t.Errorf("Expected record at index %d, but only have %d records", i, len(records))
			continue
		}
		record := records[i]
		if record.Revision != expected.revision {
			t.Errorf("Record %d: expected revision %d, got %d", i, expected.revision, record.Revision)
		}
		if record.Operation != expected.operation {
			t.Errorf("Record %d: expected operation %s, got %s", i, expected.operation, record.Operation)
		}
		if string(record.Key) != expected.key {
			t.Errorf("Record %d: expected key %s, got %s", i, expected.key, string(record.Key))
		}
		if string(record.Value) != expected.value {
			t.Errorf("Record %d: expected value %s, got %s", i, expected.value, string(record.Value))
		}
	}

	// Get current revision
	currentRev, err := memoryLog.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRev != 4 {
		t.Errorf("Expected current revision 4, got %d", currentRev)
	}
	t.Logf("4. Current revision: %d", currentRev)

	// Retrieve values from storage
	t.Log("5. Retrieving values from storage...")
	kv1, err := store.Get(ctx, []byte("key1"), 0)
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if string(kv1.Value) != "updated-value1" {
		t.Errorf("Expected key1=updated-value1, got %s", string(kv1.Value))
	}
	if kv1.CreateRevision != 1 {
		t.Errorf("Expected key1 create revision 1, got %d", kv1.CreateRevision)
	}
	t.Logf("   key1 = %s (revision %d)", string(kv1.Value), kv1.CreateRevision)

	// Try to get deleted key
	_, err = store.Get(ctx, []byte("key2"), 0)
	if err == nil {
		t.Error("Expected key2 to be not found, but got a value")
	} else {
		t.Logf("   key2 = not found (as expected, it was deleted)")
	}

	t.Log("=== Integration test completed successfully! ===")
}
