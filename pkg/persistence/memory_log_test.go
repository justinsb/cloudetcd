package persistence

import (
	"context"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestMemoryLog_Append(t *testing.T) {
	log := NewMemoryLog()
	ctx := context.Background()

	// Test first append
	revision, err := log.Append(ctx, "PUT", []byte("key1"), []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Failed to append: %v", err)
	}
	if logRecord1.Revision != 1 {
		t.Errorf("Expected revision 1, got %d", logRecord1.Revision)
	}

	// Test second append
	revision, err = log.Append(ctx, "DELETE", []byte("key1"), nil, 0)
	if err != nil {
		t.Fatalf("Failed to append: %v", err)
	}
	if logRecord2.Revision != 2 {
		t.Errorf("Expected revision 2, got %d", logRecord2.Revision)
	}

	// Test current revision
	currentRev, err := log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRev != 2 {
		t.Errorf("Expected current revision 2, got %d", currentRev)
	}
}

func TestMemoryLog_Read(t *testing.T) {
	log := NewMemoryLog()
	ctx := context.Background()

	// Add some records
	log.Append(ctx, "PUT", []byte("key1"), []byte("value1"), 0)
	log.Append(ctx, "PUT", []byte("key2"), []byte("value2"), 0)
	log.Append(ctx, "DELETE", []byte("key1"), nil, 0)

	// Read all records from revision 1
	records, err := log.Read(ctx, 1, 10)
	if err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// Check first record
	if records[0].Revision != 1 {
		t.Errorf("Expected revision 1, got %d", records[0].Revision)
	}
	if records[0].Operation != mvccpb.PUT {
		t.Errorf("Expected operation PUT, got %s", records[0].Operation)
	}
	if string(records[0].Key) != "key1" {
		t.Errorf("Expected key key1, got %s", string(records[0].Key))
	}

	// Check second record
	if records[1].Revision != 2 {
		t.Errorf("Expected revision 2, got %d", records[1].Revision)
	}
	if records[1].Operation != mvccpb.PUT {
		t.Errorf("Expected operation PUT, got %s", records[1].Operation)
	}

	// Check third record
	if records[2].Revision != 3 {
		t.Errorf("Expected revision 3, got %d", records[2].Revision)
	}
	if records[2].Operation != mvccpb.DELETE {
		t.Errorf("Expected operation DELETE, got %s", records[2].Operation)
	}
}

func TestMemoryLog_ReadWithLimit(t *testing.T) {
	log := NewMemoryLog()
	ctx := context.Background()

	// Add some records
	log.Append(ctx, "PUT", []byte("key1"), []byte("value1"), 0)
	log.Append(ctx, "PUT", []byte("key2"), []byte("value2"), 0)
	log.Append(ctx, "DELETE", []byte("key1"), nil, 0)

	// Read with limit 2
	records, err := log.Read(ctx, 1, 2)
	if err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}

	if len(records) != 2 {
		t.Errorf("Expected 2 records, got %d", len(records))
	}
}

func TestMemoryLog_ReadFromInvalidRevision(t *testing.T) {
	log := NewMemoryLog()
	ctx := context.Background()

	// Try to read from invalid revision
	records, err := log.Read(ctx, 1234567, 10)
	if err == nil {
		t.Error("Expected error for invalid revision, got nil")
	}
	if len(records) != 0 {
		t.Errorf("Expected 0 records, got %d", len(records))
	}
}

func TestMemoryLog_ConcurrentAppend(t *testing.T) {
	log := NewMemoryLog()
	ctx := context.Background()

	// Test concurrent appends
	const numGoroutines = 10
	const appendsPerGoroutine = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < appendsPerGoroutine; j++ {
				_, err := log.Append(ctx, "PUT", []byte("key"), []byte("value"), 0)
				if err != nil {
					t.Errorf("Failed to append: %v", err)
				}
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Check final revision
	expectedRevision := Revision(numGoroutines * appendsPerGoroutine)
	currentRev, err := log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRev != expectedRevision {
		t.Errorf("Expected revision %d, got %d", expectedRevision, currentRev)
	}
}
