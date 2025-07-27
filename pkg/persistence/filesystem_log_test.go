package persistence

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func TestFilesystemLog_BasicOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem_log_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new filesystem log
	log, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}
	defer log.Close()

	ctx := context.Background()

	// Test initial revision
	revision, err := log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if revision != 0 {
		t.Errorf("Expected initial revision to be 0, got %d", revision)
	}

	// Test appending a record
	key := []byte("test-key")
	value := []byte("test-value")
	revision, err = log.Append(ctx, "PUT", key, value, 0)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}
	if revision != 1 {
		t.Errorf("Expected revision 1, got %d", revision)
	}

	// Test reading the record
	records, err := log.Read(ctx, 1, 10)
	if err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("Expected 1 record, got %d", len(records))
	}

	record := records[0]
	if record.Revision != 1 {
		t.Errorf("Expected revision 1, got %d", record.Revision)
	}
	if record.Operation != "PUT" {
		t.Errorf("Expected operation PUT, got %s", record.Operation)
	}
	if string(record.Key) != "test-key" {
		t.Errorf("Expected key test-key, got %s", string(record.Key))
	}
	if string(record.Value) != "test-value" {
		t.Errorf("Expected value test-value, got %s", string(record.Value))
	}
}

func TestFilesystemLog_Restart(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem_log_restart_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()

	// Create first log instance and add some records
	log1, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create first filesystem log: %v", err)
	}

	// Add some records
	revision1, err := log1.Append(ctx, "PUT", []byte("key1"), []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Failed to append first record: %v", err)
	}

	revision2, err := log1.Append(ctx, "PUT", []byte("key2"), []byte("value2"), 0)
	if err != nil {
		t.Fatalf("Failed to append second record: %v", err)
	}

	log1.Close()

	// Create second log instance (simulating restart)
	log2, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create second filesystem log: %v", err)
	}
	defer log2.Close()

	// Check that the revision was properly restored
	currentRevision, err := log2.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRevision != revision2 {
		t.Errorf("Expected revision %d after restart, got %d", revision2, currentRevision)
	}

	// Read all records to verify they were preserved
	records, err := log2.Read(ctx, 1, 10)
	if err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("Expected 2 records after restart, got %d", len(records))
	}

	// Verify first record
	if records[0].Revision != revision1 {
		t.Errorf("Expected first record revision %d, got %d", revision1, records[0].Revision)
	}
	if string(records[0].Key) != "key1" {
		t.Errorf("Expected first record key key1, got %s", string(records[0].Key))
	}

	// Verify second record
	if records[1].Revision != revision2 {
		t.Errorf("Expected second record revision %d, got %d", revision2, records[1].Revision)
	}
	if string(records[1].Key) != "key2" {
		t.Errorf("Expected second record key key2, got %s", string(records[1].Key))
	}

	// Add a new record after restart
	revision3, err := log2.Append(ctx, "PUT", []byte("key3"), []byte("value3"), 0)
	if err != nil {
		t.Fatalf("Failed to append record after restart: %v", err)
	}
	if revision3 != revision2+1 {
		t.Errorf("Expected revision %d, got %d", revision2+1, revision3)
	}
}

func TestFilesystemLog_ReadWithLimit(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem_log_limit_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	log, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}
	defer log.Close()

	ctx := context.Background()

	// Add multiple records
	for i := 1; i <= 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		_, err := log.Append(ctx, "PUT", key, value, 0)
		if err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
	}

	// Test reading with limit
	records, err := log.Read(ctx, 1, 3)
	if err != nil {
		t.Fatalf("Failed to read records with limit: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// Verify the records are in order
	for i, record := range records {
		expectedRevision := int64(i + 1)
		if record.Revision != expectedRevision {
			t.Errorf("Expected revision %d at position %d, got %d", expectedRevision, i, record.Revision)
		}
	}
}

func TestFilesystemLog_ReadFromRevision(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem_log_from_revision_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	log, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}
	defer log.Close()

	ctx := context.Background()

	// Add multiple records
	for i := 1; i <= 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		_, err := log.Append(ctx, "PUT", key, value, 0)
		if err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
	}

	// Test reading from revision 3
	records, err := log.Read(ctx, 3, 10)
	if err != nil {
		t.Fatalf("Failed to read records from revision 3: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("Expected 3 records from revision 3, got %d", len(records))
	}

	// Verify the records start from revision 3
	for i, record := range records {
		expectedRevision := int64(i + 3)
		if record.Revision != expectedRevision {
			t.Errorf("Expected revision %d at position %d, got %d", expectedRevision, i, record.Revision)
		}
	}
}

func TestFilesystemLog_InvalidFromRevision(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem_log_invalid_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	log, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}
	defer log.Close()

	ctx := context.Background()

	// Test reading with invalid fromRevision
	_, err = log.Read(ctx, -1, 10)
	if err == nil {
		t.Error("Expected error for invalid fromRevision, got nil")
	}
}

func TestFilesystemLog_EmptyDirectory(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem_log_empty_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new filesystem log in empty directory
	log, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}
	defer log.Close()

	ctx := context.Background()

	// Test initial revision
	revision, err := log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if revision != 0 {
		t.Errorf("Expected initial revision to be 0, got %d", revision)
	}

	// Test reading from empty log
	records, err := log.Read(ctx, 1, 10)
	if err != nil {
		t.Fatalf("Failed to read from empty log: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("Expected 0 records from empty log, got %d", len(records))
	}
}

func TestFilesystemLog_ExampleUsage(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem_log_example_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new filesystem log
	fsLog, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}
	defer fsLog.Close()

	ctx := context.Background()

	// Add some sample records
	revision1, err := fsLog.Append(ctx, "PUT", []byte("user:1"), []byte("Alice"), 0)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}
	if revision1 != 1 {
		t.Errorf("Expected revision 1, got %d", revision1)
	}

	revision2, err := fsLog.Append(ctx, "PUT", []byte("user:2"), []byte("Bob"), 0)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}
	if revision2 != 2 {
		t.Errorf("Expected revision 2, got %d", revision2)
	}

	revision3, err := fsLog.Append(ctx, "DELETE", []byte("user:1"), nil, 0)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}
	if revision3 != 3 {
		t.Errorf("Expected revision 3, got %d", revision3)
	}

	// Read all records
	records, err := fsLog.Read(ctx, 1, 10)
	if err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// Verify first record
	if records[0].Revision != 1 || records[0].Operation != "PUT" || string(records[0].Key) != "user:1" || string(records[0].Value) != "Alice" {
		t.Errorf("First record mismatch: %+v", records[0])
	}

	// Verify second record
	if records[1].Revision != 2 || records[1].Operation != "PUT" || string(records[1].Key) != "user:2" || string(records[1].Value) != "Bob" {
		t.Errorf("Second record mismatch: %+v", records[1])
	}

	// Verify third record
	if records[2].Revision != 3 || records[2].Operation != "DELETE" || string(records[2].Key) != "user:1" {
		t.Errorf("Third record mismatch: %+v", records[2])
	}

	// Simulate a restart by creating a new log instance
	fsLog.Close()

	newLog, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create new log after restart: %v", err)
	}
	defer newLog.Close()

	// Check current revision after restart
	currentRevision, err := newLog.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRevision != 3 {
		t.Errorf("Expected revision 3 after restart, got %d", currentRevision)
	}

	// Add a new record after restart
	revision4, err := newLog.Append(ctx, "PUT", []byte("user:3"), []byte("Charlie"), 0)
	if err != nil {
		t.Fatalf("Failed to append record after restart: %v", err)
	}
	if revision4 != 4 {
		t.Errorf("Expected revision 4, got %d", revision4)
	}

	// Read all records again
	records, err = newLog.Read(ctx, 1, 10)
	if err != nil {
		t.Fatalf("Failed to read records after restart: %v", err)
	}
	if len(records) != 4 {
		t.Errorf("Expected 4 records after restart, got %d", len(records))
	}

	// Verify the new record
	if records[3].Revision != 4 || records[3].Operation != "PUT" || string(records[3].Key) != "user:3" || string(records[3].Value) != "Charlie" {
		t.Errorf("Fourth record mismatch: %+v", records[3])
	}
}
