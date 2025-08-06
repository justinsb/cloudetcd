package persistence

import (
	"context"
	"fmt"
	"os"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
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
	revision1, ok, err := log.Append(ctx, revision, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   key,
					Value: value,
				},
			},
		},
	})
	if err != nil || !ok {
		t.Fatalf("Failed to append record: %v", err)
	}
	if revision1 != 1 {
		t.Errorf("Expected revision 1, got %d", revision)
	}

	// Test reading the record
	records := make(map[Revision]*LogRecord)
	if err := log.Read(ctx, 1, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("Expected 1 record, got %d", len(records))
	}

	record := records[1]
	if record == nil {
		t.Errorf("Expected revision 1, got %v", record)
	}
	if record.Events[0].Type != mvccpb.PUT {
		t.Errorf("Expected operation PUT, got %s", record.Events[0].Type)
	}
	if string(record.Events[0].Kv.Key) != "test-key" {
		t.Errorf("Expected key test-key, got %s", string(record.Events[0].Kv.Key))
	}
	if string(record.Events[0].Kv.Value) != "test-value" {
		t.Errorf("Expected value test-value, got %s", string(record.Events[0].Kv.Value))
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
	revision1, ok, err := log1.Append(ctx, 0, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
			},
		},
	})
	if err != nil || !ok {
		t.Fatalf("Failed to append first record: %v", err)
	}

	revision2, ok, err := log1.Append(ctx, 1, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
			},
		},
	})
	if err != nil || !ok {
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
	records := make(map[Revision]*LogRecord)
	if err := log2.Read(ctx, 1, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("Expected 2 records after restart, got %d", len(records))
	}

	// Verify first record
	if records[revision1] == nil {
		t.Errorf("Expected first record revision %d, got %v", revision1, records[revision1])
	}
	if string(records[revision1].Events[0].Kv.Key) != "key1" {
		t.Errorf("Expected first record key key1, got %s", string(records[revision1].Events[0].Kv.Key))
	}

	// Verify second record
	if records[revision2] == nil {
		t.Errorf("Expected second record revision %d, got %v", revision2, records[revision2])
	}
	if string(records[revision2].Events[0].Kv.Key) != "key2" {
		t.Errorf("Expected second record key key2, got %s", string(records[revision2].Events[0].Kv.Key))
	}

	// Add a new record after restart
	revision3, ok, err := log2.Append(ctx, 2, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("key3"),
					Value: []byte("value3"),
				},
			},
		},
	})
	if err != nil || !ok {
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
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		_, ok, err := log.Append(ctx, Revision(i), &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   key,
						Value: value,
					},
				},
			},
		})
		if err != nil || !ok {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
	}

	// Test reading with limit
	records := make(map[Revision]*LogRecord)
	var revisions []Revision
	if err := log.Read(ctx, 1, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		revisions = append(revisions, revision)
		if len(records) >= 3 {
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("Failed to read records with limit: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// Verify the records are in order
	for i, revision := range revisions {
		expectedRevision := Revision(i + 1)
		if revision != expectedRevision {
			t.Errorf("Expected revision %d at position %d, got %d", expectedRevision, i, revision)
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
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		_, ok, err := log.Append(ctx, Revision(i), &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   key,
						Value: value,
					},
				},
			},
		})
		if err != nil || !ok {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
	}

	// Test reading from revision 3
	records := make(map[Revision]*LogRecord)
	var revisions []Revision
	if err := log.Read(ctx, 3, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		revisions = append(revisions, revision)
		return true
	}); err != nil {
		t.Fatalf("Failed to read records from revision 3: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("Expected 3 records from revision 3, got %d", len(records))
	}

	// Verify the records start from revision 3
	for i, revision := range revisions {
		expectedRevision := Revision(i + 3)
		if revision != expectedRevision {
			t.Errorf("Expected revision %d at position %d, got %d", expectedRevision, i, revision)
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
	records := make(map[Revision]*LogRecord)
	if err := log.Read(ctx, 1234567, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Errorf("Expected no error from invalid fromRevision, got %v", err)
	}
	if len(records) != 0 {
		t.Errorf("Expected 0 records, got %d", len(records))
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
	records := make(map[Revision]*LogRecord)
	if err := log.Read(ctx, 1, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
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
	logRevision1, ok, err := fsLog.Append(ctx, 0, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("user:1"),
					Value: []byte("Alice"),
				},
			},
		},
	})
	if err != nil || !ok {
		t.Fatalf("Failed to append record: %v", err)
	}
	if logRevision1 != 1 {
		t.Errorf("Expected revision 1, got %d", logRevision1)
	}

	logRevision2, ok, err := fsLog.Append(ctx, 1, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("user:2"),
					Value: []byte("Bob"),
				},
			},
		},
	})
	if err != nil || !ok {
		t.Fatalf("Failed to append record: %v", err)
	}
	if logRevision2 != 2 {
		t.Errorf("Expected revision 2, got %d", logRevision2)
	}

	logRevision3, ok, err := fsLog.Append(ctx, 2, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.DELETE,
				Kv: &mvccpb.KeyValue{
					Key: []byte("user:1"),
				},
			},
		},
	})
	if err != nil || !ok {
		t.Fatalf("Failed to append record: %v", err)
	}
	if logRevision3 != 3 {
		t.Errorf("Expected revision 3, got %d", logRevision3)
	}

	// Read all records
	records := make(map[Revision]*LogRecord)
	if err := fsLog.Read(ctx, 1, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// Verify first record
	if records[1] == nil || records[1].Events[0].Type != mvccpb.PUT || string(records[1].Events[0].Kv.Key) != "user:1" || string(records[1].Events[0].Kv.Value) != "Alice" {
		t.Errorf("First record mismatch: %+v", records[1])
	}

	// Verify second record
	if records[2] == nil || records[2].Events[0].Type != mvccpb.PUT || string(records[2].Events[0].Kv.Key) != "user:2" || string(records[2].Events[0].Kv.Value) != "Bob" {
		t.Errorf("Second record mismatch: %+v", records[2])
	}

	// Verify third record
	if records[3] == nil || records[3].Events[0].Type != mvccpb.DELETE || string(records[3].Events[0].Kv.Key) != "user:1" {
		t.Errorf("Third record mismatch: %+v", records[3])
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
	logRevision4, ok, err := newLog.Append(ctx, 3, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("user:3"),
					Value: []byte("Charlie"),
				},
			},
		},
	})
	if err != nil || !ok {
		t.Fatalf("Failed to append record after restart: %v", err)
	}
	if logRevision4 != 4 {
		t.Errorf("Expected revision 4, got %d", logRevision4)
	}

	// Read all records again
	records = make(map[Revision]*LogRecord)
	if err := newLog.Read(ctx, 1, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Fatalf("Failed to read records after restart: %v", err)
	}
	if len(records) != 4 {
		t.Errorf("Expected 4 records after restart, got %d", len(records))
	}

	// Verify the new record
	if records[4] == nil || records[4].Events[0].Type != mvccpb.PUT || string(records[4].Events[0].Kv.Key) != "user:3" || string(records[4].Events[0].Kv.Value) != "Charlie" {
		t.Errorf("Fourth record mismatch: %+v", records[4])
	}
}
