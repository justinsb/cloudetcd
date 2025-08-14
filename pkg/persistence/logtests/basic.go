package logtests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/persistence"
)

type LogRecord = persistence.LogRecord
type Revision = persistence.Revision

func NewTxnMeta(snapshotRevision Revision) *persistence.TxnMeta {
	return persistence.NewTxnMeta(snapshotRevision)
}

func RunAll(t *testing.T, logFactory func(t *testing.T) persistence.Log) {
	t.Run("Append", func(t *testing.T) {
		log := logFactory(t)
		TestLog_Append(t, log)
	})
	t.Run("Read", func(t *testing.T) {
		log := logFactory(t)
		TestLog_Read(t, log)
	})
	t.Run("ReadWithLimit", func(t *testing.T) {
		log := logFactory(t)
		TestLog_ReadWithLimit(t, log)
	})
	t.Run("ReadFromInvalidRevision", func(t *testing.T) {
		log := logFactory(t)
		TestLog_ReadFromInvalidRevision(t, log)
	})
	t.Run("ConcurrentAppend", func(t *testing.T) {
		log := logFactory(t)
		TestLog_ConcurrentAppend(t, log)
	})
	t.Run("EmptyDirectory", func(t *testing.T) {
		log := logFactory(t)
		TestLog_EmptyDirectory(t, log)
	})
	t.Run("BasicOperations", func(t *testing.T) {
		log := logFactory(t)
		TestLog_BasicOperations(t, log)
	})
	t.Run("BatchCommit", func(t *testing.T) {
		log := logFactory(t)
		TestLog_BatchCommit(t, log)
	})
	t.Run("BatchCommit_ReadWriteConflicts", func(t *testing.T) {
		log := logFactory(t)
		TestLog_BatchCommit_ReadWriteConflicts(t, log)
	})
	t.Run("ReadFromRevision", func(t *testing.T) {
		log := logFactory(t)
		TestLog_ReadFromRevision(t, log)
	})
}

func TestLog_Append(t *testing.T, log persistence.Log) {
	ctx := context.Background()

	// Add the dummy record
	revision0, ok, err := log.Append(ctx, &LogRecord{}, NewTxnMeta(0))
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	if revision0 != 1 {
		t.Errorf("Expected revision 1, got %d", revision0)
	}

	// Test first append
	revision1, ok, err := log.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
			},
		},
	}, NewTxnMeta(1))
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	if revision1 != 2 {
		t.Errorf("Expected revision 2, got %d", revision1)
	}

	// Test second append
	revision2, ok, err := log.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.DELETE,
				Kv: &mvccpb.KeyValue{
					Key: []byte("key1"),
				},
			},
		},
	}, NewTxnMeta(2))
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	if revision2 != 3 {
		t.Errorf("Expected revision 3, got %d", revision2)
	}

	// Test current revision
	currentRev, err := log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRev != 3 {
		t.Errorf("Expected current revision 3, got %d", currentRev)
	}
}

func TestLog_Read(t *testing.T, log persistence.Log) {
	ctx := context.Background()

	// Add the dummy record
	rev, ok, err := log.Append(ctx, &LogRecord{}, NewTxnMeta(0))
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	if rev != 1 {
		t.Errorf("Expected revision 1, got %d", rev)
	}

	// Add some records
	rev, ok, err = log.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
			},
		},
	}, NewTxnMeta(1))
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	_, ok, err = log.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
			},
		},
	}, NewTxnMeta(2))
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	_, ok, err = log.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.DELETE,
				Kv: &mvccpb.KeyValue{
					Key: []byte("key1"),
				},
			},
		},
	}, NewTxnMeta(3))

	// Read all records from revision 1
	records := make(map[Revision]*LogRecord)
	if err := log.Read(ctx, 2, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// Check first record
	if records[2].Events[0].Type != mvccpb.PUT {
		t.Errorf("Expected operation PUT, got %s", records[2].Events[0].Type)
	}
	if string(records[2].Events[0].Kv.Key) != "key1" {
		t.Errorf("Expected key key1, got %s", string(records[2].Events[0].Kv.Key))
	}

	// Check second record
	if records[3].Events[0].Type != mvccpb.PUT {
		t.Errorf("Expected operation PUT, got %s", records[3].Events[0].Type)
	}

	// Check third record
	if records[4].Events[0].Type != mvccpb.DELETE {
		t.Errorf("Expected operation DELETE, got %s", records[4].Events[0].Type)
	}
}

func TestLog_ReadWithLimit(t *testing.T, log persistence.Log) {

	ctx := context.Background()

	{ // Add the dummy record
		revision0, ok, err := log.Append(ctx, &LogRecord{}, NewTxnMeta(0))
		if err != nil || !ok {
			t.Fatalf("Failed to append: %v", err)
		}
		if revision0 != 1 {
			t.Errorf("Expected revision 1, got %d", revision0)
		}
	}
	// Add multiple records
	for i := 1; i <= 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		revision, ok, err := log.Append(ctx, &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   key,
						Value: value,
					},
				},
			},
		}, NewTxnMeta(Revision(i)))
		if err != nil || !ok {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}

		if revision != Revision(i+1) {
			t.Errorf("Expected revision %d, got %d", Revision(i), revision)
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

func TestLog_ReadFromInvalidRevision(t *testing.T, log persistence.Log) {
	ctx := context.Background()

	// Try to read from invalid revision
	records := make(map[Revision]*LogRecord)
	if err := log.Read(ctx, 1234567, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Errorf("Expected no error for invalid revision, got error %v", err)
	}
	if len(records) != 0 {
		t.Errorf("Expected 0 records, got %d", len(records))
	}
}

func TestLog_ConcurrentAppend(t *testing.T, log persistence.Log) {
	ctx := context.Background()

	// Add the dummy record
	revision0, ok, err := log.Append(ctx, &LogRecord{}, NewTxnMeta(0))
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	if revision0 != 1 {
		t.Errorf("Expected revision 1, got %d", revision0)
	}

	// Test concurrent appends
	const numGoroutines = 10
	const appendsPerGoroutine = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < appendsPerGoroutine; j++ {
				done := false
				for attempt := 0; attempt < 10; attempt++ {
					revision, err := log.GetCurrentRevision(ctx)
					if err != nil {
						t.Errorf("Failed to get current revision: %v", err)
					}
					_, ok, err := log.Append(ctx, &LogRecord{
						Events: []*mvccpb.Event{
							{
								Type: mvccpb.PUT,
								Kv: &mvccpb.KeyValue{
									Key:   []byte("key"),
									Value: []byte("value"),
								},
							},
						},
					}, NewTxnMeta(revision))
					if err != nil {
						t.Errorf("Failed to append: %v", err)
					}
					if ok {
						done = true
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if !done {
					t.Errorf("Failed to append after 10 attempts")
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
	expectedRevision := Revision(1 + numGoroutines*appendsPerGoroutine)
	currentRev, err := log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRev != expectedRevision {
		t.Errorf("Expected revision %d, got %d", expectedRevision, currentRev)
	}
}

func TestLog_ReadFromRevision(t *testing.T, log persistence.Log) {

	ctx := context.Background()

	// Add multiple records
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		_, ok, err := log.Append(ctx, &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   key,
						Value: value,
					},
				},
			},
		}, NewTxnMeta(Revision(i)))
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

func TestLog_EmptyDirectory(t *testing.T, log persistence.Log) {

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

func TestLog_BasicOperations(t *testing.T, log persistence.Log) {
	ctx := context.Background()

	// Test initial state
	revision, err := log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if revision != 0 {
		t.Errorf("Expected revision 0, got %d", revision)
	}

	// Add a dummy record
	_, ok, err := log.Append(ctx, &LogRecord{}, NewTxnMeta(0))
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}

	// Test appending a record
	record := &persistence.LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
			},
		},
	}

	newRevision, success, err := log.Append(ctx, record, NewTxnMeta(1))
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}
	if !success {
		t.Errorf("Expected success, got false")
	}
	if newRevision != 2 {
		t.Errorf("Expected revision 2, got %d", newRevision)
	}

	// Test getting current revision
	currentRevision, err := log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRevision != 2 {
		t.Errorf("Expected revision 2, got %d", currentRevision)
	}

	// Test getting log entry
	retrievedRecord, err := log.GetLogEntry(2)
	if err != nil {
		t.Fatalf("Failed to get log entry: %v", err)
	}
	if retrievedRecord == nil {
		t.Fatalf("Expected log entry to be non-nil")
	}
	if len(record.Events) != len(retrievedRecord.Events) {
		t.Errorf("Expected %d events, got %d", len(record.Events), len(retrievedRecord.Events))
	}
	if record.Events[0].Type != retrievedRecord.Events[0].Type {
		t.Errorf("Expected type %d, got %d", record.Events[0].Type, retrievedRecord.Events[0].Type)
	}
	if string(record.Events[0].Kv.Key) != string(retrievedRecord.Events[0].Kv.Key) {
		t.Errorf("Expected key %s, got %s", string(record.Events[0].Kv.Key), string(retrievedRecord.Events[0].Kv.Key))
	}
	if string(record.Events[0].Kv.Value) != string(retrievedRecord.Events[0].Kv.Value) {
		t.Errorf("Expected value %s, got %s", string(record.Events[0].Kv.Value), string(retrievedRecord.Events[0].Kv.Value))
	}

	// Test conditional append with wrong condition
	_, success, err = log.Append(ctx, record, NewTxnMeta(1)) // Wrong condition position
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}
	if success {
		t.Errorf("Expected failure, got success")
	}

	// Test conditional append with correct condition
	newRevision2, success, err := log.Append(ctx, record, NewTxnMeta(2))
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}
	if !success {
		t.Errorf("Expected success, got false")
	}
	if newRevision2 != 3 {
		t.Errorf("Expected revision 3, got %d", newRevision2)
	}

	// Test reading from log
	var readRecords []Revision
	err = log.Read(ctx, 1, func(revision Revision, record *persistence.LogRecord) bool {
		readRecords = append(readRecords, revision)
		return true
	})
	if err != nil {
		t.Fatalf("Failed to read log: %v", err)
	}
	if len(readRecords) != 3 {
		t.Errorf("Expected 2 records, got %d", len(readRecords))
	}
	if diff := cmp.Diff(readRecords, []Revision{1, 2, 3}); diff != "" {
		t.Errorf("Unexpected diff: %s", diff)
	}

	// Test reading with callback that returns false
	readRecords = nil
	err = log.Read(ctx, 1, func(revision Revision, record *persistence.LogRecord) bool {
		readRecords = append(readRecords, revision)
		return false // Stop after first record
	})
	if err != nil {
		t.Fatalf("Failed to read log: %v", err)
	}
	if len(readRecords) != 1 {
		t.Errorf("Expected 1 record, got %d", len(readRecords))
	}
	if readRecords[0] != 1 {
		t.Errorf("Expected revision 1, got %d", readRecords[0])
	}
}

func TestLog_BatchCommit(t *testing.T, log persistence.Log) {
	ctx := context.Background()

	// Initialize with dummy record
	_, ok, err := log.Append(ctx, &LogRecord{}, NewTxnMeta(0))
	if err != nil || !ok {
		t.Fatalf("Failed to initialize log: %v", err)
	}

	t.Run("compatible transactions batch together", func(t *testing.T) {
		// Create two compatible transactions
		txn1Meta := NewTxnMeta(1)
		txn1Meta.AddWrite("user:1")

		txn1Record := &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   []byte("user:1"),
						Value: []byte("Alice"),
					},
				},
			},
		}

		txn2Meta := NewTxnMeta(1)
		txn2Meta.AddWrite("user:2")

		txn2Record := &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   []byte("user:2"),
						Value: []byte("Bob"),
					},
				},
			},
		}

		// Execute both transactions concurrently
		var wg sync.WaitGroup
		var rev1, rev2 Revision
		var err1, err2 error
		var ok1, ok2 bool

		wg.Add(2)
		go func() {
			defer wg.Done()
			rev1, ok1, err1 = log.Append(ctx, txn1Record, txn1Meta)
		}()
		go func() {
			defer wg.Done()
			rev2, ok2, err2 = log.Append(ctx, txn2Record, txn2Meta)
		}()

		wg.Wait()

		// Both should succeed
		if err1 != nil || !ok1 {
			t.Errorf("Transaction 1 failed: ok=%v, err=%v", ok1, err1)
		}
		if err2 != nil || !ok2 {
			t.Errorf("Transaction 2 failed: ok=%v, err=%v", ok2, err2)
		}

		// Should get consecutive revisions (batched)
		if rev1 == 0 || rev2 == 0 {
			t.Errorf("Expected non-zero revisions, got rev1=%d, rev2=%d", rev1, rev2)
		}

		// Verify both records were written
		record1, err := log.GetLogEntry(rev1)
		if err != nil {
			t.Fatalf("Failed to get log entry for revision %d: %v", rev1, err)
		}
		if len(record1.Events) != 1 || string(record1.Events[0].Kv.Key) != "user:1" {
			t.Errorf("Unexpected record1: %+v", record1)
		}

		record2, err := log.GetLogEntry(rev2)
		if err != nil {
			t.Fatalf("Failed to get log entry for revision %d: %v", rev2, err)
		}
		if len(record2.Events) != 1 || string(record2.Events[0].Kv.Key) != "user:2" {
			t.Errorf("Unexpected record2: %+v", record2)
		}
	})

	t.Run("conflicting transactions execute separately", func(t *testing.T) {
		currentRev, err := log.GetCurrentRevision(ctx)
		if err != nil {
			t.Fatalf("Failed to get current revision: %v", err)
		}

		// Create two conflicting transactions (same key)
		txn1Meta := NewTxnMeta(currentRev)
		txn1Meta.AddWrite("user:3")

		txn1Record := &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   []byte("user:3"),
						Value: []byte("Charlie"),
					},
				},
			},
		}

		txn2Meta := NewTxnMeta(currentRev)
		txn2Meta.AddWrite("user:3") // Same key - conflict!

		txn2Record := &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   []byte("user:3"),
						Value: []byte("Dave"),
					},
				},
			},
		}

		// Execute both transactions concurrently
		var wg sync.WaitGroup
		var rev1, rev2 Revision
		var err1, err2 error
		var ok1, ok2 bool

		wg.Add(2)
		go func() {
			defer wg.Done()
			rev1, ok1, err1 = log.Append(ctx, txn1Record, txn1Meta)
		}()
		go func() {
			defer wg.Done()
			// Note: This will fail because both try to append at the same revision
			// but they conflict, so they can't be batched and will execute separately
			rev2, ok2, err2 = log.Append(ctx, txn2Record, txn2Meta)
		}()

		wg.Wait()

		// One should succeed, one should fail due to condition mismatch
		successCount := 0
		if err1 == nil && ok1 {
			successCount++
		}
		if err2 == nil && ok2 {
			successCount++
		}

		if successCount != 1 {
			t.Errorf("Expected exactly 1 success, got %d (rev1=%d,ok1=%v,err1=%v) (rev2=%d,ok2=%v,err2=%v)",
				successCount, rev1, ok1, err1, rev2, ok2, err2)
		}
	})

	// t.Run("batch timeout triggers execution", func(t *testing.T) {
	// 	// Create a log with a very short timeout for testing
	// 	shortTimeoutLog := &MemoryLog{
	// 		records:      make(map[Revision]*LogRecord),
	// 		lastRevision: 0,
	// 		batchTimeout: 1 * time.Millisecond, // Very short timeout
	// 	}

	// 	// Initialize with dummy record
	// 	_, ok, err := shortTimeoutLog.Append(ctx, 0, &LogRecord{}, nil)
	// 	if err != nil || !ok {
	// 		t.Fatalf("Failed to initialize short timeout log: %v", err)
	// 	}

	// 	txnMeta := NewTxnMeta()
	// 	txnMeta.AddWrite("timeout:test")

	// 	record := &LogRecord{
	// 		Events: []*mvccpb.Event{
	// 			{
	// 				Type: mvccpb.PUT,
	// 				Kv: &mvccpb.KeyValue{
	// 					Key:   []byte("timeout:test"),
	// 					Value: []byte("test-value"),
	// 				},
	// 			},
	// 		},
	// 	}

	// 	start := time.Now()
	// 	rev, ok, err := shortTimeoutLog.Append(ctx, 1, record, txnMeta)
	// 	duration := time.Since(start)

	// 	if err != nil || !ok {
	// 		t.Errorf("Timeout batch append failed: ok=%v, err=%v", ok, err)
	// 	}
	// 	if rev != 2 {
	// 		t.Errorf("Expected revision 2, got %d", rev)
	// 	}

	// 	// Should complete quickly due to timeout
	// 	if duration > 100*time.Millisecond {
	// 		t.Errorf("Batch took too long: %v (expected < 100ms)", duration)
	// 	}
	// })
}

func TestLog_BatchCommit_ReadWriteConflicts(t *testing.T, log persistence.Log) {
	ctx := context.Background()

	// Initialize with dummy record and some initial data
	log.Append(ctx, &LogRecord{}, NewTxnMeta(0))
	log.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("existing:key"),
					Value: []byte("initial-value"),
				},
			},
		},
	}, NewTxnMeta(1))

	t.Run("read-write conflict prevents batching", func(t *testing.T) {
		// Transaction 1: Read existing key, write to audit
		txn1Meta := NewTxnMeta(2)
		txn1Meta.AddRead("existing:key")
		txn1Meta.AddWrite("audit:1")

		txn1Record := &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   []byte("audit:1"),
						Value: []byte("read existing:key"),
					},
				},
			},
		}

		// Transaction 2: Update the existing key
		txn2Meta := NewTxnMeta(2)
		txn2Meta.AddWrite("existing:key") // Conflicts with txn1's read

		txn2Record := &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   []byte("existing:key"),
						Value: []byte("updated-value"),
					},
				},
			},
		}

		// // Verify they cannot be batched
		// canBatch := txn1Meta.CanBatchWith(txn2Meta)
		// if canBatch {
		// 	t.Error("Expected transactions to have read-write conflict, but CanBatchWith returned true")
		// }

		// Execute both transactions concurrently
		var wg sync.WaitGroup
		var rev1, rev2 Revision
		var err1, err2 error
		var ok1, ok2 bool

		wg.Add(2)
		go func() {
			defer wg.Done()
			rev1, ok1, err1 = log.Append(ctx, txn1Record, txn1Meta)
		}()
		go func() {
			defer wg.Done()
			rev2, ok2, err2 = log.Append(ctx, txn2Record, txn2Meta)
		}()

		wg.Wait()

		// One should succeed, one should fail
		successCount := 0
		if err1 == nil && ok1 {
			successCount++
			t.Logf("Transaction 1 succeeded with revision %d", rev1)
		}
		if err2 == nil && ok2 {
			successCount++
			t.Logf("Transaction 2 succeeded with revision %d", rev2)
		}

		if successCount != 1 {
			t.Errorf("Expected exactly 1 success due to conflict, got %d", successCount)
		}
	})
}
