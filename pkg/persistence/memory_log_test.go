package persistence

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestMemoryLog_Append(t *testing.T) {
	log := NewMemoryLog()
	ctx := context.Background()

	// Add the dummy record
	revision0, ok, err := log.Append(ctx, 0, &LogRecord{})
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	if revision0 != 1 {
		t.Errorf("Expected revision 1, got %d", revision0)
	}

	// Test first append
	revision1, ok, err := log.Append(ctx, 1, &LogRecord{
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
		t.Fatalf("Failed to append: %v", err)
	}
	if revision1 != 2 {
		t.Errorf("Expected revision 2, got %d", revision1)
	}

	// Test second append
	revision2, ok, err := log.Append(ctx, 2, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.DELETE,
				Kv: &mvccpb.KeyValue{
					Key: []byte("key1"),
				},
			},
		},
	})
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

func TestMemoryLog_Read(t *testing.T) {
	log := NewMemoryLog()
	ctx := context.Background()

	// Add the dummy record
	rev, ok, err := log.Append(ctx, 0, &LogRecord{})
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
	rev, ok, err = log.Append(ctx, 1, &LogRecord{
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
		t.Fatalf("Failed to append: %v", err)
	}
	_, ok, err = log.Append(ctx, 2, &LogRecord{
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
		t.Fatalf("Failed to append: %v", err)
	}
	_, ok, err = log.Append(ctx, 3, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.DELETE,
				Kv: &mvccpb.KeyValue{
					Key: []byte("key1"),
				},
			},
		},
	})

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

func TestMemoryLog_ReadWithLimit(t *testing.T) {
	log := NewMemoryLog()
	ctx := context.Background()

	// Add the dummy record
	revision0, ok, err := log.Append(ctx, 0, &LogRecord{})
	if err != nil || !ok {
		t.Fatalf("Failed to append: %v", err)
	}
	if revision0 != 1 {
		t.Errorf("Expected revision 1, got %d", revision0)
	}

	// Add some records
	_, ok, err = log.Append(ctx, 1, &LogRecord{
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
		t.Fatalf("Failed to append: %v", err)
	}
	_, ok, err = log.Append(ctx, 2, &LogRecord{
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
		t.Fatalf("Failed to append: %v", err)
	}
	_, ok, err = log.Append(ctx, 3, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.DELETE,
				Kv: &mvccpb.KeyValue{
					Key: []byte("key1"),
				},
			},
		},
	})

	// Read with limit 2
	records := make(map[Revision]*LogRecord)
	if err := log.Read(ctx, 2, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		if len(records) >= 2 {
			return false
		}
		return true
	}); err != nil {
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

func TestMemoryLog_ConcurrentAppend(t *testing.T) {
	log := NewMemoryLog()
	ctx := context.Background()

	// Add the dummy record
	revision0, ok, err := log.Append(ctx, 0, &LogRecord{})
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
					_, ok, err := log.Append(ctx, revision, &LogRecord{
						Events: []*mvccpb.Event{
							{
								Type: mvccpb.PUT,
								Kv: &mvccpb.KeyValue{
									Key:   []byte("key"),
									Value: []byte("value"),
								},
							},
						},
					})
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
