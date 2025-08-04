package persistence

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// MemoryLog is a memory-backed implementation of the Log interface
type MemoryLog struct {
	mu       sync.RWMutex
	records  []*LogRecord
	revision uint64 // Atomic counter for revision numbers
}

var _ Log = &MemoryLog{}

// NewMemoryLog creates a new memory-backed log
func NewMemoryLog() *MemoryLog {
	return &MemoryLog{
		records:  make([]*LogRecord, 0),
		revision: 0,
	}
}

// Append adds a new record to the log and returns the revision number
func (m *MemoryLog) Append(ctx context.Context, operation string, key []byte, value []byte, leaseID int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Atomically increment the revision number
	revision := atomic.AddInt64(&m.revision, 1)

	record := &LogRecord{
		Revision:  revision,
		Timestamp: time.Now(),
		Operation: operation,
		Key:       key,
		Value:     value,
		LeaseID:   leaseID,
	}

	m.records = append(m.records, record)

	return revision, nil
}

// GetCurrentRevision returns the current revision number
func (m *MemoryLog) GetCurrentRevision(ctx context.Context) (Revision, error) {
	return Revision(atomic.LoadUint64(&m.revision)), nil
}

// Read reads records from the log starting from the given revision
func (m *MemoryLog) Read(ctx context.Context, fromRevision Revision, limit int) ([]*LogRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 {
		limit = len(m.records) // Default to all records if limit is not positive
	}

	var result []*LogRecord
	for _, record := range m.records {
		if record.Revision >= fromRevision {
			result = append(result, record)
			if len(result) >= limit {
				break
			}
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no records found for fromRevision: %d", fromRevision)
	}

	return result, nil
}

// Close closes the log and releases any resources
func (m *MemoryLog) Close() error {
	// For memory implementation, there's nothing to clean up
	return nil
}

// GetLogEntry returns the log entry for the given revision
func (m *MemoryLog) GetLogEntry(revision Revision) *LogRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, record := range m.records {
		if record.Revision == revision {
			return record
		}
	}

	return nil
}

type memoryTransaction struct {
	timestamp Revision
	// log       *MemoryLog
	records []*LogRecord
}

var _ Transaction = &memoryTransaction{}

func (t *memoryTransaction) Timestamp() Revision {
	return t.timestamp
}

func (t *memoryTransaction) Put(ctx context.Context, newKV *mvccpb.KeyValue, leaseID int64) error {
	logRecord := &LogRecord{
		Revision:       t.timestamp,
		Operation:      mvccpb.PUT,
		Key:            newKV.Key,
		Value:          newKV.Value,
		LeaseID:        leaseID,
		CreateRevision: Revision(newKV.CreateRevision),
		Version:        newKV.Version,
		Timestamp:      time.Now(),
	}
	t.records = append(t.records, logRecord)
	return nil
}

func (t *memoryTransaction) Delete(ctx context.Context, oldKV *mvccpb.KeyValue) error {
	logRecord := &LogRecord{
		Revision:       t.timestamp,
		Operation:      mvccpb.PUT,
		Key:            oldKV.Key,
		Value:          oldKV.Value,
		LeaseID:        oldKV.Lease,
		CreateRevision: Revision(oldKV.CreateRevision),
		Version:        oldKV.Version,
		Timestamp:      time.Now(),
	}
	t.records = append(t.records, logRecord)
	return nil
}
