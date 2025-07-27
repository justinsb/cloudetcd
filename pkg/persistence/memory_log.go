package persistence

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryLog is a memory-backed implementation of the Log interface
type MemoryLog struct {
	mu       sync.RWMutex
	records  []*LogRecord
	revision int64 // Atomic counter for revision numbers
}

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
func (m *MemoryLog) GetCurrentRevision(ctx context.Context) (int64, error) {
	return atomic.LoadInt64(&m.revision), nil
}

// Read reads records from the log starting from the given revision
func (m *MemoryLog) Read(ctx context.Context, fromRevision int64, limit int) ([]*LogRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if fromRevision < 0 {
		return nil, fmt.Errorf("invalid fromRevision: %d", fromRevision)
	}

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

	return result, nil
}

// Close closes the log and releases any resources
func (m *MemoryLog) Close() error {
	// For memory implementation, there's nothing to clean up
	return nil
}
