package persistence

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// MemoryLog is a memory-backed implementation of the Log interface
type MemoryLog struct {
	mu           sync.RWMutex
	records      []*LogRecord
	lastRevision uint64 // Atomic counter for revision numbers
}

var _ Log = &MemoryLog{}

// NewMemoryLog creates a new memory-backed log
func NewMemoryLog() *MemoryLog {
	return &MemoryLog{
		records:      make([]*LogRecord, 0),
		lastRevision: 1,
	}
}

// Append adds a new record to the log and returns the revision number
func (m *MemoryLog) Append(ctx context.Context, conditionPosition Revision, logRecord *LogRecord) (*LogRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if conditionPosition != Revision(m.lastRevision) {
		return nil, false, nil
	}

	if logRecord.Revision != Revision(m.lastRevision+1) {
		return nil, false, fmt.Errorf("log record revision does not match current revision")
	}

	// Increment revision number
	m.lastRevision++

	m.records = append(m.records, logRecord)

	return logRecord, true, nil
}

// GetCurrentRevision returns the current revision number
func (m *MemoryLog) GetCurrentRevision(ctx context.Context) (Revision, error) {
	return Revision(atomic.LoadUint64(&m.lastRevision)), nil
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
