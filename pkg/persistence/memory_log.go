package persistence

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/klog/v2"
)

// MemoryLog is a memory-backed implementation of the Log interface
type MemoryLog struct {
	mu           sync.RWMutex
	records      map[Revision]*LogRecord
	lastRevision uint64 // Atomic counter for revision numbers

	listener LogListener
}

var _ Log = &MemoryLog{}

// NewMemoryLog creates a new memory-backed log
func NewMemoryLog() *MemoryLog {
	return &MemoryLog{
		records:      make(map[Revision]*LogRecord),
		lastRevision: 0,
	}
}

// Append adds a new record to the log and returns the revision number
func (m *MemoryLog) Append(ctx context.Context, conditionPosition Revision, logRecord *LogRecord) (Revision, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if conditionPosition != Revision(m.lastRevision) {
		return 0, false, nil
	}

	// if logRecord.Revision != Revision(m.lastRevision+1) {
	// 	return 0, false, fmt.Errorf("log record revision does not match current revision")
	// }

	// Increment revision number
	m.lastRevision++
	revision := Revision(m.lastRevision)

	m.records[revision] = logRecord

	if m.listener != nil {
		m.listener.OnLogEntry(revision)
	}

	return revision, true, nil
}

// GetCurrentRevision returns the current revision number
func (m *MemoryLog) GetCurrentRevision(ctx context.Context) (Revision, error) {
	return Revision(atomic.LoadUint64(&m.lastRevision)), nil
}

// Read reads records from the log starting from the given revision
func (m *MemoryLog) Read(ctx context.Context, fromRevision Revision, callback func(Revision, *LogRecord) bool) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for revision, record := range m.records {
		if revision >= fromRevision {
			if !callback(revision, record) {
				break
			}
		}
	}

	return nil
}

// Close closes the log and releases any resources
func (m *MemoryLog) Close() error {
	// For memory implementation, there's nothing to clean up
	return nil
}

// GetLogEntry returns the log entry for the given revision
func (m *MemoryLog) GetLogEntry(revision Revision) (*LogRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, ok := m.records[revision]
	if !ok {
		klog.Infof("m.records is +%v", m.records)
		klog.Fatalf("log entry not found for revision %d", revision)

		return nil, fmt.Errorf("log entry not found for revision %d", revision)
	}

	return record, nil
}

// SetListener sets the log listener
func (m *MemoryLog) SetListener(listener LogListener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listener = listener
}
