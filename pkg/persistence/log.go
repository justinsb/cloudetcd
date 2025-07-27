package persistence

import (
	"context"
	"time"
)

// LogRecord represents a single entry in the log
type LogRecord struct {
	Revision  int64     // The revision number for this record
	Timestamp time.Time // When this record was created
	Operation string    // The type of operation (PUT, DELETE, etc.)
	Key       []byte    // The key being operated on
	Value     []byte    // The value (for PUT operations)
	LeaseID   int64     // Associated lease ID (if any)
}

// Log is the interface for the persistence log
type Log interface {
	// Append adds a new record to the log and returns the revision number
	Append(ctx context.Context, operation string, key []byte, value []byte, leaseID int64) (int64, error)

	// GetCurrentRevision returns the current revision number
	GetCurrentRevision(ctx context.Context) (int64, error)

	// Read reads records from the log starting from the given revision
	Read(ctx context.Context, fromRevision int64, limit int) ([]*LogRecord, error)

	// Close closes the log and releases any resources
	Close() error
}
