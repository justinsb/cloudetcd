package persistence

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

type Revision uint64

// LogRecord represents a single entry in the log
type LogRecord struct {
	// Events are the events that occurred as a result of this record
	Events []*mvccpb.Event
	// Revision       Revision               // The revision number for this record
	// CreateRevision Revision               // The revision number for the creation of this record
	// Version        int64                  // The version number for this record
	// Timestamp      time.Time              // When this record was created
	// Operation      mvccpb.Event_EventType // The type of operation (PUT, DELETE, etc.)
	// Key            []byte                 // The key being operated on
	// Value          []byte                 // The value (for PUT operations)
	// LeaseID        int64                  // Associated lease ID (if any)
}

// Log is the interface for the persistence log
type Log interface {
	// Append adds a new record to the log and returns the revision number
	// The bool indicates whether the append was successful, which is false if the condition position does not match the current revision.
	Append(ctx context.Context, conditionPosition Revision, logRecord *LogRecord) (Revision, bool, error)

	// GetCurrentRevision returns the current revision number
	GetCurrentRevision(ctx context.Context) (Revision, error)

	// Read reads records from the log starting from the given revision
	Read(ctx context.Context, fromRevision Revision, callback func(Revision, *LogRecord) bool) error

	// Close closes the log and releases any resources
	Close() error

	// GetLogEntry returns the log entry for the given key and revision
	GetLogEntry(revision Revision) *LogRecord

	// SetListener sets the log listener
	SetListener(listener LogListener)
}

// LogListener is the interface for the persistence log
type LogListener interface {
	// OnLogEntry is called when a new log entry is added
	OnLogEntry(revision Revision)
}
