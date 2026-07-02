// Copyright 2026 Justin Santa Barbara
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	// The txnMeta parameter enables batch commits by allowing serializable transactions to be grouped together.
	Append(ctx context.Context, logRecord *LogRecord, txnMeta *TxnMeta) (Revision, bool, error)

	// GetCurrentRevision returns the current revision number
	GetCurrentRevision(ctx context.Context) (Revision, error)

	// Read reads records from the log starting from the given revision
	Read(ctx context.Context, fromRevision Revision, callback func(Revision, *LogRecord) bool) error

	// Close closes the log and releases any resources
	Close() error

	// GetLogEntry returns the log entry for the given key and revision
	GetLogEntry(revision Revision) (*LogRecord, error)

	// SetListener sets the log listener
	SetListener(listener LogListener)
}

// LogListener is the interface for the persistence log
type LogListener interface {
	// OnLogEntry is called when a new log entry is added
	OnLogEntry(revision Revision)
}

// BatchAppender is an optional interface for Log implementations that can
// append a contiguous range of records in a single storage operation.
// It is used when copying records between logs (e.g. tiering or replication):
// revision numbers are preserved exactly, and the whole range lands in one
// object/file rather than one per record.
type BatchAppender interface {
	// AppendBatch appends records so that the first record is assigned
	// revision lastRevision+1. The records must already carry their final
	// revisions in their events; no revision stamping is performed.
	// It returns false (with no error) if the log's current revision is not
	// lastRevision, analogous to the conditional-append semantics of Append.
	AppendBatch(ctx context.Context, lastRevision Revision, records []*LogRecord) (bool, error)
}

// Truncater is an optional interface for Log implementations that can discard
// old records, e.g. after they have been copied to an archive tier.
type Truncater interface {
	// Truncate discards records with revisions <= throughRevision.
	// Implementations may retain more than requested (for example, whole-file
	// granularity, or keeping the newest records so that the current revision
	// can be recovered after a restart).
	Truncate(ctx context.Context, throughRevision Revision) error
}
