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

package memorylog

import (
	"context"
	"fmt"
	"sync"
	"time"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/batch"
	"k8s.io/klog/v2"
)

type Revision = persistence.Revision
type LogRecord = persistence.LogRecord
type LogListener = persistence.LogListener
type TxnBatch = batch.TxnBatch

// MemoryLog is a memory-backed implementation of the Log interface
type MemoryLog struct {
	batching *batch.Batching

	mu sync.RWMutex

	records      []*LogRecord
	lastRevision Revision // Atomic counter for revision numbers

	listener LogListener

	// commitLatency is an artificial delay applied to each batch commit.
	commitLatency time.Duration
}

var _ persistence.Log = &MemoryLog{}
var _ persistence.BatchAppender = &MemoryLog{}

// Option configures a MemoryLog.
type Option func(*MemoryLog)

// WithCommitLatency makes every batch commit take at least d. It emulates the
// round-trip of an object-store backend (a GCS conditional write is on the
// order of tens of milliseconds) without needing one, so that batching
// behaviour under realistic commit latency can be benchmarked offline.
func WithCommitLatency(d time.Duration) Option {
	return func(l *MemoryLog) { l.commitLatency = d }
}

// New creates a new memory-backed log
func New(opts ...Option) *MemoryLog {
	log := &MemoryLog{
		lastRevision: 0,
	}
	for _, opt := range opts {
		opt(log)
	}

	// No replay is possible here

	log.batching = batch.NewBatching(log.lastRevision, log.commitBatch)
	return log
}

// Append adds a new record to the log and returns the revision number
func (l *MemoryLog) Append(ctx context.Context, logRecord *LogRecord, txnMeta *persistence.TxnMeta) (Revision, bool, error) {
	return l.batching.Add(ctx, logRecord, txnMeta)
}

// AppendBatch appends a contiguous range of records starting at lastRevision+1, preserving revisions.
func (l *MemoryLog) AppendBatch(ctx context.Context, lastRevision Revision, records []*LogRecord) (bool, error) {
	return l.batching.AddBatch(ctx, lastRevision, records)
}

// commitBatch "writes" a batch (there is nothing to write; the optional
// commit latency stands in for a backend round-trip) and returns the commit
// that publishes it.
func (l *MemoryLog) commitBatch(ctx context.Context, lastLogPosition Revision, b *batch.BatchCommit) (batch.Commit, error) {
	if len(b.Transactions) == 0 {
		return nil, fmt.Errorf("batch contains no transactions")
	}

	if l.commitLatency > 0 {
		select {
		case <-time.After(l.commitLatency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &memoryCommit{log: l, lastLogPosition: lastLogPosition, batch: b}, nil
}

type memoryCommit struct {
	log             *MemoryLog
	lastLogPosition Revision
	batch           *batch.BatchCommit
}

func (c *memoryCommit) Publish() {
	l := c.log
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lastRevision != c.lastLogPosition {
		// Batching publishes in order; this cannot happen.
		panic(fmt.Sprintf("batch published out of order: expected to publish after %d, log is at %d", c.lastLogPosition, l.lastRevision))
	}
	startRevision := l.lastRevision + 1
	for _, txn := range c.batch.Transactions {
		l.lastRevision++
		l.records = append(l.records, txn.LogRecord)
	}
	if l.listener != nil {
		l.listener.OnLogEntry(l.lastRevision)
	}
	klog.V(2).Infof("Executed batch of %d transactions, revisions %d-%d",
		len(c.batch.Transactions), startRevision, l.lastRevision)
}

// GetCurrentRevision returns the current revision number
func (m *MemoryLog) GetCurrentRevision(ctx context.Context) (Revision, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.lastRevision, nil
}

// Read reads records from the log starting from the given revision
func (m *MemoryLog) Read(ctx context.Context, fromRevision Revision, callback func(Revision, *LogRecord) bool) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for i, record := range m.records {
		revision := Revision(i + 1)
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
	if err := m.batching.Close(); err != nil {
		return err
	}

	return nil
}

// GetLogEntry returns the log entry for the given revision
func (m *MemoryLog) GetLogEntry(revision Revision) (*LogRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if revision <= 0 || int(revision) > len(m.records) {
		klog.V(2).Infof("log entry not found for revision %d, records length is %d", revision, len(m.records))
		return nil, fmt.Errorf("log entry not found for revision %d", revision)
	}

	record := m.records[revision-1]

	return record, nil
}

// SetListener sets the log listener
func (m *MemoryLog) SetListener(listener LogListener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listener = listener
}
