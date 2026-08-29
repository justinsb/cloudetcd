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

package batch

import (
	"context"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/persistence"
	"k8s.io/klog/v2"
)

type Revision = persistence.Revision
type LogRecord = persistence.LogRecord
type TxnMeta = persistence.TxnMeta

type TxnBatch struct {
	mu sync.RWMutex

	// // open is true if the batch is open for new transactions
	// // It is false when the batch is being flushed or has been flushed
	// open bool

	// flushed is true if the batch has been flushed
	flushed bool

	// committedWrites is shared across all batches (owned by Batching) and
	// records, for each key, the highest revision at which it has been
	// written. It is only accessed under the Batching flushLock, which
	// serializes all flushes. It is used for per-key conflict validation.
	committedWrites map[string]Revision

	pendingBatch []*PendingTxn
	// pendingBatch []*PendingTransaction
	// batchTimer   *time.Timer
	// batchTimeout time.Duration
}

func (b *TxnBatch) CanBatchWith(txnMeta *TxnMeta) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, txn := range b.pendingBatch {
		if !CanBatchTogether(txn.Meta, txnMeta) {
			return false
		}
	}
	return true
}

// CanBatchWith checks if this transaction can be batched with another
// Transactions are serializable if:
// 1. They don't write to the same keys (write-write conflict)
// 2. One doesn't read a key that the other writes (read-write conflict)
func CanBatchTogether(existing *TxnMeta, other *TxnMeta) bool {
	// Check write-write conflicts
	for key := range existing.WriteSet {
		if other.WriteSet[key] {
			return false
		}
	}

	// Check read-write conflicts
	for key := range other.ReadSet {
		if existing.WriteSet[key] {
			return false
		}
	}

	return true
}

// BatchCommit represents a group of transactions that can be committed together
type BatchCommit struct {
	// Transactions in this batch
	Transactions []*PendingTxn
}

// pendingTxn represents a transaction waiting to be batched
type PendingTxn struct {
	LogRecord  *LogRecord
	Meta       *TxnMeta
	resultChan chan BatchResult
}

// BatchResult contains the result of a batched transaction
type BatchResult struct {
	Revision Revision
	Success  bool
	Error    error
}

// addToBatch adds a pending transaction to the current batch
func (b *TxnBatch) add(ctx context.Context, logRecord *LogRecord, txnMeta *TxnMeta) chan BatchResult {
	b.mu.Lock()
	defer b.mu.Unlock()

	resultChan := make(chan BatchResult, 1)

	pendingTxn := &PendingTxn{
		LogRecord:  logRecord,
		Meta:       txnMeta,
		resultChan: resultChan,
	}

	b.pendingBatch = append(b.pendingBatch, pendingTxn)

	return resultChan

	// // Set or reset the batch timer
	// if b.batchTimer != nil {
	// 	b.batchTimer.Stop()
	// }
	// b.batchTimer = time.AfterFunc(b.batchTimeout, func() {
	// 	b.mu.Lock()
	// 	defer b.mu.Unlock()
	// 	if len(b.pendingBatch) > 0 {
	// 		b.executeBatch()
	// 	}
	// })
}

// prepare validates the batch's transactions against the writes committed
// since their snapshots, assigns each surviving transaction its revision
// (starting at lastLogPosition+1) and records its writes in committedWrites,
// and rejects the rest. It returns the commit to hand to the backend and the
// result channels of its transactions, in revision order; a nil commit means
// nothing survived. The batch's records are then written by the backend and
// acknowledged with deliver, possibly concurrently with later batches.
func (b *TxnBatch) prepare(ctx context.Context, lastLogPosition Revision) (*BatchCommit, []chan BatchResult) {
	log := klog.FromContext(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.flushed || len(b.pendingBatch) == 0 {
		return nil, nil
	}
	b.flushed = true

	commit := &BatchCommit{}
	resultChannels := make([]chan BatchResult, 0, len(b.pendingBatch))
	revision := lastLogPosition
	for _, txn := range b.pendingBatch {
		if !validateTxn(txn.Meta, lastLogPosition, b.committedWrites) {
			log.Info("Skipping transaction due to conflict", "snapshotRevision", txn.Meta.SnapshotRevision, "lastLogPosition", lastLogPosition, "hasRangeRead", txn.Meta.HasRangeRead)
			txn.resultChan <- BatchResult{
				Revision: 0,
				Success:  false,
				Error:    nil,
			}
			continue
		}

		revision++
		// Transactions stamp their events with snapshot+1; with several
		// transactions per batch (or concurrent batches) the committed
		// revision differs, so re-stamp every event here.
		for _, event := range txn.LogRecord.Events {
			switch event.Type {
			case mvccpb.PUT:
				if event.Kv.CreateRevision == event.Kv.ModRevision {
					event.Kv.CreateRevision = int64(revision)
				}
				event.Kv.ModRevision = int64(revision)
			case mvccpb.DELETE:
				event.Kv.ModRevision = int64(revision)
			}
		}
		// Record the writes at their assigned revision now, so that later
		// batches validate against them. If this batch then fails to
		// commit, every later batch fails too (see Batching), so nothing
		// validates against a write that did not happen.
		for key := range txn.Meta.WriteSet {
			b.committedWrites[key] = revision
		}

		commit.Transactions = append(commit.Transactions, txn)
		resultChannels = append(resultChannels, txn.resultChan)
	}

	if len(commit.Transactions) == 0 {
		return nil, nil
	}
	return commit, resultChannels
}

// failAll rejects every transaction in the batch with err.
func (b *TxnBatch) failAll(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.flushed {
		return
	}
	b.flushed = true
	for _, txn := range b.pendingBatch {
		txn.resultChan <- BatchResult{Error: err}
	}
}

// validateTxn reports whether a transaction can still be committed given the
// keys that have been written since its snapshot (committedWrites).
//
// This is textbook optimistic-concurrency backward-validation, done per key:
//   - a point key we read must not have been written by a concurrently
//     committed transaction since we read it; and
//   - a key we write must not have been written by a concurrently committed
//     transaction since our snapshot (write-write conflict).
//
// Disjoint transactions therefore never abort each other, unlike the previous
// coarse "did the global revision move" check.
//
// A transaction that performed a range read cannot be validated per-key (it is
// exposed to phantoms), so it falls back to the conservative whole-snapshot
// check: it commits only if nothing at all has committed since its snapshot.
func validateTxn(meta *TxnMeta, lastLogPosition Revision, committedWrites map[string]Revision) bool {
	if meta.HasRangeRead {
		return meta.SnapshotRevision == lastLogPosition
	}

	// Read validation: every point key we read must not have changed since
	// the version we observed for it.
	for key, readRevision := range meta.ReadSet {
		if committedWrites[key] > readRevision {
			return false
		}
	}

	// Write validation: a key we write must not have been written by another
	// transaction that committed after our snapshot.
	for key := range meta.WriteSet {
		if committedWrites[key] > meta.SnapshotRevision {
			return false
		}
	}

	return true
}
