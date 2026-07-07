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

	flushFunc func(ctx context.Context, lastLogPosition Revision, batch *BatchCommit) error

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

func (b *TxnBatch) flush(ctx context.Context, lastLogPosition Revision) (int, error) {
	log := klog.FromContext(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.flushed {
		return 0, nil
	}

	if len(b.pendingBatch) == 0 {
		return 0, nil
	}

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
		for _, event := range txn.LogRecord.Events {
			if event.Type == mvccpb.PUT {
				if event.Kv.CreateRevision == event.Kv.ModRevision {
					event.Kv.CreateRevision = int64(revision)
				}
				event.Kv.ModRevision = int64(revision)
			}
		}

		commit.Transactions = append(commit.Transactions, txn)
		resultChannels = append(resultChannels, txn.resultChan)
	}

	if len(commit.Transactions) == 0 {
		return 0, nil
	}

	err := b.flushFunc(ctx, lastLogPosition, commit)
	b.flushed = true
	if err != nil {
		for _, resultChannel := range resultChannels {
			resultChannel <- BatchResult{
				Revision: 0,
				Success:  false,
				Error:    err,
			}
		}
		return 0, err
	}

	revision = lastLogPosition
	for i, resultChannel := range resultChannels {
		revision++
		// Record this transaction's writes at their committed revision so
		// that subsequent transactions (in later batches) validate their
		// reads and writes against them.
		for key := range commit.Transactions[i].Meta.WriteSet {
			b.committedWrites[key] = revision
		}
		resultChannel <- BatchResult{
			Revision: revision,
			Success:  true,
			Error:    nil,
		}
	}

	return len(commit.Transactions), nil
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
