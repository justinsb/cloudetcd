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

func (b *TxnBatch) isFlushed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.flushed
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
		if txn.Meta.SnapshotRevision != lastLogPosition {
			log.Info("Skipping transaction with unexpected snapshot revision", "snapshotTimestamp", txn.Meta.SnapshotRevision, "lastLogPosition", lastLogPosition)
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
	for _, resultChannel := range resultChannels {
		revision++
		resultChannel <- BatchResult{
			Revision: revision,
			Success:  true,
			Error:    nil,
		}
	}

	return len(commit.Transactions), nil
}
