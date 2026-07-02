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
	"time"

	"k8s.io/klog/v2"
)

type FlushFunc func(ctx context.Context, lastLogPosition Revision, batch *BatchCommit) error

type Batching struct {
	flushFunc FlushFunc

	batchLock     sync.Mutex
	batchLockCond *sync.Cond
	openBatch     *TxnBatch
	stop          bool
	flushQueue    []*TxnBatch

	flushLock       sync.Mutex
	lastLogPosition Revision
	// committedWrites records, for each key, the highest revision at which it
	// has been written since batching started. It is only accessed under
	// flushLock (held for the duration of flushBatch), so per-key conflict
	// validation and its updates are serialized with the log commit.
	committedWrites map[string]Revision
}

func NewBatching(lastLogPosition Revision, flushFunc FlushFunc) *Batching {
	b := &Batching{
		flushFunc:       flushFunc,
		lastLogPosition: lastLogPosition,
		committedWrites: make(map[string]Revision),
	}
	b.batchLockCond = sync.NewCond(&b.batchLock)

	go b.doBackgroundFlush()
	return b
}

func newTxnBatch(flushFunc FlushFunc, committedWrites map[string]Revision) *TxnBatch {
	return &TxnBatch{
		flushFunc:       flushFunc,
		committedWrites: committedWrites,
	}
}

func (b *Batching) Add(ctx context.Context, logRecord *LogRecord, txnMeta *TxnMeta) (Revision, bool, error) {
	b.batchLock.Lock()

	shouldNotify := false

	var resultChan chan BatchResult
	if b.openBatch != nil {
		if b.openBatch.CanBatchWith(txnMeta) {
			resultChan = b.openBatch.add(ctx, logRecord, txnMeta)
		} else {
			b.flushQueue = append(b.flushQueue, b.openBatch)
			b.openBatch = nil
			shouldNotify = true
		}
	}

	if resultChan == nil {
		batch := newTxnBatch(b.flushFunc, b.committedWrites)
		b.openBatch = batch
		shouldNotify = true

		resultChan = batch.add(ctx, logRecord, txnMeta)
	}

	if shouldNotify {
		b.batchLockCond.Broadcast()
	}
	b.batchLock.Unlock()

	select {
	case result := <-resultChan:
		return result.Revision, result.Success, result.Error
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
}

func (b *Batching) doBackgroundFlush() {
	for {
		b.batchLock.Lock()
		// This loop waits for work.                                                                                                                                                                                                                                                                                                                                                                                       │

		for !b.stop && len(b.flushQueue) == 0 && b.openBatch == nil {
			b.batchLockCond.Wait()
		}

		// If we woke up because of a new openBatch, wait a moment for more transactions to join.                                                                                                                                                                                                                                                                                                                          │
		if len(b.flushQueue) == 0 && b.openBatch != nil {
			// A small delay to allow more transactions to be batched
			b.batchLock.Unlock()
			time.Sleep(10 * time.Millisecond) // Batching window
			b.batchLock.Lock()
		}

		var flush *TxnBatch
		if len(b.flushQueue) > 0 {
			flush = b.flushQueue[0]
			b.flushQueue = b.flushQueue[1:]
		} else if b.openBatch != nil {
			// Grab the open batch
			flush = b.openBatch
			b.openBatch = nil
		}
		stop := b.stop
		b.batchLock.Unlock()

		if flush != nil {
			b.flushBatch(flush)
		} else if stop {
			return
		}
	}
}

func (b *Batching) flushBatch(batch *TxnBatch) {
	ctx := context.Background()
	log := klog.FromContext(ctx)

	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if n, err := batch.flush(ctx, b.lastLogPosition); err != nil {
		log.Error(err, "failed to flush batch")
	} else {
		b.lastLogPosition += Revision(n)
	}
}

func (b *Batching) Close() error {
	// ctx := context.Background()
	// log := klog.FromContext(ctx)

	b.batchLock.Lock()
	b.stop = true
	b.batchLockCond.Broadcast()
	b.batchLock.Unlock()

	return nil
}
