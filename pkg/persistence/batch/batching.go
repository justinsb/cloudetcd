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
	"fmt"
	"sync"
	"time"
)

// FlushFunc commits a batch whose first record has revision lastLogPosition+1:
// durably writes it and makes it visible. Batching calls it for one batch at
// a time, in order.
//
// A batch has been assigned its position by the time FlushFunc is called,
// and transactions behind it are being assigned positions after it: there
// is no way to not write it. If the write fails, Batching stops the process
// (a person has to look at the backend), and on restart the log is re-read.
type FlushFunc func(ctx context.Context, lastLogPosition Revision, batch *BatchCommit) error

// Options tunes Batching.
type Options struct {
	// Window is how long an open batch collects transactions before it is
	// flushed. Default 10ms.
	Window time.Duration
}

const defaultWindow = 10 * time.Millisecond

// Batching groups transactions into batches (group commit).
//
// Transactions are collected into an open batch for up to Window; each batch
// is validated per key against the writes committed since its transactions'
// snapshots, assigned contiguous revisions, committed with one FlushFunc
// call, and then acknowledged. So the backend's commit (an fsync, say) is
// paid once per batch rather than once per transaction.
type Batching struct {
	flushFunc FlushFunc
	window    time.Duration

	batchLock     sync.Mutex
	batchLockCond *sync.Cond
	openBatch     *TxnBatch
	stop          bool
	flushQueue    []*TxnBatch
	flusherDone   chan struct{}

	// lastLogPosition is the last committed revision; committedWrites
	// records, for each key, the highest revision at which it has been
	// written since batching started, for per-key conflict validation. Both
	// are only touched by the flusher goroutine.
	lastLogPosition Revision
	committedWrites map[string]Revision
}

// NewBatching creates a Batching with default Options.
func NewBatching(lastLogPosition Revision, flushFunc FlushFunc) *Batching {
	return NewBatchingWithOptions(lastLogPosition, flushFunc, Options{})
}

// NewBatchingWithOptions creates a Batching.
func NewBatchingWithOptions(lastLogPosition Revision, flushFunc FlushFunc, opts Options) *Batching {
	if opts.Window <= 0 {
		opts.Window = defaultWindow
	}
	b := &Batching{
		flushFunc:       flushFunc,
		window:          opts.Window,
		lastLogPosition: lastLogPosition,
		committedWrites: make(map[string]Revision),
		flusherDone:     make(chan struct{}),
	}
	b.batchLockCond = sync.NewCond(&b.batchLock)

	go b.doBackgroundFlush()
	return b
}

func newTxnBatch(committedWrites map[string]Revision) *TxnBatch {
	return &TxnBatch{committedWrites: committedWrites}
}

// Add queues a transaction and blocks until its batch is committed. It
// returns the transaction's revision and true on success, or false (without
// error) if per-key validation rejected it. If ctx ends first the
// transaction is still committed; only the wait is abandoned.
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
		batch := newTxnBatch(b.committedWrites)
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
		return result.Revision, result.Success, nil
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
}

func (b *Batching) doBackgroundFlush() {
	defer close(b.flusherDone)
	for {
		b.batchLock.Lock()
		// This loop waits for work.
		for !b.stop && len(b.flushQueue) == 0 && b.openBatch == nil {
			b.batchLockCond.Wait()
		}

		// If we woke up because of a new openBatch, wait a moment for more transactions to join.
		if len(b.flushQueue) == 0 && b.openBatch != nil {
			b.batchLock.Unlock()
			time.Sleep(b.window)
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

// flushBatch validates the batch, assigns its revisions, commits it and
// delivers the results.
func (b *Batching) flushBatch(batch *TxnBatch) {
	ctx := context.Background()
	commit, accepted, rejected := batch.prepare(ctx, b.lastLogPosition)
	start := b.lastLogPosition
	if commit != nil {
		if err := b.flushFunc(ctx, start, commit); err != nil {
			// See FlushFunc: the batch's position is taken and transactions
			// behind it depend on it; there is nothing to do but stop.
			panic(fmt.Sprintf("failed to commit log batch (revisions %d-%d): %v", start+1, start+Revision(len(commit.Transactions)), err))
		}
		b.lastLogPosition += Revision(len(commit.Transactions))
	}
	for i, resultChan := range accepted {
		resultChan <- BatchResult{Revision: start + Revision(i) + 1, Success: true}
	}
	// Delivered after the commit, so a rejected transaction's retry sees the
	// write that beat it.
	for _, resultChan := range rejected {
		resultChan <- BatchResult{Success: false}
	}
}

// Close stops the batching after flushing what is queued.
func (b *Batching) Close() error {
	b.batchLock.Lock()
	b.stop = true
	b.batchLockCond.Broadcast()
	b.batchLock.Unlock()
	<-b.flusherDone
	return nil
}
