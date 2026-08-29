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

// FlushFunc writes a batch whose first record has revision lastLogPosition+1
// to the backend, durably but without making it visible, and returns the
// Commit that publishes it. Batching may call it for consecutive batches
// concurrently; it publishes the Commits strictly in batch order.
//
// A batch has been assigned its position by the time FlushFunc is called,
// and transactions behind it are being assigned positions after it: there
// is no way to not write it. If the write fails, Batching stops the process
// (a person has to look at the backend), and on restart the log is re-read.
type FlushFunc func(ctx context.Context, lastLogPosition Revision, batch *BatchCommit) (Commit, error)

// Commit is a written but not yet visible batch.
type Commit interface {
	// Publish makes the batch visible: the log's current revision advances
	// past it, readers can fetch its records, and the listener is notified.
	// Batching calls Publish for consecutive batches in order.
	Publish()
}

// Options tunes Batching.
type Options struct {
	// Window is how long an open batch collects transactions before it is
	// flushed. Default 10ms.
	Window time.Duration
	// MaxInFlight is how many batches may be written to the backend
	// concurrently. Default 8.
	MaxInFlight int
}

const (
	defaultWindow      = 10 * time.Millisecond
	defaultMaxInFlight = 8
)

// Batching groups transactions into batches and pipelines their commits.
//
// Transactions are collected into an open batch for up to Window; each batch
// is validated and assigned revisions in order (so revisions are contiguous
// and per-key conflict validation is serialized), then handed to the backend
// to write without waiting for the previous batch's write to finish. Written
// batches are published in order, and transactions are acknowledged once
// their batch and every batch before it is published. So the backend's
// commit round-trip is paid once per batch rather than once per transaction,
// and several batches can be in flight at once.
type Batching struct {
	flushFunc   FlushFunc
	window      time.Duration
	maxInFlight int

	batchLock     sync.Mutex
	batchLockCond *sync.Cond
	openBatch     *TxnBatch
	stop          bool
	flushQueue    []*TxnBatch
	flusherDone   chan struct{}

	// flushLock serializes revision assignment (prepare) and guards the
	// fields below.
	flushLock sync.Mutex
	flushCond *sync.Cond
	// lastLogPosition is the last revision assigned to a batch; it runs
	// ahead of what the backend has published while batches are in flight.
	lastLogPosition Revision
	// committedWrites records, for each key, the highest revision at which it
	// has been written since batching started, for per-key conflict
	// validation.
	committedWrites map[string]Revision
	// inflight holds the batches handed to the backend and not yet
	// acknowledged, in batch order; the acknowledger takes them from the
	// front, so publication is in order by construction.
	inflight []*flushJob
	closed   bool
	ackDone  chan struct{}
}

// flushJob is one batch handed to the backend (commit may be nil if every
// transaction was rejected; the job then only delivers the rejections in
// order).
type flushJob struct {
	start    Revision
	commit   *BatchCommit
	accepted []chan BatchResult
	rejected []chan BatchResult
	// written receives the backend's Commit when its write finishes.
	written chan Commit
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
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = defaultMaxInFlight
	}
	b := &Batching{
		flushFunc:       flushFunc,
		window:          opts.Window,
		maxInFlight:     opts.MaxInFlight,
		lastLogPosition: lastLogPosition,
		committedWrites: make(map[string]Revision),
		flusherDone:     make(chan struct{}),
		ackDone:         make(chan struct{}),
	}
	b.batchLockCond = sync.NewCond(&b.batchLock)
	b.flushCond = sync.NewCond(&b.flushLock)

	go b.doBackgroundFlush()
	go b.doAcknowledge()
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

// AddBatch appends a contiguous batch of records whose first record lands at
// lastLogPosition+1, bypassing the batching window and conflict checks.
// The records must already carry their final revisions in their events; unlike
// Add, no revision stamping is performed. This is intended for bulk copies
// between logs (e.g. tiering or replication) where revision numbers must be
// preserved exactly.
// It returns false (with no error) if the log has moved past lastLogPosition.
func (b *Batching) AddBatch(ctx context.Context, lastLogPosition Revision, records []*LogRecord) (bool, error) {
	if len(records) == 0 {
		return true, nil
	}

	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	// Wait for the pipeline to drain so the position is settled.
	for len(b.inflight) > 0 {
		b.flushCond.Wait()
	}
	if b.lastLogPosition != lastLogPosition {
		return false, nil
	}

	commit := &BatchCommit{
		Transactions: make([]*PendingTxn, len(records)),
	}
	for i, record := range records {
		commit.Transactions[i] = &PendingTxn{LogRecord: record}
	}

	written, err := b.flushFunc(ctx, lastLogPosition, commit)
	if err != nil {
		return false, err
	}
	written.Publish()
	b.lastLogPosition += Revision(len(records))
	return true, nil
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

// flushBatch validates the batch, assigns its revisions, and starts the
// backend write. It returns as soon as the write is started; doAcknowledge
// publishes and delivers the results once the write, and every write before
// it, is done.
func (b *Batching) flushBatch(batch *TxnBatch) {
	b.flushLock.Lock()
	// While MaxInFlight batches are in flight, stop starting new writes
	// until one completes; the open batch keeps growing meanwhile.
	for len(b.inflight) >= b.maxInFlight {
		b.flushCond.Wait()
	}
	commit, accepted, rejected := batch.prepare(context.Background(), b.lastLogPosition)
	if commit == nil && len(rejected) == 0 {
		b.flushLock.Unlock()
		return
	}
	job := &flushJob{
		start:    b.lastLogPosition,
		commit:   commit,
		accepted: accepted,
		rejected: rejected,
		written:  make(chan Commit, 1),
	}
	if commit != nil {
		b.lastLogPosition += Revision(len(commit.Transactions))
	}
	b.inflight = append(b.inflight, job)
	b.flushCond.Broadcast()
	b.flushLock.Unlock()

	if commit == nil {
		return
	}
	go func() {
		written, err := b.flushFunc(context.Background(), job.start, job.commit)
		if err != nil {
			// See FlushFunc: the batch's position is taken and transactions
			// behind it depend on it; there is nothing to do but stop.
			panic(fmt.Sprintf("failed to commit log batch (revisions %d-%d): %v", job.start+1, job.start+Revision(len(job.commit.Transactions)), err))
		}
		job.written <- written
	}()
}

// doAcknowledge publishes written batches and delivers their results, in
// batch order.
func (b *Batching) doAcknowledge() {
	defer close(b.ackDone)
	for {
		b.flushLock.Lock()
		for len(b.inflight) == 0 && !b.closed {
			b.flushCond.Wait()
		}
		if len(b.inflight) == 0 {
			b.flushLock.Unlock()
			return
		}
		job := b.inflight[0]
		b.flushLock.Unlock()

		if job.commit != nil {
			written := <-job.written
			written.Publish()
		}
		for i, resultChan := range job.accepted {
			resultChan <- BatchResult{Revision: job.start + Revision(i) + 1, Success: true}
		}
		// Now that this batch (and every batch before it) is published, a
		// rejected transaction's retry will see the write that beat it.
		for _, resultChan := range job.rejected {
			resultChan <- BatchResult{Success: false}
		}

		b.flushLock.Lock()
		b.inflight = b.inflight[1:]
		b.flushCond.Broadcast()
		b.flushLock.Unlock()
	}
}

// Close stops the batching after flushing what is queued.
func (b *Batching) Close() error {
	b.batchLock.Lock()
	b.stop = true
	b.batchLockCond.Broadcast()
	b.batchLock.Unlock()
	<-b.flusherDone

	b.flushLock.Lock()
	b.closed = true
	b.flushCond.Broadcast()
	b.flushLock.Unlock()
	<-b.ackDone
	return nil
}
