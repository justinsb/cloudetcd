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

// Package tieredlog composes two Log implementations into a tiered log:
// writes commit to a fast tier (e.g. a local or replicated disk), and a
// background loop periodically drains the fast tier into an archive tier
// (e.g. GCS) in large contiguous batches, then prunes the fast tier.
//
// This takes the archive tier off the commit path: clients are acknowledged
// as soon as the fast tier accepts the write, and the archive receives one
// storage object per flush interval instead of one per commit.
//
// The durability contract is therefore that of the fast tier for the most
// recent flush window, and of the archive tier beyond it. If the fast tier is
// a single local disk, up to one flush window of acknowledged writes can be
// lost if that disk is lost; use replicated storage (e.g. a regional
// persistent disk) for the fast tier if that is not acceptable.
package tieredlog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"justinsb.com/cloudetcd/pkg/persistence"
	"k8s.io/klog/v2"
)

type Revision = persistence.Revision
type LogRecord = persistence.LogRecord
type LogListener = persistence.LogListener
type TxnMeta = persistence.TxnMeta

// DefaultFlushInterval is how often the fast tier is drained to the archive
// tier when Options.FlushInterval is not set.
const DefaultFlushInterval = 1 * time.Minute

// Options configures a TieredLog.
type Options struct {
	// FlushInterval is how often the fast tier is drained to the archive
	// tier. Defaults to DefaultFlushInterval.
	FlushInterval time.Duration
}

// TieredLog is a Log that commits to a fast tier and asynchronously drains to
// an archive tier.
type TieredLog struct {
	fast    persistence.Log
	archive persistence.Log

	flushInterval time.Duration

	// mu guards the drain/prune cycle against concurrent reads: pruning the
	// fast tier must not race with a merged read that has finished the archive
	// tier but not yet read the fast tier.
	mu sync.RWMutex

	// flushMu serializes Flush calls (the background loop, Close, and any
	// direct callers), so that concurrent flushes are not misdiagnosed as a
	// second writer on the archive tier.
	flushMu sync.Mutex

	stopCh   chan struct{}
	stopOnce sync.Once
	doneCh   chan struct{}
}

var _ persistence.Log = &TieredLog{}

// NewTieredLog creates a TieredLog over the given fast and archive tiers.
// If the archive tier is ahead of the fast tier (e.g. the fast tier is a fresh
// disk after a machine replacement), the fast tier is first backfilled from
// the archive so that revision numbering continues correctly.
func NewTieredLog(ctx context.Context, fast persistence.Log, archive persistence.Log, options Options) (*TieredLog, error) {
	flushInterval := options.FlushInterval
	if flushInterval == 0 {
		flushInterval = DefaultFlushInterval
	}

	t := &TieredLog{
		fast:          fast,
		archive:       archive,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	if err := t.bootstrap(ctx); err != nil {
		return nil, err
	}

	go t.drainLoop()

	return t, nil
}

// bootstrap backfills the fast tier from the archive tier if the archive is
// ahead (for example when starting with an empty fast tier).
func (t *TieredLog) bootstrap(ctx context.Context) error {
	fastRevision, err := t.fast.GetCurrentRevision(ctx)
	if err != nil {
		return fmt.Errorf("getting fast tier revision: %w", err)
	}
	archiveRevision, err := t.archive.GetCurrentRevision(ctx)
	if err != nil {
		return fmt.Errorf("getting archive tier revision: %w", err)
	}
	if archiveRevision <= fastRevision {
		return nil
	}

	records, err := readContiguous(ctx, t.archive, fastRevision+1)
	if err != nil {
		return fmt.Errorf("reading archive tier for bootstrap: %w", err)
	}
	if len(records) == 0 {
		return fmt.Errorf("archive tier is at revision %d but has no records after fast tier revision %d", archiveRevision, fastRevision)
	}

	klog.Infof("backfilling fast tier with %d records from archive (revisions %d-%d)", len(records), fastRevision+1, fastRevision+Revision(len(records)))
	ok, err := appendBatch(ctx, t.fast, fastRevision, records)
	if err != nil {
		return fmt.Errorf("backfilling fast tier from archive: %w", err)
	}
	if !ok {
		return fmt.Errorf("backfilling fast tier from archive: fast tier changed concurrently")
	}
	return nil
}

func (t *TieredLog) drainLoop() {
	defer close(t.doneCh)

	ticker := time.NewTicker(t.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			ctx := context.Background()
			if err := t.Flush(ctx); err != nil {
				klog.Errorf("failed to drain log to archive tier: %v", err)
			}
		}
	}
}

// Flush drains any records the archive tier does not yet have from the fast
// tier into the archive tier, then prunes archived records from the fast tier
// (if it supports truncation). It is called periodically in the background,
// and can also be called directly (e.g. before shutdown).
func (t *TieredLog) Flush(ctx context.Context) error {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()

	archiveRevision, err := t.archive.GetCurrentRevision(ctx)
	if err != nil {
		return fmt.Errorf("getting archive tier revision: %w", err)
	}

	records, err := readContiguous(ctx, t.fast, archiveRevision+1)
	if err != nil {
		return fmt.Errorf("reading fast tier: %w", err)
	}
	if len(records) == 0 {
		return nil
	}

	ok, err := appendBatch(ctx, t.archive, archiveRevision, records)
	if err != nil {
		return fmt.Errorf("appending to archive tier: %w", err)
	}
	if !ok {
		// Another writer advanced the archive tier; this suggests two
		// instances are draining to the same archive, which the single-writer
		// design does not allow.
		return fmt.Errorf("archive tier advanced past revision %d concurrently; is another instance writing to the same archive?", archiveRevision)
	}

	archivedThrough := archiveRevision + Revision(len(records))
	if truncater, ok := t.fast.(persistence.Truncater); ok {
		t.mu.Lock()
		defer t.mu.Unlock()
		if err := truncater.Truncate(ctx, archivedThrough); err != nil {
			return fmt.Errorf("pruning fast tier: %w", err)
		}
	}

	klog.V(2).Infof("drained %d records to archive tier (through revision %d)", len(records), archivedThrough)
	return nil
}

// readContiguous reads all records from the given log starting at
// fromRevision, verifying that they form a contiguous range.
func readContiguous(ctx context.Context, log persistence.Log, fromRevision Revision) ([]*LogRecord, error) {
	var records []*LogRecord
	next := fromRevision
	var gapErr error
	if err := log.Read(ctx, fromRevision, func(revision Revision, record *LogRecord) bool {
		if revision != next {
			gapErr = fmt.Errorf("log has a gap: expected revision %d, got %d", next, revision)
			return false
		}
		records = append(records, record)
		next++
		return true
	}); err != nil {
		return nil, err
	}
	if gapErr != nil {
		return nil, gapErr
	}
	return records, nil
}

// appendBatch appends a contiguous range of records to the given log so that
// the first record lands at lastRevision+1. It uses the BatchAppender fast
// path when available, and falls back to sequential conditional appends.
func appendBatch(ctx context.Context, log persistence.Log, lastRevision Revision, records []*LogRecord) (bool, error) {
	if batchAppender, ok := log.(persistence.BatchAppender); ok {
		return batchAppender.AppendBatch(ctx, lastRevision, records)
	}

	for i, record := range records {
		wantRevision := lastRevision + Revision(i) + 1
		revision, ok, err := log.Append(ctx, record, persistence.NewTxnMeta(wantRevision-1))
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		if revision != wantRevision {
			return false, fmt.Errorf("log assigned revision %d, expected %d", revision, wantRevision)
		}
	}
	return true, nil
}

// Append adds a new record to the log; it commits to the fast tier only.
func (t *TieredLog) Append(ctx context.Context, logRecord *LogRecord, txnMeta *TxnMeta) (Revision, bool, error) {
	return t.fast.Append(ctx, logRecord, txnMeta)
}

// GetCurrentRevision returns the current revision number.
func (t *TieredLog) GetCurrentRevision(ctx context.Context) (Revision, error) {
	return t.fast.GetCurrentRevision(ctx)
}

// GetLogEntry returns the log entry for the given revision, checking the fast
// tier first and falling back to the archive tier for pruned revisions.
func (t *TieredLog) GetLogEntry(revision Revision) (*LogRecord, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	record, err := t.fast.GetLogEntry(revision)
	if err == nil {
		return record, nil
	}
	return t.archive.GetLogEntry(revision)
}

// Read reads records from the log starting from the given revision, merging
// the archive tier (older records) and the fast tier (recent records).
func (t *TieredLog) Read(ctx context.Context, fromRevision Revision, callback func(Revision, *LogRecord) bool) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	next := fromRevision
	stopped := false
	if err := t.archive.Read(ctx, fromRevision, func(revision Revision, record *LogRecord) bool {
		if !callback(revision, record) {
			stopped = true
			return false
		}
		next = revision + 1
		return true
	}); err != nil {
		return fmt.Errorf("reading archive tier: %w", err)
	}
	if stopped {
		return nil
	}

	if err := t.fast.Read(ctx, next, func(revision Revision, record *LogRecord) bool {
		return callback(revision, record)
	}); err != nil {
		return fmt.Errorf("reading fast tier: %w", err)
	}
	return nil
}

// SetListener sets the log listener; new entries are observed on the fast
// tier, which is where writes commit.
func (t *TieredLog) SetListener(listener LogListener) {
	t.fast.SetListener(listener)
}

// Close flushes outstanding records to the archive tier (best effort) and
// closes both tiers.
func (t *TieredLog) Close() error {
	t.stopOnce.Do(func() {
		close(t.stopCh)
	})
	<-t.doneCh

	var errs []error
	if err := t.Flush(context.Background()); err != nil {
		errs = append(errs, fmt.Errorf("flushing to archive tier on close: %w", err))
	}
	if err := t.fast.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing fast tier: %w", err))
	}
	if err := t.archive.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing archive tier: %w", err))
	}
	return errors.Join(errs...)
}
