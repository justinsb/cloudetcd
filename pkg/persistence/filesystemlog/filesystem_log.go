// Copyright 2026 Google LLC
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

package filesystemlog

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/batch"
	"k8s.io/klog/v2"
)

type Revision = persistence.Revision
type LogRecord = persistence.LogRecord
type LogListener = persistence.LogListener
type TxnMeta = persistence.TxnMeta

// FilesystemLog is a filesystem-backed implementation of the Log interface
type FilesystemLog struct {
	batching *batch.Batching

	mu           sync.RWMutex
	dir          string
	lastRevision Revision
	listener     LogListener
}

var _ persistence.Log = &FilesystemLog{}

// NewFilesystemLog creates a new filesystem-backed log
func NewFilesystemLog(dir string) (*FilesystemLog, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	log := &FilesystemLog{
		dir: dir,
	}

	// Replay existing log entries to determine current revision
	if err := log.replay(); err != nil {
		return nil, fmt.Errorf("failed to replay existing log: %w", err)
	}

	log.batching = batch.NewBatching(log.lastRevision, log.commitBatch)

	return log, nil
}

// replay reads all existing log files to determine the current revision
func (f *FilesystemLog) replay() error {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	var revisions []Revision
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse revision from filename
		filename := entry.Name()
		revision, err := filenameToRevision(filename)
		if err != nil {
			// Skip invalid filenames
			continue
		}

		revisions = append(revisions, revision)
	}

	// Find the highest revision
	if len(revisions) > 0 {
		slices.Sort(revisions)
		f.lastRevision = revisions[len(revisions)-1]
	}

	return nil
}

// Append adds a new record to the log and returns the revision number
func (f *FilesystemLog) Append(ctx context.Context, logRecord *LogRecord, txnMeta *TxnMeta) (Revision, bool, error) {
	return f.batching.Add(ctx, logRecord, txnMeta)
}

type persistedBatch struct {
	Records []*persistence.LogRecord
}

// commitBatch commits all transactions in the current batch
func (l *FilesystemLog) commitBatch(ctx context.Context, lastLogPosition Revision, batch *batch.BatchCommit) error {
	// Check if all transactions have the same condition position
	if len(batch.Transactions) == 0 {
		return fmt.Errorf("batch contains no transactions")
	}

	// Execute the batch under the main lock
	l.mu.Lock()
	defer l.mu.Unlock()

	if lastLogPosition != l.lastRevision {
		return fmt.Errorf("batch is not contiguous with the last batch, expected %d, got %d", l.lastRevision, lastLogPosition)
	}

	// Commit all transactions in the batch
	startRevision := l.lastRevision + 1

	// Create filename with hex-encoded revision
	filename := revisionToFilename(startRevision)
	filepath := filepath.Join(l.dir, filename)

	// Serialize record to JSON
	// TODO: Use proto for speed
	data := &persistedBatch{
		Records: make([]*persistence.LogRecord, len(batch.Transactions)),
	}
	for i, txn := range batch.Transactions {
		data.Records[i] = txn.LogRecord
	}
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal log records: %w", err)
	}

	// Write to file atomically
	if err := os.WriteFile(filepath, b, 0644); err != nil {
		return fmt.Errorf("failed to write log file: %w", err)
	}

	l.lastRevision += Revision(len(batch.Transactions))

	if l.listener != nil {
		l.listener.OnLogEntry(persistence.Revision(l.lastRevision))
	}

	klog.V(2).Infof("Executed batch of %d transactions, revisions %d-%d",
		len(batch.Transactions), startRevision+1, l.lastRevision)

	return nil
}

// GetCurrentRevision returns the current revision number
func (f *FilesystemLog) GetCurrentRevision(ctx context.Context) (Revision, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastRevision, nil
}

// GetLogEntry returns the log entry for the given revision
func (f *FilesystemLog) GetLogEntry(revision Revision) (*LogRecord, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.getLogEntry(revision)
}

func (f *FilesystemLog) getLogEntry(revision Revision) (*LogRecord, error) {
	filename := revisionToFilename(revision)
	filepath := filepath.Join(f.dir, filename)
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read log file: %w", err)
	}

	record := &persistedBatch{}
	if err := json.Unmarshal(data, record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal log record from file %s: %w", filepath, err)
	}

	pos := 0 // TODO
	if pos >= len(record.Records) {
		return nil, fmt.Errorf("log entry not found in batch for revision %d", revision)
	}
	return record.Records[pos], nil
}

// Read reads records from the log starting from the given revision
func (f *FilesystemLog) Read(ctx context.Context, fromRevision Revision, callback func(Revision, *LogRecord) bool) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	var matches []Revision
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !strings.HasSuffix(filename, ".log") {
			continue
		}

		// Parse revision from filename
		revision, err := filenameToRevision(filename)
		if err != nil {
			continue
		}

		if revision < fromRevision {
			continue
		}
		matches = append(matches, revision)
	}

	slices.Sort(matches)

	for _, revision := range matches {
		record, err := f.getLogEntry(revision)
		if err != nil {
			return fmt.Errorf("failed to get log entry for revision %d: %w", revision, err)
		}
		if record == nil {
			return fmt.Errorf("log entry not found for revision %d", revision)
		}
		if !callback(revision, record) {
			break
		}
	}

	return nil
}

// Close closes the log and releases any resources
func (f *FilesystemLog) Close() error {
	// For filesystem implementation, there's nothing to clean up
	return nil
}

// SetListener sets the log listener
func (f *FilesystemLog) SetListener(listener LogListener) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listener = listener
}

// revisionToFilename converts a revision number to a filename
func revisionToFilename(revision Revision) string {
	revisionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(revisionBytes, uint64(revision))
	return hex.EncodeToString(revisionBytes) + ".log"
}

// filenameToRevision converts a filename to a revision number
func filenameToRevision(filename string) (Revision, error) {
	if !strings.HasSuffix(filename, ".log") {
		return 0, fmt.Errorf("invalid filename format: %s", filename)
	}

	hexRevision := strings.TrimSuffix(filename, ".log")
	revisionBytes, err := hex.DecodeString(hexRevision)
	if err != nil {
		return 0, fmt.Errorf("failed to decode hex revision: %w", err)
	}

	if len(revisionBytes) != 8 {
		return 0, fmt.Errorf("invalid revision bytes length: %d", len(revisionBytes))
	}

	revision := Revision(binary.BigEndian.Uint64(revisionBytes))
	return revision, nil
}
