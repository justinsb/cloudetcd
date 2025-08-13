package filesystemlog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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

type logFileMeta struct {
	firstRevision Revision
	count         int
}

// FilesystemLog is a filesystem-backed implementation of the Log interface
type FilesystemLog struct {
	batching *batch.Batching

	mu           sync.RWMutex
	dir          string
	lastRevision Revision
	listener     LogListener

	// logFiles is an in-memory index of log files, sorted by firstRevision
	logFiles []logFileMeta
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

	f.logFiles = nil

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse revision from filename
		filename := entry.Name()
		firstRevision, count, err := filenameToMeta(filename)
		if err != nil {
			// Skip invalid filenames
			klog.Warningf("ignoring file with unexpected name %q: %v", filename, err)
			continue
		}

		f.logFiles = append(f.logFiles, logFileMeta{firstRevision: firstRevision, count: count})
	}

	// Sort the log files by revision
	slices.SortFunc(f.logFiles, func(a, b logFileMeta) int {
		if a.firstRevision < b.firstRevision {
			return -1
		}
		if a.firstRevision > b.firstRevision {
			return 1
		}
		return 0
	})

	// Find the highest revision
	if len(f.logFiles) > 0 {
		lastFile := f.logFiles[len(f.logFiles)-1]
		f.lastRevision = lastFile.firstRevision + Revision(lastFile.count) - 1
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
	count := len(batch.Transactions)

	// Create filename with hex-encoded revision
	filename := batchToFilename(startRevision, count)
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

	l.logFiles = append(l.logFiles, logFileMeta{firstRevision: startRevision, count: count})
	l.lastRevision += Revision(count)

	if l.listener != nil {
		l.listener.OnLogEntry(persistence.Revision(l.lastRevision))
	}

	klog.V(2).Infof("Executed batch of %d transactions, revisions %d-%d",
		count, startRevision, l.lastRevision)

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
	fileMeta, ok := f.findFileForRevision(revision)
	if !ok {
		return nil, fmt.Errorf("log entry for revision %d not found in any file", revision)
	}

	filename := batchToFilename(fileMeta.firstRevision, fileMeta.count)
	filepath := filepath.Join(f.dir, filename)
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read log file %q: %w", filepath, err)
	}

	record := &persistedBatch{}
	if err := json.Unmarshal(data, record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal log record from file %s: %w", filepath, err)
	}

	pos := int(revision - fileMeta.firstRevision)
	if pos < 0 || pos >= len(record.Records) {
		return nil, fmt.Errorf("log entry not found in batch for revision %d (pos %d, count %d)", revision, pos, len(record.Records))
	}
	if len(record.Records) != fileMeta.count {
		// This would be a corruption error
		klog.Warningf("log file %s has mismatched record count, file meta says %d, found %d", filepath, fileMeta.count, len(record.Records))
	}
	return record.Records[pos], nil
}

// findFileForRevision finds the log file containing the given revision.
// It uses the in-memory index.
func (f *FilesystemLog) findFileForRevision(revision Revision) (logFileMeta, bool) {
	// The logFiles slice is sorted by firstRevision.
	// A reverse loop is simple and efficient enough, especially as recent revisions are more likely to be requested.
	for i := len(f.logFiles) - 1; i >= 0; i-- {
		fileMeta := f.logFiles[i]
		if fileMeta.firstRevision <= revision {
			if revision < fileMeta.firstRevision+Revision(fileMeta.count) {
				return fileMeta, true
			}
			// We've gone past our revision, and because the list is sorted,
			// no earlier file will contain it.
			return logFileMeta{}, false
		}
	}
	return logFileMeta{}, false
}

// Read reads records from the log starting from the given revision
func (f *FilesystemLog) Read(ctx context.Context, fromRevision Revision, callback func(Revision, *LogRecord) bool) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, fileMeta := range f.logFiles {
		fileLastRevision := fileMeta.firstRevision + Revision(fileMeta.count) - 1
		if fileLastRevision < fromRevision {
			continue
		}

		filename := batchToFilename(fileMeta.firstRevision, fileMeta.count)
		filepath := filepath.Join(f.dir, filename)
		data, err := os.ReadFile(filepath)
		if err != nil {
			return fmt.Errorf("failed to read log file %q: %w", filepath, err)
		}

		pBatch := &persistedBatch{}
		if err := json.Unmarshal(data, pBatch); err != nil {
			return fmt.Errorf("failed to unmarshal log record from file %s: %w", filepath, err)
		}

		for i, record := range pBatch.Records {
			revision := fileMeta.firstRevision + Revision(i)
			if revision < fromRevision {
				continue
			}
			if !callback(revision, record) {
				return nil
			}
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

// batchToFilename converts a first-revision and count to a filename
func batchToFilename(firstRevision Revision, count int) string {
	return fmt.Sprintf("%016x-%x.log", uint64(firstRevision), count)
}

// filenameToMeta converts a filename to a first-revision and count
func filenameToMeta(filename string) (Revision, int, error) {
	if !strings.HasSuffix(filename, ".log") {
		return 0, 0, fmt.Errorf("invalid filename format: %s", filename)
	}

	base := strings.TrimSuffix(filename, ".log")
	parts := strings.SplitN(base, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid filename format (expected <revision>-<count>.log): %s", filename)
	}

	revisionVal, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parsing revision from %q: %w", filename, err)
	}

	countVal, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parsing count from %q: %w", filename, err)
	}

	return Revision(revisionVal), int(countVal), nil
}
