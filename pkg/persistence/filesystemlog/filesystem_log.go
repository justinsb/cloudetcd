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

package filesystemlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/batch"
	"justinsb.com/cloudetcd/pkg/persistence/logcodec"
	"justinsb.com/cloudetcd/pkg/persistence/recordcache"
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

// Options configures a FilesystemLog.
type Options struct {
	// CacheBytes bounds the in-memory cache of decoded records; records not
	// in it are read from disk on demand. <= 0 means unbounded.
	CacheBytes int64
}

// DefaultCacheBytes is the record cache budget when Options.CacheBytes is 0.
const DefaultCacheBytes = 256 << 20

// FilesystemLog is a filesystem-backed implementation of the Log interface.
//
// Values live on disk: the log keeps in memory only its file index, the byte
// offset of each record within its file, and a bounded cache of recently
// written or read records. Reading a record that is not cached is a single
// positioned read of that record's bytes.
type FilesystemLog struct {
	batching *batch.Batching

	mu           sync.RWMutex
	dir          string
	lastRevision Revision
	listener     LogListener

	// logFiles is an in-memory index of log files, sorted by firstRevision
	logFiles []logFileMeta

	// spans locates each record within its file, keyed by the file's first
	// revision. Filled at commit time; rebuilt on first use for files that
	// existed at startup. Guarded by spansMu.
	spansMu sync.Mutex
	spans   map[Revision][]logcodec.Span

	// cache holds recently written and read records.
	cache *recordcache.Cache
}

var _ persistence.Log = &FilesystemLog{}
var _ persistence.BatchAppender = &FilesystemLog{}
var _ persistence.Truncater = &FilesystemLog{}

// NewFilesystemLog creates a new filesystem-backed log with default Options.
func NewFilesystemLog(dir string) (*FilesystemLog, error) {
	return NewFilesystemLogWithOptions(dir, Options{})
}

// NewFilesystemLogWithOptions creates a new filesystem-backed log.
func NewFilesystemLogWithOptions(dir string, opts Options) (*FilesystemLog, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}
	cacheBytes := opts.CacheBytes
	if cacheBytes == 0 {
		cacheBytes = DefaultCacheBytes
	}

	log := &FilesystemLog{
		dir:   dir,
		spans: map[Revision][]logcodec.Span{},
		cache: recordcache.New(cacheBytes),
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

	// The files must be contiguous. A gap means a write that was never
	// acknowledged was left behind (a batch after a failed one that could
	// not be aborted), or worse; either way it needs a person to look
	// before anything is discarded.
	for i := 1; i < len(f.logFiles); i++ {
		prev := f.logFiles[i-1]
		if next := f.logFiles[i]; next.firstRevision != prev.firstRevision+Revision(prev.count) {
			return fmt.Errorf("log has a gap: %s ends at revision %d but %s starts at %d; recovery needed",
				batchToFilename(prev.firstRevision, prev.count), prev.firstRevision+Revision(prev.count)-1,
				batchToFilename(next.firstRevision, next.count), next.firstRevision)
		}
	}

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

// AppendBatch appends a contiguous range of records starting at lastRevision+1, preserving revisions.
func (f *FilesystemLog) AppendBatch(ctx context.Context, lastRevision Revision, records []*LogRecord) (bool, error) {
	return f.batching.AddBatch(ctx, lastRevision, records)
}

// Truncate discards records with revisions <= throughRevision.
// It only removes whole log files, and always retains the newest file so that
// the current revision can be recovered after a restart.
func (f *FilesystemLog) Truncate(ctx context.Context, throughRevision Revision) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var kept []logFileMeta
	for i, fileMeta := range f.logFiles {
		fileLastRevision := fileMeta.firstRevision + Revision(fileMeta.count) - 1
		if fileLastRevision <= throughRevision && i < len(f.logFiles)-1 {
			filename := batchToFilename(fileMeta.firstRevision, fileMeta.count)
			if err := os.Remove(filepath.Join(f.dir, filename)); err != nil {
				f.logFiles = append(kept, f.logFiles[i:]...)
				return fmt.Errorf("failed to remove log file %q: %w", filename, err)
			}
			f.spansMu.Lock()
			delete(f.spans, fileMeta.firstRevision)
			f.spansMu.Unlock()
			for r := fileMeta.firstRevision; r <= fileLastRevision; r++ {
				f.cache.Remove(r)
			}
		} else {
			kept = append(kept, fileMeta)
		}
	}
	f.logFiles = kept
	return nil
}

type persistedBatch struct {
	Records []*persistence.LogRecord
}

// commitBatch commits all transactions in the current batch
func (l *FilesystemLog) commitBatch(ctx context.Context, lastLogPosition Revision, bc *batch.BatchCommit) (batch.Commit, error) {
	// Check if all transactions have the same condition position
	if len(bc.Transactions) == 0 {
		return nil, fmt.Errorf("batch contains no transactions")
	}

	// The write happens with no lock held, so that consecutive batches can
	// be in flight at once; publication below is ordered by the sequencer.
	startRevision := lastLogPosition + 1
	count := len(bc.Transactions)

	// Create filename with hex-encoded revision
	filename := batchToFilename(startRevision, count)
	filepath := filepath.Join(l.dir, filename)

	records := make([]*persistence.LogRecord, len(bc.Transactions))
	for i, txn := range bc.Transactions {
		records[i] = txn.LogRecord
	}
	b, err := logcodec.Encode(records)
	if err != nil {
		return nil, fmt.Errorf("failed to encode log records: %w", err)
	}

	// Write and fsync: this is the commit point, so the record must be
	// durable on disk (not just in the page cache) before we acknowledge.
	if err := writeFileSync(filepath, b, 0644); err != nil {
		return nil, fmt.Errorf("failed to write log file: %w", err)
	}

	spans, err := logcodec.Offsets(b)
	if err != nil {
		return nil, fmt.Errorf("indexing log file %s: %w", filepath, err)
	}
	return &fileCommit{log: l, path: filepath, lastLogPosition: lastLogPosition, records: records, spans: spans}, nil
}

// fileCommit is a written, unpublished log file.
type fileCommit struct {
	log             *FilesystemLog
	path            string
	lastLogPosition Revision
	records         []*persistence.LogRecord
	spans           []logcodec.Span
}

func (c *fileCommit) Publish() {
	l := c.log
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lastRevision != c.lastLogPosition {
		// Batching publishes in order; this cannot happen.
		panic(fmt.Sprintf("batch published out of order: expected to publish after %d, log is at %d", c.lastLogPosition, l.lastRevision))
	}
	startRevision := l.lastRevision + 1
	count := len(c.records)
	l.spansMu.Lock()
	l.spans[startRevision] = c.spans
	l.spansMu.Unlock()
	// Just-published records are the hottest: the transactions that wrote
	// them, the index apply and the watchers all read them next.
	for i, r := range c.records {
		l.cache.Put(startRevision+Revision(i), r)
	}
	l.logFiles = append(l.logFiles, logFileMeta{firstRevision: startRevision, count: count})
	l.lastRevision += Revision(count)
	if l.listener != nil {
		l.listener.OnLogEntry(l.lastRevision)
	}
	klog.V(2).Infof("Executed batch of %d transactions, revisions %d-%d",
		count, startRevision, l.lastRevision)
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
	if record, ok := f.cache.Get(revision); ok {
		return record, nil
	}

	fileMeta, ok := f.findFileForRevision(revision)
	if !ok {
		return nil, fmt.Errorf("log entry for revision %d not found in any file", revision)
	}
	pos := int(revision - fileMeta.firstRevision)
	if pos < 0 || pos >= fileMeta.count {
		return nil, fmt.Errorf("log entry not found in batch for revision %d (pos %d, count %d)", revision, pos, fileMeta.count)
	}

	filename := batchToFilename(fileMeta.firstRevision, fileMeta.count)
	filepath := filepath.Join(f.dir, filename)
	spans, err := f.fileSpans(fileMeta, filepath)
	if err != nil {
		return nil, err
	}
	if pos >= len(spans) {
		return nil, fmt.Errorf("log file %s has %d records, expected %d", filepath, len(spans), fileMeta.count)
	}

	// One positioned read of exactly this record's bytes.
	span := spans[pos]
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %q: %w", filepath, err)
	}
	defer file.Close()
	buf := make([]byte, span.Length)
	if _, err := file.ReadAt(buf, int64(span.Offset)); err != nil {
		return nil, fmt.Errorf("failed to read record %d from log file %q: %w", revision, filepath, err)
	}
	record, err := logcodec.DecodeMessage(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to decode log record %d from file %s: %w", revision, filepath, err)
	}
	f.cache.Put(revision, record)
	return record, nil
}

// fileSpans returns the record spans of a log file, scanning the file's
// length prefixes on first use for files that predate this process.
func (f *FilesystemLog) fileSpans(fileMeta logFileMeta, filepath string) ([]logcodec.Span, error) {
	f.spansMu.Lock()
	spans, ok := f.spans[fileMeta.firstRevision]
	f.spansMu.Unlock()
	if ok {
		return spans, nil
	}
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read log file %q: %w", filepath, err)
	}
	spans, err = logcodec.Offsets(data)
	if err != nil {
		return nil, fmt.Errorf("indexing log file %s: %w", filepath, err)
	}
	f.spansMu.Lock()
	f.spans[fileMeta.firstRevision] = spans
	f.spansMu.Unlock()
	return spans, nil
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
		if pBatch.Records, err = logcodec.Decode(data); err != nil {
			return fmt.Errorf("failed to decode log records from file %s: %w", filepath, err)
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

// writeFileSync writes data to path and fsyncs both the file and its parent
// directory, so that the file and its directory entry survive a machine crash
// or power loss, not just a process crash.
func writeFileSync(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
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
