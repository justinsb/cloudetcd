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

// Package filesystemlog is the log: an append-only sequence of files in a
// directory, optionally archived to an object store.
//
// Batches are appended to the active file and fsynced; that is the commit.
// The active file is rotated when it reaches a size or an age, and each
// closed file is uploaded to the Archive (if there is one) in the
// background. Local files are the read store: the log keeps in memory only
// its file index, each record's byte span within its file, and a bounded
// cache of recently written records; a closed file is memory-mapped on
// first read and its records decoded in place. On a machine with no local
// files, the archive is downloaded first.
//
// Compaction rewrites closed files keeping only the records that are still
// live (the latest write of each key as of the compaction point), so a
// compacted file is sparse in revision: every record carries its revision,
// and a compacted file's name carries the revision range it covers.
package filesystemlog

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/batch"
	"justinsb.com/cloudetcd/pkg/persistence/logcodec"
	"justinsb.com/cloudetcd/pkg/persistence/recordcache"
)

type Revision = persistence.Revision
type LogRecord = persistence.LogRecord
type LogListener = persistence.LogListener
type TxnMeta = persistence.TxnMeta

// liveFunc is the callback of persistence.Log.Compact: whether the record at
// revision is still live for key.
type liveFunc = func(key []byte, revision Revision) bool

// Archive is an object store that closed log files are copied to, and
// restored from on a machine that has none.
type Archive interface {
	// List returns the names of the archived files.
	List(ctx context.Context) ([]string, error)
	// Upload stores the file at path under name. It succeeds if the archive
	// already holds an identical file, and returns an error wrapping
	// persistence.ErrRevisionConflict if it holds a different one: another
	// writer is using the archive, which the single-writer design does not
	// allow.
	Upload(ctx context.Context, name string, path string) error
	// Download copies the archived file name to path.
	Download(ctx context.Context, name string, path string) error
	// Delete removes an archived file (one superseded by compaction). A
	// missing file is not an error.
	Delete(ctx context.Context, name string) error
}

// Options configures a FilesystemLog.
type Options struct {
	// CacheBytes bounds the in-memory cache of recently written records;
	// everything else is read from disk on demand. <= 0 means unbounded.
	CacheBytes int64
	// RotateBytes rotates the active file once it is this large; it is also
	// the size compaction aims for when it merges files.
	RotateBytes int64
	// RotateAfter rotates the active file once it is this old and has any
	// records. So a busy log rotates by size every few seconds and an idle
	// one by age, and the archive is never more than RotateAfter behind.
	RotateAfter time.Duration
	// Archive, if set, receives every closed file.
	Archive Archive
}

// Defaults for Options.
const (
	DefaultCacheBytes  = 256 << 20
	DefaultRotateBytes = 64 << 20
	DefaultRotateAfter = 5 * time.Minute
)

// fileInfo is what a log file's name says about it: the revisions it
// covers, and whether it holds every one of them.
type fileInfo struct {
	// first and last bound the revisions the file covers. A dense file (one
	// the log appended to) holds every revision in [first, last]; a sparse
	// file (one compaction wrote) holds only the live ones. A dense file's
	// name gives only first; last is set from its record count.
	first, last Revision
	sparse      bool
}

// filename is the file's name: its first revision, or for a sparse file
// its range, in hex so that names sort in revision order.
func (fi fileInfo) filename() string {
	if fi.sparse {
		return fmt.Sprintf("%016x-%016x.log", uint64(fi.first), uint64(fi.last))
	}
	return fmt.Sprintf("%016x.log", uint64(fi.first))
}

// parseFilename is the inverse of filename. A dense file's last is 0.
func parseFilename(filename string) (fileInfo, error) {
	if !strings.HasSuffix(filename, ".log") {
		return fileInfo{}, fmt.Errorf("invalid filename format: %s", filename)
	}
	base := strings.TrimSuffix(filename, ".log")
	parts := strings.SplitN(base, "-", 2)
	first, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return fileInfo{}, fmt.Errorf("invalid filename format: %s", filename)
	}
	if len(parts) == 1 {
		return fileInfo{first: Revision(first)}, nil
	}
	last, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil || last < first {
		return fileInfo{}, fmt.Errorf("invalid filename format: %s", filename)
	}
	return fileInfo{first: Revision(first), last: Revision(last), sparse: true}, nil
}

// logFile is one file of the log. All but the last are closed.
type logFile struct {
	fileInfo
	name string
	// count is the number of records in the file; size its length in bytes,
	// header included.
	count int
	size  int64
	// opened is when the file was created, for age-based rotation.
	opened time.Time
	// archived is whether the archive holds this (closed) file.
	archived bool

	// mu guards the state below, which is built as the file is written or
	// read.
	mu sync.Mutex
	// spans locates each record in the file, and revisions gives each
	// record's revision (nil for a dense file, whose record i has revision
	// first+i). The active file's are extended as it is appended to; a
	// closed file's are built on first read, when indexed becomes true.
	indexed   bool
	spans     []logcodec.Span
	revisions []Revision
	// mapped is the read-only memory mapping of a closed file, made on first
	// read; records are decoded straight out of it, so old records live in
	// the page cache rather than on the heap. nil where the platform cannot
	// map. pins counts the reads decoding from the mapping right now; a
	// file replaced by compaction while pinned is marked retire, and the
	// last unpin unmaps it.
	mapped []byte
	pins   int
	retire bool
	// reader is the file opened for reading, on first use, and kept: it
	// serves positioned reads of the active file (or of any file where
	// mapping is unavailable) and is what a closed file is mapped from.
	reader *os.File
}

// archiveOp is work for the archiver: copy a closed file to the archive, or
// remove one that compaction superseded.
type archiveOp struct {
	name   string
	remove bool
}

// FilesystemLog is the log; see the package comment.
//
// Locks, in the order they are taken: writeMu, then mu, then a file's mu.
// compactMu is taken first by Compact and never inside another.
type FilesystemLog struct {
	dir  string
	opts Options

	batching *batch.Batching

	// writeMu serializes appends and rotation, so that a commit's fsync does
	// not hold mu and block readers.
	writeMu sync.Mutex
	// active is the file being appended to, the last of files.
	active *os.File

	// mu guards the index below.
	mu           sync.RWMutex
	files        []*logFile
	lastRevision Revision
	listener     LogListener

	// cache holds recently written records (reads of the active file).
	cache *recordcache.Cache

	// compactMu serializes compactions.
	compactMu sync.Mutex

	// archiver copies closed files to the archive and removes superseded
	// ones, in the background.
	archiver *worker[archiveOp]
	// rotator rotates the active file by age.
	rotator *ticker

	removeOnClose bool
}

var _ persistence.Log = &FilesystemLog{}

// NewFilesystemLog creates or opens the log in dir with default Options.
func NewFilesystemLog(dir string) (*FilesystemLog, error) {
	return NewFilesystemLogWithOptions(dir, Options{})
}

// NewTempLog creates a log in a new temporary directory that is removed when
// the log is closed. It is the log for tests and benchmarks: the real log on
// real files, so anything that works here works on disk.
func NewTempLog(opts Options) (*FilesystemLog, error) {
	dir, err := os.MkdirTemp("", "cloudetcd-log-")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary log directory: %w", err)
	}
	log, err := NewFilesystemLogWithOptions(dir, opts)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	log.removeOnClose = true
	return log, nil
}

// NewFilesystemLogWithOptions creates or opens the log in dir.
func NewFilesystemLogWithOptions(dir string, opts Options) (*FilesystemLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}
	if opts.CacheBytes == 0 {
		opts.CacheBytes = DefaultCacheBytes
	}
	if opts.RotateBytes <= 0 {
		opts.RotateBytes = DefaultRotateBytes
	}
	if opts.RotateAfter <= 0 {
		opts.RotateAfter = DefaultRotateAfter
	}

	l := &FilesystemLog{
		dir:      dir,
		opts:     opts,
		cache:    recordcache.New(opts.CacheBytes),
		archiver: newWorker[archiveOp](4096),
	}

	if err := l.open(context.Background()); err != nil {
		return nil, err
	}

	l.batching = batch.NewBatching(l.lastRevision, l.commitBatch)
	l.archiver.start(l.archive)
	l.rotator = startTicker(time.Second, l.tick)
	return l, nil
}

// Dir returns the log's directory.
func (l *FilesystemLog) Dir() string { return l.dir }

// open builds the index from the files in the directory (and the archive),
// repairs a torn tail on the last file, and opens it for appending.
func (l *FilesystemLog) open(ctx context.Context) error {
	files, err := l.listFiles()
	if err != nil {
		return err
	}

	if l.opts.Archive != nil {
		if files, err = l.reconcileArchive(ctx, files); err != nil {
			return err
		}
	}

	for i, f := range files {
		data, err := os.ReadFile(filepath.Join(l.dir, f.name))
		if err != nil {
			return fmt.Errorf("failed to read log file %s: %w", f.name, err)
		}
		spans, revisions, complete, err := logcodec.Scan(data)
		if err != nil {
			return fmt.Errorf("log file %s: %w", f.name, err)
		}
		if complete < len(data) {
			if i < len(files)-1 || f.sparse {
				return fmt.Errorf("log file %s is truncated at byte %d but is not the active file; recovery needed", f.name, complete)
			}
			// A write cut short by a crash: nothing past it was acknowledged.
			klog.Warningf("log file %s ends in a torn record; truncating from %d to %d bytes", f.name, len(data), complete)
			if err := os.Truncate(filepath.Join(l.dir, f.name), int64(complete)); err != nil {
				return fmt.Errorf("truncating log file %s: %w", f.name, err)
			}
		}
		f.count = len(spans)
		f.size = int64(complete)
		if f.sparse {
			for j, r := range revisions {
				if r < f.first || r > f.last || (j > 0 && r <= revisions[j-1]) {
					return fmt.Errorf("log file %s: record %d has revision %d, outside or out of order for %d-%d; recovery needed", f.name, j, r, f.first, f.last)
				}
			}
		} else {
			for j, r := range revisions {
				if r != f.first+Revision(j) {
					return fmt.Errorf("log file %s: record %d has revision %d, want %d; recovery needed", f.name, j, r, f.first+Revision(j))
				}
			}
			f.last = f.first + Revision(f.count) - 1
		}
		if i > 0 {
			prev := files[i-1]
			if f.first != prev.last+1 {
				return fmt.Errorf("log has a gap: %s ends at revision %d but %s starts at %d; recovery needed", prev.name, prev.last, f.name, f.first)
			}
		} else if f.first != 1 {
			return fmt.Errorf("log starts at revision %d, not 1; recovery needed", f.first)
		}
		if i == len(files)-1 && !f.sparse {
			// The active file's index is kept from the start; a closed
			// file's is rebuilt on first read.
			f.spans = spans
			f.indexed = true
		}
	}

	l.files = files
	if len(files) == 0 {
		return l.newActiveFile()
	}
	last := files[len(files)-1]
	l.lastRevision = last.last

	// Closed files that never made it to the archive (a crash after
	// rotating, before the upload finished).
	if l.opts.Archive != nil {
		for _, f := range files[:len(files)-1] {
			if !f.archived {
				l.archiver.add(archiveOp{name: f.name})
			}
		}
	}

	if last.archived || last.sparse {
		// An archived file is never appended to again (its object would no
		// longer match), nor is a compacted one; start the next file.
		if !last.archived && l.opts.Archive != nil {
			l.archiver.add(archiveOp{name: last.name})
		}
		return l.newActiveFile()
	}
	l.active, err = os.OpenFile(filepath.Join(l.dir, last.name), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening log file %s for append: %w", last.name, err)
	}
	last.opened = time.Now()
	return nil
}

// listFiles returns the log files in the directory, sorted, with files
// superseded by a compacted file removed (a crash between writing the
// compacted file and removing the originals leaves both).
func (l *FilesystemLog) listFiles() ([]*logFile, error) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read log directory: %w", err)
	}
	var files []*logFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := parseFilename(e.Name())
		if err != nil {
			klog.Warningf("ignoring file with unexpected name %q: %v", e.Name(), err)
			continue
		}
		files = append(files, &logFile{fileInfo: info, name: e.Name()})
	}
	slices.SortFunc(files, func(a, b *logFile) int {
		if a.first != b.first {
			return int(a.first) - int(b.first)
		}
		// A compacted file sorts before the dense file it replaced.
		if a.sparse != b.sparse {
			if a.sparse {
				return -1
			}
			return 1
		}
		return 0
	})

	var kept []*logFile
	for _, f := range files {
		if n := len(kept); n > 0 && kept[n-1].sparse && f.first >= kept[n-1].first && f.first <= kept[n-1].last {
			// Removing data is permanent, so to be clear about why this is
			// safe: the compacted file was synced and renamed into place
			// before compaction started removing the files it replaced, and
			// it covers this file's revisions, so this file is one that
			// compaction was about to remove when it was interrupted.
			// Keeping it would gain nothing: even if we archived it, the
			// next compaction would delete it again.
			klog.Infof("removing log file %s, superseded by compacted %s", f.name, kept[n-1].name)
			if err := os.Remove(filepath.Join(l.dir, f.name)); err != nil {
				return nil, fmt.Errorf("removing superseded log file %s: %w", f.name, err)
			}
			continue
		}
		kept = append(kept, f)
	}
	return kept, nil
}

// reconcileArchive brings the directory and the archive into agreement: a
// directory with no files is restored from the archive; otherwise every
// archived file must be present locally or superseded by a local compacted
// file (the archive is written only by this log), and closed local files
// missing from the archive are uploaded.
func (l *FilesystemLog) reconcileArchive(ctx context.Context, files []*logFile) ([]*logFile, error) {
	names, err := l.opts.Archive.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing archive: %w", err)
	}

	if len(files) == 0 && len(names) > 0 {
		klog.Infof("log directory is empty; restoring %d file(s) from the archive", len(names))
		for _, name := range names {
			if err := l.opts.Archive.Download(ctx, name, filepath.Join(l.dir, name)); err != nil {
				return nil, fmt.Errorf("restoring %s from the archive: %w", name, err)
			}
		}
		if files, err = l.listFiles(); err != nil {
			return nil, err
		}
	}

	local := map[string]*logFile{}
	for _, f := range files {
		local[f.name] = f
	}
	for _, name := range names {
		if f, ok := local[name]; ok {
			f.archived = true
			continue
		}
		info, err := parseFilename(name)
		if err == nil && l.superseded(files, info.first) {
			// Compaction replaced it locally but had not yet removed it.
			l.archiver.add(archiveOp{name: name, remove: true})
			continue
		}
		return nil, fmt.Errorf("archive holds %s, which this log does not have; is another instance writing to the archive?", name)
	}
	return files, nil
}

// superseded reports whether a compacted file in files covers first.
func (l *FilesystemLog) superseded(files []*logFile, first Revision) bool {
	for _, f := range files {
		if f.sparse && first >= f.first && first <= f.last {
			return true
		}
	}
	return false
}

// newActiveFile starts the file for revision lastRevision+1. Called with
// writeMu and mu held (or before the log is shared).
func (l *FilesystemLog) newActiveFile() error {
	info := fileInfo{first: l.lastRevision + 1, last: l.lastRevision}
	name := info.filename()
	f, err := os.OpenFile(filepath.Join(l.dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("creating log file %s: %w", name, err)
	}
	header := logcodec.Header()
	if _, err := f.Write(header); err != nil {
		f.Close()
		return fmt.Errorf("writing log file %s: %w", name, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("syncing log file %s: %w", name, err)
	}
	if err := syncDir(l.dir); err != nil {
		f.Close()
		return err
	}
	l.files = append(l.files, &logFile{fileInfo: info, name: name, size: int64(len(header)), opened: time.Now(), indexed: true})
	l.active = f
	return nil
}

// Append adds a new record to the log and returns the revision number
func (l *FilesystemLog) Append(ctx context.Context, logRecord *LogRecord, txnMeta *TxnMeta) (Revision, bool, error) {
	return l.batching.Add(ctx, logRecord, txnMeta)
}

// commitBatch appends a batch to the active file, fsyncs it, and publishes
// it. This is the commit point.
func (l *FilesystemLog) commitBatch(ctx context.Context, lastLogPosition Revision, bc *batch.BatchCommit) error {
	if len(bc.Transactions) == 0 {
		return fmt.Errorf("batch contains no transactions")
	}
	records := make([]*persistence.LogRecord, len(bc.Transactions))
	for i, txn := range bc.Transactions {
		records[i] = txn.LogRecord
	}
	buf, spans, err := logcodec.AppendRecords(nil, lastLogPosition+1, records)
	if err != nil {
		return fmt.Errorf("failed to encode log records: %w", err)
	}

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	if lastLogPosition != l.lastRevision {
		return fmt.Errorf("batch is not contiguous with the log: batch starts after %d, log is at %d", lastLogPosition, l.lastRevision)
	}
	active := l.files[len(l.files)-1]
	if _, err := l.active.Write(buf); err != nil {
		return fmt.Errorf("failed to write log file %s: %w", active.name, err)
	}
	if err := l.active.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file %s: %w", active.name, err)
	}

	// Publish.
	l.mu.Lock()
	base := int(active.size)
	for i := range spans {
		spans[i].Offset += base
	}
	active.mu.Lock()
	active.spans = append(active.spans, spans...)
	active.mu.Unlock()
	startRevision := l.lastRevision + 1
	for i, r := range records {
		l.cache.Put(startRevision+Revision(i), r)
	}
	active.count += len(records)
	active.size += int64(len(buf))
	l.lastRevision += Revision(len(records))
	active.last = l.lastRevision
	if l.listener != nil {
		l.listener.OnLogEntry(l.lastRevision)
	}
	klog.V(2).Infof("committed batch of %d transactions, revisions %d-%d", len(records), startRevision, l.lastRevision)

	var rotateErr error
	if active.size >= l.opts.RotateBytes {
		rotateErr = l.rotateLocked()
	}
	l.mu.Unlock()
	return rotateErr
}

// rotateLocked closes the active file and starts the next one. Called with
// writeMu and mu held. The next file is created before the old one is handed
// to the archiver, so the last file in the directory is always the active
// one and a closed file is never appended to again.
func (l *FilesystemLog) rotateLocked() error {
	old := l.files[len(l.files)-1]
	if old.count == 0 {
		return nil
	}
	if err := l.active.Close(); err != nil {
		return fmt.Errorf("closing log file %s: %w", old.name, err)
	}
	if err := l.newActiveFile(); err != nil {
		return err
	}
	klog.V(2).Infof("rotated log file %s (%d records, %d bytes)", old.name, old.count, old.size)
	if l.opts.Archive != nil {
		l.archiver.add(archiveOp{name: old.name})
	}
	return nil
}

// tick rotates the active file once it is RotateAfter old.
func (l *FilesystemLog) tick() {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	active := l.files[len(l.files)-1]
	if active.count > 0 && time.Since(active.opened) >= l.opts.RotateAfter {
		if err := l.rotateLocked(); err != nil {
			klog.Errorf("rotating log file: %v", err)
		}
	}
}

// archive carries out one archive operation, retrying until it succeeds. A
// conflict means another writer is using the archive, which is fatal.
func (l *FilesystemLog) archive(op archiveOp) {
	ctx := context.Background()
	backoff := time.Second
	for {
		var err error
		if op.remove {
			err = l.opts.Archive.Delete(ctx, op.name)
		} else {
			err = l.opts.Archive.Upload(ctx, op.name, filepath.Join(l.dir, op.name))
		}
		if err == nil {
			if !op.remove {
				klog.V(2).Infof("archived log file %s", op.name)
				l.mu.Lock()
				for _, f := range l.files {
					if f.name == op.name {
						f.archived = true
					}
				}
				l.mu.Unlock()
			}
			return
		}
		if errors.Is(err, persistence.ErrRevisionConflict) {
			panic(fmt.Sprintf("archiving log file %s: %v", op.name, err))
		}
		klog.Errorf("archive operation on %s (retrying in %s): %v", op.name, backoff, err)
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// GetCurrentRevision returns the current revision number
func (l *FilesystemLog) GetCurrentRevision(ctx context.Context) (Revision, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastRevision, nil
}

// GetLogEntry returns the log entry for the given revision
func (l *FilesystemLog) GetLogEntry(revision Revision) (*LogRecord, error) {
	if record, ok := l.cache.Get(revision); ok {
		return record, nil
	}
	m, release, err := l.recordBytes(revision)
	if err != nil {
		return nil, err
	}
	if release != nil {
		// The bytes alias the file's mapping, which stays pinned until
		// release; the decode copies them out, so the record owns its
		// memory and nothing outlives the pin. Not cached: the cache is
		// for the write-recent set, and these reads are already one copy
		// from the page cache.
		record, err := m.Decode()
		release()
		if err != nil {
			return nil, fmt.Errorf("failed to decode log record %d: %w", revision, err)
		}
		return record, nil
	}
	record, err := m.Decode()
	if err != nil {
		return nil, fmt.Errorf("failed to decode log record %d: %w", revision, err)
	}
	l.cache.Put(revision, record)
	return record, nil
}

// recordBytes returns the encoded bytes of revision's record. For a closed
// file they alias its memory mapping, which is pinned until the returned
// release is called: decode, then release, and let nothing alias the bytes
// afterwards. For the active file, or where mapping is unavailable, they
// are one positioned read into a fresh buffer, and release is nil.
func (l *FilesystemLog) recordBytes(revision Revision) (m logcodec.EncodedRecord, release func(), err error) {
	l.mu.RLock()
	f, ok := l.findFile(revision)
	active := ok && f == l.files[len(l.files)-1]
	l.mu.RUnlock()
	if !ok {
		return nil, nil, fmt.Errorf("log entry for revision %d not found in any file", revision)
	}

	f.mu.Lock()
	if !active {
		if err := l.loadLocked(f); err != nil {
			f.mu.Unlock()
			return nil, nil, err
		}
	}
	pos, err := f.position(revision)
	if err != nil {
		f.mu.Unlock()
		return nil, nil, err
	}
	span := f.spans[pos]
	if f.mapped != nil {
		f.pins++
		mapped := f.mapped
		f.mu.Unlock()
		return span.Bytes(mapped), func() { l.unpin(f) }, nil
	}
	file, err := l.readerLocked(f)
	f.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	buf := make([]byte, span.Length)
	if _, err := file.ReadAt(buf, int64(span.Offset)); err != nil {
		return nil, nil, fmt.Errorf("failed to read record %d from log file %s: %w", revision, f.name, err)
	}
	return buf, nil, nil
}

// unpin releases one read's pin on a file's mapping, and unmaps it if
// compaction retired the file while it was pinned.
func (l *FilesystemLog) unpin(f *logFile) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pins--
	if f.pins == 0 && f.retire && f.mapped != nil {
		if err := munmapFile(f.mapped); err != nil {
			klog.Errorf("unmapping retired log file %s: %v", f.name, err)
		}
		f.mapped = nil
	}
}

// loadLocked prepares a closed file for reading: maps it, where the platform
// can, and indexes its records. Both happen once. A mapping that fails where
// it should work is fatal: the alternative would be a positioned read per
// record, a large and silent change in how the server performs. Called with
// f.mu held.
func (l *FilesystemLog) loadLocked(f *logFile) error {
	file, err := l.readerLocked(f)
	if err != nil {
		return err
	}
	if f.mapped == nil {
		m, err := mmapFile(file, f.size)
		if err != nil && !errors.Is(err, errNoMmap) {
			panic(fmt.Sprintf("memory-mapping log file %s (%d bytes): %v", f.name, f.size, err))
		}
		f.mapped = m
	}
	if f.indexed {
		return nil
	}
	data := f.mapped
	if data == nil {
		data = make([]byte, f.size)
		if _, err := io.ReadFull(io.NewSectionReader(file, 0, f.size), data); err != nil {
			return fmt.Errorf("failed to read log file %s: %w", f.name, err)
		}
	}
	spans, revisions, err := logcodec.Offsets(data)
	if err != nil {
		return fmt.Errorf("indexing log file %s: %w", f.name, err)
	}
	f.spans = spans
	if f.sparse {
		f.revisions = revisions
	}
	f.indexed = true
	return nil
}

// readerLocked returns the file's read handle, opening it on first use.
// Called with f.mu held.
func (l *FilesystemLog) readerLocked(f *logFile) (*os.File, error) {
	if f.reader == nil {
		file, err := os.Open(filepath.Join(l.dir, f.name))
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", f.name, err)
		}
		f.reader = file
	}
	return f.reader, nil
}

// position returns the index of revision's record in the file. For a sparse
// file, a revision that compaction dropped is ErrCompacted. Called with f.mu
// held and the file indexed.
func (f *logFile) position(revision Revision) (int, error) {
	if !f.sparse {
		pos := int(revision - f.first)
		if pos < 0 || pos >= len(f.spans) {
			return 0, fmt.Errorf("log entry for revision %d not found in file %s (%d records)", revision, f.name, len(f.spans))
		}
		return pos, nil
	}
	pos := sort.Search(len(f.revisions), func(i int) bool { return f.revisions[i] >= revision })
	if pos >= len(f.revisions) || f.revisions[pos] != revision {
		return 0, fmt.Errorf("revision %d: %w", revision, persistence.ErrCompacted)
	}
	return pos, nil
}

// revisionAt returns the revision of the file's record i. The file must be
// indexed.
func (f *logFile) revisionAt(i int) Revision {
	if f.revisions != nil {
		return f.revisions[i]
	}
	return f.first + Revision(i)
}

// findFile finds the file covering revision. Called with mu held.
func (l *FilesystemLog) findFile(revision Revision) (*logFile, bool) {
	// Recent revisions are the most requested; search from the end.
	for i := len(l.files) - 1; i >= 0; i-- {
		f := l.files[i]
		if f.first <= revision {
			return f, revision <= f.last
		}
	}
	return nil, false
}

// Read reads records from the log starting from the given revision
func (l *FilesystemLog) Read(ctx context.Context, fromRevision Revision, callback func(Revision, *LogRecord) bool) error {
	l.mu.RLock()
	files := append([]*logFile(nil), l.files...)
	sizes := make([]int64, len(files))
	for i, f := range files {
		sizes[i] = f.size
	}
	l.mu.RUnlock()

	for i, f := range files {
		if f.count == 0 || f.last < fromRevision {
			continue
		}
		// Read only what was published; the active file may be growing.
		data := make([]byte, sizes[i])
		file, err := os.Open(filepath.Join(l.dir, f.name))
		if err != nil {
			return fmt.Errorf("failed to open log file %s: %w", f.name, err)
		}
		_, err = io.ReadFull(file, data)
		file.Close()
		if err != nil {
			return fmt.Errorf("failed to read log file %s: %w", f.name, err)
		}
		spans, revisions, err := logcodec.Offsets(data)
		if err != nil {
			return fmt.Errorf("failed to decode log file %s: %w", f.name, err)
		}
		for j, span := range spans {
			if revisions[j] < fromRevision {
				continue
			}
			record, err := span.Bytes(data).Decode()
			if err != nil {
				return fmt.Errorf("failed to decode log record %d from file %s: %w", revisions[j], f.name, err)
			}
			if !callback(revisions[j], record) {
				return nil
			}
		}
	}
	return nil
}

// Compact rewrites the closed files at or below through so that they hold
// only their live records. A record is live if live(key, revision) says so
// for a key it touches; the index decides what that means (see
// memorystorage.Compact: the key's latest put or delete at or below
// through). Compact merges neighbouring files while their live bytes fit in
// RotateBytes, leaves a lone file alone while fewer than a tenth of its
// records are dead, and removes each replaced file, locally and from the
// archive, once its replacement is durable.
func (l *FilesystemLog) Compact(ctx context.Context, through Revision, live liveFunc) error {
	l.compactMu.Lock()
	defer l.compactMu.Unlock()

	l.mu.RLock()
	var eligible []*logFile
	for _, f := range l.files[:len(l.files)-1] {
		if f.last <= through {
			eligible = append(eligible, f)
		}
	}
	l.mu.RUnlock()
	if len(eligible) == 0 {
		return nil
	}

	// Measure each file first, then walk them in order.
	type measured struct {
		f         *logFile
		liveBytes int64
		dead      int
	}
	var files []measured
	for _, f := range eligible {
		var liveBytes int64
		dead, err := l.liveRecords(f, live, func(m logcodec.EncodedRecord, rev Revision) error {
			liveBytes += int64(len(m)) + 4
			return nil
		})
		if err != nil {
			return err
		}
		files = append(files, measured{f: f, liveBytes: liveBytes, dead: dead})
	}

	var group []*logFile
	var groupBytes int64
	var groupDead int
	flush := func() error {
		if len(group) == 0 {
			return nil
		}
		defer func() { group, groupBytes, groupDead = nil, 0, 0 }()
		if len(group) == 1 && (groupDead == 0 || groupDead*10 < group[0].count) {
			// Rewriting a lone file that is still (nearly) all live would
			// gain nothing. It stays as it is, and is measured again next
			// time.
			return nil
		}
		return l.compactGroup(ctx, group, live)
	}
	for _, m := range files {
		if groupBytes > 0 && groupBytes+m.liveBytes > l.opts.RotateBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		group = append(group, m.f)
		groupBytes += m.liveBytes
		groupDead += m.dead
	}
	return flush()
}

// compactGroup replaces a run of neighbouring closed files with one file
// holding their live records.
func (l *FilesystemLog) compactGroup(ctx context.Context, group []*logFile, live liveFunc) error {
	info := fileInfo{first: group[0].first, last: group[len(group)-1].last, sparse: true}
	name := info.filename()

	// Stream the live records to a temporary file and rename it into place,
	// so that a crash leaves either the originals alone or the originals
	// beside a complete replacement (which listFiles resolves).
	tmp := filepath.Join(l.dir, name+".tmp")
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("creating compacted log file %s: %w", name, err)
	}
	fail := func(err error) error {
		out.Close()
		os.Remove(tmp)
		return err
	}
	w := bufio.NewWriterSize(out, 1<<20)
	var size int64
	var spans []logcodec.Span
	var revisions []Revision
	n, err := w.Write(logcodec.Header())
	if err != nil {
		return fail(fmt.Errorf("writing compacted log file %s: %w", name, err))
	}
	size += int64(n)
	for _, f := range group {
		if _, err := l.liveRecords(f, live, func(m logcodec.EncodedRecord, rev Revision) error {
			n, err := logcodec.WriteRecord(w, m)
			if err != nil {
				return fmt.Errorf("writing compacted log file %s: %w", name, err)
			}
			spans = append(spans, logcodec.Span{Offset: int(size) + n - len(m), Length: len(m)})
			revisions = append(revisions, rev)
			size += int64(n)
			return nil
		}); err != nil {
			return fail(err)
		}
	}
	if err := w.Flush(); err != nil {
		return fail(fmt.Errorf("writing compacted log file %s: %w", name, err))
	}
	if err := out.Sync(); err != nil {
		return fail(fmt.Errorf("syncing compacted log file %s: %w", name, err))
	}
	if err := out.Close(); err != nil {
		return fail(fmt.Errorf("closing compacted log file %s: %w", name, err))
	}
	if err := os.Rename(tmp, filepath.Join(l.dir, name)); err != nil {
		return fail(fmt.Errorf("renaming compacted log file %s: %w", name, err))
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}
	compacted := &logFile{fileInfo: info, name: name, count: len(spans), size: size, opened: time.Now(), indexed: true, spans: spans, revisions: revisions}

	// Swap it into the index and retire the originals: their mappings are
	// unmapped now, or by the last unpin if a reader is decoding from one;
	// their read handles closed (a replaced-in-place file's handle is the
	// old inode); their files removed.
	l.mu.Lock()
	i := slices.Index(l.files, group[0])
	l.files = slices.Replace(l.files, i, i+len(group), compacted)
	for _, f := range group {
		f.mu.Lock()
		if f.mapped != nil {
			if f.pins == 0 {
				if err := munmapFile(f.mapped); err != nil {
					klog.Errorf("unmapping compacted-away log file %s: %v", f.name, err)
				}
				f.mapped = nil
			} else {
				f.retire = true
			}
		}
		if f.reader != nil {
			f.reader.Close()
			f.reader = nil
		}
		f.mu.Unlock()
	}
	l.mu.Unlock()

	var oldBytes int64
	for _, f := range group {
		oldBytes += f.size
		if f.name == name {
			continue
		}
		if err := os.Remove(filepath.Join(l.dir, f.name)); err != nil {
			klog.Errorf("removing compacted-away log file %s: %v", f.name, err)
		}
	}
	klog.V(1).Infof("compacted %d file(s) covering revisions %d-%d into %s: %d records kept, %d MB -> %d MB",
		len(group), info.first, info.last, name, len(spans), oldBytes>>20, size>>20)

	if l.opts.Archive != nil {
		l.archiver.add(archiveOp{name: name})
		for _, f := range group {
			if f.name != name {
				l.archiver.add(archiveOp{name: f.name, remove: true})
			}
		}
	}
	return nil
}

// liveRecords calls keep with each live record of a closed file, in order,
// and returns how many records were dead. A record is live if live says so
// for any key it touches, whether by a put or a delete: a delete is live
// while it is the key's latest write as of the compaction point, and the
// index (which decided that) still refers to it.
func (l *FilesystemLog) liveRecords(f *logFile, live liveFunc, keep func(m logcodec.EncodedRecord, rev Revision) error) (dead int, err error) {
	f.mu.Lock()
	if err := l.loadLocked(f); err != nil {
		f.mu.Unlock()
		return 0, err
	}
	data, spans, file := f.mapped, f.spans, f.reader
	f.mu.Unlock()
	if data == nil {
		data = make([]byte, f.size)
		if _, err := io.ReadFull(io.NewSectionReader(file, 0, f.size), data); err != nil {
			return 0, fmt.Errorf("failed to read log file %s: %w", f.name, err)
		}
	}
	for i, span := range spans {
		m := span.Bytes(data)
		rev := f.revisionAt(i)
		record, err := m.DecodeAlias()
		if err != nil {
			return 0, fmt.Errorf("failed to decode log record %d from file %s: %w", rev, f.name, err)
		}
		isLive := false
		for _, ev := range record.Events {
			if live(ev.Kv.Key, rev) {
				isLive = true
				break
			}
		}
		if !isLive {
			dead++
			continue
		}
		if keep != nil {
			if err := keep(m, rev); err != nil {
				return 0, err
			}
		}
	}
	return dead, nil
}

// Close flushes queued transactions, rotates the active file so that
// everything committed is archived, waits for uploads, and releases the log.
func (l *FilesystemLog) Close() error {
	var errs []error
	if err := l.batching.Close(); err != nil {
		errs = append(errs, err)
	}
	l.rotator.stop()

	l.writeMu.Lock()
	l.mu.Lock()
	if l.opts.Archive != nil {
		if err := l.rotateLocked(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := l.active.Close(); err != nil {
		errs = append(errs, err)
	}
	l.mu.Unlock()
	l.writeMu.Unlock()

	if !l.archiver.stop(30 * time.Second) {
		klog.Warningf("log closed with archive operations still pending; they will resume on the next start")
	}

	l.mu.Lock()
	for _, f := range l.files {
		f.mu.Lock()
		if err := munmapFile(f.mapped); err != nil {
			errs = append(errs, err)
		}
		f.mapped = nil
		if f.reader != nil {
			if err := f.reader.Close(); err != nil {
				errs = append(errs, err)
			}
			f.reader = nil
		}
		f.mu.Unlock()
	}
	l.mu.Unlock()

	if l.removeOnClose {
		if err := os.RemoveAll(l.dir); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// SetListener sets the log listener
func (l *FilesystemLog) SetListener(listener LogListener) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.listener = listener
}

// syncDir fsyncs a directory so that new directory entries are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
