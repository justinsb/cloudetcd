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
// live (the latest put of each key), so a compacted file is sparse in
// revision: every record carries its revision, and a compacted file's name
// carries the revision range it covers.
package filesystemlog

import (
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

	"go.etcd.io/etcd/api/v3/mvccpb"
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

	// unmapAfter is how long a mapping of a file removed by compaction is
	// kept before it is unmapped: records decoded from it alias its bytes,
	// and a reader may still be using one.
	unmapAfter = time.Minute
)

// logFile is one file of the log. All but the last are closed.
type logFile struct {
	name string
	// first and last bound the revisions the file covers. A dense file (one
	// the log appended to) holds every revision in [first, last]; a
	// compacted file holds only the live ones.
	first, last Revision
	sparse      bool
	// revisions lists a sparse file's records' revisions, parallel to its
	// spans; nil for dense files, whose record i has revision first+i.
	revisions []Revision
	count     int
	// size is the file's length in bytes, header included.
	size int64
	// opened is when the file was created, for age-based rotation.
	opened time.Time
	// archived is whether the archive holds this (closed) file.
	archived bool
	// mapped is the read-only memory mapping of a closed file, made on first
	// read; records read from it alias its bytes (see
	// logcodec.DecodeMessageAlias), so old records live in the page cache
	// rather than on the heap.
	mapped []byte
	// reader is the file opened for positioned reads (the active file, or
	// any file where mapping is unavailable) and for mapping, opened on
	// first use and kept.
	reader *os.File
}

// archiveOp is work for the uploader: copy a closed file to the archive, or
// remove one that compaction superseded.
type archiveOp struct {
	name   string
	remove bool
}

// FilesystemLog is the log; see the package comment.
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

	// spans locates each record within its file, keyed by the file's first
	// revision. Maintained for the active file as records are appended;
	// rebuilt on first use for closed files.
	spansMu sync.Mutex
	spans   map[Revision][]logcodec.Span

	// cache holds recently written records (reads of the active file).
	cache *recordcache.Cache

	// mapMu serializes creating mappings and guards retired.
	mapMu sync.Mutex
	// retired holds mappings of files removed by compaction until it is
	// safe to unmap them.
	retired []retiredMapping

	// compactMu serializes compactions.
	compactMu sync.Mutex

	// archiveOps carries work to the uploader goroutine.
	archiveOps   chan archiveOp
	uploaderDone chan struct{}
	stop         chan struct{}
	rotatorDone  chan struct{}

	removeOnClose bool
}

type retiredMapping struct {
	mapped []byte
	at     time.Time
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
		dir:          dir,
		opts:         opts,
		spans:        map[Revision][]logcodec.Span{},
		cache:        recordcache.New(opts.CacheBytes),
		archiveOps:   make(chan archiveOp, 4096),
		uploaderDone: make(chan struct{}),
		stop:         make(chan struct{}),
		rotatorDone:  make(chan struct{}),
	}

	ctx := context.Background()
	if err := l.open(ctx); err != nil {
		return nil, err
	}

	l.batching = batch.NewBatching(l.lastRevision, l.commitBatch)
	go l.uploader(ctx)
	go l.rotator()
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
			f.revisions = revisions
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
			l.spans[f.first] = spans
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
				l.archiveOps <- archiveOp{name: f.name}
			}
		}
	}

	if last.archived || last.sparse {
		// An archived file is never appended to again (its object would no
		// longer match), nor is a compacted one; start the next file.
		if !last.archived && l.opts.Archive != nil {
			l.archiveOps <- archiveOp{name: last.name}
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
		first, last, sparse, err := parseFilename(e.Name())
		if err != nil {
			klog.Warningf("ignoring file with unexpected name %q: %v", e.Name(), err)
			continue
		}
		files = append(files, &logFile{name: e.Name(), first: first, last: last, sparse: sparse})
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
		first, _, _, err := parseFilename(name)
		if err == nil && l.superseded(files, first) {
			// Compaction replaced it locally but had not yet removed it.
			l.archiveOps <- archiveOp{name: name, remove: true}
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
	first := l.lastRevision + 1
	name := revisionToFilename(first)
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
	l.files = append(l.files, &logFile{name: name, first: first, last: first - 1, size: int64(len(header)), opened: time.Now()})
	l.spansMu.Lock()
	l.spans[first] = nil
	l.spansMu.Unlock()
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
	l.spansMu.Lock()
	l.spans[active.first] = append(l.spans[active.first], spans...)
	l.spansMu.Unlock()
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
// to the uploader, so the last file in the directory is always the active
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
		l.archiveOps <- archiveOp{name: old.name}
	}
	return nil
}

// rotator rotates the active file by age, and unmaps retired mappings.
func (l *FilesystemLog) rotator() {
	defer close(l.rotatorDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
		}
		l.writeMu.Lock()
		l.mu.Lock()
		active := l.files[len(l.files)-1]
		if active.count > 0 && time.Since(active.opened) >= l.opts.RotateAfter {
			if err := l.rotateLocked(); err != nil {
				klog.Errorf("rotating log file: %v", err)
			}
		}
		l.mu.Unlock()
		l.writeMu.Unlock()
		l.unmapRetired(false)
	}
}

// uploader copies closed files to the archive and removes superseded ones,
// retrying until each succeeds. A conflict means another writer is using
// the archive, which is fatal.
func (l *FilesystemLog) uploader(ctx context.Context) {
	defer close(l.uploaderDone)
	if l.opts.Archive == nil {
		return
	}
	for op := range l.archiveOps {
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
				break
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
	m, aliased, err := l.recordBytes(revision)
	if err != nil {
		return nil, err
	}
	if aliased {
		// Decode in place, aliasing the key and value bytes rather than
		// copying them. Not cached: the cache is for the write-recent set,
		// and a cached alias would pin the mapping past compaction.
		record, err := logcodec.DecodeMessageAlias(m)
		if err != nil {
			return nil, fmt.Errorf("failed to decode log record %d: %w", revision, err)
		}
		return record, nil
	}
	record, err := logcodec.DecodeMessage(m)
	if err != nil {
		return nil, fmt.Errorf("failed to decode log record %d: %w", revision, err)
	}
	l.cache.Put(revision, record)
	return record, nil
}

// recordBytes returns the encoded bytes of revision's record. For a closed
// file they alias its memory mapping (aliased is true) and must not be
// retained; for the active file, or where mapping is unavailable, they are
// one positioned read into a fresh buffer.
func (l *FilesystemLog) recordBytes(revision Revision) (m []byte, aliased bool, err error) {
	l.mu.RLock()
	f, ok := l.findFile(revision)
	active := ok && f == l.files[len(l.files)-1]
	l.mu.RUnlock()
	if !ok {
		return nil, false, fmt.Errorf("log entry for revision %d not found in any file", revision)
	}

	if !active {
		m, err := l.mapping(f)
		if err != nil {
			return nil, false, err
		}
		if m != nil {
			spans, err := l.fileSpansFrom(f, m)
			if err != nil {
				return nil, false, err
			}
			pos, err := f.position(revision, len(spans))
			if err != nil {
				return nil, false, err
			}
			span := spans[pos]
			return m[span.Offset : span.Offset+span.Length], true, nil
		}
	}

	spans, err := l.fileSpans(f)
	if err != nil {
		return nil, false, err
	}
	pos, err := f.position(revision, len(spans))
	if err != nil {
		return nil, false, err
	}
	span := spans[pos]
	file, err := l.reader(f)
	if err != nil {
		return nil, false, err
	}
	buf := make([]byte, span.Length)
	if _, err := file.ReadAt(buf, int64(span.Offset)); err != nil {
		return nil, false, fmt.Errorf("failed to read record %d from log file %s: %w", revision, f.name, err)
	}
	return buf, false, nil
}

// position returns the index of revision's record in the file, or
// ErrCompacted if the file is sparse and compaction dropped it.
func (f *logFile) position(revision Revision, count int) (int, error) {
	if !f.sparse {
		pos := int(revision - f.first)
		if pos < 0 || pos >= count {
			return 0, fmt.Errorf("log entry for revision %d not found in file %s (%d records)", revision, f.name, count)
		}
		return pos, nil
	}
	pos := sort.Search(len(f.revisions), func(i int) bool { return f.revisions[i] >= revision })
	if pos >= len(f.revisions) || f.revisions[pos] != revision {
		return 0, fmt.Errorf("revision %d: %w", revision, persistence.ErrCompacted)
	}
	return pos, nil
}

// mapping returns a closed file's memory mapping, creating it on first use
// from the file's read handle, or nil where the platform has no memory
// mapping. A mapping that fails where it should work is fatal: the
// alternative would be a positioned read per record, a large and silent
// change in how the server performs.
func (l *FilesystemLog) mapping(f *logFile) ([]byte, error) {
	l.mapMu.Lock()
	defer l.mapMu.Unlock()
	if f.mapped == nil {
		file, err := l.readerLocked(f)
		if err != nil {
			return nil, err
		}
		m, err := mmapFile(file, f.size)
		if errors.Is(err, errNoMmap) {
			return nil, nil
		}
		if err != nil {
			panic(fmt.Sprintf("memory-mapping log file %s (%d bytes): %v", f.name, f.size, err))
		}
		f.mapped = m
	}
	return f.mapped, nil
}

// reader returns a file's handle for positioned reads, opening it on first
// use and keeping it open.
func (l *FilesystemLog) reader(f *logFile) (*os.File, error) {
	l.mapMu.Lock()
	defer l.mapMu.Unlock()
	return l.readerLocked(f)
}

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

// fileSpans returns the record spans of a file, scanning its length
// prefixes on first use for closed files.
func (l *FilesystemLog) fileSpans(f *logFile) ([]logcodec.Span, error) {
	l.spansMu.Lock()
	spans, ok := l.spans[f.first]
	l.spansMu.Unlock()
	if ok {
		return spans, nil
	}
	data, err := os.ReadFile(filepath.Join(l.dir, f.name))
	if err != nil {
		return nil, fmt.Errorf("failed to read log file %s: %w", f.name, err)
	}
	return l.fileSpansFrom(f, data)
}

// fileSpansFrom is fileSpans for a file whose bytes are already at hand.
func (l *FilesystemLog) fileSpansFrom(f *logFile, data []byte) ([]logcodec.Span, error) {
	l.spansMu.Lock()
	spans, ok := l.spans[f.first]
	l.spansMu.Unlock()
	if ok {
		return spans, nil
	}
	spans, _, err := logcodec.Offsets(data)
	if err != nil {
		return nil, fmt.Errorf("indexing log file %s: %w", f.name, err)
	}
	l.spansMu.Lock()
	l.spans[f.first] = spans
	l.spansMu.Unlock()
	return spans, nil
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
			record, err := logcodec.DecodeMessage(data[span.Offset : span.Offset+span.Length])
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

// Compact rewrites the closed files at or below through, keeping only their
// live records: puts that live(key, revision) confirms are still the latest
// for their key. Deletes and superseded puts are dropped. Files are merged
// while the result stays under RotateBytes; a compacted file that is still
// mostly live is left alone. The originals are removed (from the archive
// too) once the replacement is durable.
func (l *FilesystemLog) Compact(ctx context.Context, through Revision, live func(key []byte, revision Revision) bool) error {
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

	// Group consecutive files by their live bytes so that each output stays
	// under RotateBytes: a dense file that is mostly dead merges with its
	// neighbours; a mostly live one stays on its own.
	var group []*logFile
	var groupBytes int64
	flush := func() error {
		if len(group) == 0 {
			return nil
		}
		err := l.compactGroup(ctx, group, live)
		group, groupBytes = nil, 0
		return err
	}
	for _, f := range eligible {
		var liveBytes int64
		if _, err := l.liveRecords(f, live, func(m []byte, rev Revision) { liveBytes += int64(len(m)) + 4 }); err != nil {
			return err
		}
		if groupBytes > 0 && groupBytes+liveBytes > l.opts.RotateBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		group = append(group, f)
		groupBytes += liveBytes
	}
	return flush()
}

// compactGroup replaces a run of consecutive closed files with one file
// holding their live records.
func (l *FilesystemLog) compactGroup(ctx context.Context, group []*logFile, live func(key []byte, revision Revision) bool) error {
	first, last := group[0].first, group[len(group)-1].last
	name := rangeToFilename(first, last)
	if len(group) == 1 && group[0].name == name {
		// Already compacted into this exact file; see whether it has rotted.
		if dead, err := l.liveRecords(group[0], live, nil); err != nil {
			return err
		} else if dead*10 < group[0].count {
			return nil
		}
	}

	buf := append([]byte(nil), logcodec.Header()...)
	var revisions []Revision
	var spans []logcodec.Span
	var kept int
	for _, f := range group {
		if _, err := l.liveRecords(f, live, func(m []byte, rev Revision) {
			var span logcodec.Span
			buf, span = logcodec.AppendRaw(buf, m)
			spans = append(spans, span)
			revisions = append(revisions, rev)
			kept++
		}); err != nil {
			return err
		}
	}

	tmp := filepath.Join(l.dir, name+".tmp")
	if err := os.WriteFile(tmp, buf, 0644); err != nil {
		return fmt.Errorf("writing compacted log file %s: %w", name, err)
	}
	if err := syncFile(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(l.dir, name)); err != nil {
		return fmt.Errorf("renaming compacted log file %s: %w", name, err)
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}
	compacted := &logFile{name: name, first: first, last: last, sparse: true, revisions: revisions, count: kept, size: int64(len(buf)), opened: time.Now(), archived: false}

	// Swap it into the index, retire the originals' mappings, and remove
	// the originals.
	l.mu.Lock()
	i := slices.Index(l.files, group[0])
	l.files = slices.Replace(l.files, i, i+len(group), compacted)
	l.spansMu.Lock()
	for _, f := range group {
		delete(l.spans, f.first)
	}
	l.spans[first] = spans
	l.spansMu.Unlock()
	l.mu.Unlock()

	l.mapMu.Lock()
	for _, f := range group {
		if f.mapped != nil {
			l.retired = append(l.retired, retiredMapping{mapped: f.mapped, at: time.Now()})
			f.mapped = nil
		}
		if f.reader != nil && f.name != name {
			f.reader.Close()
			f.reader = nil
		}
	}
	l.mapMu.Unlock()

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
		len(group), first, last, name, kept, oldBytes>>20, compacted.size>>20)

	if l.opts.Archive != nil {
		l.archiveOps <- archiveOp{name: name}
		for _, f := range group {
			if f.name != name {
				l.archiveOps <- archiveOp{name: f.name, remove: true}
			}
		}
	}
	return nil
}

// liveRecords scans a closed file, calling keep for each live record with
// its raw bytes and revision, and returns the number of records dropped. A
// record is live if any of its puts is still the latest for its key;
// deletes are never live.
func (l *FilesystemLog) liveRecords(f *logFile, live func(key []byte, revision Revision) bool, keep func(m []byte, rev Revision)) (int, error) {
	data, err := l.mapping(f)
	if err != nil {
		return 0, err
	}
	if data == nil {
		if data, err = os.ReadFile(filepath.Join(l.dir, f.name)); err != nil {
			return 0, fmt.Errorf("failed to read log file %s: %w", f.name, err)
		}
	}
	spans, revisions, err := logcodec.Offsets(data)
	if err != nil {
		return 0, fmt.Errorf("indexing log file %s: %w", f.name, err)
	}
	dropped := 0
	for i, span := range spans {
		m := data[span.Offset : span.Offset+span.Length]
		record, err := logcodec.DecodeMessageAlias(m)
		if err != nil {
			return 0, fmt.Errorf("failed to decode log record %d from file %s: %w", revisions[i], f.name, err)
		}
		isLive := false
		for _, ev := range record.Events {
			if ev.Type == mvccpb.PUT && live(ev.Kv.Key, revisions[i]) {
				isLive = true
				break
			}
		}
		if !isLive {
			dropped++
			continue
		}
		if keep != nil {
			keep(m, revisions[i])
		}
	}
	return dropped, nil
}

// unmapRetired unmaps mappings retired long enough ago (or all of them).
func (l *FilesystemLog) unmapRetired(all bool) {
	l.mapMu.Lock()
	defer l.mapMu.Unlock()
	kept := l.retired[:0]
	for _, r := range l.retired {
		if all || time.Since(r.at) >= unmapAfter {
			if err := munmapFile(r.mapped); err != nil {
				klog.Errorf("unmapping retired log file: %v", err)
			}
			continue
		}
		kept = append(kept, r)
	}
	l.retired = kept
}

// Close flushes queued transactions, rotates the active file so that
// everything committed is archived, waits for uploads, and releases the log.
func (l *FilesystemLog) Close() error {
	var errs []error
	if err := l.batching.Close(); err != nil {
		errs = append(errs, err)
	}
	close(l.stop)
	<-l.rotatorDone

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

	close(l.archiveOps)
	select {
	case <-l.uploaderDone:
	case <-time.After(30 * time.Second):
		klog.Warningf("log closed with archive operations still pending; they will resume on the next start")
	}

	l.unmapRetired(true)
	l.mapMu.Lock()
	for _, f := range l.files {
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
	}
	l.mapMu.Unlock()

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

// syncFile fsyncs a file.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
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

// revisionToFilename names a dense file by its first revision.
func revisionToFilename(first Revision) string {
	return fmt.Sprintf("%016x.log", uint64(first))
}

// rangeToFilename names a compacted file by the revision range it covers.
func rangeToFilename(first, last Revision) string {
	return fmt.Sprintf("%016x-%016x.log", uint64(first), uint64(last))
}

// parseFilename is the inverse of revisionToFilename and rangeToFilename.
// A dense file's last revision is not in its name (it is derived from its
// record count) and is returned as 0.
func parseFilename(filename string) (first, last Revision, sparse bool, err error) {
	if !strings.HasSuffix(filename, ".log") {
		return 0, 0, false, fmt.Errorf("invalid filename format: %s", filename)
	}
	base := strings.TrimSuffix(filename, ".log")
	parts := strings.SplitN(base, "-", 2)
	f, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("invalid filename format: %s", filename)
	}
	if len(parts) == 1 {
		return Revision(f), 0, false, nil
	}
	l, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil || l < f {
		return 0, 0, false, fmt.Errorf("invalid filename format: %s", filename)
	}
	return Revision(f), Revision(l), true, nil
}
