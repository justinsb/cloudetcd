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
// cache of recently written or read records; anything else is a positioned
// read of one record. On a machine with no local files, the archive is
// downloaded first.
package filesystemlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
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
}

// Options configures a FilesystemLog.
type Options struct {
	// CacheBytes bounds the in-memory cache of decoded records; records not
	// in it are read from disk on demand. <= 0 means unbounded.
	CacheBytes int64
	// RotateBytes rotates the active file once it is this large.
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

// logFile is one file of the log. All but the last are closed.
type logFile struct {
	name  string
	first Revision
	count int
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
	// any file where mapping is unavailable), opened on first read and kept.
	reader *os.File
}

func (f *logFile) last() Revision { return f.first + Revision(f.count) - 1 }

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

	// cache holds recently written and read records.
	cache *recordcache.Cache

	// mapMu serializes creating mappings.
	mapMu sync.Mutex

	// uploads carries closed files to the uploader goroutine.
	uploads      chan string
	uploaderDone chan struct{}
	stop         chan struct{}
	rotatorDone  chan struct{}

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
		dir:          dir,
		opts:         opts,
		spans:        map[Revision][]logcodec.Span{},
		cache:        recordcache.New(opts.CacheBytes),
		uploads:      make(chan string, 1024),
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
		spans, complete, err := logcodec.Scan(data)
		if err != nil {
			return fmt.Errorf("log file %s: %w", f.name, err)
		}
		if complete < len(data) {
			if i < len(files)-1 {
				return fmt.Errorf("log file %s is truncated at byte %d but is not the last file; recovery needed", f.name, complete)
			}
			// A write cut short by a crash: nothing past it was acknowledged.
			klog.Warningf("log file %s ends in a torn record; truncating from %d to %d bytes", f.name, len(data), complete)
			if err := os.Truncate(filepath.Join(l.dir, f.name), int64(complete)); err != nil {
				return fmt.Errorf("truncating log file %s: %w", f.name, err)
			}
		}
		f.count = len(spans)
		f.size = int64(complete)
		if i > 0 {
			prev := files[i-1]
			if f.first != prev.first+Revision(prev.count) {
				return fmt.Errorf("log has a gap: %s ends at revision %d but %s starts at %d; recovery needed", prev.name, prev.last(), f.name, f.first)
			}
		} else if f.first != 1 {
			return fmt.Errorf("log starts at revision %d, not 1; recovery needed", f.first)
		}
		if i == len(files)-1 {
			l.spans[f.first] = spans
		}
	}

	l.files = files
	if len(files) == 0 {
		return l.newActiveFile()
	}
	last := files[len(files)-1]
	l.lastRevision = last.last()

	// Closed files that never made it to the archive (a crash after
	// rotating, before the upload finished).
	if l.opts.Archive != nil {
		for _, f := range files[:len(files)-1] {
			if !f.archived {
				l.uploads <- f.name
			}
		}
	}

	if last.archived {
		// An archived file is never appended to again (its object would no
		// longer match); start the next one.
		return l.newActiveFile()
	}
	l.active, err = os.OpenFile(filepath.Join(l.dir, last.name), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening log file %s for append: %w", last.name, err)
	}
	last.opened = time.Now()
	return nil
}

// listFiles returns the log files in the directory, sorted.
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
		first, err := filenameToRevision(e.Name())
		if err != nil {
			klog.Warningf("ignoring file with unexpected name %q: %v", e.Name(), err)
			continue
		}
		files = append(files, &logFile{name: e.Name(), first: first})
	}
	slices.SortFunc(files, func(a, b *logFile) int { return int(a.first) - int(b.first) })
	return files, nil
}

// reconcileArchive brings the directory and the archive into agreement: a
// directory with no files is restored from the archive; otherwise every
// archived file must be present locally (the archive is written only by
// this log), and closed local files missing from the archive are uploaded.
func (l *FilesystemLog) reconcileArchive(ctx context.Context, files []*logFile) ([]*logFile, error) {
	names, err := l.opts.Archive.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing archive: %w", err)
	}
	archived := map[string]bool{}
	for _, name := range names {
		archived[name] = true
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
		f, ok := local[name]
		if !ok {
			return nil, fmt.Errorf("archive holds %s, which this log does not have; is another instance writing to the archive?", name)
		}
		f.archived = true
	}
	return files, nil
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
	l.files = append(l.files, &logFile{name: name, first: first, size: int64(len(header)), opened: time.Now()})
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

type persistedBatch struct {
	Records []*persistence.LogRecord
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
	buf, spans, err := logcodec.AppendRecords(nil, records)
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
		l.uploads <- old.name
	}
	return nil
}

// rotator rotates the active file by age.
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
	}
}

// uploader copies closed files to the archive, retrying until each succeeds.
// A conflict means another writer is using the archive, which is fatal.
func (l *FilesystemLog) uploader(ctx context.Context) {
	defer close(l.uploaderDone)
	if l.opts.Archive == nil {
		return
	}
	for name := range l.uploads {
		backoff := time.Second
		for {
			err := l.opts.Archive.Upload(ctx, name, filepath.Join(l.dir, name))
			if err == nil {
				klog.V(2).Infof("archived log file %s", name)
				l.mu.Lock()
				for _, f := range l.files {
					if f.name == name {
						f.archived = true
					}
				}
				l.mu.Unlock()
				break
			}
			if errors.Is(err, persistence.ErrRevisionConflict) {
				panic(fmt.Sprintf("archiving log file %s: %v", name, err))
			}
			klog.Errorf("archiving log file %s (retrying in %s): %v", name, backoff, err)
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

	l.mu.RLock()
	f, ok := l.findFile(revision)
	closed := ok && f != l.files[len(l.files)-1]
	l.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("log entry for revision %d not found in any file", revision)
	}
	pos := int(revision - f.first)

	var record *LogRecord
	if m := l.mapping(f, closed); m != nil {
		// A closed file: decode straight out of its mapping, aliasing the
		// key and value bytes rather than copying them.
		spans, err := l.fileSpansFrom(f, m)
		if err != nil {
			return nil, err
		}
		if pos < 0 || pos >= len(spans) {
			return nil, fmt.Errorf("log entry for revision %d not found in file %s (%d records)", revision, f.name, len(spans))
		}
		span := spans[pos]
		record, err = logcodec.DecodeMessageAlias(m[span.Offset : span.Offset+span.Length])
		if err != nil {
			return nil, fmt.Errorf("failed to decode log record %d from file %s: %w", revision, f.name, err)
		}
	} else {
		// The active file (or a platform without mmap): one positioned read
		// of exactly this record's bytes.
		spans, err := l.fileSpans(f)
		if err != nil {
			return nil, err
		}
		if pos < 0 || pos >= len(spans) {
			return nil, fmt.Errorf("log entry for revision %d not found in file %s (%d records)", revision, f.name, len(spans))
		}
		span := spans[pos]
		file, err := l.reader(f)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, span.Length)
		if _, err := file.ReadAt(buf, int64(span.Offset)); err != nil {
			return nil, fmt.Errorf("failed to read record %d from log file %s: %w", revision, f.name, err)
		}
		record, err = logcodec.DecodeMessage(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to decode log record %d from file %s: %w", revision, f.name, err)
		}
	}
	l.cache.Put(revision, record)
	return record, nil
}

// mapping returns the memory mapping of a closed file, creating it on first
// use, or nil if the file is active (still growing) or the platform has no
// memory mapping. A mapping that fails where it should work is fatal: the
// fallback would be a positioned read per record, a large and silent change
// in how the server performs.
func (l *FilesystemLog) mapping(f *logFile, closed bool) []byte {
	if !closed {
		return nil
	}
	l.mapMu.Lock()
	defer l.mapMu.Unlock()
	if f.mapped == nil {
		m, err := mapFile(filepath.Join(l.dir, f.name), f.size)
		if errors.Is(err, errNoMmap) {
			return nil
		}
		if err != nil {
			panic(fmt.Sprintf("memory-mapping log file %s (%d bytes): %v", f.name, f.size, err))
		}
		f.mapped = m
	}
	return f.mapped
}

// reader returns a file's handle for positioned reads, opening it on first
// use and keeping it open.
func (l *FilesystemLog) reader(f *logFile) (*os.File, error) {
	l.mapMu.Lock()
	defer l.mapMu.Unlock()
	if f.reader == nil {
		file, err := os.Open(filepath.Join(l.dir, f.name))
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", f.name, err)
		}
		f.reader = file
	}
	return f.reader, nil
}

// fileSpansFrom is fileSpans for a file whose bytes are already at hand.
func (l *FilesystemLog) fileSpansFrom(f *logFile, data []byte) ([]logcodec.Span, error) {
	l.spansMu.Lock()
	spans, ok := l.spans[f.first]
	l.spansMu.Unlock()
	if ok {
		return spans, nil
	}
	spans, err := logcodec.Offsets(data)
	if err != nil {
		return nil, fmt.Errorf("indexing log file %s: %w", f.name, err)
	}
	l.spansMu.Lock()
	l.spans[f.first] = spans
	l.spansMu.Unlock()
	return spans, nil
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
	spans, err = logcodec.Offsets(data)
	if err != nil {
		return nil, fmt.Errorf("indexing log file %s: %w", f.name, err)
	}
	l.spansMu.Lock()
	l.spans[f.first] = spans
	l.spansMu.Unlock()
	return spans, nil
}

// findFile finds the file containing revision. Called with mu held.
func (l *FilesystemLog) findFile(revision Revision) (*logFile, bool) {
	// Recent revisions are the most requested; search from the end.
	for i := len(l.files) - 1; i >= 0; i-- {
		f := l.files[i]
		if f.first <= revision {
			return f, revision <= f.last()
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
		if f.count == 0 || f.last() < fromRevision {
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
		records, err := logcodec.Decode(data)
		if err != nil {
			return fmt.Errorf("failed to decode log file %s: %w", f.name, err)
		}
		for j, record := range records {
			revision := f.first + Revision(j)
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

// Close flushes queued transactions, rotates the active file so that
// everything committed is archived, waits for uploads, and releases the log.
func (l *FilesystemLog) Close() error {
	if err := l.batching.Close(); err != nil {
		return err
	}
	close(l.stop)
	<-l.rotatorDone

	l.writeMu.Lock()
	l.mu.Lock()
	var err error
	if l.opts.Archive != nil {
		err = l.rotateLocked()
	}
	if cerr := l.active.Close(); err == nil {
		err = cerr
	}
	l.mu.Unlock()
	l.writeMu.Unlock()

	close(l.uploads)
	select {
	case <-l.uploaderDone:
	case <-time.After(30 * time.Second):
		klog.Warningf("log closed with uploads still pending; they will resume on the next start")
	}

	l.mapMu.Lock()
	for _, f := range l.files {
		if uerr := unmapFile(f.mapped); uerr != nil && err == nil {
			err = uerr
		}
		f.mapped = nil
		if f.reader != nil {
			if cerr := f.reader.Close(); cerr != nil && err == nil {
				err = cerr
			}
			f.reader = nil
		}
	}
	l.mapMu.Unlock()

	if l.removeOnClose {
		if rerr := os.RemoveAll(l.dir); err == nil {
			err = rerr
		}
	}
	return err
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

// revisionToFilename names the file whose first record has this revision.
func revisionToFilename(first Revision) string {
	return fmt.Sprintf("%016x.log", uint64(first))
}

// filenameToRevision is the inverse of revisionToFilename.
func filenameToRevision(filename string) (Revision, error) {
	if !strings.HasSuffix(filename, ".log") {
		return 0, fmt.Errorf("invalid filename format: %s", filename)
	}
	n, err := strconv.ParseUint(strings.TrimSuffix(filename, ".log"), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid filename format: %s", filename)
	}
	return Revision(n), nil
}
