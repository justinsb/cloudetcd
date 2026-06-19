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

package gcslog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/batch"
	"k8s.io/klog/v2"
)

type Revision = persistence.Revision

type logFileMeta struct {
	firstRevision Revision
	count         int
}

// GCSLog is a Google Cloud Storage-backed implementation of the Log interface
type GCSLog struct {
	client *storage.Client
	bucket *storage.BucketHandle
	prefix string // Prefix for log objects

	batching *batch.Batching

	cache *Cache

	mu           sync.RWMutex
	lastRevision Revision // Highest revision number
	listener     persistence.LogListener

	// logFiles is an in-memory index of log files, sorted by firstRevision
	logFiles []logFileMeta
}

var _ persistence.Log = &GCSLog{}

type persistedBatch struct {
	Records []*persistence.LogRecord
}

// NewGCSLog creates a new Google Cloud Storage-backed log
func NewGCSLog(ctx context.Context, bucketName, prefix string) (*GCSLog, error) {
	// client, err := storage.NewClient(ctx)
	client, err := storage.NewGRPCClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	bucket := client.Bucket(bucketName)

	// Check if bucket exists and is accessible
	if _, err := bucket.Attrs(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to access bucket %s: %w", bucketName, err)
	}

	log := &GCSLog{
		client: client,
		bucket: bucket,
		prefix: prefix,
		cache:  NewCache(),
	}

	// Replay existing log entries to determine current revision
	if err := log.replay(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to replay existing log: %w", err)
	}

	log.batching = batch.NewBatching(log.lastRevision, log.commitBatch)

	return log, nil
}

// replay reads all existing log objects to determine the current revision
func (g *GCSLog) replay(ctx context.Context) error {
	query := &storage.Query{
		Prefix: g.prefix,
	}

	it := g.bucket.Objects(ctx, query)
	g.logFiles = nil

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// Parse revision from object name
		objectName := attrs.Name
		firstRevision, count, err := g.objectNameToMeta(objectName)
		if err != nil {
			// Skip invalid object names
			klog.V(2).Infof("Skipping invalid object name: %s", objectName)
			continue
		}

		g.logFiles = append(g.logFiles, logFileMeta{firstRevision: firstRevision, count: count})
	}

	// Sort the log files by revision
	slices.SortFunc(g.logFiles, func(a, b logFileMeta) int {
		if a.firstRevision < b.firstRevision {
			return -1
		}
		if a.firstRevision > b.firstRevision {
			return 1
		}
		return 0
	})

	// Find the highest revision
	if len(g.logFiles) > 0 {
		lastFile := g.logFiles[len(g.logFiles)-1]
		g.lastRevision = lastFile.firstRevision + Revision(lastFile.count) - 1
	}

	// Preload all entries in parallel for faster startup
	if len(g.logFiles) > 0 {
		klog.V(2).Infof("Preloading %d log objects for faster startup...", len(g.logFiles))
		g.preloadBatch(ctx, g.logFiles)

		klog.V(2).Infof("Replayed %d log objects, current revision: %d", len(g.logFiles), g.lastRevision)
	}

	return nil
}

// Append adds a new record to the log and returns the revision number
func (g *GCSLog) Append(ctx context.Context, logRecord *persistence.LogRecord, txnMeta *persistence.TxnMeta) (Revision, bool, error) {
	return g.batching.Add(ctx, logRecord, txnMeta)
}

// commitBatch commits all transactions in the current batch
func (l *GCSLog) commitBatch(ctx context.Context, lastLogPosition Revision, batch *batch.BatchCommit) error {
	log := klog.FromContext(ctx)

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

	startRevision := l.lastRevision + 1
	count := len(batch.Transactions)

	// Create object name with hex-encoded revision
	objectName := l.batchToObjectName(startRevision, count)
	obj := l.bucket.Object(objectName)

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
		return fmt.Errorf("failed to marshal log record: %w", err)
	}

	// Write to GCS
	log.Info("Writing log entry to GCS object", "objectName", objectName)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/json"

	if _, err := writer.Write(b); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write log record to GCS: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	l.logFiles = append(l.logFiles, logFileMeta{firstRevision: startRevision, count: count})
	newRevision := l.lastRevision + Revision(len(batch.Transactions))
	l.lastRevision = newRevision

	l.cache.notifyBatch(l.lastRevision, data)

	if l.listener != nil {
		l.listener.OnLogEntry(newRevision)
	}

	klog.V(2).Infof("Executed batch of %d transactions, revisions %d-%d",
		len(batch.Transactions), startRevision, newRevision)

	return nil
}

// GetCurrentRevision returns the current revision number
func (g *GCSLog) GetCurrentRevision(ctx context.Context) (Revision, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.lastRevision, nil
}

// GetLogEntry returns the log entry for the given revision
func (g *GCSLog) GetLogEntry(revision Revision) (*persistence.LogRecord, error) {
	ctx := context.Background() // TODO: Consider passing context through interface
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.getLogEntry(ctx, revision)
}

func (g *GCSLog) loadBatch(ctx context.Context, fileMeta logFileMeta) (*persistedBatch, error) {
	log := klog.FromContext(ctx)

	objectName := g.batchToObjectName(fileMeta.firstRevision, fileMeta.count)
	obj := g.bucket.Object(objectName)

	log.Info("Reading log entry from GCS object", "objectName", objectName)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS reader for object %s: %w", objectName, err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data from GCS object %s: %w", objectName, err)
	}

	pBatch := &persistedBatch{}
	if err := json.Unmarshal(data, pBatch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal log record from GCS object %s: %w", objectName, err)
	}
	return pBatch, nil
}

func (g *GCSLog) getLogEntry(ctx context.Context, revision Revision) (*persistence.LogRecord, error) {
	// log := klog.FromContext(ctx)

	fileMeta, ok := g.findFileForRevision(revision)
	if !ok {
		return nil, fmt.Errorf("log entry for revision %d not found in any file", revision)
	}

	// This is a bit inefficient, as we fetch the whole batch to get one entry.
	// However, the cache helps, and often we will read sequentially.
	logEntry, err := g.cache.Get(ctx, revision, g.loadBatch, fileMeta)
	return logEntry, err
}

// findFileForRevision finds the log file containing the given revision.
// It uses the in-memory index.
func (f *GCSLog) findFileForRevision(revision Revision) (logFileMeta, bool) {
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

// preloadBatch starts loading multiple log entries into the cache in parallel
func (g *GCSLog) preloadBatch(ctx context.Context, logFiles []logFileMeta) {
	log := klog.FromContext(ctx)

	// Use a semaphore to limit concurrent downloads to avoid overwhelming GCS
	const maxConcurrent = 10
	sem := make(chan struct{}, maxConcurrent)

	var wg sync.WaitGroup

	for _, fileMeta := range logFiles {
		// We check the first revision in the batch; if it's cached we assume the whole batch is.
		// This is not perfect, but it's a reasonable heuristic.
		if g.cache.Has(fileMeta.firstRevision) {
			continue
		}

		wg.Add(1)
		go func(meta logFileMeta) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// This will fetch the batch and populate the cache
			_, err := g.getLogEntry(ctx, meta.firstRevision)
			if err != nil {
				log.Error(err, "failed to preload log entry", "revision", meta.firstRevision)
			}
		}(fileMeta)
	}

	// Wait for all preloads to complete
	wg.Wait()
}

// Read reads records from the log starting from the given revision
func (g *GCSLog) Read(ctx context.Context, fromRevision Revision, callback func(Revision, *persistence.LogRecord) bool) error {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for _, fileMeta := range g.logFiles {
		fileLastRevision := fileMeta.firstRevision + Revision(fileMeta.count) - 1
		if fileLastRevision < fromRevision {
			continue
		}

		// This will use the cache if populated, or fetch from GCS
		// We fetch the first record to trigger a load of the whole batch
		if _, err := g.getLogEntry(ctx, fileMeta.firstRevision); err != nil {
			return fmt.Errorf("failed to get log entry for revision %d: %w", fileMeta.firstRevision, err)
		}

		for i := 0; i < fileMeta.count; i++ {
			revision := fileMeta.firstRevision + Revision(i)
			if revision < fromRevision {
				continue
			}
			record, err := g.getLogEntry(ctx, revision)
			if err != nil {
				return fmt.Errorf("failed to get log entry for revision %d: %w", revision, err)
			}
			if record == nil {
				return fmt.Errorf("log entry not found for revision %d", revision)
			}
			if !callback(revision, record) {
				return nil
			}
		}
	}

	return nil
}

// Close closes the log and releases any resources
func (g *GCSLog) Close() error {
	if err := g.batching.Close(); err != nil {
		return err
	}

	if g.client != nil {
		return g.client.Close()
	}
	return nil
}

// SetListener sets the log listener
func (g *GCSLog) SetListener(listener persistence.LogListener) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.listener = listener
}

// batchToObjectName converts a first-revision and count to a filename
func (g *GCSLog) batchToObjectName(firstRevision Revision, count int) string {
	return fmt.Sprintf("%s%016x-%x.log", g.prefix, uint64(firstRevision), count)
}

// objectNameToMeta converts a GCS object name to a revision number
func (g *GCSLog) objectNameToMeta(objectName string) (Revision, int, error) {
	if !strings.HasPrefix(objectName, g.prefix) {
		return 0, 0, fmt.Errorf("object name does not have expected prefix: %s", objectName)
	}
	trimmed := strings.TrimPrefix(objectName, g.prefix)

	if !strings.HasSuffix(trimmed, ".log") {
		return 0, 0, fmt.Errorf("object name does not have .log suffix: %s", objectName)
	}
	trimmed = strings.TrimSuffix(trimmed, ".log")

	parts := strings.SplitN(trimmed, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid filename format (expected <revision>-<count>.log): %s", objectName)
	}

	revisionVal, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parsing revision from %q: %w", objectName, err)
	}

	countVal, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parsing count from %q: %w", objectName, err)
	}

	return Revision(revisionVal), int(countVal), nil
}
