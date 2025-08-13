package gcslog

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/batch"
	"k8s.io/klog/v2"
)

type Revision = persistence.Revision

// GCSLog is a Google Cloud Storage-backed implementation of the Log interface
type GCSLog struct {
	batching *batch.Batching

	mu           sync.RWMutex
	client       *storage.Client
	bucket       *storage.BucketHandle
	prefix       string   // Prefix for log objects
	lastRevision Revision // Highest revision number
	listener     persistence.LogListener

	cache *Cache

	// pendingBatch []*persistence.PendingTransaction
	// batchTimer   *time.Timer
	// batchTimeout time.Duration
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
		// batchTimeout: 50 * time.Millisecond, // Longer timeout for network storage
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
	var revisions []Revision

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
		revision, err := g.objectNameToRevision(objectName)
		if err != nil {
			// Skip invalid object names
			klog.V(2).Infof("Skipping invalid object name: %s", objectName)
			continue
		}

		revisions = append(revisions, revision)
	}

	// Find the highest revision and preload all entries
	if len(revisions) > 0 {
		slices.Sort(revisions)
		g.lastRevision = revisions[len(revisions)-1]

		// Preload all entries in parallel for faster startup
		klog.V(2).Infof("Preloading %d log entries for faster startup...", len(revisions))
		g.preloadBatch(ctx, revisions)

		klog.V(2).Infof("Replayed %d log entries, current revision: %d", len(revisions), g.lastRevision)
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

	// Create object name with hex-encoded revision
	objectName := l.revisionToObjectName(startRevision)
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

	newRevision := l.lastRevision + Revision(len(batch.Transactions))

	if l.listener != nil {
		l.listener.OnLogEntry(newRevision)
	}

	klog.V(2).Infof("Executed batch of %d transactions, revisions %d-%d",
		len(batch.Transactions), startRevision+1, newRevision)

	return nil
}

// // appendImmediate performs immediate commit without batching
// func (g *GCSLog) appendImmediate(ctx context.Context, conditionPosition Revision, logRecord *persistence.LogRecord) (Revision, bool, error) {
// 	log := klog.FromContext(ctx)

// 	g.mu.Lock()
// 	defer g.mu.Unlock()

// 	if conditionPosition != g.revision {
// 		return 0, false, nil
// 	}

// 	// Increment revision number
// 	g.lastRevision++
// 	newRevision := g.lastRevision

// 	// Create object name with hex-encoded revision
// 	objectName := g.revisionToObjectName(g.revision)
// 	obj := g.bucket.Object(objectName)

// 	// Serialize record to JSON
// 	data, err := json.Marshal(logRecord)
// 	if err != nil {
// 		return 0, false, fmt.Errorf("failed to marshal log record: %w", err)
// 	}

// 	// Write to GCS
// 	log.Info("Writing log entry to GCS object", "objectName", objectName)
// 	writer := obj.NewWriter(ctx)
// 	writer.ContentType = "application/json"

// 	if _, err := writer.Write(data); err != nil {
// 		writer.Close()
// 		return 0, false, fmt.Errorf("failed to write log record to GCS: %w", err)
// 	}

// 	if err := writer.Close(); err != nil {
// 		return 0, false, fmt.Errorf("failed to close GCS writer: %w", err)
// 	}

// 	if g.listener != nil {
// 		g.listener.OnLogEntry(newRevision)
// 	}

// 	klog.V(3).Infof("Appended log entry at revision %d to GCS object %s", newRevision, objectName)
// 	return newRevision, true, nil
// }

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

func (g *GCSLog) getLogEntry(ctx context.Context, revision Revision) (*persistence.LogRecord, error) {
	log := klog.FromContext(ctx)

	logEntry, err := g.cache.Get(revision, func() (*persistence.LogRecord, error) {
		objectName := g.revisionToObjectName(revision)
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

		record := &persistence.LogRecord{}
		if err := json.Unmarshal(data, record); err != nil {
			return nil, fmt.Errorf("failed to unmarshal log record from GCS object %s: %w", objectName, err)
		}

		return record, nil
	})
	return logEntry, err
}

// preloadBatch starts loading multiple log entries into the cache in parallel
func (g *GCSLog) preloadBatch(ctx context.Context, revisions []Revision) {
	log := klog.FromContext(ctx)

	// Use a semaphore to limit concurrent downloads to avoid overwhelming GCS
	const maxConcurrent = 10
	sem := make(chan struct{}, maxConcurrent)

	var wg sync.WaitGroup

	for _, revision := range revisions {
		// Skip if already cached
		if g.cache.Has(revision) {
			continue
		}

		wg.Add(1)
		go func(rev Revision) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			_, err := g.getLogEntry(ctx, rev)
			if err != nil {
				log.Error(err, "failed to preload log entry", "revision", rev)
			}
		}(revision)
	}

	// Wait for all preloads to complete
	wg.Wait()
}

// Read reads records from the log starting from the given revision
func (g *GCSLog) Read(ctx context.Context, fromRevision Revision, callback func(Revision, *persistence.LogRecord) bool) error {
	log := klog.FromContext(ctx)

	g.mu.RLock()
	defer g.mu.RUnlock()

	// fromRevision is uint64, so no need to check for < 0

	query := &storage.Query{
		Prefix: g.prefix,
	}

	it := g.bucket.Objects(ctx, query)
	var matches []Revision

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
		revision, err := g.objectNameToRevision(objectName)
		if err != nil {
			continue
		}

		if revision < fromRevision {
			continue
		}
		matches = append(matches, revision)
	}

	// Sort revisions in ascending order
	slices.Sort(matches)

	// Start parallel preloading of all entries
	log.Info("Preloading log entries", "count", len(matches))
	g.preloadBatch(ctx, matches)

	// Now read entries sequentially (they should be cached from preloading)
	for _, revision := range matches {
		record, err := g.getLogEntry(ctx, revision)
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
func (g *GCSLog) Close() error {
	if err := g.batching.Close(); err != nil {
		return err
	}

	// // Execute any pending batch before closing
	// g.batchMu.Lock()
	// if len(g.pendingBatch) > 0 {
	// 	g.executeBatch()
	// }
	// if g.batchTimer != nil {
	// 	g.batchTimer.Stop()
	// 	g.batchTimer = nil
	// }
	// g.batchMu.Unlock()

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

// revisionToObjectName converts a revision number to a GCS object name
func (g *GCSLog) revisionToObjectName(revision Revision) string {
	revisionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(revisionBytes, uint64(revision))
	hexRevision := hex.EncodeToString(revisionBytes)
	return fmt.Sprintf("%s%s.log", g.prefix, hexRevision)
}

// objectNameToRevision converts a GCS object name to a revision number
func (g *GCSLog) objectNameToRevision(objectName string) (Revision, error) {
	if !strings.HasPrefix(objectName, g.prefix) {
		return 0, fmt.Errorf("object name does not have expected prefix: %s", objectName)
	}

	if !strings.HasSuffix(objectName, ".log") {
		return 0, fmt.Errorf("object name does not have .log suffix: %s", objectName)
	}

	// Extract hex revision from object name
	hexRevision := strings.TrimPrefix(objectName, g.prefix)
	hexRevision = strings.TrimSuffix(hexRevision, ".log")

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
