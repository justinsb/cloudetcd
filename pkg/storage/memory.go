package storage

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/bptree"
	"justinsb.com/cloudetcd/pkg/persistence"
	"k8s.io/klog/v2"
)

// memoryWatcher implements the Watcher interface
type memoryWatcher struct {
	id            int64
	key           []byte
	rangeEnd      []byte // Store the range end for proper filtering
	startRevision Revision
	ch            chan *WatchResponse
	closed        int32 // atomic flag
	closeCh       chan struct{}
}

func (w *memoryWatcher) Chan() <-chan *WatchResponse {
	return w.ch
}

func (w *memoryWatcher) Close() {
	if atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		close(w.closeCh)
		close(w.ch)
	}
}

func (w *memoryWatcher) isClosed() bool {
	return atomic.LoadInt32(&w.closed) == 1
}

// MemoryStorage is an in-memory implementation of the Storage interface.
type MemoryStorage struct {
	mu sync.RWMutex

	revisions bptree.BPTree
	watchers  map[int64]*memoryWatcher
	watcherID int64
	watcherMu sync.RWMutex
	log       persistence.Log // Persistence log
}

// NewMemoryStorage creates a new in-memory storage instance with the given log.
// It returns an error if it cannot replay the log to restore the storage state.
func NewMemoryStorage(log persistence.Log) (*MemoryStorage, error) {
	ms := &MemoryStorage{
		watchers:  make(map[int64]*memoryWatcher),
		watcherID: 0,
		log:       log,
	}

	// Replay the log to restore state
	if err := ms.ReplayLog(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to replay log on startup: %w", err)
	}

	return ms, nil
}

// ReplayLog replays the persistence log to restore the storage state
func (m *MemoryStorage) ReplayLog(ctx context.Context) error {
	// Get the current revision from the log
	currentRevision, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current revision: %w", err)
	}

	// If no log entries exist, we're done
	if currentRevision == 0 {
		return nil
	}

	// Read all log records starting from revision 1
	records, err := m.log.Read(ctx, 1, 0) // 0 means no limit
	if err != nil {
		return fmt.Errorf("failed to read log records: %w", err)
	}

	// Replay each record in order
	for _, record := range records {
		switch record.Operation {
		case mvccpb.PUT:
			// Replay PUT operation
			m.revisions.AddRevision(record.Key, record.Revision)

		case mvccpb.DELETE:
			// Replay DELETE operation
			m.revisions.AddRevision(record.Key, record.Revision)

		default:
			// Skip unknown operations
			klog.Fatalf("unknown operation: %s", record.Operation)
		}
	}

	return nil
}

// convertToMVCCKeyValue converts a storage.KeyValue to mvccpb.KeyValue
func logEntryToKeyValue(r *persistence.LogRecord) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:            r.Key,
		Value:          r.Value,
		CreateRevision: int64(r.CreateRevision),
		ModRevision:    int64(r.Revision),
		Version:        r.Version,
		Lease:          0, // For now, no lease
	}
}

// broadcastEvent sends an event to all relevant watchers
func (m *MemoryStorage) broadcastEvent(event *mvccpb.Event, revision Revision) {
	m.watcherMu.RLock()
	defer m.watcherMu.RUnlock()

	for _, watcher := range m.watchers {
		if watcher.isClosed() {
			continue
		}

		// Check if this watcher should receive this event
		shouldNotify := false
		if len(watcher.rangeEnd) == 0 {
			// Empty rangeEnd means prefix watch or exact key match
			// If the key is the same as the watcher key, it's an exact match
			// Otherwise, it's a prefix match
			if string(event.Kv.Key) == string(watcher.key) {
				shouldNotify = true
			} else if len(watcher.key) > 0 {
				// Prefix match: check if event key starts with watcher key
				eventKey := string(event.Kv.Key)
				watcherKey := string(watcher.key)
				if len(eventKey) >= len(watcherKey) && eventKey[:len(watcherKey)] == watcherKey {
					shouldNotify = true
				}
			}
		} else {
			// Range match: check if event key is in range [key, rangeEnd)
			eventKey := string(event.Kv.Key)
			startKey := string(watcher.key)
			endKey := string(watcher.rangeEnd)
			shouldNotify = eventKey >= startKey && eventKey < endKey
		}

		if shouldNotify && revision >= watcher.startRevision {
			response := &WatchResponse{
				Events:   []*mvccpb.Event{event},
				Revision: revision,
			}

			// Non-blocking send
			select {
			case watcher.ch <- response:
			case <-watcher.closeCh:
				// Watcher was closed, skip
			default:
				// Channel is full, skip to avoid blocking
			}
		}
	}
}

// Put writes a key-value pair to the storage.
func (m *MemoryStorage) Put(ctx context.Context, key []byte, value []byte, leaseID int64) (Revision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshotTimestamp, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get current revision: %w", err)
	}

	existingRevision, hasExisting := m.revisions.GetLatestRevisionByKey(key, snapshotTimestamp)

	logRecord := &persistence.LogRecord{
		Revision:  Revision(snapshotTimestamp + 1),
		Operation: mvccpb.PUT,
		Key:       key,
		Value:     value,
	}

	var existing *KeyValue
	if hasExisting {
		logEntry := m.log.GetLogEntry(existingRevision)
		if logEntry == nil {
			klog.Fatalf("log entry not found for revision %d", existingRevision)
		}
		existing = logEntryToKeyValue(logEntry)
		logRecord.CreateRevision = logEntry.CreateRevision
		logRecord.Version = logEntry.Version + 1
	} else {
		logRecord.CreateRevision = logRecord.Revision
		logRecord.Version = 1
	}

	// Let's see if we can commit this transaction without conflicts
	newLogRecord, ok, err := m.log.Append(ctx, snapshotTimestamp, logRecord)
	if err != nil || !ok {
		return 0, fmt.Errorf("failed to append to log: %w", err)
	}

	kv := logEntryToKeyValue(newLogRecord)

	// kv := &KeyValue{
	// 	Key:   key,
	// 	Value: value,
	// }

	// if existing != nil {
	// 	// Key exists, keep the original create revision
	// 	kv.CreateRevision = existing.CreateRevision
	// } else {
	// 	// New key
	// 	kv.CreateRevision = m.revision
	// }

	m.revisions.AddRevision(kv.Key, newLogRecord.Revision)

	// Create and broadcast watch event
	event := &mvccpb.Event{
		Type: mvccpb.PUT,
		Kv:   kv,
	}
	if existing != nil {
		event.PrevKv = existing
	}

	m.broadcastEvent(event, newLogRecord.Revision)

	return newLogRecord.Revision, nil
}

// Get retrieves a key-value pair from the storage.
func (m *MemoryStorage) Get(ctx context.Context, key []byte, atRevision Revision) (*KeyValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keyStr := string(key)

	snapshotTimestamp := Revision(atRevision)
	if snapshotTimestamp == 0 {
		// TODO: Get latest revision from log?
		snapshotTimestamp = MAX_REVISION
	}

	latestRevision, exists := m.revisions.GetLatestRevisionByKey(key, snapshotTimestamp)
	if !exists {
		return nil, fmt.Errorf("key not found: %s", keyStr)
	}

	logEntry := m.log.GetLogEntry(latestRevision)
	if logEntry == nil {
		klog.Fatalf("log entry not found for revision %d", latestRevision)
	}

	switch logEntry.Operation {
	case mvccpb.PUT:
		kv := logEntryToKeyValue(logEntry)
		return kv, nil
	case mvccpb.DELETE:
		return nil, fmt.Errorf("key not found: %s", keyStr)
	default:
		panic(fmt.Sprintf("unknown operation: %s", logEntry.Operation))
	}
}

// Delete removes a key from the storage.
func (m *MemoryStorage) Delete(ctx context.Context, key []byte) (Revision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshotTimestamp, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get current revision: %w", err)
	}

	latestRevision, exists := m.revisions.GetLatestRevisionByKey(key, snapshotTimestamp)
	if !exists {
		return 0, fmt.Errorf("key not found: %s", key)
	}

	oldLogEntry := m.log.GetLogEntry(latestRevision)
	if oldLogEntry == nil {
		klog.Fatalf("log entry not found for revision %d", latestRevision)
	}

	// Append to the persistence log first
	newLogRecord, ok, err := m.log.Append(ctx, snapshotTimestamp, &persistence.LogRecord{
		Revision:  Revision(snapshotTimestamp + 1),
		Operation: mvccpb.DELETE,
		Key:       key,
		Value:     nil,
	})
	if err != nil || !ok {
		return 0, fmt.Errorf("failed to append to log: %w", err)
	}

	m.revisions.AddRevision(key, newLogRecord.Revision)

	// Create and broadcast watch event

	// A DELETE/EXPIRE event contains the deleted key with
	// its modification revision set to the revision of deletion.

	event := &mvccpb.Event{
		Type: mvccpb.DELETE,
		Kv: &mvccpb.KeyValue{
			Key:            key,
			Value:          oldLogEntry.Value,
			CreateRevision: int64(oldLogEntry.CreateRevision),
			ModRevision:    int64(newLogRecord.Revision),
			Version:        0, // TODO: Is this right?
		},
	}
	// Note: we do not set prev_kv; we only send the value if prev_kv is requested in the watch.
	// (But to reuse the event, we just send it to all watchers.)
	// TODO: Only send Value if prev_kv is requested in the watch.

	m.broadcastEvent(event, newLogRecord.Revision)

	return newLogRecord.Revision, nil
}

// List returns a range of key-value pairs.
// If rangeEnd is empty, it returns all keys with the given prefix.
// If rangeEnd is specified, it returns keys in the range [key, rangeEnd).
func (m *MemoryStorage) List(ctx context.Context, key []byte, rangeEnd []byte, atRevision Revision) ([]*KeyValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// TODO: Convert to callback?

	if atRevision == 0 {
		// TODO: Or max revision?
		logRevision, err := m.log.GetCurrentRevision(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get current revision: %w", err)
		}
		atRevision = logRevision
	}

	var result []*KeyValue
	// keyStr := string(key)
	// rangeEndStr := string(rangeEnd)

	m.revisions.ListRevisionsByKeyRange(key, atRevision, func(key []byte, revisions []Revision) bool {
		if len(rangeEnd) != 0 {
			if bytes.Compare(key, rangeEnd) >= 0 {
				return false
			}
		}

		latest := Revision(0)
		found := false

		// Find the latest revision that is less than or equal to atRevision
		for _, revision := range revisions {
			if revision <= atRevision {
				latest = revision
				found = true
			}
		}

		if found {
			logEntry := m.log.GetLogEntry(latest)
			if logEntry == nil {
				klog.Fatalf("log entry not found for revision %d", latest)
			}
			if logEntry.Operation == mvccpb.PUT {
				result = append(result, logEntryToKeyValue(logEntry))
			}
		}

		return true
	})

	// TODO: Do we need to sort?

	// // Sort by key for consistent ordering
	// sort.Slice(result, func(i, j int) bool {
	// 	return string(result[i].Key) < string(result[j].Key)
	// })

	return result, nil
}

// Watch creates a watcher for the given key/range starting from the specified revision
// If rangeEnd is empty, it watches a single key.
// If rangeEnd is specified, it watches the range [key, rangeEnd).
func (m *MemoryStorage) Watch(ctx context.Context, key []byte, rangeEnd []byte, startRevision Revision) (Watcher, error) {
	m.watcherMu.Lock()
	defer m.watcherMu.Unlock()

	m.watcherID++
	watcher := &memoryWatcher{
		id:            m.watcherID,
		key:           key,
		rangeEnd:      rangeEnd,
		startRevision: startRevision,
		ch:            make(chan *WatchResponse, 100), // Buffered channel
		closeCh:       make(chan struct{}),
	}

	m.watchers[watcher.id] = watcher

	// Start a goroutine to clean up the watcher when the context is done
	go func() {
		select {
		case <-ctx.Done():
			watcher.Close()
			m.removeWatcher(watcher.id)
		case <-watcher.closeCh:
			m.removeWatcher(watcher.id)
		}
	}()

	return watcher, nil
}

// removeWatcher removes a watcher from the storage
func (m *MemoryStorage) removeWatcher(id int64) {
	m.watcherMu.Lock()
	defer m.watcherMu.Unlock()
	delete(m.watchers, id)
}

// // GetCurrentRevision returns the current revision number
// func (m *MemoryStorage) GetCurrentRevision() Revision {
// 	m.mu.RLock()
// 	defer m.mu.RUnlock()

// 	return m.revisions.GetCurrentRevision()
// }

// ForceReplayLog manually triggers a replay of the log
// This can be useful for testing or explicit recovery scenarios
func (m *MemoryStorage) ForceReplayLog(ctx context.Context) error {
	// Clear current state
	m.mu.Lock()
	m.revisions = bptree.BPTree{}
	m.mu.Unlock()

	// Replay the log
	return m.ReplayLog(ctx)
}
