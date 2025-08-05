package storage

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
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

	log persistence.Log // Persistence log

	watcherMu     sync.RWMutex
	watchers      map[int64]*memoryWatcher
	nextWatcherID int64
}

var _ Storage = &MemoryStorage{}

// NewMemoryStorage creates a new in-memory storage instance with the given log.
// It returns an error if it cannot replay the log to restore the storage state.
func NewMemoryStorage(log persistence.Log) (*MemoryStorage, error) {
	ms := &MemoryStorage{
		watchers:      make(map[int64]*memoryWatcher),
		nextWatcherID: 1,
		log:           log,
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
	if currentRevision <= 1 {
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
func (m *MemoryStorage) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := req.GetKey()
	value := req.GetValue()

	if req.GetLease() != 0 {
		return nil, fmt.Errorf("lease is not supported")
	}

	snapshotTimestamp, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current revision: %w", err)
	}

	existingRevision, hasExisting := m.revisions.GetLatestRevisionByKey(key, snapshotTimestamp)

	logRecord := &persistence.LogRecord{
		Revision:  Revision(snapshotTimestamp + 1),
		Operation: mvccpb.PUT,
		Key:       key,
		Value:     value,
	}

	var prevKv *KeyValue
	if hasExisting {
		logEntry := m.log.GetLogEntry(existingRevision)
		if logEntry == nil {
			klog.Fatalf("log entry not found for revision %d", existingRevision)
		}
		prevKv = logEntryToKeyValue(logEntry)
		logRecord.CreateRevision = logEntry.CreateRevision
		logRecord.Version = logEntry.Version + 1
	} else {
		logRecord.CreateRevision = logRecord.Revision
		logRecord.Version = 1
	}

	// Let's see if we can commit this transaction without conflicts
	newLogRecord, ok, err := m.log.Append(ctx, snapshotTimestamp, logRecord)
	if err != nil || !ok {
		return nil, fmt.Errorf("failed to append to log: %w", err)
	}

	kv := logEntryToKeyValue(newLogRecord)

	m.revisions.AddRevision(kv.Key, newLogRecord.Revision)

	// Create and broadcast watch event
	event := &mvccpb.Event{
		Type: mvccpb.PUT,
		Kv:   kv,
	}
	if prevKv != nil {
		event.PrevKv = prevKv
	}

	m.broadcastEvent(event, newLogRecord.Revision)

	response := &etcdserverpb.PutResponse{
		Header: createHeader(newLogRecord.Revision),
	}
	if req.PrevKv {
		response.PrevKv = prevKv
	}

	return response, nil
}

// Get retrieves a key-value pair from the storage.
func (m *MemoryStorage) Get(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if req.Key == nil {
		return nil, fmt.Errorf("key is required by Get")
	}

	if req.RangeEnd != nil {
		return nil, fmt.Errorf("range end is not supported by Get")
	}

	snapshotTimestamp := Revision(0)
	if req.Revision > 0 {
		snapshotTimestamp = Revision(req.Revision)
	} else {
		currentRevision, err := m.log.GetCurrentRevision(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get current revision: %w", err)
		}
		snapshotTimestamp = currentRevision
	}

	if req.CountOnly {
		return nil, fmt.Errorf("count only is not supported by Get")
	}

	resp := &etcdserverpb.RangeResponse{
		Header: createHeader(snapshotTimestamp),
		Count:  0,
	}

	latestRevision, exists := m.revisions.GetLatestRevisionByKey(req.Key, snapshotTimestamp)
	if !exists {
		return resp, nil
	}

	logEntry := m.log.GetLogEntry(latestRevision)
	if logEntry == nil {
		klog.Fatalf("log entry not found for revision %d", latestRevision)
	}

	switch logEntry.Operation {
	case mvccpb.PUT:
		kv := logEntryToKeyValue(logEntry)
		resp.Kvs = []*mvccpb.KeyValue{kv}
		resp.Count = 1
		return resp, nil

	case mvccpb.DELETE:
		return resp, nil

	default:
		panic(fmt.Sprintf("unknown operation: %s", logEntry.Operation))
	}
}

// Delete removes a key from the storage.
func (m *MemoryStorage) Delete(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if req.RangeEnd != nil {
		return nil, fmt.Errorf("range end is not yet supported by Delete")
	}

	// if req.Key == nil {
	// 	return nil, status.Error(codes.InvalidArgument, "key is required")
	// }

	// var deleted int64
	// var prevKvs []*mvccpb.KeyValue

	// if len(req.RangeEnd) == 0 {
	// 	// Single key deletion
	// 	if req.PrevKv {
	// 		existingResp, err := s.storage.Get(ctx, &etcdserverpb.RangeRequest{Key: req.Key})
	// 		if err != nil {
	// 			return nil, status.Error(codes.Internal, err.Error())
	// 		}
	// 		if len(existingResp.Kvs) > 0 {
	// 			prevKvs = []*mvccpb.KeyValue{existingResp.Kvs[0]}
	// 		}
	// 	}

	// 	// Check if key exists before deleting
	// 	_, err := s.storage.Get(ctx, &etcdserverpb.RangeRequest{Key: req.Key})
	// 	if err != nil {
	// 		// TODO: Handle not found?
	// 		return nil, fmt.Errorf("failed to get key: %w", err)
	// 	}

	// 	deleted = 1

	// 	// TODO: Compare and swap or similar?
	// 	deleteResponse, err := s.storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: req.Key})
	// 	if err != nil {
	// 		return nil, status.Error(codes.Internal, err.Error())
	// 	}

	// 	return &etcdserverpb.DeleteRangeResponse{
	// 		Header:  s.createHeader(storage.Revision(deleteResponse.Header.Revision)),
	// 		Deleted: deleted,
	// 		PrevKvs: prevKvs,
	// 	}, nil
	// }

	// // Range deletion - use the storage layer's efficient range query
	// keysResp, err := s.storage.List(ctx, &etcdserverpb.RangeRequest{Key: req.Key, RangeEnd: req.RangeEnd})
	// if err != nil {
	// 	return nil, status.Error(codes.Internal, err.Error())
	// }
	// keys := keysResp.Kvs
	// var maxRevision storage.Revision
	// for _, kv := range keys {
	// 	if req.PrevKv {
	// 		prevKvs = append(prevKvs, kv)
	// 	}

	// 	// TODO: Compare and swap or similar?
	// 	deleteResponse, err := s.storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: kv.Key})
	// 	if err != nil {
	// 		return nil, status.Error(codes.Internal, err.Error())
	// 	}
	// 	if storage.Revision(deleteResponse.Header.Revision) > maxRevision {
	// 		maxRevision = storage.Revision(deleteResponse.Header.Revision)
	// 	}
	// 	deleted++
	// }

	// return &etcdserverpb.DeleteRangeResponse{
	// 	Header:  s.createHeader(maxRevision),
	// 	Deleted: deleted,
	// 	PrevKvs: prevKvs,
	// }, nil

	key := req.Key

	snapshotTimestamp, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current revision: %w", err)
	}

	latestRevision, exists := m.revisions.GetLatestRevisionByKey(key, snapshotTimestamp)
	if !exists {
		resp := &etcdserverpb.DeleteRangeResponse{
			Header:  createHeader(snapshotTimestamp),
			Deleted: 0,
		}

		return resp, nil
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
		return nil, fmt.Errorf("failed to append to log: %w", err)
	}

	m.revisions.AddRevision(key, newLogRecord.Revision)

	resp := &etcdserverpb.DeleteRangeResponse{
		Header:  createHeader(newLogRecord.Revision),
		Deleted: 1,
	}

	// Create and broadcast watch event

	// A DELETE/EXPIRE event contains the deleted key with
	// its modification revision set to the revision of deletion.

	event := &mvccpb.Event{
		Type: mvccpb.DELETE,
		Kv: &mvccpb.KeyValue{
			Key:         key,
			ModRevision: int64(newLogRecord.Revision),
			Version:     0, // version is set to 0 for DELETE events
		},
	}

	// Note: we do not set prev_kv; we only send the value if prev_kv is requested in the watch.
	// (But to reuse the event, we just send it to all watchers.)
	// TODO: Only send Value if prev_kv is requested in the watch.
	includePrevKv := true
	if includePrevKv {
		event.PrevKv = &mvccpb.KeyValue{
			Key:            key,
			CreateRevision: int64(oldLogEntry.CreateRevision),
			ModRevision:    int64(oldLogEntry.Revision),
			Value:          oldLogEntry.Value,
			Version:        oldLogEntry.Version,
		}
	}

	m.broadcastEvent(event, newLogRecord.Revision)

	return resp, nil
}

// List returns a range of key-value pairs.
// If rangeEnd is empty, it returns all keys with the given prefix.
// If rangeEnd is specified, it returns keys in the range [key, rangeEnd).
func (m *MemoryStorage) List(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if req.RangeEnd == nil {
		return nil, fmt.Errorf("range end is required by List")
	}

	snapshotTimestamp := Revision(0)
	if req.Revision > 0 {
		snapshotTimestamp = Revision(req.Revision)
	} else {
		currentRevision, err := m.log.GetCurrentRevision(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get current revision: %w", err)
		}
		snapshotTimestamp = currentRevision
	}

	resp := &etcdserverpb.RangeResponse{
		Header: createHeader(snapshotTimestamp),
	}

	hasRangeEnd := req.RangeEnd != nil && !bytes.Equal(req.RangeEnd, []byte{0})

	m.revisions.ListRevisionsByKeyRange(req.Key, snapshotTimestamp, func(key []byte, revisions []Revision) bool {
		if hasRangeEnd && bytes.Compare(key, req.RangeEnd) >= 0 {
			// Stop iterating
			return false
		}

		latest := Revision(0)
		found := false

		// Find the latest revision that is less than or equal to atRevision
		for _, revision := range revisions {
			if revision <= snapshotTimestamp {
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
				resp.Count++
				// TODO: Can we store whether this is a delete, so we don't need a log lookup?
				if !req.CountOnly {
					resp.Kvs = append(resp.Kvs, logEntryToKeyValue(logEntry))
				}
			}
			if req.Limit > 0 && resp.Count >= req.Limit {
				resp.More = true
				// Stop iterating
				return false
			}
		}

		return true
	})

	return resp, nil
}

// Watch creates a watcher for the given key/range starting from the specified revision
// If rangeEnd is empty, it watches a single key.
// If rangeEnd is specified, it watches the range [key, rangeEnd).
func (m *MemoryStorage) Watch(ctx context.Context, key []byte, rangeEnd []byte, startRevision Revision) (Watcher, error) {
	m.watcherMu.Lock()
	defer m.watcherMu.Unlock()

	watcherID := m.nextWatcherID
	m.nextWatcherID++
	watcher := &memoryWatcher{
		id:            watcherID,
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

func createHeader(revision Revision) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		ClusterId: 1, // Simple cluster ID
		MemberId:  1, // Simple member ID
		Revision:  int64(revision),
		RaftTerm:  1, // Simple term
	}
}

// GracefulStop stops the storage gracefully.
func (m *MemoryStorage) GracefulStop() {
	m.watcherMu.Lock()
	defer m.watcherMu.Unlock()
	for _, watcher := range m.watchers {
		klog.InfoS("closing watcher", "id", watcher.id)
		watcher.Close()
	}
}
