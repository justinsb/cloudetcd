package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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
	mu        sync.RWMutex
	revisions map[string][]*KeyValue // All revisions of each key, sorted by revision
	revision  Revision
	watchers  map[int64]*memoryWatcher
	watcherID int64
	watcherMu sync.RWMutex
}

// NewMemoryStorage creates a new in-memory storage instance.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		revisions: make(map[string][]*KeyValue),
		revision:  0,
		watchers:  make(map[int64]*memoryWatcher),
		watcherID: 0,
	}
}

// broadcastEvent sends an event to all relevant watchers
func (m *MemoryStorage) broadcastEvent(event *WatchEvent, revision Revision) {
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
			if string(event.Key) == string(watcher.key) {
				shouldNotify = true
			} else if len(watcher.key) > 0 {
				// Prefix match: check if event key starts with watcher key
				eventKey := string(event.Key)
				watcherKey := string(watcher.key)
				if len(eventKey) >= len(watcherKey) && eventKey[:len(watcherKey)] == watcherKey {
					shouldNotify = true
				}
			}
		} else {
			// Range match: check if event key is in range [key, rangeEnd)
			eventKey := string(event.Key)
			startKey := string(watcher.key)
			endKey := string(watcher.rangeEnd)
			shouldNotify = eventKey >= startKey && eventKey < endKey
		}

		if shouldNotify && revision >= watcher.startRevision {
			response := &WatchResponse{
				Events:   []*WatchEvent{event},
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

	m.revision++
	keyStr := string(key)

	// Check if key already exists by looking at revisions
	revisions, exists := m.revisions[keyStr]
	var existing *KeyValue
	if exists && len(revisions) > 0 {
		existing = revisions[len(revisions)-1]
	}

	kv := &KeyValue{
		Key:     key,
		Value:   value,
		Deleted: false,
	}

	if existing != nil && !existing.Deleted {
		// Key exists, keep the original create revision
		kv.CreateRevision = existing.CreateRevision
	} else {
		// New key
		kv.CreateRevision = m.revision
	}

	m.revisions[keyStr] = append(m.revisions[keyStr], kv)

	// Create and broadcast watch event
	event := &WatchEvent{
		Type:  WatchEventTypePut,
		Key:   key,
		Value: value,
		Kv:    kv,
	}
	if existing != nil && !existing.Deleted {
		event.PrevKv = existing
	}

	m.broadcastEvent(event, m.revision)

	return m.revision, nil
}

// Get retrieves a key-value pair from the storage.
func (m *MemoryStorage) Get(ctx context.Context, key []byte, atRevision Revision) (*KeyValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keyStr := string(key)
	revisions, exists := m.revisions[keyStr]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", keyStr)
	}

	// If no specific revision requested, return the latest version
	if atRevision == 0 {
		latest := revisions[len(revisions)-1]
		if latest.Deleted {
			return nil, fmt.Errorf("key not found: %s", keyStr)
		}
		return latest, nil
	}

	// Find the revision at or before the requested revision
	// We need to track the actual revision number for each operation
	// For now, we'll use a simple approach where each operation gets the next revision number
	// In a more sophisticated implementation, we'd store the revision number with each record
	for i := len(revisions) - 1; i >= 0; i-- {
		// For this simple implementation, we'll assume revisions are sequential
		// starting from the first operation on this key
		revisionNumber := Revision(i + 1)
		if revisionNumber <= atRevision {
			// Return the version at this revision (could be deleted)
			return revisions[i], nil
		}
	}

	return nil, fmt.Errorf("key not found at revision %d", atRevision)
}

// Delete removes a key from the storage.
func (m *MemoryStorage) Delete(ctx context.Context, key []byte) (Revision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.revision++
	keyStr := string(key)

	revisions, exists := m.revisions[keyStr]
	var existing *KeyValue
	if exists && len(revisions) > 0 {
		existing = revisions[len(revisions)-1]
	}

	if existing == nil || existing.Deleted {
		return m.revision, nil // Key doesn't exist, nothing to delete
	}

	// Create a tombstone entry
	tombstone := &KeyValue{
		Key:            key,
		Value:          nil,
		CreateRevision: existing.CreateRevision,
		Deleted:        true,
	}

	// Add to revisions
	m.revisions[keyStr] = append(m.revisions[keyStr], tombstone)

	// Create and broadcast watch event
	event := &WatchEvent{
		Type:   WatchEventTypeDelete,
		Key:    key,
		Value:  nil,
		Kv:     tombstone,
		PrevKv: existing,
	}

	m.broadcastEvent(event, m.revision)

	return m.revision, nil
}

// List returns a range of key-value pairs.
// If rangeEnd is empty, it returns all keys with the given prefix.
// If rangeEnd is specified, it returns keys in the range [key, rangeEnd).
func (m *MemoryStorage) List(ctx context.Context, key []byte, rangeEnd []byte, atRevision Revision) ([]*KeyValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*KeyValue
	keyStr := string(key)
	rangeEndStr := string(rangeEnd)

	for storageKey, revisions := range m.revisions {
		// Check if key is in the specified range
		var shouldInclude bool

		if len(rangeEnd) == 0 {
			// No range end specified - treat as prefix query
			shouldInclude = len(key) == 0 || (len(storageKey) >= len(keyStr) && storageKey[:len(keyStr)] == keyStr)
		} else {
			// Range query: include keys where key >= startKey and key < endKey
			shouldInclude = storageKey >= keyStr && storageKey < rangeEndStr
		}

		if shouldInclude {
			// Get the appropriate revision
			var kv *KeyValue
			if atRevision == 0 {
				// Get the latest version, but skip if it's deleted
				latest := revisions[len(revisions)-1]
				if !latest.Deleted {
					kv = latest
				}
			} else {
				// Find the revision at or before the requested revision
				for i := len(revisions) - 1; i >= 0; i-- {
					revisionNumber := Revision(i + 1)
					if revisionNumber <= atRevision {
						kv = revisions[i]
						break
					}
				}
			}
			if kv != nil {
				result = append(result, kv)
			}
		}
	}

	// Sort by key for consistent ordering
	sort.Slice(result, func(i, j int) bool {
		return string(result[i].Key) < string(result[j].Key)
	})

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
