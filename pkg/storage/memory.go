package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// MemoryStorage is an in-memory implementation of the Storage interface.
type MemoryStorage struct {
	mu        sync.RWMutex
	revisions map[string][]*KeyValue // All revisions of each key, sorted by revision
	revision  Revision
}

// NewMemoryStorage creates a new in-memory storage instance.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		revisions: make(map[string][]*KeyValue),
		revision:  0,
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

	return m.revision, nil
}

// List returns a range of key-value pairs with the given prefix.
func (m *MemoryStorage) List(ctx context.Context, prefix []byte, atRevision Revision) ([]*KeyValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*KeyValue
	prefixStr := string(prefix)

	for key, revisions := range m.revisions {
		// Check if key starts with prefix
		if len(prefix) == 0 || (len(key) >= len(prefixStr) && key[:len(prefixStr)] == prefixStr) {
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
