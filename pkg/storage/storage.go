package storage

import "context"

// Revision is the version of the key-value store.
type Revision int64

// KeyValue represents a single key-value pair from the store with MVCC support.
type KeyValue struct {
	Key            []byte
	Value          []byte
	CreateRevision Revision // The revision when this key was first created
	Deleted        bool     // Whether this is a tombstone (deleted entry)
}

// WatchEventType represents the type of watch event
type WatchEventType int

const (
	WatchEventTypePut WatchEventType = iota
	WatchEventTypeDelete
)

// WatchEvent represents a single watch event
type WatchEvent struct {
	Type   WatchEventType
	Key    []byte
	Value  []byte
	Kv     *KeyValue // Current key-value pair
	PrevKv *KeyValue // Previous key-value pair (optional)
}

// WatchResponse represents a response from watching
type WatchResponse struct {
	Events   []*WatchEvent
	Revision Revision
}

// Watcher represents a single watch subscription
type Watcher interface {
	// Chan returns the channel to receive watch events
	Chan() <-chan *WatchResponse
	// Close closes the watcher
	Close()
}

// Storage is the interface for the underlying storage layer.
type Storage interface {
	// Put writes a key-value pair to the storage.
	Put(ctx context.Context, key []byte, value []byte, leaseID int64) (Revision, error)

	// Get retrieves a key-value pair from the storage.
	Get(ctx context.Context, key []byte, atRevision Revision) (*KeyValue, error)

	// Delete removes a key from the storage.
	Delete(ctx context.Context, key []byte) (Revision, error)

	// List returns a range of key-value pairs.
	List(ctx context.Context, prefix []byte, atRevision Revision) ([]*KeyValue, error)

	// Watch creates a watcher for the given key/prefix starting from the specified revision
	Watch(ctx context.Context, key []byte, prefix bool, startRevision Revision) (Watcher, error)
}
