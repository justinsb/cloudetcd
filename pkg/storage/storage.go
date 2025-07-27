package storage

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// Revision is the version of the key-value store.
type Revision int64

// KeyValue represents a single key-value pair from the store with MVCC support.
type KeyValue struct {
	Key            []byte
	Value          []byte
	CreateRevision Revision // The revision when this key was first created
	Deleted        bool     // Whether this is a tombstone (deleted entry)
}

// WatchResponse represents a response from watching
type WatchResponse struct {
	Events   []*mvccpb.Event
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
	// If rangeEnd is empty, it returns all keys with the given prefix.
	// If rangeEnd is specified, it returns keys in the range [key, rangeEnd).
	List(ctx context.Context, key []byte, rangeEnd []byte, atRevision Revision) ([]*KeyValue, error)

	// Watch creates a watcher for the given key/range starting from the specified revision
	// If rangeEnd is empty, it watches a single key.
	// If rangeEnd is specified, it watches the range [key, rangeEnd).
	Watch(ctx context.Context, key []byte, rangeEnd []byte, startRevision Revision) (Watcher, error)
}
