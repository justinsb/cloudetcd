package storage

import "context"

// Revision is the version of the key-value store.
type Revision int64

// KeyValue represents a single key-value pair from the store with MVCC support.
type KeyValue struct {
	Key            []byte
	Value          []byte
	CreateRevision Revision // The revision when this key was first created
	ModRevision    Revision // The revision when this key was last modified
	Deleted        bool     // Whether this is a tombstone (deleted entry)
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
}
