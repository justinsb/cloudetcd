package storage

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/persistence"
)

// Revision is the version of the key-value store.
type Revision = persistence.Revision

// TODO: Just use zero to mean latest revision?
const MAX_REVISION = Revision(^uint64(0))

// // KeyValue represents a single key-value pair from the store with MVCC support.
// type KeyValue struct {
// 	Key            []byte
// 	Value          []byte
// 	CreateRevision Revision // The revision when this key was first created
// 	Deleted        bool     // Whether this is a tombstone (deleted entry)
// }

// Watcher represents a single watch subscription
type Watcher interface {
	// Close closes the watcher
	Close()
	// ID returns the ID of the watcher
	ID() int64

	// Run starts the watcher.
	Run(ctx context.Context) error
}

// Storage is the interface for the underlying storage layer.
type Storage interface {
	// GetCurrentRevision returns the current revision of the log.
	GetCurrentRevision(ctx context.Context) (Revision, error)

	// Put writes a key-value pair to the storage.
	Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error)

	// Get retrieves a key-value pair from the storage.
	Get(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// Delete removes a key from the storage.
	Delete(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error)

	// List returns a range of key-value pairs.
	List(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// Watch creates a watcher for the given key/range starting from the specified revision
	// If rangeEnd is empty, it watches a single key.
	// If rangeEnd is specified, it watches the range [key, rangeEnd).
	Watch(ctx context.Context, req *etcdserverpb.WatchCreateRequest, callback func(event *etcdserverpb.WatchResponse) error) (Watcher, Revision, error)

	// Txn executes a transaction against the storage.
	Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error)

	// GracefulStop stops the storage gracefully.
	GracefulStop()

	// Status returns the status of the storage.
	Status(ctx context.Context) (*etcdserverpb.StatusResponse, error)
}

type KeyValue = mvccpb.KeyValue
