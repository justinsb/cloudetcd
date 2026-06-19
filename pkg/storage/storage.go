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

	LeaseManager() LeaseManager
}

type LeaseManager interface {
	// Run will run until the context is closed
	Run(ctx context.Context)

	// OnLogEvent is called by the storage when an event has been committed to the log or been replayed
	OnLogEvent(event *mvccpb.Event)

	// HasLease is called by the storage to check if a lease exists
	HasLease(leaseID int64) bool

	// LeaseGrant creates a lease which expires if the server does not receive a keepAlive
	// within a given time to live period. All keys attached to the lease will be expired and
	// deleted if the lease expires. Each expired key generates a delete event in the event history.
	LeaseGrant(context.Context, *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error)
	// LeaseRevoke revokes a lease. All keys attached to the lease will expire and be deleted.
	LeaseRevoke(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error)
	// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
	// to the server and streaming keep alive responses from the server to the client.
	LeaseKeepAlive(context.Context, *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error)
	// LeaseTimeToLive retrieves lease information.
	LeaseTimeToLive(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error)
	// ListLeases lists all existing leases.
	ListLeases(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error)
}

type KeyValue = mvccpb.KeyValue
