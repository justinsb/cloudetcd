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

package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"justinsb.com/cloudetcd/pkg/storage"
	"k8s.io/klog/v2"
)

type LeaseID int64

// Leases are persisted as ordinary log records under an internal key:
// granting a lease puts leaseKey(id) with the TTL as its value, and
// revoking or expiring it deletes the key. The log is thus the leases'
// persistence: replay rebuilds the table (each replayed grant gets its
// full TTL again, as etcd does after a restart — keepalives are not
// persisted), and compaction needs no lease-specific rules, because a
// grant is the key's latest put until the revoke deletes it.
var leaseKeyPrefix = append(bytes.Clone(storage.InternalPrefix), "lease/"...)

func leaseKey(id LeaseID) []byte {
	return fmt.Appendf(bytes.Clone(leaseKeyPrefix), "%016x", uint64(id))
}

func parseLeaseKey(key []byte) (LeaseID, bool) {
	if !bytes.HasPrefix(key, leaseKeyPrefix) {
		return 0, false
	}
	id, err := strconv.ParseUint(string(key[len(leaseKeyPrefix):]), 16, 64)
	if err != nil {
		return 0, false
	}
	return LeaseID(id), true
}

// leaseValue is the value of a lease record.
type leaseValue struct {
	TTL int64 `json:"ttl"`
}

type Lease struct {
	// keys associated with the lease
	keys [][]byte

	ID         LeaseID
	TTL        int64
	Expiration time.Time
}

type LeaseManager struct {
	storage storage.Storage

	mu     sync.RWMutex
	leases map[LeaseID]*Lease
	// expiring marks leases whose expiry has been decided and its revoke
	// record sent to the log, so a slow apply does not expire them twice.
	expiring map[LeaseID]bool

	deleteQueue *queue[*Lease]
}

type queue[T any] struct {
	mu   sync.Mutex
	cond *sync.Cond

	closed bool
	head   *queueItem[T]
	tail   *queueItem[T]
}

type queueItem[T any] struct {
	value T
	next  *queueItem[T]
}

func newQueue[T any]() *queue[T] {
	q := &queue[T]{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

func (q *queue[T]) Push(value T) {
	q.mu.Lock()
	defer q.mu.Unlock()

	item := &queueItem[T]{value: value}
	if q.tail != nil {
		q.tail.next = item
	}
	q.tail = item
	if q.head == nil {
		q.head = item
	}
	q.cond.Broadcast()
}

func (q *queue[T]) Wait() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.head == nil && !q.closed {
		q.cond.Wait()
	}

	if q.head == nil {
		// closed
		var zero T
		return zero, false
	}

	value := q.head.value
	q.head = q.head.next
	if q.head == nil {
		q.tail = nil
	}
	return value, true
}

func (q *queue[T]) Pop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		var zero T
		return zero, false
	}

	value := q.head.value
	q.head = q.head.next
	if q.head == nil {
		q.tail = nil
	}
	return value, true
}

func NewLeaseManager(storage storage.Storage) *LeaseManager {
	lm := &LeaseManager{
		leases:   make(map[LeaseID]*Lease),
		expiring: make(map[LeaseID]bool),
		storage:  storage,
	}
	lm.deleteQueue = newQueue[*Lease]()
	return lm
}

func (m *LeaseManager) Run(ctx context.Context) {
	go m.processDeletionQueue(ctx)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkExpiredOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (m *LeaseManager) checkExpiredOnce(ctx context.Context) {
	log := klog.FromContext(ctx)

	now := time.Now()
	m.mu.Lock()
	var expired []LeaseID
	// TODO: Move to a more efficient and fair data structure
	for id, lease := range m.leases {
		if lease.Expiration.Before(now) && !m.expiring[id] {
			m.expiring[id] = true
			expired = append(expired, id)
		}
	}
	m.mu.Unlock()

	// An expiry is a revoke: delete the lease record, and applying that
	// delete retires the lease and queues its keys for deletion. Not under
	// mu: the delete's application takes it.
	for _, id := range expired {
		log.Info("Lease expired", "leaseID", id)
		resp, err := m.storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: leaseKey(id)})
		if err != nil {
			log.Error(err, "failed to record lease expiry; will retry", "leaseID", id)
			m.mu.Lock()
			delete(m.expiring, id)
			m.mu.Unlock()
			continue
		}
		if resp.Deleted == 0 {
			// No lease record in the log: a placeholder lease from recovery
			// (see OnLogEvent). No delete event will arrive, so retire it
			// here.
			m.mu.Lock()
			if lease, ok := m.leases[id]; ok {
				delete(m.leases, id)
				delete(m.expiring, id)
				if len(lease.keys) > 0 {
					m.deleteQueue.Push(lease)
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *LeaseManager) processDeletionQueue(ctx context.Context) {
	log := klog.FromContext(ctx)

	go func() {
		<-ctx.Done()
		m.deleteQueue.Close()
	}()

	for {
		lease, ok := m.deleteQueue.Wait()
		if !ok {
			break
		}

		if err := lease.deleteKeysForLease(ctx, m.storage); err != nil {
			log.Error(err, "failed to delete keys for expired lease", "leaseID", lease.ID)
			// Re-enqueue after a short delay
			go func() {
				time.Sleep(time.Millisecond * 100)
				m.deleteQueue.Push(lease)
			}()
		}
	}
}

func (m *LeaseManager) LeaseKeepAlive(ctx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := LeaseID(req.GetID())
	if id == 0 {
		return nil, rpctypes.ErrGRPCLeaseNotFound
	}

	lease, ok := m.leases[id]
	if !ok {
		return nil, rpctypes.ErrGRPCLeaseNotFound
	}
	lease.Expiration = time.Now().Add(time.Duration(lease.TTL) * time.Second)

	return &etcdserverpb.LeaseKeepAliveResponse{
		Header: m.createHeader(0),
		ID:     int64(lease.ID),
		TTL:    lease.TTL,
	}, nil
}

func (m *LeaseManager) ListLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	resp := &etcdserverpb.LeaseLeasesResponse{
		Header: m.createHeader(0),
	}

	for _, l := range m.leases {
		resp.Leases = append(resp.Leases, &etcdserverpb.LeaseStatus{ID: int64(l.ID)})
	}
	return resp, nil
}

// Helper methods
func (m *LeaseManager) createHeader(revision storage.Revision) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		ClusterId: 1, // Simple cluster ID
		MemberId:  1, // Simple member ID
		Revision:  int64(revision),
		RaftTerm:  1, // Simple term
	}
}

func (m *LeaseManager) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	id := LeaseID(req.GetID())

	lease, ok := m.leases[id]
	if !ok {
		// Lease not found, return TTL=-1 as per etcd spec
		return &etcdserverpb.LeaseTimeToLiveResponse{
			Header:     m.createHeader(0),
			ID:         req.ID,
			TTL:        -1,
			GrantedTTL: 0,
		}, nil
	}

	ttl := int64(time.Until(lease.Expiration).Seconds())
	if ttl < 0 {
		ttl = 0
	}

	resp := &etcdserverpb.LeaseTimeToLiveResponse{
		Header:     m.createHeader(0),
		ID:         req.ID,
		TTL:        ttl,
		GrantedTTL: lease.TTL,
	}

	if req.Keys {
		resp.Keys = lease.keys
	}

	return resp, nil
}

func (m *LeaseManager) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	id := LeaseID(req.GetID())
	if id == 0 || !m.HasLease(int64(id)) {
		return nil, rpctypes.ErrGRPCLeaseNotFound
	}

	// Deleting the lease record is the revoke; applying the delete retires
	// the lease and queues its keys for deletion (OnLogEvent). The delete
	// is applied before it returns, so the lease is gone when we respond.
	if _, err := m.storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: leaseKey(id)}); err != nil {
		return nil, fmt.Errorf("recording lease revoke: %w", err)
	}

	return &etcdserverpb.LeaseRevokeResponse{
		Header: m.createHeader(0),
	}, nil
}

func (m *LeaseManager) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	id := LeaseID(req.GetID())
	if id == 0 {
		id = LeaseID(time.Now().UnixNano())
	} else if m.HasLease(int64(id)) {
		return nil, rpctypes.ErrGRPCLeaseExist
	}

	value, err := json.Marshal(leaseValue{TTL: req.TTL})
	if err != nil {
		return nil, fmt.Errorf("encoding lease record: %w", err)
	}
	// Putting the lease record is the grant; applying the put creates the
	// lease (OnLogEvent), and the put is applied before it returns, so the
	// lease exists when we respond.
	if _, err := m.storage.Put(ctx, &etcdserverpb.PutRequest{Key: leaseKey(id), Value: value}); err != nil {
		return nil, fmt.Errorf("recording lease grant: %w", err)
	}

	return &etcdserverpb.LeaseGrantResponse{
		Header: m.createHeader(0),
		ID:     int64(id),
		TTL:    req.TTL,
	}, nil
}

func (l *Lease) deleteKeysForLease(ctx context.Context, storage storage.Storage) error {
	log := klog.FromContext(ctx)
	log.Info("lease expired, deleting keys", "leaseID", l.ID)

	var errs []error
	for _, key := range l.keys {
		req := &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{
				{
					Result: etcdserverpb.Compare_EQUAL,
					Target: etcdserverpb.Compare_LEASE,
					TargetUnion: &etcdserverpb.Compare_Lease{
						Lease: int64(l.ID),
					},
					Key: key,
				},
			},
			Success: []*etcdserverpb.RequestOp{
				{
					Request: &etcdserverpb.RequestOp_RequestDeleteRange{
						RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
							Key: key,
						},
					},
				},
			},
		}

		if result, err := storage.Txn(ctx, req); err != nil {
			// TODO: Ignore not found
			errs = append(errs, fmt.Errorf("failed to delete key %q for lease %v: %w", string(key), l.ID, err))
		} else if !result.Succeeded {
			// This is OK though, the new lease owns the key
			log.Info("lease expiry but key already had new lease", "key", string(key), "leaseID", l.ID)
		}
	}
	return errors.Join(errs...)
}

// onLeaseRecord applies a lease record: a put is a grant (a replayed grant
// starts with its full TTL again, as in etcd; keepalives are not
// persisted), a delete is a revoke or an expiry.
func (m *LeaseManager) onLeaseRecord(id LeaseID, event *mvccpb.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch event.Type {
	case mvccpb.PUT:
		var v leaseValue
		if err := json.Unmarshal(event.Kv.Value, &v); err != nil {
			klog.Errorf("invalid lease record for lease %x: %v", uint64(id), err)
			return
		}
		lease, ok := m.leases[id]
		if !ok {
			lease = &Lease{ID: id}
			m.leases[id] = lease
		}
		lease.TTL = v.TTL
		lease.Expiration = time.Now().Add(time.Duration(v.TTL) * time.Second)

	case mvccpb.DELETE:
		if lease, ok := m.leases[id]; ok {
			delete(m.leases, id)
			delete(m.expiring, id)
			if len(lease.keys) > 0 {
				m.deleteQueue.Push(lease)
			}
		}

	default:
		klog.Fatalf("unexpected event type: %s", event.Type)
	}
}

func (m *LeaseManager) HasLease(leaseID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.leases[LeaseID(leaseID)]
	return ok
}

func (m *LeaseManager) OnLogEvent(event *mvccpb.Event) {
	if id, ok := parseLeaseKey(event.Kv.Key); ok {
		m.onLeaseRecord(id, event)
		return
	}

	newLeaseID := int64(0)
	oldLease := int64(0)
	if event.Kv != nil {
		newLeaseID = event.Kv.Lease
	}
	if event.PrevKv != nil {
		oldLease = event.PrevKv.Lease
	}

	if newLeaseID == 0 && oldLease == 0 {
		return // Ignore events without a lease
	}

	if event.Kv == nil {
		klog.Fatalf("event.Kv is nil for event: %v", event)
	}

	if event.Type != mvccpb.PUT && event.Type != mvccpb.DELETE {
		klog.Fatalf("unexpected event type: %s", event.Type)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// A put that keeps the key's existing lease changes nothing: the key
	// is already on the lease's list, and appending again would grow it
	// with every refresh of the key.
	if newLeaseID != 0 && newLeaseID != oldLease {
		newLease, ok := m.leases[LeaseID(newLeaseID)]
		if !ok {
			// A key attached to a lease we have no record of: a log written
			// before leases were persisted, or a crash between logging a
			// revoke and deleting its keys. Create a placeholder that
			// expires at once, so the keys are cleaned up rather than
			// leaked.
			now := time.Now()
			newLease = &Lease{
				ID:         LeaseID(newLeaseID),
				TTL:        0,
				Expiration: now,
			}
			m.leases[LeaseID(newLeaseID)] = newLease
		}

		newLease.keys = append(newLease.keys, event.Kv.Key)
	}

	if newLeaseID != oldLease && oldLease != 0 {
		oldLeaseObj, ok := m.leases[LeaseID(oldLease)]
		if ok {
			// Remove the key from the old lease
			for i, key := range oldLeaseObj.keys {
				if bytes.Equal(key, event.Kv.Key) {
					oldLeaseObj.keys = append(oldLeaseObj.keys[:i], oldLeaseObj.keys[i+1:]...)
					break
				}
			}

			if len(oldLeaseObj.keys) == 0 {
				delete(m.leases, LeaseID(oldLease))
			}
		}
	}
}
