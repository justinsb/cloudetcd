package lease

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"justinsb.com/cloudetcd/pkg/storage"
	"k8s.io/klog/v2"
)

type LeaseID int64

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
		leases:  make(map[LeaseID]*Lease),
		storage: storage,
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

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for expired keys
	now := time.Now()

	// TODO: Move to a more efficient and fair data structure
	for _, lease := range m.leases {
		if lease.Expiration.Before(now) {
			log.Info("Lease expired", "leaseID", lease.ID)
			// Lease is expired
			m.deleteQueue.Push(lease)
			delete(m.leases, lease.ID)
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
	delete(m.leases, id)

	m.deleteQueue.Push(lease)

	return &etcdserverpb.LeaseRevokeResponse{
		Header: m.createHeader(0),
	}, nil
}

func (m *LeaseManager) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := LeaseID(time.Now().UnixNano())
	lease := &Lease{
		ID:         id,
		TTL:        req.TTL,
		Expiration: time.Now().Add(time.Duration(req.TTL) * time.Second),
	}
	m.leases[id] = lease

	return &etcdserverpb.LeaseGrantResponse{
		Header: m.createHeader(0),
		ID:     int64(lease.ID),
		TTL:    lease.TTL,
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

func (m *LeaseManager) HasLease(leaseID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.leases[LeaseID(leaseID)]
	return ok
}

func (m *LeaseManager) OnLogEvent(event *mvccpb.Event) {
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

	if newLeaseID != 0 {
		newLease, ok := m.leases[LeaseID(newLeaseID)]
		if !ok {
			// Create a short-lived lease
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
