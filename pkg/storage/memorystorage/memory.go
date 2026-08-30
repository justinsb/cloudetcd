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

package memorystorage

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
	"justinsb.com/cloudetcd/pkg/bptree"
	"justinsb.com/cloudetcd/pkg/lease"
	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/storage"
	"k8s.io/klog/v2"
)

// MemoryStorage is an in-memory implementation of the Storage interface.
type MemoryStorage struct {
	leaseManager *lease.LeaseManager

	mu sync.RWMutex

	revisions bptree.BPTree

	// compacted is the revision history has been discarded through.
	compacted Revision
	compactWG sync.WaitGroup

	// applied is the highest log revision whose events have been applied to
	// revisions (and the lease manager). Records are committed by the log's
	// batching layer, possibly many per batch, and applied here in log order
	// by applyUpTo; guarded by mu.
	applied Revision

	log persistence.Log // Persistence log

	watcherMu sync.RWMutex
	watchers  []*memoryWatcher
}

var _ storage.Storage = &MemoryStorage{}

// NewMemoryStorage creates a new in-memory storage instance with the given log.
// It returns an error if it cannot replay the log to restore the storage state.
func NewMemoryStorage(log persistence.Log) (*MemoryStorage, error) {
	ms := &MemoryStorage{
		log: log,
	}

	log.SetListener(ms)

	ms.leaseManager = lease.NewLeaseManager(ms)

	// Replay the log to restore state
	if err := ms.ReplayLog(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to replay log on startup: %w", err)
	}

	return ms, nil
}

func (m *MemoryStorage) LeaseManager() storage.LeaseManager {
	return m.leaseManager
}

func (m *MemoryStorage) GetCurrentRevision(ctx context.Context) (Revision, error) {
	return m.log.GetCurrentRevision(ctx)
}

// ReplayLog replays the persistence log to restore the storage state
func (m *MemoryStorage) ReplayLog(ctx context.Context) error {
	// Get the current revision from the log
	currentRevision, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current revision: %w", err)
	}

	// If no log entries exist, we're done
	if currentRevision == 0 {
		// We want the log to start at revision 1, so we'll add a dummy event
		newRevision, ok, err := m.log.Append(ctx, &persistence.LogRecord{}, persistence.NewTxnMeta(0))
		if err != nil {
			return fmt.Errorf("failed to append dummy event: %w", err)
		}
		if !ok {
			return fmt.Errorf("failed to append dummy event")
		}
		if newRevision != 1 {
			return fmt.Errorf("expected revision 1, got %d", newRevision)
		}
		m.applied = newRevision
		return nil
	}

	// Read all log records starting from revision 1

	callback := func(revision Revision, record *persistence.LogRecord) bool {
		for _, event := range record.Events {
			switch event.Type {
			case mvccpb.PUT:
				// Replay PUT operation
				m.revisions.AddRevision(event.Kv.Key, revision, false)

			case mvccpb.DELETE:
				// Replay DELETE operation
				m.revisions.AddRevision(event.Kv.Key, revision, true)

			default:
				// Skip unknown operations
				klog.Fatalf("unknown operation: %s", event.Type)
			}

			m.leaseManager.OnLogEvent(event)
		}

		return true
	}

	if err := m.log.Read(ctx, 1, callback); err != nil {
		return fmt.Errorf("failed to read log records: %w", err)
	}
	m.applied = currentRevision

	return nil
}

// applyUpTo applies every committed log record up to and including rev to
// the index, in log order. Any goroutine may call it: a transaction after its
// append returns, or a reader before it evaluates at a snapshot, so that the
// index always reflects the revision a caller is about to read at, no matter
// which goroutine's append happened to commit it.
func (m *MemoryStorage) applyUpTo(ctx context.Context, rev Revision) error {
	m.mu.RLock()
	applied := m.applied
	m.mu.RUnlock()
	if applied >= rev {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for r := m.applied + 1; r <= rev; r++ {
		record, err := m.log.GetLogEntry(r)
		if err != nil {
			return fmt.Errorf("reading log record %d: %w", r, err)
		}
		if record == nil {
			return fmt.Errorf("log record %d not found", r)
		}
		for _, event := range record.Events {
			switch event.Type {
			case mvccpb.PUT:
				m.revisions.AddRevision(event.Kv.Key, r, false)
			case mvccpb.DELETE:
				m.revisions.AddRevision(event.Kv.Key, r, true)
			default:
				klog.Fatalf("unknown operation: %s", event.Type)
			}
			m.leaseManager.OnLogEvent(event)
		}
		m.applied = r
	}
	return nil
}

// // convertToMVCCKeyValue converts a storage.KeyValue to mvccpb.KeyValue
// func logEntryToKeyValue(r *persistence.LogRecord) *mvccpb.KeyValue {
// 	return &mvccpb.KeyValue{
// 		Key:            r.Key,
// 		Value:          r.Value,
// 		CreateRevision: int64(r.CreateRevision),
// 		ModRevision:    int64(r.Revision),
// 		Version:        r.Version,
// 		Lease:          0, // For now, no lease
// 	}
// }

// Put writes a key-value pair to the storage.
func (m *MemoryStorage) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	txn := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: req}},
		},
	}

	txnResp, err := m.Txn(ctx, txn)
	if err != nil {
		return nil, err
	}

	putResp := txnResp.Responses[0].GetResponsePut()
	if putResp == nil {
		return nil, fmt.Errorf("expected put response, got %T", txnResp.Responses[0].Response)
	}
	// TODO: Are the headers set in the individual responses?
	putResp.Header = txnResp.Header

	return putResp, nil
}

func (t *txn) put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	key := req.GetKey()
	value := req.GetValue()

	if req.GetLease() != 0 {
		if !t.storage.leaseManager.HasLease(req.GetLease()) {
			return nil, rpctypes.ErrGRPCLeaseNotFound
		}
	}

	existingRevision, hasExisting := t.storage.revisions.GetLatestRevisionByKey(key, t.snapshotTimestamp)

	newRevision := Revision(t.snapshotTimestamp + 1)

	newKV := &mvccpb.KeyValue{
		Key:         key,
		Value:       value,
		ModRevision: int64(newRevision),
		Lease:       req.GetLease(),
	}

	var prevKv *mvccpb.KeyValue
	if hasExisting {
		logEntry, err := t.storage.log.GetLogEntry(existingRevision)
		if logEntry == nil || err != nil {
			klog.Fatalf("log entry not found for revision %d", existingRevision)
		}
		prevEvent := findEvent(logEntry, key)
		if prevEvent == nil {
			klog.Fatalf("prevKv not found for key %s", key)
		}
		if prevEvent.Type == mvccpb.PUT {
			prevKv = prevEvent.Kv
		} else if prevEvent.Type == mvccpb.DELETE {
			prevKv = nil
		} else {
			klog.Fatalf("unknown operation: %s", prevEvent.Type)
		}
	}

	if prevKv != nil {
		newKV.CreateRevision = prevKv.CreateRevision
		newKV.Version = prevKv.Version + 1
	} else {
		newKV.CreateRevision = int64(newRevision)
		newKV.Version = 1
	}

	// Create and broadcast watch event
	event := &mvccpb.Event{
		Type:   mvccpb.PUT,
		Kv:     newKV,
		PrevKv: prevKv,
	}

	t.logEvents = append(t.logEvents, event)
	t.meta.AddWrite(string(key))

	// TODO: Do individual responses have headers?
	response := &etcdserverpb.PutResponse{}
	if req.PrevKv {
		response.PrevKv = prevKv
	}

	return response, nil
}

// commit appends the transaction's events to the log. It returns false (and
// no error) if the batching layer rejected the transaction because a key it
// read or wrote was committed by another transaction after its snapshot; the
// caller re-evaluates at a fresh snapshot.
func (t *txn) commit(ctx context.Context, resp *etcdserverpb.TxnResponse) (bool, error) {
	log := klog.FromContext(ctx)

	if len(t.logEvents) == 0 {
		return true, nil
	}

	// Let's see if we can commit this transaction without conflicts

	log.V(4).Info("committing transaction", "events", &etcdserverpb.WatchResponse{Events: t.logEvents})

	logRecord := &persistence.LogRecord{
		Events: t.logEvents,
	}

	newLogRevision, ok, err := t.storage.log.Append(ctx, logRecord, t.meta)
	if err != nil {
		return false, fmt.Errorf("failed to append to log: %w", err)
	}
	if !ok {
		return false, nil
	}

	// The batch may have committed other transactions ahead of ours; apply
	// everything up to our revision so the response is visible to reads.
	if err := t.storage.applyUpTo(ctx, newLogRevision); err != nil {
		return false, err
	}

	resp.Header.Revision = int64(newLogRevision)
	// TODO: Do individual responses have headers?
	for _, response := range resp.Responses {
		switch response := response.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponsePut:
			response.ResponsePut.Header = resp.Header

		case *etcdserverpb.ResponseOp_ResponseDeleteRange:
			response.ResponseDeleteRange.Header = resp.Header

		case *etcdserverpb.ResponseOp_ResponseRange:
			response.ResponseRange.Header = resp.Header

		default:
			return false, fmt.Errorf("unsupported response type: %T", response)
		}
	}

	return true, nil
}

// maxTxnAttempts bounds how many times a transaction is re-evaluated after
// losing optimistic validation. Kubernetes transactions touch one key each,
// so a retry only happens when that key was concurrently written, and the
// re-evaluation then fails the compare rather than conflicting again.
const maxTxnAttempts = 64

// Txn executes a transaction against the storage.
//
// Transactions run optimistically: each is evaluated at a snapshot under a
// read lock, its events are appended to the log without any storage lock held
// (so the log's batching layer can commit many transactions per write to the
// backend), and the batching layer validates per key that nothing the
// transaction read or wrote was committed after its snapshot. A transaction
// that loses that validation is re-evaluated at a fresh snapshot, where its
// compares see the concurrent write.
func (m *MemoryStorage) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	for attempt := 0; attempt < maxTxnAttempts; attempt++ {
		resp, committed, err := m.tryTxn(ctx, req)
		if err != nil {
			return nil, err
		}
		if committed {
			return resp, nil
		}
	}
	return nil, fmt.Errorf("transaction conflicted with concurrent writes %d times", maxTxnAttempts)
}

func (m *MemoryStorage) tryTxn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, bool, error) {
	snapshotTimestamp, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get current revision: %w", err)
	}
	// The log may have committed records that no goroutine has applied yet.
	if err := m.applyUpTo(ctx, snapshotTimestamp); err != nil {
		return nil, false, err
	}

	resp, txn, err := m.evaluate(ctx, req, snapshotTimestamp)
	if err != nil {
		return nil, false, err
	}
	committed, err := txn.commit(ctx, resp)
	if err != nil {
		return nil, false, err
	}
	return resp, committed, nil
}

// evaluate runs the transaction's compares and operations against the index
// at snapshotTimestamp, staging its events; nothing is written.
func (m *MemoryStorage) evaluate(ctx context.Context, req *etcdserverpb.TxnRequest, snapshotTimestamp Revision) (*etcdserverpb.TxnResponse, *txn, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var err error
	txn := &txn{
		snapshotTimestamp: snapshotTimestamp,
		storage:           m,
	}
	txn.meta = persistence.NewTxnMeta(snapshotTimestamp)

	conditionsFailed := false
	for _, cond := range req.Compare {
		key := cond.GetKey()

		existingRevision, hasExisting := m.revisions.GetLatestRevisionByKey(key, snapshotTimestamp)

		// Record the mod_revision we observed for this key so that batch
		// commit can validate it per-key rather than against the whole
		// keyspace. A key that does not exist is recorded as revision 0.
		readRevision := Revision(0)
		if hasExisting {
			readRevision = existingRevision
		}
		txn.meta.AddRead(string(key), readRevision)

		var prevEvent *mvccpb.Event
		if hasExisting {
			logEntry, err := m.log.GetLogEntry(existingRevision)
			if logEntry == nil || err != nil {
				klog.Fatalf("log entry not found for revision %d", existingRevision)
			}
			prevEvent = findEvent(logEntry, key)
			if prevEvent == nil {
				klog.Fatalf("prevKv not found for key %s", key)
			}
		}

		switch cond.GetTarget() {
		case etcdserverpb.Compare_MOD:
			modRevision := Revision(0)
			if prevEvent == nil || prevEvent.Type == mvccpb.DELETE {
				modRevision = Revision(0)
			} else {
				modRevision = Revision(prevEvent.Kv.ModRevision)
			}

			targetValue, ok := cond.GetTargetUnion().(*etcdserverpb.Compare_ModRevision)
			if !ok {
				return nil, nil, fmt.Errorf("unsupported target: %T", cond.GetTargetUnion())
			}

			switch cond.GetResult() {
			case etcdserverpb.Compare_EQUAL:
				if modRevision != Revision(targetValue.ModRevision) {
					// klog.Infof("condition failed: modRevision %d != targetValue %d (prevEvent: %v, snapshotTimestamp: %d)", modRevision, targetValue.ModRevision, prevEvent, snapshotTimestamp)
					conditionsFailed = true
					break
				}

			default:
				return nil, nil, fmt.Errorf("unsupported compare result: %s", cond.GetResult())
			}

		case etcdserverpb.Compare_VERSION:
			// kube-apiserver's compactor guards compact_rev_key with
			// version(key) == v; a missing or deleted key has version 0.
			version := int64(0)
			if prevEvent != nil && prevEvent.Type != mvccpb.DELETE {
				version = prevEvent.Kv.Version
			}

			targetValue, ok := cond.GetTargetUnion().(*etcdserverpb.Compare_Version)
			if !ok {
				return nil, nil, fmt.Errorf("unsupported target: %T", cond.GetTargetUnion())
			}

			switch cond.GetResult() {
			case etcdserverpb.Compare_EQUAL:
				if version != targetValue.Version {
					conditionsFailed = true
				}

			default:
				return nil, nil, fmt.Errorf("unsupported compare result: %s", cond.GetResult())
			}

		case etcdserverpb.Compare_LEASE:
			lease := int64(0)
			if prevEvent == nil || prevEvent.Type == mvccpb.DELETE {
				lease = 0
			} else {
				lease = prevEvent.Kv.Lease
			}

			targetValue, ok := cond.GetTargetUnion().(*etcdserverpb.Compare_Lease)
			if !ok {
				return nil, nil, fmt.Errorf("unsupported target: %T", cond.GetTargetUnion())
			}

			switch cond.GetResult() {
			case etcdserverpb.Compare_EQUAL:
				if lease != targetValue.Lease {
					conditionsFailed = true
					break
				}

			default:
				return nil, nil, fmt.Errorf("unsupported compare result: %s", cond.GetResult())
			}

		default:
			return nil, nil, fmt.Errorf("unsupported compare target: %s", cond.GetTarget())
		}
	}

	resp := &etcdserverpb.TxnResponse{
		Header:    createHeader(snapshotTimestamp),
		Succeeded: !conditionsFailed,
	}

	operations := req.Success
	if conditionsFailed {
		operations = req.Failure
	}

	for _, op := range operations {
		switch op.Request.(type) {
		case *etcdserverpb.RequestOp_RequestPut:
			putRequest := op.GetRequestPut()
			putResp, err := txn.put(ctx, putRequest)
			if err != nil {
				return nil, nil, err
			}
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: putResp},
			})

		case *etcdserverpb.RequestOp_RequestDeleteRange:
			deleteResp, err := txn.delete(ctx, op.GetRequestDeleteRange())
			if err != nil {
				return nil, nil, err
			}
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: deleteResp},
			})

		case *etcdserverpb.RequestOp_RequestRange:
			var rangeResp *etcdserverpb.RangeResponse
			if op.GetRequestRange().GetRangeEnd() == nil {
				rangeResp, err = txn.get(ctx, op.GetRequestRange())
				if err != nil {
					return nil, nil, err
				}
				rangeKey := op.GetRequestRange().Key
				readRevision := Revision(0)
				if rev, ok := m.revisions.GetLatestRevisionByKey(rangeKey, snapshotTimestamp); ok {
					readRevision = rev
				}
				txn.meta.AddRead(string(rangeKey), readRevision)
			} else {
				rangeResp, err = txn.list(ctx, op.GetRequestRange())
				if err != nil {
					return nil, nil, err
				}
				txn.meta.AddList(op.GetRequestRange())
			}

			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: rangeResp},
			})

		default:
			return nil, nil, fmt.Errorf("unsupported operation: %T", op.Request)
		}
	}

	return resp, txn, nil
}

type txn struct {
	snapshotTimestamp Revision
	logEvents         []*mvccpb.Event
	storage           *MemoryStorage

	meta *persistence.TxnMeta
}

// Get retrieves a key-value pair from the storage.
func (m *MemoryStorage) Get(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	txn := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: req}},
		},
	}

	txnResp, err := m.Txn(ctx, txn)
	if err != nil {
		return nil, err
	}

	rangeResp := txnResp.Responses[0].GetResponseRange()
	if rangeResp == nil {
		return nil, fmt.Errorf("expected range response, got %T", txnResp.Responses[0].Response)
	}
	// TODO: Are the headers set in the individual responses?
	rangeResp.Header = txnResp.Header

	return rangeResp, nil
}

func (t *txn) get(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	m := t.storage

	if req.Key == nil {
		return nil, fmt.Errorf("key is required by Get")
	}

	if req.RangeEnd != nil {
		return nil, fmt.Errorf("range end is not supported by Get")
	}

	snapshotTimestamp := t.snapshotTimestamp
	if req.Revision > 0 {
		snapshotTimestamp = Revision(req.Revision)
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

	logEntry, err := m.log.GetLogEntry(latestRevision)
	if err != nil {
		return nil, fmt.Errorf("failed to get log entry: %w", err)
	}
	if logEntry == nil {
		klog.Fatalf("log entry not found for revision %d", latestRevision)
	}

	event := findEvent(logEntry, req.Key)
	if event == nil {
		klog.Fatalf("key not found in log entry for revision %d", latestRevision)
	}

	switch event.Type {
	case mvccpb.PUT:
		if req.CountOnly {
			resp.Count = 1
			return resp, nil
		}

		kv := event.Kv
		if req.KeysOnly {
			kv = copyWithoutValue(kv)
		}
		resp.Kvs = []*mvccpb.KeyValue{kv}
		resp.Count = 1
		if req.Limit == 1 {
			resp.More = true
		}
		return resp, nil

	case mvccpb.DELETE:
		return resp, nil

	default:
		panic(fmt.Sprintf("unknown operation: %s", event.Type))
	}
}

func findEvent(logEntry *persistence.LogRecord, key []byte) *mvccpb.Event {
	for _, event := range logEntry.Events {
		if bytes.Equal(event.Kv.Key, key) {
			return event
		}
	}
	return nil
}

// Delete removes a key from the storage.
func (m *MemoryStorage) Delete(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	txn := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: req}},
		},
	}

	txnResp, err := m.Txn(ctx, txn)
	if err != nil {
		return nil, err
	}

	deleteResp := txnResp.Responses[0].GetResponseDeleteRange()
	if deleteResp == nil {
		return nil, fmt.Errorf("expected delete response, got %T", txnResp.Responses[0].Response)
	}
	// TODO: Are the headers set in the individual responses?
	deleteResp.Header = txnResp.Header

	return deleteResp, nil
}

func (t *txn) delete(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	m := t.storage

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

	latestRevision, exists := m.revisions.GetLatestRevisionByKey(key, t.snapshotTimestamp)
	if !exists {
		resp := &etcdserverpb.DeleteRangeResponse{
			Header:  createHeader(t.snapshotTimestamp),
			Deleted: 0,
		}

		return resp, nil
	}

	oldLogEntry, err := m.log.GetLogEntry(latestRevision)
	if oldLogEntry == nil || err != nil {
		klog.Fatalf("log entry not found for revision %d", latestRevision)
	}

	oldEvent := findEvent(oldLogEntry, key)
	if oldEvent == nil {
		klog.Fatalf("old event not found for key %s", key)
	}
	if oldEvent.Type != mvccpb.PUT {
		klog.Fatalf("old event is not a PUT for key %s", key)
	}

	// Append to the persistence log

	// A DELETE/EXPIRE event contains the deleted key with
	// its modification revision set to the revision of deletion.

	event := &mvccpb.Event{
		Type: mvccpb.DELETE,
		Kv: &mvccpb.KeyValue{
			Key:         key,
			ModRevision: int64(t.snapshotTimestamp + 1),
			Version:     0, // version is set to 0 for DELETE events
		},
	}

	// Note: we always include prev_kv in the log
	event.PrevKv = oldEvent.Kv

	t.logEvents = append(t.logEvents, event)
	t.meta.AddWrite(string(key))

	resp := &etcdserverpb.DeleteRangeResponse{
		Deleted: 1,
	}

	return resp, nil
}

// List returns a range of key-value pairs.
func (m *MemoryStorage) List(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	txn := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: req}},
		},
	}

	txnResp, err := m.Txn(ctx, txn)
	if err != nil {
		return nil, err
	}

	rangeResp := txnResp.Responses[0].GetResponseRange()
	if rangeResp == nil {
		return nil, fmt.Errorf("expected range response, got %T", txnResp.Responses[0].Response)
	}
	// TODO: Are the headers set in the individual responses?
	rangeResp.Header = txnResp.Header

	return rangeResp, nil
}

func (t *txn) list(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	m := t.storage

	if req.RangeEnd == nil {
		return nil, fmt.Errorf("range end is required by List")
	}

	snapshotTimestamp := t.snapshotTimestamp
	if req.Revision > 0 {
		snapshotTimestamp = Revision(req.Revision)
	}

	resp := &etcdserverpb.RangeResponse{
		Header: createHeader(snapshotTimestamp),
	}

	hasRangeEnd := req.RangeEnd != nil && !bytes.Equal(req.RangeEnd, []byte{0})

	var errs []error
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
			// TODO: Can we store whether this is a delete, so we don't need a log lookup?

			logEntry, err := m.log.GetLogEntry(latest)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to get log entry: %w", err))
				return false
			}
			if logEntry == nil {
				klog.Fatalf("log entry not found for revision %d", latest)
			}
			for _, event := range logEntry.Events {
				if event.Type == mvccpb.PUT {
					if req.CountOnly {
						resp.Count++
					} else if req.KeysOnly {
						resp.Count++
						resp.Kvs = append(resp.Kvs, copyWithoutValue(event.Kv))
					} else {
						resp.Count++
						resp.Kvs = append(resp.Kvs, event.Kv)
					}
				}
				if req.Limit > 0 && resp.Count >= req.Limit {
					resp.More = true
					// Stop iterating
					return false
				}
			}
		}

		return true
	})

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return resp, nil
}

func copyWithoutValue(kv *mvccpb.KeyValue) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:            kv.Key,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Lease:          kv.Lease,
	}
}

// // removeWatcher removes a watcher from the storage
// func (m *MemoryStorage) removeWatcher(id int64) {
// 	m.watcherMu.Lock()
// 	defer m.watcherMu.Unlock()
// 	delete(m.watchers, id)
// }

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
	m.applied = 0
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

// Compact discards history at or below revision. The revision is lowered if
// an open watcher has not yet delivered past it, so that watchers are not
// cut off; it is then applied to the index (each key keeps its latest
// write at or below the point, and keys whose latest write is a delete go)
// and to the log (files below the point are rewritten with only the
// records the index still refers to).
func (m *MemoryStorage) Compact(ctx context.Context, revision Revision) (Revision, error) {
	m.mu.Lock()
	through := revision
	if through > m.applied {
		through = m.applied
	}
	m.watcherMu.RLock()
	for _, w := range m.watchers {
		w.stateMutex.Lock()
		closed := w.closed
		w.stateMutex.Unlock()
		if closed {
			continue
		}
		if delivered := Revision(w.delivered.Load()); delivered < through {
			through = delivered
		}
	}
	m.watcherMu.RUnlock()
	if through <= m.compacted {
		m.mu.Unlock()
		return m.compacted, nil
	}
	keysRemoved, revisionsDropped := m.revisions.Compact(through)
	m.compacted = through
	m.mu.Unlock()
	klog.V(1).Infof("compacted index through revision %d: %d keys removed, %d revisions dropped", through, keysRemoved, revisionsDropped)

	// The log rewrite runs in the background: it reads closed files and
	// takes seconds at scale, and nothing waits on it. The log serializes
	// compactions, so overlapping requests queue.
	// A record is live if it is a key's state as of the compaction point:
	// its latest put or delete at or below through, which is what the index
	// kept. Not simply the key's latest revision: a read at a revision
	// between the point and a later write of the key still needs it.
	live := func(key []byte, rev Revision) bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		latest, ok := m.revisions.GetLatestRevisionByKey(key, through)
		return ok && latest == rev
	}
	m.compactWG.Add(1)
	go func() {
		defer m.compactWG.Done()
		start := time.Now()
		if err := m.log.Compact(context.Background(), through, live); err != nil {
			klog.Errorf("compacting log through revision %d: %v", through, err)
			return
		}
		klog.V(1).Infof("compacted log through revision %d in %s", through, time.Since(start).Round(time.Millisecond))
	}()
	return through, nil
}

// WaitForCompaction blocks until background log compactions have finished,
// for tests.
func (m *MemoryStorage) WaitForCompaction() {
	m.compactWG.Wait()
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

func (m *MemoryStorage) Status(ctx context.Context) (*etcdserverpb.StatusResponse, error) {
	revision, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}

	return &etcdserverpb.StatusResponse{
		Header:  createHeader(revision),
		Version: "3.5.21",
		// DbSize:  0,
	}, nil
}

// OnLogEntry implements LogListener interface - called when a new log entry is added
func (m *MemoryStorage) OnLogEntry(logPosition persistence.Revision) {
	// Notify watchers about the new log entry
	m.watcherMu.RLock()
	defer m.watcherMu.RUnlock()

	for _, w := range m.watchers {
		w.stateMutex.Lock()
		w.logPosition = logPosition
		w.stateCond.Broadcast()
		w.stateMutex.Unlock()
	}
}
