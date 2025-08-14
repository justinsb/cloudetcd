package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/bptree"
	"justinsb.com/cloudetcd/pkg/persistence"
	"k8s.io/klog/v2"
)

// MemoryStorage is an in-memory implementation of the Storage interface.
type MemoryStorage struct {
	mu sync.RWMutex

	revisions bptree.BPTree

	log persistence.Log // Persistence log

	watcherMu sync.RWMutex
	watchers  []*memoryWatcher
}

var _ Storage = &MemoryStorage{}

// NewMemoryStorage creates a new in-memory storage instance with the given log.
// It returns an error if it cannot replay the log to restore the storage state.
func NewMemoryStorage(log persistence.Log) (*MemoryStorage, error) {
	ms := &MemoryStorage{
		log: log,
	}

	log.SetListener(ms)

	// Replay the log to restore state
	if err := ms.ReplayLog(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to replay log on startup: %w", err)
	}

	return ms, nil
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
		return nil
	}

	// Read all log records starting from revision 1

	callback := func(revision Revision, record *persistence.LogRecord) bool {
		for _, event := range record.Events {
			switch event.Type {
			case mvccpb.PUT:
				// Replay PUT operation
				m.revisions.AddRevision(event.Kv.Key, revision)

			case mvccpb.DELETE:
				// Replay DELETE operation
				m.revisions.AddRevision(event.Kv.Key, revision)

			default:
				// Skip unknown operations
				klog.Fatalf("unknown operation: %s", event.Type)
			}
		}

		return true
	}

	if err := m.log.Read(ctx, 1, callback); err != nil {
		return fmt.Errorf("failed to read log records: %w", err)
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
		return nil, fmt.Errorf("lease is not supported")
	}

	existingRevision, hasExisting := t.storage.revisions.GetLatestRevisionByKey(key, t.snapshotTimestamp)

	newRevision := Revision(t.snapshotTimestamp + 1)

	newKV := &mvccpb.KeyValue{
		Key:         key,
		Value:       value,
		ModRevision: int64(newRevision),
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

func (t *txn) commit(ctx context.Context, resp *etcdserverpb.TxnResponse) error {
	log := klog.FromContext(ctx)

	if len(t.logEvents) == 0 {
		return nil
	}

	// Let's see if we can commit this transaction without conflicts

	log.Info("committing transaction", "events", &etcdserverpb.WatchResponse{Events: t.logEvents})

	logRecord := &persistence.LogRecord{
		Events: t.logEvents,
	}

	newLogRevision, ok, err := t.storage.log.Append(ctx, logRecord, t.meta)
	if err != nil || !ok {
		return fmt.Errorf("failed to append to log: %w", err)
	}

	// TODO: Can we reuse replay?

	for _, event := range logRecord.Events {
		switch event.Type {
		case mvccpb.PUT:
			t.storage.revisions.AddRevision(event.Kv.Key, newLogRevision)

		case mvccpb.DELETE:
			t.storage.revisions.AddRevision(event.Kv.Key, newLogRevision)

		default:
			// Skip unknown operations
			klog.Fatalf("unknown operation: %s", event.Type)
		}
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
			return fmt.Errorf("unsupported response type: %T", response)
		}
	}

	return nil
}

// Txn executes a transaction against the storage.
func (m *MemoryStorage) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshotTimestamp, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current revision: %w", err)
	}

	txn := &txn{
		snapshotTimestamp: snapshotTimestamp,
		storage:           m,
	}
	txn.meta = persistence.NewTxnMeta(snapshotTimestamp)

	conditionsFailed := false
	for _, cond := range req.Compare {
		key := cond.GetKey()

		existingRevision, hasExisting := m.revisions.GetLatestRevisionByKey(key, snapshotTimestamp)

		txn.meta.AddRead(string(key))

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
				return nil, fmt.Errorf("unsupported compare target: %T", cond.GetTargetUnion())
			}

			switch cond.GetResult() {
			case etcdserverpb.Compare_EQUAL:
				if modRevision != Revision(targetValue.ModRevision) {
					// klog.Infof("condition failed: modRevision %d != targetValue %d (prevEvent: %v, snapshotTimestamp: %d)", modRevision, targetValue.ModRevision, prevEvent, snapshotTimestamp)
					conditionsFailed = true
					break
				}

			default:
				return nil, fmt.Errorf("unsupported compare result: %s", cond.GetResult())
			}

		default:
			return nil, fmt.Errorf("unsupported compare target: %s", cond.GetTarget())
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
			putResp, err := txn.put(ctx, op.GetRequestPut())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: putResp},
			})

		case *etcdserverpb.RequestOp_RequestDeleteRange:
			deleteResp, err := txn.delete(ctx, op.GetRequestDeleteRange())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: deleteResp},
			})

		case *etcdserverpb.RequestOp_RequestRange:
			var rangeResp *etcdserverpb.RangeResponse
			if op.GetRequestRange().GetRangeEnd() == nil {
				rangeResp, err = txn.get(ctx, op.GetRequestRange())
				if err != nil {
					return nil, err
				}
				txn.meta.AddRead(string(op.GetRequestRange().Key))
			} else {
				rangeResp, err = txn.list(ctx, op.GetRequestRange())
				if err != nil {
					return nil, err
				}
				txn.meta.AddList(op.GetRequestRange())
			}

			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: rangeResp},
			})

		default:
			return nil, fmt.Errorf("unsupported operation: %T", op.Request)
		}
	}

	if err := txn.commit(ctx, resp); err != nil {
		return nil, err
	}

	return resp, nil
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

	snapshotTimestamp := Revision(0)
	if req.Revision > 0 {
		snapshotTimestamp = Revision(req.Revision)
	} else {
		currentRevision, err := m.log.GetCurrentRevision(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get current revision: %w", err)
		}
		snapshotTimestamp = currentRevision
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

	snapshotTimestamp := Revision(0)
	if req.Revision > 0 {
		snapshotTimestamp = Revision(req.Revision)
	} else {
		currentRevision, err := m.log.GetCurrentRevision(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get current revision: %w", err)
		}
		snapshotTimestamp = currentRevision
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
