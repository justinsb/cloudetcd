package api

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"justinsb.com/cloudetcd/pkg/storage"
)

// watchStream represents a single watch stream with multiple watches
type watchStream struct {
	server  *Server
	stream  etcdserverpb.Watch_WatchServer
	watches map[int64]*activeWatch
	watchID int64
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
}

// activeWatch represents a single active watch
type activeWatch struct {
	id      int64
	watcher storage.Watcher
	key     []byte
	prefix  bool
	prevKv  bool
	filters []etcdserverpb.WatchCreateRequest_FilterType
	closed  int32 // atomic flag
}

func (aw *activeWatch) isClosed() bool {
	return atomic.LoadInt32(&aw.closed) == 1
}

func (aw *activeWatch) close() {
	if atomic.CompareAndSwapInt32(&aw.closed, 0, 1) {
		if aw.watcher != nil {
			aw.watcher.Close()
		}
	}
}

// Server implements the etcd v3 API
type Server struct {
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedWatchServer
	etcdserverpb.UnimplementedLeaseServer

	storage storage.Storage
	grpc    *grpc.Server
}

// NewServer creates a new etcd API server
func NewServer(store storage.Storage) *Server {
	return &Server{
		storage: store,
	}
}

// Start starts the gRPC server on the given address
func (s *Server) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s.grpc = grpc.NewServer()
	etcdserverpb.RegisterKVServer(s.grpc, s)
	etcdserverpb.RegisterWatchServer(s.grpc, s)
	etcdserverpb.RegisterLeaseServer(s.grpc, s)

	fmt.Printf("Starting etcd API server on %s\n", addr)
	return s.grpc.Serve(lis)
}

// Stop gracefully stops the server
func (s *Server) Stop() {
	if s.grpc != nil {
		s.grpc.GracefulStop()
	}
}

// Range implements the Range RPC method
func (s *Server) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if req.Key == nil {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	// Handle prefix-based range queries
	if len(req.RangeEnd) > 0 {
		return s.handleRangeWithEnd(ctx, req)
	}

	// Handle single key or prefix queries
	return s.handleSingleKeyOrPrefix(ctx, req)
}

func (s *Server) handleSingleKeyOrPrefix(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	var revision storage.Revision
	if req.Revision > 0 {
		revision = storage.Revision(req.Revision)
	}

	var kvs []*mvccpb.KeyValue
	var count int64

	if req.CountOnly {
		// Count-only query
		if len(req.RangeEnd) == 0 {
			// Single key count
			_, err := s.storage.Get(ctx, req.Key, revision)
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					return &etcdserverpb.RangeResponse{
						Header: s.createHeader(revision),
						Count:  0,
					}, nil
				}
				return nil, status.Error(codes.Internal, err.Error())
			}
			count = 1
		} else {
			// Prefix count
			keys, err := s.storage.List(ctx, req.Key, revision)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			count = int64(len(keys))
		}
	} else {
		// Full query
		if len(req.RangeEnd) == 0 {
			// Single key query
			kv, err := s.storage.Get(ctx, req.Key, revision)
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					return &etcdserverpb.RangeResponse{
						Header: s.createHeader(revision),
						Count:  0,
					}, nil
				}
				return nil, status.Error(codes.Internal, err.Error())
			}
			kvs = []*mvccpb.KeyValue{s.convertToMVCCKeyValue(kv)}
			count = 1
		} else {
			// Prefix query
			keys, err := s.storage.List(ctx, req.Key, revision)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			kvs = make([]*mvccpb.KeyValue, len(keys))
			for i, kv := range keys {
				kvs[i] = s.convertToMVCCKeyValue(kv)
			}
			count = int64(len(kvs))
		}
	}

	return &etcdserverpb.RangeResponse{
		Header: s.createHeader(revision),
		Kvs:    kvs,
		Count:  count,
	}, nil
}

func (s *Server) handleRangeWithEnd(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// For now, we'll implement a simple range query
	// In a full implementation, we'd need to implement proper range queries
	// This is a simplified version that treats it as a prefix query

	var revision storage.Revision
	if req.Revision > 0 {
		revision = storage.Revision(req.Revision)
	}

	keys, err := s.storage.List(ctx, req.Key, revision)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Filter keys that are less than RangeEnd
	var filteredKeys []*storage.KeyValue
	for _, kv := range keys {
		if len(req.RangeEnd) > 0 && string(kv.Key) >= string(req.RangeEnd) {
			continue
		}
		filteredKeys = append(filteredKeys, kv)
	}

	kvs := make([]*mvccpb.KeyValue, len(filteredKeys))
	for i, kv := range filteredKeys {
		kvs[i] = s.convertToMVCCKeyValue(kv)
	}

	return &etcdserverpb.RangeResponse{
		Header: s.createHeader(revision),
		Kvs:    kvs,
		Count:  int64(len(kvs)),
	}, nil
}

// Put implements the Put RPC method
func (s *Server) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if req.Key == nil {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	// Get current revision for the key to check if it exists
	var prevKv *mvccpb.KeyValue
	if req.PrevKv {
		existing, err := s.storage.Get(ctx, req.Key, 0)
		if err == nil {
			prevKv = s.convertToMVCCKeyValue(existing)
		}
	}

	// Write the key-value pair
	revision, err := s.storage.Put(ctx, req.Key, req.Value, req.Lease)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &etcdserverpb.PutResponse{
		Header: s.createHeader(storage.Revision(revision)),
		PrevKv: prevKv,
	}, nil
}

// DeleteRange implements the DeleteRange RPC method
func (s *Server) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if req.Key == nil {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	var deleted int64
	var prevKvs []*mvccpb.KeyValue

	if len(req.RangeEnd) == 0 {
		// Single key deletion
		if req.PrevKv {
			existing, err := s.storage.Get(ctx, req.Key, 0)
			if err == nil {
				prevKvs = []*mvccpb.KeyValue{s.convertToMVCCKeyValue(existing)}
			}
		}

		// Check if key exists before deleting
		existing, err := s.storage.Get(ctx, req.Key, 0)
		if err == nil && !existing.Deleted {
			deleted = 1
		}

		revision, err := s.storage.Delete(ctx, req.Key)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		return &etcdserverpb.DeleteRangeResponse{
			Header:  s.createHeader(storage.Revision(revision)),
			Deleted: deleted,
			PrevKvs: prevKvs,
		}, nil
	}

	// Range deletion - for now, we'll implement a simple version
	// that deletes keys with the given prefix
	keys, err := s.storage.List(ctx, req.Key, 0)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var maxRevision storage.Revision
	for _, kv := range keys {
		if len(req.RangeEnd) > 0 && string(kv.Key) >= string(req.RangeEnd) {
			continue
		}

		if req.PrevKv {
			prevKvs = append(prevKvs, s.convertToMVCCKeyValue(kv))
		}

		revision, err := s.storage.Delete(ctx, kv.Key)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if storage.Revision(revision) > maxRevision {
			maxRevision = storage.Revision(revision)
		}
		deleted++
	}

	return &etcdserverpb.DeleteRangeResponse{
		Header:  s.createHeader(maxRevision),
		Deleted: deleted,
		PrevKvs: prevKvs,
	}, nil
}

// Txn implements the Txn RPC method
func (s *Server) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	// For now, implement a simple transaction that just executes the success operations
	// In a full implementation, we'd need to evaluate the compare operations

	var revision storage.Revision
	var responses []*etcdserverpb.ResponseOp

	for _, op := range req.Success {
		switch op.Request.(type) {
		case *etcdserverpb.RequestOp_RequestRange:
			resp, err := s.Range(ctx, op.GetRequestRange())
			if err != nil {
				return nil, err
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: resp,
				},
			})
			if storage.Revision(resp.Header.Revision) > revision {
				revision = storage.Revision(resp.Header.Revision)
			}

		case *etcdserverpb.RequestOp_RequestPut:
			resp, err := s.Put(ctx, op.GetRequestPut())
			if err != nil {
				return nil, err
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: resp,
				},
			})
			if storage.Revision(resp.Header.Revision) > revision {
				revision = storage.Revision(resp.Header.Revision)
			}

		case *etcdserverpb.RequestOp_RequestDeleteRange:
			resp, err := s.DeleteRange(ctx, op.GetRequestDeleteRange())
			if err != nil {
				return nil, err
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
					ResponseDeleteRange: resp,
				},
			})
			if storage.Revision(resp.Header.Revision) > revision {
				revision = storage.Revision(resp.Header.Revision)
			}
		}
	}

	return &etcdserverpb.TxnResponse{
		Header:    s.createHeader(revision),
		Succeeded: true, // For now, always succeed
		Responses: responses,
	}, nil
}

// Compact implements the Compact RPC method
func (s *Server) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	// For now, return success without actual compaction
	// In a full implementation, we'd need to implement actual compaction logic
	return &etcdserverpb.CompactionResponse{
		Header: s.createHeader(storage.Revision(req.Revision)),
	}, nil
}

// Watch implements the Watch RPC method
func (s *Server) Watch(stream etcdserverpb.Watch_WatchServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	ws := &watchStream{
		server:  s,
		stream:  stream,
		watches: make(map[int64]*activeWatch),
		watchID: 0,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Start goroutine to handle incoming requests
	go ws.handleRequests()

	// Wait for context to be done
	<-ctx.Done()

	// Clean up all watches
	ws.cleanup()

	return nil
}

// handleRequests processes incoming watch requests
func (ws *watchStream) handleRequests() {
	for {
		req, err := ws.stream.Recv()
		if err != nil {
			ws.cancel()
			return
		}

		switch r := req.RequestUnion.(type) {
		case *etcdserverpb.WatchRequest_CreateRequest:
			ws.handleCreateRequest(r.CreateRequest)
		case *etcdserverpb.WatchRequest_CancelRequest:
			ws.handleCancelRequest(r.CancelRequest)
		case *etcdserverpb.WatchRequest_ProgressRequest:
			ws.handleProgressRequest(r.ProgressRequest)
		}
	}
}

// handleCreateRequest handles watch creation
func (ws *watchStream) handleCreateRequest(req *etcdserverpb.WatchCreateRequest) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.watchID++
	watchID := ws.watchID

	// Determine if this is a prefix watch
	prefix := len(req.RangeEnd) > 0

	// Create storage watcher
	startRevision := storage.Revision(req.StartRevision)
	watcher, err := ws.server.storage.Watch(ws.ctx, req.Key, prefix, startRevision)
	if err != nil {
		// Send error response
		resp := &etcdserverpb.WatchResponse{
			Header:   ws.server.createHeader(0),
			WatchId:  watchID,
			Created:  false,
			Canceled: true,
		}
		ws.stream.Send(resp)
		return
	}

	// Create active watch
	activeWatch := &activeWatch{
		id:      watchID,
		watcher: watcher,
		key:     req.Key,
		prefix:  prefix,
		prevKv:  req.PrevKv,
		filters: req.Filters,
	}

	ws.watches[watchID] = activeWatch

	// Send creation response
	resp := &etcdserverpb.WatchResponse{
		Header:  ws.server.createHeader(0),
		WatchId: watchID,
		Created: true,
	}
	ws.stream.Send(resp)

	// Start watching for events
	go ws.watchEvents(activeWatch)
}

// handleCancelRequest handles watch cancellation
func (ws *watchStream) handleCancelRequest(req *etcdserverpb.WatchCancelRequest) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	watchID := req.WatchId
	if watch, exists := ws.watches[watchID]; exists {
		watch.close()
		delete(ws.watches, watchID)

		// Send cancellation response
		resp := &etcdserverpb.WatchResponse{
			Header:   ws.server.createHeader(0),
			WatchId:  watchID,
			Canceled: true,
		}
		ws.stream.Send(resp)
	}
}

// handleProgressRequest handles progress requests
func (ws *watchStream) handleProgressRequest(req *etcdserverpb.WatchProgressRequest) {
	// For now, just send a progress response without events
	resp := &etcdserverpb.WatchResponse{
		Header: ws.server.createHeader(0),
	}
	ws.stream.Send(resp)
}

// watchEvents monitors storage events for a specific watch
func (ws *watchStream) watchEvents(watch *activeWatch) {
	defer watch.close()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case watchResp, ok := <-watch.watcher.Chan():
			if !ok {
				return
			}

			if watch.isClosed() {
				return
			}

			// Convert storage events to etcd events
			var events []*mvccpb.Event
			for _, storageEvent := range watchResp.Events {
				// Apply filters
				if ws.shouldFilterEvent(storageEvent, watch.filters) {
					continue
				}

				event := &mvccpb.Event{
					Kv: ws.server.convertToMVCCKeyValue(storageEvent.Kv),
				}

				// Set event type
				switch storageEvent.Type {
				case storage.WatchEventTypePut:
					event.Type = mvccpb.PUT
				case storage.WatchEventTypeDelete:
					event.Type = mvccpb.DELETE
				}

				// Add previous KV if requested and available
				if watch.prevKv && storageEvent.PrevKv != nil {
					event.PrevKv = ws.server.convertToMVCCKeyValue(storageEvent.PrevKv)
				}

				events = append(events, event)
			}

			// Send response if we have events
			if len(events) > 0 {
				resp := &etcdserverpb.WatchResponse{
					Header:  ws.server.createHeader(storage.Revision(watchResp.Revision)),
					WatchId: watch.id,
					Events:  events,
				}

				if err := ws.stream.Send(resp); err != nil {
					return
				}
			}
		}
	}
}

// shouldFilterEvent checks if an event should be filtered out
func (ws *watchStream) shouldFilterEvent(event *storage.WatchEvent, filters []etcdserverpb.WatchCreateRequest_FilterType) bool {
	for _, filter := range filters {
		switch filter {
		case etcdserverpb.WatchCreateRequest_NOPUT:
			if event.Type == storage.WatchEventTypePut {
				return true
			}
		case etcdserverpb.WatchCreateRequest_NODELETE:
			if event.Type == storage.WatchEventTypeDelete {
				return true
			}
		}
	}
	return false
}

// cleanup closes all watches
func (ws *watchStream) cleanup() {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for _, watch := range ws.watches {
		watch.close()
	}
	ws.watches = make(map[int64]*activeWatch)
}

// Grant implements the Grant RPC method
func (s *Server) Grant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	// For now, return a simple lease grant
	// In a full implementation, we'd need to implement actual lease management
	return &etcdserverpb.LeaseGrantResponse{
		Header: s.createHeader(0),
		ID:     req.ID,
		TTL:    req.TTL,
	}, nil
}

// Revoke implements the Revoke RPC method
func (s *Server) Revoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	// For now, return success
	// In a full implementation, we'd need to implement actual lease revocation
	return &etcdserverpb.LeaseRevokeResponse{
		Header: s.createHeader(0),
	}, nil
}

// LeaseTimeToLive implements the LeaseTimeToLive RPC method
func (s *Server) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	// For now, return a simple response
	// In a full implementation, we'd need to implement actual lease TTL tracking
	return &etcdserverpb.LeaseTimeToLiveResponse{
		Header:     s.createHeader(0),
		ID:         req.ID,
		TTL:        -1, // Indicates no TTL
		GrantedTTL: -1,
	}, nil
}

// LeaseLeases implements the LeaseLeases RPC method
func (s *Server) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	// For now, return empty leases
	// In a full implementation, we'd need to implement actual lease tracking
	return &etcdserverpb.LeaseLeasesResponse{
		Header: s.createHeader(0),
		Leases: []*etcdserverpb.LeaseStatus{},
	}, nil
}

// Helper methods

func (s *Server) createHeader(revision storage.Revision) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		ClusterId: 1, // Simple cluster ID
		MemberId:  1, // Simple member ID
		Revision:  int64(revision),
		RaftTerm:  1, // Simple term
	}
}

func (s *Server) convertToMVCCKeyValue(kv *storage.KeyValue) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:            kv.Key,
		Value:          kv.Value,
		CreateRevision: int64(kv.CreateRevision),
		ModRevision:    int64(kv.CreateRevision), // For now, use CreateRevision as ModRevision
		Version:        1,                        // For now, always version 1
		Lease:          0,                        // For now, no lease
	}
}
