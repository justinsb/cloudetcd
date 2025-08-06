package api

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"justinsb.com/cloudetcd/pkg/storage"
	"k8s.io/klog/v2"
)

// watchStream represents a single watch stream with multiple watches
type watchStream struct {
	server *Server
	stream etcdserverpb.Watch_WatchServer
	cancel context.CancelFunc

	mu          sync.RWMutex
	watches     map[int64]*activeWatch
	nextWatchID int64
}

// activeWatch represents a single active watch
type activeWatch struct {
	id       int64
	watcher  storage.Watcher
	key      []byte
	rangeEnd []byte // Store the range end for proper filtering
	prefix   bool
	prevKv   bool
	filters  []etcdserverpb.WatchCreateRequest_FilterType
	closed   int32 // atomic flag
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

	mu                sync.RWMutex
	watchStreams      map[int64]*watchStream
	nextWatchStreamID int64
}

// NewServer creates a new etcd API server
func NewServer(store storage.Storage) *Server {
	return &Server{
		storage:           store,
		watchStreams:      make(map[int64]*watchStream),
		nextWatchStreamID: 1,
	}
}

// Start starts the gRPC server on the given address
func (s *Server) Start(ctx context.Context, addr string) error {
	log := klog.FromContext(ctx)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s.grpc = grpc.NewServer()
	etcdserverpb.RegisterKVServer(s.grpc, s)
	etcdserverpb.RegisterWatchServer(s.grpc, s)
	etcdserverpb.RegisterLeaseServer(s.grpc, s)

	log.Info("Starting etcd API server", "addr", addr)

	go func() {
		<-ctx.Done()
		log.Info("Stopping etcd API server (gracefully)")
		s.GracefulStop()
	}()
	return s.grpc.Serve(lis)
}

// HardStop stops the gRPC server (non-gracefully)
func (s *Server) HardStop() error {
	s.grpc.Stop()
	return nil
}

// GracefulStop stops the gRPC server gracefully
func (s *Server) GracefulStop() error {
	s.storage.GracefulStop()

	s.mu.Lock()
	for id, ws := range s.watchStreams {
		klog.InfoS("stopping watch stream", "id", id)
		ws.cleanup(true)
	}
	s.mu.Unlock()

	s.grpc.GracefulStop()
	return nil
}

// Range implements the Range RPC method
func (s *Server) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	log := klog.FromContext(ctx)

	log.Info("grpc request: Range", "request", req)

	if req.Key == nil {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	// Handle range queries with RangeEnd
	if len(req.RangeEnd) > 0 {
		return s.handleRangeWithEnd(ctx, req)
	}

	// Handle single key queries
	return s.handleSingleKey(ctx, req)
}

func (s *Server) handleSingleKey(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// Query for single key
	resp, err := s.storage.Get(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (s *Server) handleRangeWithEnd(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// Use the storage layer's efficient range query
	resp, err := s.storage.List(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Put implements the Put RPC method
func (s *Server) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	log := klog.FromContext(ctx)

	log.Info("grpc request: Put", "request", req)

	if req.Key == nil {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	response, err := s.storage.Put(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return response, nil
}

// DeleteRange implements the DeleteRange RPC method
func (s *Server) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	log := klog.FromContext(ctx)

	log.Info("grpc request: DeleteRange", "request", req)

	return s.storage.Delete(ctx, req)
}

// Txn implements the Txn RPC method
func (s *Server) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	log := klog.FromContext(ctx)

	log.Info("grpc request: Txn", "request", req)

	return s.storage.Txn(ctx, req)
}

// Compact implements the Compact RPC method
func (s *Server) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	log := klog.FromContext(ctx)

	log.Info("grpc request: Compact", "request", req)

	// For now, return success without actual compaction
	// In a full implementation, we'd need to implement actual compaction logic
	return &etcdserverpb.CompactionResponse{
		Header: s.createHeader(storage.Revision(req.Revision)),
	}, nil
}

// Watch implements the Watch RPC method
func (s *Server) Watch(stream etcdserverpb.Watch_WatchServer) error {
	ctx := stream.Context()
	log := klog.FromContext(ctx)

	log.Info("grpc request: Watch stream")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ws := &watchStream{
		server:      s,
		stream:      stream,
		watches:     make(map[int64]*activeWatch),
		nextWatchID: 1,
		cancel:      cancel,
	}

	s.mu.Lock()
	watchStreamID := s.nextWatchStreamID
	s.nextWatchStreamID++
	s.watchStreams[watchStreamID] = ws
	s.mu.Unlock()

	// Start goroutine to handle incoming requests
	go func() {
		ws.handleRequests(ctx)
		cancel()
	}()

	// Wait for context to be done
	<-ctx.Done()

	// Clean up all watches
	ws.cleanup(false)

	s.mu.Lock()
	delete(s.watchStreams, watchStreamID)
	s.mu.Unlock()

	return nil
}

// handleRequests processes incoming watch requests
func (ws *watchStream) handleRequests(ctx context.Context) {
	log := klog.FromContext(ctx)

	for {
		req, err := ws.stream.Recv()
		if err != nil {
			return
		}

		log.Info("grpc watch request", "request", req)

		switch r := req.RequestUnion.(type) {
		case *etcdserverpb.WatchRequest_CreateRequest:
			ws.handleCreateRequest(ctx, r.CreateRequest)
		case *etcdserverpb.WatchRequest_CancelRequest:
			ws.handleCancelRequest(r.CancelRequest)
		case *etcdserverpb.WatchRequest_ProgressRequest:
			ws.handleProgressRequest(r.ProgressRequest)
		}
	}
}

// handleCreateRequest handles watch creation
func (ws *watchStream) handleCreateRequest(ctx context.Context, req *etcdserverpb.WatchCreateRequest) {
	// log := klog.FromContext(ctx)

	ws.mu.Lock()
	defer ws.mu.Unlock()

	watchID := ws.nextWatchID
	ws.nextWatchID++

	// Determine if this is a range watch (when RangeEnd is specified)
	isRangeWatch := len(req.RangeEnd) > 0

	// Create storage watcher
	startRevision := storage.Revision(req.StartRevision)
	watcher, err := ws.server.storage.Watch(ctx, req.Key, req.RangeEnd, startRevision)
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
		id:       watchID,
		watcher:  watcher,
		key:      req.Key,
		rangeEnd: req.RangeEnd, // Store the range end
		prefix:   isRangeWatch, // Reuse the prefix field to indicate range watch
		prevKv:   req.PrevKv,
		filters:  req.Filters,
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
	go ws.watchEvents(ctx, activeWatch)
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
func (ws *watchStream) watchEvents(ctx context.Context, watch *activeWatch) {
	log := klog.FromContext(ctx)

	defer watch.close()

	// Process watch events
	for watchResp := range watch.watcher.Chan() {
		// Apply filters and convert events
		var events []*mvccpb.Event
		for _, event := range watchResp.Events {
			// Apply filters
			if ws.shouldFilterEvent(event, watch.filters) {
				continue
			}

			// Add previous KV if requested and available
			if watch.prevKv && event.PrevKv == nil {
				// We need to fetch the previous value if not provided
				// This is a simplified implementation
				prevKv, err := ws.server.storage.Get(ctx, &etcdserverpb.RangeRequest{Key: event.Kv.Key, Revision: int64(event.Kv.CreateRevision - 1)})
				if err != nil {
					// TODO: Handle not found?
				}
				if err == nil && prevKv != nil && len(prevKv.Kvs) > 0 {
					event.PrevKv = prevKv.Kvs[0]
				}
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

	log.Info("watch stream closed")
}

// shouldFilterEvent checks if an event should be filtered out
func (ws *watchStream) shouldFilterEvent(event *mvccpb.Event, filters []etcdserverpb.WatchCreateRequest_FilterType) bool {
	for _, filter := range filters {
		switch filter {
		case etcdserverpb.WatchCreateRequest_NOPUT:
			if event.Type == mvccpb.PUT {
				return true
			}
		case etcdserverpb.WatchCreateRequest_NODELETE:
			if event.Type == mvccpb.DELETE {
				return true
			}
		}
	}
	return false
}

// cleanup closes all watches
func (ws *watchStream) cleanup(closeGRPC bool) {

	// TODO: Send cancellation events to all watches?

	for _, watch := range ws.watches {
		watch.close()
	}
	ws.watches = make(map[int64]*activeWatch)

	if closeGRPC {
		ws.cancel()
	}
}

// Grant implements the Grant RPC method
func (s *Server) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	log := klog.FromContext(ctx)

	log.Info("grpc request: LeaseGrant", "request", req)

	// TODO

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
	log := klog.FromContext(ctx)

	log.Info("grpc request: LeaseTimeToLive", "request", req)

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
	log := klog.FromContext(ctx)

	log.Info("grpc request: LeaseLeases", "request", req)

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

// func (s *Server) convertToMVCCKeyValue(kv *storage.KeyValue) *mvccpb.KeyValue {
// 	return &mvccpb.KeyValue{
// 		Key:            kv.Key,
// 		Value:          kv.Value,
// 		CreateRevision: int64(kv.CreateRevision),
// 		ModRevision:    int64(kv.CreateRevision), // For now, use CreateRevision as ModRevision
// 		Version:        1,                        // For now, always version 1
// 		Lease:          0,                        // For now, no lease
// 	}
// }
