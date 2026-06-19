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

package api

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"justinsb.com/cloudetcd/pkg/storage"
	"k8s.io/klog/v2"
)

// loggingInterceptor is a gRPC unary interceptor that logs requests.
func loggingInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	log := klog.FromContext(ctx)
	log.Info("gRPC call", "method", info.FullMethod, "request", req)
	resp, err := handler(ctx, req)
	if err != nil {
		log.Error(err, "gRPC call failed", "method", info.FullMethod)
	} else {
		log.Info("gRPC call response", "method", info.FullMethod, "response", resp)
	}
	return resp, err
}

// loggingStreamInterceptor is a gRPC stream interceptor that logs requests.
func loggingStreamInterceptor(srv any, serverStream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := serverStream.Context()
	log := klog.FromContext(ctx)
	log.Info("gRPC stream call", "method", info.FullMethod)
	return handler(srv, &loggingServerStream{ServerStream: serverStream, log: log})
}

type loggingServerStream struct {
	log klog.Logger
	grpc.ServerStream
}

func (s *loggingServerStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.log.Info("gRPC stream request", "type", TypeName(m), "request", m)
	}
	return err
}

func (s *loggingServerStream) SendMsg(m any) error {
	s.log.Info("gRPC stream response", "type", TypeName(m), "response", m)
	return s.ServerStream.SendMsg(m)
}

type DelayedTypeName struct {
	v any
}

func TypeName(v any) DelayedTypeName {
	return DelayedTypeName{v: v}
}

func (d DelayedTypeName) String() string {
	return fmt.Sprintf("%T", d.v)
}

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
	id      int64
	watcher storage.Watcher
	// key      []byte
	// rangeEnd []byte // Store the range end for proper filtering
	// prefix   bool
	// prevKv   bool
	// filters  []etcdserverpb.WatchCreateRequest_FilterType
	closed int32 // atomic flag
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
	etcdserverpb.UnimplementedMaintenanceServer

	storage      storage.Storage
	leaseManager storage.LeaseManager
	grpc         *grpc.Server

	mu                sync.RWMutex
	watchStreams      map[int64]*watchStream
	nextWatchStreamID int64
}

// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
// to the server and streaming keep alive responses from the server to the client.

func (s *Server) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	ctx := stream.Context()
	log := klog.FromContext(ctx)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Error(err, "lease keepalive recv failed")
			return err
		}

		resp, err := s.leaseManager.LeaseKeepAlive(ctx, req)
		if err != nil {
			log.Error(err, "lease keepalive failed")
			return err
		}
		// if lease == nil {
		// 	// Lease not found or expired
		// 	resp := &etcdserverpb.LeaseKeepAliveResponse{
		// 		Header: s.createHeader(0),
		// 		ID:     req.ID,
		// 		TTL:    0, // Indicates lease expired
		// 	}
		// 	if err := stream.Send(resp); err != nil {
		// 		log.Error(err, "lease keepalive send failed for expired lease")
		// 		return err
		// 	}
		// 	continue
		// }

		if err := stream.Send(resp); err != nil {
			log.Error(err, "lease keepalive send failed")
			return err
		}
	}
}

// NewServer creates a new etcd API server
func NewServer(store storage.Storage) *Server {
	return &Server{
		storage:           store,
		leaseManager:      store.LeaseManager(),
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

	s.grpc = grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor),
		grpc.StreamInterceptor(loggingStreamInterceptor),
	)
	etcdserverpb.RegisterKVServer(s.grpc, s)
	etcdserverpb.RegisterWatchServer(s.grpc, s)
	etcdserverpb.RegisterLeaseServer(s.grpc, s)
	etcdserverpb.RegisterMaintenanceServer(s.grpc, s)

	log.Info("Starting etcd API server", "addr", addr)

	go func() {
		<-ctx.Done()
		log.Info("Stopping etcd API server (gracefully)")
		s.GracefulStop()
	}()

	go func() {
		s.leaseManager.Run(ctx)
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
	return s.storage.Delete(ctx, req)
}

// Txn implements the Txn RPC method
func (s *Server) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	log := klog.FromContext(ctx)

	result, err := s.storage.Txn(ctx, req)
	if err != nil {
		log.Error(err, "failed to execute Txn request", "request", req)
		return nil, err
	}
	return result, nil
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
	ctx := stream.Context()
	log := klog.FromContext(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ws := &watchStream{
		server:      s,
		stream:      stream,
		watches:     make(map[int64]*activeWatch),
		cancel:      cancel,
		nextWatchID: 1,
	}

	s.mu.Lock()
	watchStreamID := s.nextWatchStreamID
	s.nextWatchStreamID++
	s.watchStreams[watchStreamID] = ws
	s.mu.Unlock()

	// Start goroutine to handle incoming requests
	go func() {
		if err := ws.handleRequests(ctx); err != nil {
			log.Error(err, "failed to handle watch requests")
		}
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
func (ws *watchStream) handleRequests(ctx context.Context) error {
	for {
		req, err := ws.stream.Recv()
		if err != nil {
			return err
		}

		switch r := req.RequestUnion.(type) {
		case *etcdserverpb.WatchRequest_CreateRequest:
			if err := ws.handleCreateRequest(ctx, r.CreateRequest); err != nil {
				return err
			}
		case *etcdserverpb.WatchRequest_CancelRequest:
			ws.handleCancelRequest(ctx, r.CancelRequest)
		case *etcdserverpb.WatchRequest_ProgressRequest:
			if err := ws.handleProgressRequest(ctx, r.ProgressRequest); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unknown watch request type: %T", r)
		}
	}
}

// handleCreateRequest handles watch creation
func (ws *watchStream) handleCreateRequest(ctx context.Context, req *etcdserverpb.WatchCreateRequest) error {
	log := klog.FromContext(ctx)

	ws.mu.Lock()
	defer ws.mu.Unlock()

	watchID := req.WatchId
	if watchID == 0 {
		for {
			watchID = ws.nextWatchID
			ws.nextWatchID++
			if ws.watches[watchID] == nil {
				req.WatchId = watchID
				break
			}
		}
	} else {
		if _, exists := ws.watches[watchID]; exists {
			// TODO: Or send error?
			return fmt.Errorf("watch ID %d already in use", watchID)
		}
	}

	callback := func(resp *etcdserverpb.WatchResponse) error {
		return ws.stream.Send(resp)
	}

	// Create storage watcher
	watch, logPosition, err := ws.server.storage.Watch(ctx, req, callback)
	if err != nil {
		// Send error response
		resp := &etcdserverpb.WatchResponse{
			Header: ws.server.createHeader(logPosition),
			// WatchId:  watch.ID(),
			Created:  false,
			Canceled: true,
		}
		ws.stream.Send(resp)
		return err
	}

	// Send creation response
	resp := &etcdserverpb.WatchResponse{
		Header:  ws.server.createHeader(logPosition),
		WatchId: watchID,
		Created: true,
	}
	ws.stream.Send(resp)

	// Create active watch
	activeWatch := &activeWatch{
		id:      watchID,
		watcher: watch,
		// key:      req.Key,
		// rangeEnd: req.RangeEnd, // Store the range end
		// prefix:   isRangeWatch, // Reuse the prefix field to indicate range watch
		// prevKv:   req.PrevKv,
		// filters:  req.Filters,
	}
	ws.watches[watchID] = activeWatch

	// Start watching for events
	go func() {
		if err := watch.Run(ctx); err != nil {
			log.Error(err, "watch stopped with error")
			activeWatch.close()
		}
	}()

	return nil
}

// handleCancelRequest handles watch cancellation
func (ws *watchStream) handleCancelRequest(ctx context.Context, req *etcdserverpb.WatchCancelRequest) {
	log := klog.FromContext(ctx)

	ws.mu.Lock()
	defer ws.mu.Unlock()

	// TODO: Should we send the watch position or the log position?
	logPosition, err := ws.server.storage.GetCurrentRevision(ctx)
	if err != nil {
		log.Error(err, "failed to get current revision")
	}

	watchID := req.WatchId
	if watch, exists := ws.watches[watchID]; exists {
		watch.close()
		delete(ws.watches, watchID)

		// Send cancellation response
		resp := &etcdserverpb.WatchResponse{
			Header:   ws.server.createHeader(logPosition),
			WatchId:  watchID,
			Canceled: true,
		}
		ws.stream.Send(resp)
	}
}

// handleProgressRequest handles progress requests
func (ws *watchStream) handleProgressRequest(ctx context.Context, req *etcdserverpb.WatchProgressRequest) error {
	log := klog.FromContext(ctx)

	// TODO: Should we send the watch position or the log position?
	logPosition, err := ws.server.storage.GetCurrentRevision(ctx)
	if err != nil {
		log := klog.FromContext(ctx)
		log.Error(err, "failed to get current revision")
		klog.Fatalf("failed to get revision: %v", err)
	}
	if logPosition == 0 {
		klog.Warningf("logPosition is zero")
	}

	// Note: we send this to the magic -1 watch id, not to our watch specifically
	// This is sort of weird, because what position are we sending?
	resp := &etcdserverpb.WatchResponse{
		Header:  ws.server.createHeader(logPosition),
		WatchId: -1, // InvalidWatchID => broadcast
	}

	if err := ws.stream.Send(resp); err != nil {
		log.Error(err, "failed to send progress response")
	}

	return nil
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
// LeaseGrant creates a lease which expires if the server does not receive a keepAlive
// within a given time to live period. All keys attached to the lease will be expired and
// deleted if the lease expires. Each expired key generates a delete event in the event history.

func (s *Server) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	return s.leaseManager.LeaseGrant(ctx, req)
}

// LeaseRevoke implements the Revoke RPC method
// LeaseRevoke revokes a lease. All keys attached to the lease will expire and be deleted.
func (s *Server) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return s.leaseManager.LeaseRevoke(ctx, req)
}

// LeaseTimeToLive implements the LeaseTimeToLive RPC method
// LeaseTimeToLive retrieves lease information.
func (s *Server) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return s.leaseManager.LeaseTimeToLive(ctx, req)
}

// LeaseLeases implements the LeaseLeases RPC method
// LeaseLeases lists all existing leases.
func (s *Server) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	return s.leaseManager.ListLeases(ctx, req)

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
