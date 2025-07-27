package api

import (
	"context"
	"fmt"
	"net"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"justinsb.com/cloudetcd/pkg/storage"
)

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
	if req.RangeEnd != nil && len(req.RangeEnd) > 0 {
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
	// For now, return an error indicating watch is not implemented
	// In a full implementation, we'd need to implement the watch functionality
	return status.Error(codes.Unimplemented, "watch not yet implemented")
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
