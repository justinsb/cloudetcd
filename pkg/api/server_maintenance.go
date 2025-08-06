package api

import (
	"context"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"justinsb.com/cloudetcd/pkg/storage"
)

// Status returns the status of the etcd cluster.
func (s *Server) Status(ctx context.Context, req *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	// revision, err := s.storage.GetCurrentRevision(ctx)
	// if err != nil {
	// 	return nil, err
	// }

	revision := storage.Revision(0)

	return &etcdserverpb.StatusResponse{
		Header: s.createHeader(revision),
		// Version: "3.5.0",
		// DbSize:  0,
	}, nil
}
