package api

import (
	"context"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"k8s.io/klog/v2"
)

// Status returns the status of the etcd cluster.
func (s *Server) Status(ctx context.Context, req *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	log := klog.FromContext(ctx)

	log.Info("grpc Status request", "request", req)

	status, err := s.storage.Status(ctx)
	if err != nil {
		log.Error(err, "failed to get status")
		return nil, err
	}
	return status, nil
}
