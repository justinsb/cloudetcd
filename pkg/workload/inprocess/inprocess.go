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

// Package inprocess starts a cloudetcd server inside the current process for
// benchmarks and stress tests, so that a run needs nothing but `go test`.
package inprocess

import (
	"context"
	"fmt"
	"net"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"justinsb.com/cloudetcd/pkg/api"
	"justinsb.com/cloudetcd/pkg/persistence/logfactory"
	"justinsb.com/cloudetcd/pkg/storage/memorystorage"
)

// Server is a running in-process cloudetcd.
type Server struct {
	// Addr is the host:port the server listens on.
	Addr string
	// Store is the server's storage, for diagnostics.
	Store *memorystorage.MemoryStorage

	server *api.Server
	cancel context.CancelFunc
	done   chan error
}

// Start creates a server backed by the log at logURI (see logfactory) on a
// free loopback port and waits until it serves requests.
func Start(ctx context.Context, logURI string, opts ...api.Option) (*Server, error) {
	lg, err := logfactory.NewLog(ctx, logURI)
	if err != nil {
		return nil, fmt.Errorf("creating log %q: %w", logURI, err)
	}
	store, err := memorystorage.NewMemoryStorage(lg)
	if err != nil {
		return nil, fmt.Errorf("creating storage: %w", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	addr := l.Addr().String()
	_ = l.Close()

	ctx, cancel := context.WithCancel(ctx)
	s := &Server{
		Addr:   addr,
		Store:  store,
		server: api.NewServer(store, opts...),
		cancel: cancel,
		done:   make(chan error, 1),
	}
	go func() { s.done <- s.server.Start(ctx, addr) }()

	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case err := <-s.done:
			cancel()
			return nil, fmt.Errorf("server exited before becoming ready: %v", err)
		default:
		}
		cli, err := clientv3.New(clientv3.Config{Endpoints: []string{addr}, DialTimeout: 2 * time.Second})
		if err == nil {
			pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
			_, err = cli.Get(pctx, "readiness-probe")
			pcancel()
			_ = cli.Close()
			if err == nil {
				return s, nil
			}
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	return nil, fmt.Errorf("server did not become ready at %s: %v", addr, lastErr)
}

// Stop shuts the server down.
func (s *Server) Stop() {
	s.cancel()
	_ = s.server.GracefulStop()
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
	}
}

// NewClient returns an etcd client connected to the server. Like
// kube-apiserver, callers should share one client: all traffic multiplexes on
// a single gRPC connection.
func (s *Server) NewClient() (*clientv3.Client, error) {
	return NewClient(s.Addr)
}

// NewClient returns an etcd client for endpoint.
func NewClient(endpoint string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
}
