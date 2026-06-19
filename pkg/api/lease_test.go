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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
	"justinsb.com/cloudetcd/pkg/storage/memorystorage"
)

func TestLeaseFunctionality(t *testing.T) {
	ctx := context.TODO()

	// Create storage and server
	store, err := memorystorage.NewMemoryStorage(memorylog.New())
	require.NoError(t, err)
	server := NewServer(store)

	defer server.GracefulStop()

	// Start server in background
	go func() {
		if err := server.Start(ctx, ":2383"); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Create client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2383"},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 1. Grant a lease
	lease, err := cli.Grant(ctx, 2)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NotZero(t, lease.ID)
	require.Equal(t, int64(2), lease.TTL)

	// 2. Put a key with the lease
	_, err = cli.Put(ctx, "lease-key", "lease-value", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	// 3. Get the key and verify it exists
	resp, err := cli.Get(ctx, "lease-key")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "lease-value", string(resp.Kvs[0].Value))
	require.Equal(t, int64(lease.ID), resp.Kvs[0].Lease)

	// 4. Keep the lease alive once
	_, err = cli.KeepAliveOnce(ctx, lease.ID)
	require.NoError(t, err)

	// 3. Get the key and verify it exists
	resp, err = cli.Get(ctx, "lease-key")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "lease-value", string(resp.Kvs[0].Value))
	require.Equal(t, int64(lease.ID), resp.Kvs[0].Lease)

	// 5. Wait for the lease to expire
	time.Sleep(3 * time.Second)

	// 6. Get the key and verify it's gone
	resp, err = cli.Get(ctx, "lease-key")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 0)

	// 7. Grant another lease and revoke it
	lease2, err := cli.Grant(ctx, 5)
	require.NoError(t, err)
	_, err = cli.Put(ctx, "lease-key-2", "lease-value-2", clientv3.WithLease(lease2.ID))
	require.NoError(t, err)

	_, err = cli.Revoke(ctx, lease2.ID)
	require.NoError(t, err)

	resp, err = cli.Get(ctx, "lease-key-2")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 0)

	// 8. Test KeepAlive
	lease3, err := cli.Grant(ctx, 2)
	require.NoError(t, err)
	_, err = cli.Put(ctx, "lease-key-3", "lease-value-3", clientv3.WithLease(lease3.ID))
	require.NoError(t, err)

	_, err = cli.KeepAliveOnce(ctx, lease3.ID)
	require.NoError(t, err)

	// The key should still exist
	resp, err = cli.Get(ctx, "lease-key-3")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)

	// Wait for the lease to expire
	time.Sleep(3 * time.Second)

	// The key should be gone
	resp, err = cli.Get(ctx, "lease-key-3")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 0)
}

// PROMPT: Create lease_test.go, that verifies that we return  ErrGRPCLeaseNotFound
// ErrGRPCLeaseNotFound    = status.Error(codes.NotFound, "etcdserver: requested lease not found")
// ErrGRPCLeaseExist       = status.Error(codes.FailedPrecondition, "etcdserver: lease already exists")
// ErrGRPCLeaseTTLTooLarge = status.Error(codes.OutOfRange, "etcdserver: too large lease TTL")
