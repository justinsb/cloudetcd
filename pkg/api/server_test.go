package api

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"justinsb.com/cloudetcd/pkg/storage"
)

func TestEtcdAPIServer(t *testing.T) {
	// Create storage and server
	store := storage.NewMemoryStorage()
	server := NewServer(store)

	defer server.Stop()

	// Start server in background
	go func() {
		if err := server.Start(":2379"); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Create client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test PUT
	t.Run("PUT", func(t *testing.T) {
		resp, err := cli.Put(ctx, "test-key", "test-value")
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}
		if resp.Header.Revision <= 0 {
			t.Errorf("Expected positive revision, got %d", resp.Header.Revision)
		}
	})

	// Test GET
	t.Run("GET", func(t *testing.T) {
		resp, err := cli.Get(ctx, "test-key")
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		if len(resp.Kvs) != 1 {
			t.Fatalf("Expected 1 key-value pair, got %d", len(resp.Kvs))
		}
		kv := resp.Kvs[0]
		if string(kv.Key) != "test-key" {
			t.Errorf("Expected key 'test-key', got '%s'", string(kv.Key))
		}
		if string(kv.Value) != "test-value" {
			t.Errorf("Expected value 'test-value', got '%s'", string(kv.Value))
		}
	})

	// Test DELETE
	t.Run("DELETE", func(t *testing.T) {
		resp, err := cli.Delete(ctx, "test-key")
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
		if resp.Deleted != 1 {
			t.Errorf("Expected 1 deleted key, got %d", resp.Deleted)
		}
	})

	// Test GET after DELETE
	t.Run("GET after DELETE", func(t *testing.T) {
		resp, err := cli.Get(ctx, "test-key")
		if err != nil {
			t.Fatalf("GET after DELETE failed: %v", err)
		}
		if len(resp.Kvs) != 0 {
			t.Errorf("Expected 0 key-value pairs after delete, got %d", len(resp.Kvs))
		}
	})

	// Test range operations
	t.Run("Range operations", func(t *testing.T) {
		// Put some keys with prefix
		keys := []string{"prefix/key1", "prefix/key2", "prefix/key3"}
		for _, key := range keys {
			_, err := cli.Put(ctx, key, "value")
			if err != nil {
				t.Fatalf("PUT failed for %s: %v", key, err)
			}
		}

		// Get all keys with prefix
		resp, err := cli.Get(ctx, "prefix/", clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("Range query failed: %v", err)
		}
		if len(resp.Kvs) != 3 {
			t.Errorf("Expected 3 key-value pairs, got %d", len(resp.Kvs))
		}
	})
}

func TestServerMethods(t *testing.T) {
	store := storage.NewMemoryStorage()
	server := NewServer(store)

	ctx := context.Background()

	// Test Range method directly
	t.Run("Range method", func(t *testing.T) {
		// First put a key
		_, err := store.Put(ctx, []byte("test-key"), []byte("test-value"), 0)
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		// Test range request
		req := &etcdserverpb.RangeRequest{
			Key: []byte("test-key"),
		}
		resp, err := server.Range(ctx, req)
		if err != nil {
			t.Fatalf("Range failed: %v", err)
		}
		if len(resp.Kvs) != 1 {
			t.Fatalf("Expected 1 key-value pair, got %d", len(resp.Kvs))
		}
		kv := resp.Kvs[0]
		if string(kv.Key) != "test-key" {
			t.Errorf("Expected key 'test-key', got '%s'", string(kv.Key))
		}
		if string(kv.Value) != "test-value" {
			t.Errorf("Expected value 'test-value', got '%s'", string(kv.Value))
		}
	})

	// Test Put method directly
	t.Run("Put method", func(t *testing.T) {
		req := &etcdserverpb.PutRequest{
			Key:   []byte("put-test-key"),
			Value: []byte("put-test-value"),
		}
		resp, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if resp.Header.Revision <= 0 {
			t.Errorf("Expected positive revision, got %d", resp.Header.Revision)
		}
	})

	// Test DeleteRange method directly
	t.Run("DeleteRange method", func(t *testing.T) {
		// First put a key to delete
		_, err := store.Put(ctx, []byte("delete-test-key"), []byte("delete-test-value"), 0)
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		req := &etcdserverpb.DeleteRangeRequest{
			Key: []byte("delete-test-key"),
		}
		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("DeleteRange failed: %v", err)
		}
		if resp.Deleted != 1 {
			t.Errorf("Expected 1 deleted key, got %d", resp.Deleted)
		}
	})
}
