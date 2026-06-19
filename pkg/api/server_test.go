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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
	"justinsb.com/cloudetcd/pkg/storage/memorystorage"
)

func TestEtcdAPIServer(t *testing.T) {
	ctx := context.TODO()

	// Create storage and server
	store, err := memorystorage.NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	server := NewServer(store)

	defer server.GracefulStop()

	// Start server in background
	go func() {
		if err := server.Start(ctx, ":2379"); err != nil {
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

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
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

func TestWatchFunctionality(t *testing.T) {
	ctx := context.TODO()

	// Create storage and server
	store, err := memorystorage.NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	server := NewServer(store)

	defer server.GracefulStop()

	// Start server in background
	go func() {
		if err := server.Start(ctx, ":2380"); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Create client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2380"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer cli.Close()

	t.Run("Single Key Watch", func(t *testing.T) {
		var events []string
		var mu sync.Mutex

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Start watching key "watch-test"
		watchCh := cli.Watch(ctx, "watch-test")

		// Goroutine to collect watch events
		go func() {
			for watchResp := range watchCh {
				mu.Lock()
				for _, event := range watchResp.Events {
					if event.PrevKv != nil {
						t.Errorf("Expected no prev_kv, got %v", event.PrevKv)
					}

					var eventType string
					if event.Type == mvccpb.PUT {
						eventType = "PUT"
					} else if event.Type == mvccpb.DELETE {
						eventType = "DELETE"
					}
					eventStr := eventType + ":" + string(event.Kv.Key) + ":" + string(event.Kv.Value)
					events = append(events, eventStr)
				}
				mu.Unlock()
			}
		}()

		// Give watch time to establish
		time.Sleep(100 * time.Millisecond)

		// Put a key to trigger watch event
		_, err := cli.Put(ctx, "watch-test", "value1")
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Update the key
		_, err = cli.Put(ctx, "watch-test", "value2")
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Delete the key
		_, err = cli.Delete(ctx, "watch-test")
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Wait for events to be processed
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		expectedEvents := []string{
			"PUT:watch-test:value1",
			"PUT:watch-test:value2",
			"DELETE:watch-test:",
		}

		if len(events) != len(expectedEvents) {
			t.Fatalf("Expected %d events, got %d: %v", len(expectedEvents), len(events), events)
		}

		for i, expected := range expectedEvents {
			if events[i] != expected {
				t.Errorf("Event %d: expected %s, got %s", i, expected, events[i])
			}
		}
	})

	t.Run("Prefix Watch", func(t *testing.T) {
		var events []string
		var mu sync.Mutex

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Start watching prefix "prefix/"
		watchCh := cli.Watch(ctx, "prefix/", clientv3.WithPrefix())

		// Goroutine to collect watch events
		go func() {
			for watchResp := range watchCh {
				mu.Lock()
				for _, event := range watchResp.Events {
					if event.PrevKv != nil {
						t.Errorf("Expected no prev_kv, got %v", event.PrevKv)
					}
					var eventType string
					if event.Type == mvccpb.PUT {
						eventType = "PUT"
					} else if event.Type == mvccpb.DELETE {
						eventType = "DELETE"
					}
					eventStr := eventType + ":" + string(event.Kv.Key) + ":" + string(event.Kv.Value)
					events = append(events, eventStr)
				}
				mu.Unlock()
			}
		}()

		// Give watch time to establish
		time.Sleep(100 * time.Millisecond)

		// Put keys under the prefix
		_, err := cli.Put(ctx, "prefix/key1", "value1")
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		_, err = cli.Put(ctx, "prefix/key2", "value2")
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Put a key that doesn't match the prefix - should not trigger watch
		_, err = cli.Put(ctx, "other", "value3")
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Delete a key under the prefix
		_, err = cli.Delete(ctx, "prefix/key1")
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Wait for events to be processed
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		expectedEvents := []string{
			"PUT:prefix/key1:value1",
			"PUT:prefix/key2:value2",
			"DELETE:prefix/key1:",
		}

		if len(events) != len(expectedEvents) {
			t.Fatalf("Expected %d events, got %d: %v", len(expectedEvents), len(events), events)
		}

		for i, expected := range expectedEvents {
			if events[i] != expected {
				t.Errorf("Event %d: expected %s, got %s", i, expected, events[i])
			}
		}
	})

	t.Run("Watch with PrevKv", func(t *testing.T) {
		var events []*clientv3.Event
		var mu sync.Mutex

		// First put an initial value
		_, err := cli.Put(ctx, "prevkv-test", "initial")
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Start watching with prev_kv option
		watchCh := cli.Watch(ctx, "prevkv-test", clientv3.WithPrevKV())

		// Goroutine to collect watch events
		go func() {
			for watchResp := range watchCh {
				mu.Lock()
				for _, event := range watchResp.Events {
					events = append(events, event)
				}
				mu.Unlock()
			}
		}()

		// Give watch time to establish
		time.Sleep(100 * time.Millisecond)

		// Update the key
		_, err = cli.Put(ctx, "prevkv-test", "updated")
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Wait for events to be processed
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		if len(events) != 1 {
			for _, event := range events {
				t.Logf("Event: %v", event)
			}
			t.Fatalf("Expected 1 event, got %d", len(events))
		}

		event := events[0]
		if event.Type != mvccpb.PUT {
			t.Errorf("Expected PUT event, got %s", event.Type)
		}
		if string(event.Kv.Value) != "updated" {
			t.Errorf("Expected value 'updated', got '%s'", string(event.Kv.Value))
		}
		if event.PrevKv == nil {
			t.Fatal("Expected PrevKv to be set")
		}
		if string(event.PrevKv.Value) != "initial" {
			t.Errorf("Expected previous value 'initial', got '%s'", string(event.PrevKv.Value))
		}
	})
}

func TestServerMethods(t *testing.T) {
	ctx := context.TODO()

	// Create storage and server
	store, err := memorystorage.NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	server := NewServer(store)

	defer server.GracefulStop()

	// Start server in background
	go func() {
		if err := server.Start(ctx, ":2381"); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Create client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2381"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Test Range method through gRPC
	t.Run("Range method", func(t *testing.T) {
		// First put a key
		_, err := cli.Put(ctx, "test-key", "test-value")
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		// Test range request
		resp, err := cli.Get(ctx, "test-key")
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

	// Test Put method through gRPC
	t.Run("Put method", func(t *testing.T) {
		resp, err := cli.Put(ctx, "put-test-key", "put-test-value")
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if resp.Header.Revision <= 0 {
			t.Errorf("Expected positive revision, got %d", resp.Header.Revision)
		}
	})

	// Test DeleteRange method through gRPC
	t.Run("DeleteRange method", func(t *testing.T) {
		// First put a key to delete
		_, err := cli.Put(ctx, "delete-test-key", "delete-test-value")
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}

		resp, err := cli.Delete(ctx, "delete-test-key")
		if err != nil {
			t.Fatalf("DeleteRange failed: %v", err)
		}
		if resp.Deleted != 1 {
			t.Errorf("Expected 1 deleted key, got %d", resp.Deleted)
		}
	})
}

func TestRangeWithRangeEnd(t *testing.T) {
	ctx := context.TODO()

	// Create storage and server
	store, err := memorystorage.NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	server := NewServer(store)

	defer server.GracefulStop()

	// Start server in background
	go func() {
		if err := server.Start(ctx, ":2382"); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Create client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2382"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Put some test data
	_, err = cli.Put(ctx, "a", "value-a")
	require.NoError(t, err)

	_, err = cli.Put(ctx, "b", "value-b")
	require.NoError(t, err)

	_, err = cli.Put(ctx, "c", "value-c")
	require.NoError(t, err)

	_, err = cli.Put(ctx, "d", "value-d")
	require.NoError(t, err)

	// Test range query [b, d) - should return b and c
	resp, err := cli.Get(ctx, "b", clientv3.WithRange("d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), resp.Count)
	require.Len(t, resp.Kvs, 2)

	// Verify we got b and c
	keys := make([]string, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		keys[i] = string(kv.Key)
	}
	require.Contains(t, keys, "b")
	require.Contains(t, keys, "c")
	require.NotContains(t, keys, "a")
	require.NotContains(t, keys, "d")

	// Test range query [a, c) - should return a and b
	resp, err = cli.Get(ctx, "a", clientv3.WithRange("c"))
	require.NoError(t, err)
	require.Equal(t, int64(2), resp.Count)
	require.Len(t, resp.Kvs, 2)

	keys = make([]string, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		keys[i] = string(kv.Key)
	}
	require.Contains(t, keys, "a")
	require.Contains(t, keys, "b")
	require.NotContains(t, keys, "c")
	require.NotContains(t, keys, "d")
}
