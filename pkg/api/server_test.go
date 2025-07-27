package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
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

func TestWatchFunctionality(t *testing.T) {
	// Create storage and server
	store := storage.NewMemoryStorage()
	server := NewServer(store)

	defer server.Stop()

	// Start server in background
	go func() {
		if err := server.Start(":2380"); err != nil {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("Single Key Watch", func(t *testing.T) {
		var events []string
		var mu sync.Mutex

		// Start watching key "watch-test"
		watchCh := cli.Watch(ctx, "watch-test")

		// Goroutine to collect watch events
		go func() {
			for watchResp := range watchCh {
				mu.Lock()
				for _, event := range watchResp.Events {
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

		// Start watching prefix "prefix/"
		watchCh := cli.Watch(ctx, "prefix/", clientv3.WithPrefix())

		// Goroutine to collect watch events
		go func() {
			for watchResp := range watchCh {
				mu.Lock()
				for _, event := range watchResp.Events {
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

func TestWatchStorage(t *testing.T) {
	store := storage.NewMemoryStorage()
	ctx := context.Background()

	t.Run("Storage Watch Interface", func(t *testing.T) {
		// Create a watcher for exact key match
		watcher, err := store.Watch(ctx, []byte("test-key"), []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to create watcher: %v", err)
		}
		defer watcher.Close()

		var events []*storage.WatchEvent
		var mu sync.Mutex

		// Goroutine to collect events
		go func() {
			for resp := range watcher.Chan() {
				mu.Lock()
				events = append(events, resp.Events...)
				mu.Unlock()
			}
		}()

		// Give watcher time to establish
		time.Sleep(50 * time.Millisecond)

		// Put a key
		_, err = store.Put(ctx, []byte("test-key"), []byte("value1"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Update the key
		_, err = store.Put(ctx, []byte("test-key"), []byte("value2"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Delete the key
		_, err = store.Delete(ctx, []byte("test-key"))
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Wait for events
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		if len(events) != 3 {
			t.Fatalf("Expected 3 events, got %d", len(events))
		}

		// Check first event (PUT)
		if events[0].Type != storage.WatchEventTypePut {
			t.Errorf("Expected PUT event, got %d", events[0].Type)
		}
		if string(events[0].Key) != "test-key" {
			t.Errorf("Expected key 'test-key', got '%s'", string(events[0].Key))
		}
		if string(events[0].Value) != "value1" {
			t.Errorf("Expected value 'value1', got '%s'", string(events[0].Value))
		}

		// Check second event (UPDATE)
		if events[1].Type != storage.WatchEventTypePut {
			t.Errorf("Expected PUT event, got %d", events[1].Type)
		}
		if string(events[1].Value) != "value2" {
			t.Errorf("Expected value 'value2', got '%s'", string(events[1].Value))
		}
		if events[1].PrevKv == nil {
			t.Error("Expected PrevKv to be set for update")
		} else if string(events[1].PrevKv.Value) != "value1" {
			t.Errorf("Expected previous value 'value1', got '%s'", string(events[1].PrevKv.Value))
		}

		// Check third event (DELETE)
		if events[2].Type != storage.WatchEventTypeDelete {
			t.Errorf("Expected DELETE event, got %d", events[2].Type)
		}
		if events[2].PrevKv == nil {
			t.Error("Expected PrevKv to be set for delete")
		} else if string(events[2].PrevKv.Value) != "value2" {
			t.Errorf("Expected previous value 'value2', got '%s'", string(events[2].PrevKv.Value))
		}
	})

	t.Run("Storage Prefix Watch", func(t *testing.T) {
		// Create a watcher for prefix match (using empty rangeEnd for prefix behavior)
		watcher, err := store.Watch(ctx, []byte("prefix/"), []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to create watcher: %v", err)
		}
		defer watcher.Close()

		var events []*storage.WatchEvent
		var mu sync.Mutex

		// Goroutine to collect events
		go func() {
			for resp := range watcher.Chan() {
				mu.Lock()
				events = append(events, resp.Events...)
				mu.Unlock()
			}
		}()

		// Give watcher time to establish
		time.Sleep(50 * time.Millisecond)

		// Put keys under prefix
		_, err = store.Put(ctx, []byte("prefix/key1"), []byte("value1"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		_, err = store.Put(ctx, []byte("prefix/key2"), []byte("value2"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Put key that doesn't match prefix - should not trigger event
		_, err = store.Put(ctx, []byte("other"), []byte("value3"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Delete key under prefix
		_, err = store.Delete(ctx, []byte("prefix/key1"))
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Wait for events
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		// Should only get events for keys under the prefix
		expectedEventCount := 3 // prefix/key1 PUT, prefix/key2 PUT, prefix/key1 DELETE
		if len(events) != expectedEventCount {
			t.Fatalf("Expected %d events, got %d", expectedEventCount, len(events))
		}

		// Verify events are for the correct keys
		expectedKeys := []string{"prefix/key1", "prefix/key2", "prefix/key1"}
		for i, event := range events {
			if string(event.Key) != expectedKeys[i] {
				t.Errorf("Event %d: expected key '%s', got '%s'", i, expectedKeys[i], string(event.Key))
			}
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

func TestRangeWithRangeEnd(t *testing.T) {
	store := storage.NewMemoryStorage()
	server := NewServer(store)

	ctx := context.Background()

	// Put some test data
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("a"),
		Value: []byte("value-a"),
	})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("b"),
		Value: []byte("value-b"),
	})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("c"),
		Value: []byte("value-c"),
	})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("d"),
		Value: []byte("value-d"),
	})
	require.NoError(t, err)

	// Test range query [b, d) - should return b and c
	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("b"),
		RangeEnd: []byte("d"),
	})
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
	resp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("a"),
		RangeEnd: []byte("c"),
	})
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
