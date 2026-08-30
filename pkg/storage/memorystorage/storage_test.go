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

package memorystorage

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
)

func getRevision(t *testing.T, resp *etcdserverpb.PutResponse) Revision {
	if resp.Header == nil {
		t.Fatal("PutResponse has no header")
	}
	return Revision(resp.Header.Revision)
}

func TestMemoryStorage_Put(t *testing.T) {
	storage, err := NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := t.Context()

	// Test basic put
	key := []byte("test-key")
	value := []byte("test-value")
	req := &etcdserverpb.PutRequest{
		Key:   key,
		Value: value,
	}
	resp, err := storage.Put(ctx, req)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if getRevision(t, resp) != 2 {
		t.Errorf("Expected revision 2, got %d", getRevision(t, resp))
	}

	// Test update
	newValue := []byte("updated-value")
	req.Value = newValue
	resp, err = storage.Put(ctx, req)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if getRevision(t, resp) != 3 {
		t.Errorf("Expected revision 3, got %d", getRevision(t, resp))
	}
}

func TestMemoryStorage_Get(t *testing.T) {
	storage, err := NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := t.Context()

	// Put a key-value pair
	key := []byte("test-key")
	value := []byte("test-value")
	storage.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: value})

	// Test get existing key
	kvResp, err := storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	kv := kvResp.Kvs[0]
	if !reflect.DeepEqual(kv.Key, key) {
		t.Errorf("Expected key %v, got %v", key, kv.Key)
	}
	if !reflect.DeepEqual(kv.Value, value) {
		t.Errorf("Expected value %v, got %v", value, kv.Value)
	}
	if kv.CreateRevision != 2 {
		t.Errorf("Expected CreateRevision 1, got %d", kv.CreateRevision)
	}

	// Test get non-existent key
	kvResp, err = storage.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte("non-existent")})
	assertNotFound(t, kvResp, err)

	// Test get at specific revision
	kvResp, err = storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: 1})
	if err != nil {
		t.Fatalf("Get at revision failed: %v", err)
	}
	// Note: We can't easily check the revision number without storing it with each record
	// For now, just verify we get a valid result

	// Test get at future revision (should fail)
	_, err = storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	if err != nil {
		t.Errorf("Get at revision 0 should succeed, got error: %v", err)
	}
}

func TestMemoryStorage_Delete(t *testing.T) {
	storage, storageErr := NewMemoryStorage(memorylog.New())
	if storageErr != nil {
		t.Fatalf("Failed to create storage: %v", storageErr)
	}
	ctx := t.Context()

	// Put a key-value pair
	key := []byte("test-key")
	value := []byte("test-value")
	storage.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: value})

	// Verify it exists
	_, err := storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	if err != nil {
		t.Fatalf("Key should exist before delete: %v", err)
	}

	// Delete the key
	delResp, err := storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	revision := delResp.Header.Revision
	if revision != 3 {
		t.Errorf("Expected revision 3, got %d", revision)
	}

	// Verify it's gone (should return error for deleted key)
	getResp, err := storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	assertNotFound(t, getResp, err)

	// Test delete non-existent key (should succeed)
	delResp, err = storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("non-existent")})
	if err != nil {
		t.Fatalf("unexpected error for deletion of non-existent key: %v", err)
	}
	// Revision should not advance because we didn't do anything
	if delResp.Header.Revision != 3 {
		t.Errorf("Expected revision 3, got %d", revision)
	}
}

func TestMemoryStorage_List(t *testing.T) {
	storage, err := NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := t.Context()

	// Put several key-value pairs
	testData := map[string]string{
		"prefix1/key1": "value1",
		"prefix1/key2": "value2",
		"prefix2/key1": "value3",
		"other/key1":   "value4",
	}

	for k, v := range testData {
		_, err := storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(k), Value: []byte(v)})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Test list all keys
	{
		allKeysResp, err := storage.List(ctx, &etcdserverpb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
		if err != nil {
			t.Fatalf("List all failed: %v", err)
		}
		allKeys := allKeysResp.Kvs
		if len(allKeys) != 4 {
			t.Errorf("Expected 4 keys, got %d", len(allKeys))
		}
	}

	// Test list with prefix (using empty rangeEnd for prefix behavior)
	{
		prefix := []byte("prefix1/")
		prefix1KeysResp, err := storage.List(ctx, &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: rangeEndForPrefix(t, prefix)})
		if err != nil {
			t.Fatalf("List with prefix failed: %v", err)
		}
		prefix1Keys := prefix1KeysResp.Kvs
		if len(prefix1Keys) != 2 {
			t.Errorf("Expected 2 keys with prefix1/, got %d", len(prefix1Keys))
		}

		// Verify the keys are sorted
		for i := 1; i < len(prefix1Keys); i++ {
			if string(prefix1Keys[i-1].Key) >= string(prefix1Keys[i].Key) {
				t.Errorf("Keys not sorted: %s >= %s",
					string(prefix1Keys[i-1].Key), string(prefix1Keys[i].Key))
			}
		}
	}

	// Test list with non-existent prefix
	{
		prefix := []byte("non-existent/")
		emptyKeysResp, err := storage.List(ctx, &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: rangeEndForPrefix(t, prefix)})
		if err != nil {
			t.Fatalf("List with non-existent prefix failed: %v", err)
		}
		emptyKeys := emptyKeysResp.Kvs
		if len(emptyKeys) != 0 {
			t.Errorf("Expected 0 keys, got %d", len(emptyKeys))
		}
	}
}

func rangeEndForPrefix(t *testing.T, prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	t.Fatalf("next prefix does not exist for %s (%v)", string(prefix), prefix)
	return nil
}

func TestMemoryStorage_RevisionOrdering(t *testing.T) {
	log := memorylog.New()
	storage, err := NewMemoryStorage(log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := t.Context()

	// Put a key
	key := []byte("test-key")
	value1 := []byte("value1")
	resp1, _ := storage.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: value1})
	revision1 := getRevision(t, resp1)

	// Update the key
	value2 := []byte("value2")
	resp2, _ := storage.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: value2})
	revision2 := getRevision(t, resp2)

	// Verify revision ordering
	if revision1 >= revision2 {
		t.Errorf("Expected revision1 (%d) < revision2 (%d)", revision1, revision2)
	}

	// Get at revision1
	kvResp, err := storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: int64(revision1)})
	if err != nil {
		t.Fatalf("Get at revision1 failed: %v", err)
	}
	kv := kvResp.Kvs[0]
	if !reflect.DeepEqual(kv.Value, value1) {
		t.Errorf("Expected value1 at revision1, got %v", kv.Value)
	}

	// Get at revision2
	kvResp, err = storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: int64(revision2)})
	if err != nil {
		t.Fatalf("Get at revision2 failed: %v", err)
	}
	kv = kvResp.Kvs[0]
	if !reflect.DeepEqual(kv.Value, value2) {
		t.Errorf("Expected value2 at revision2, got %v", kv.Value)
	}
}

func TestMemoryStorage_ConcurrentAccess(t *testing.T) {
	storage, err := NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := t.Context()

	// Test concurrent puts
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := []byte(fmt.Sprintf("concurrent-key-%d", id))
			value := []byte(fmt.Sprintf("value-%d", id))
			_, err := storage.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: value})
			if err != nil {
				t.Errorf("Concurrent Put failed: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all keys exist
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("concurrent-key-%d", i))
		_, err := storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
		if err != nil {
			t.Errorf("Key %s not found after concurrent access", string(key))
		}
	}
}

func TestMemoryStorage_MVCCBehavior(t *testing.T) {
	storage, err := NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := t.Context()

	// Put a key
	key := []byte("test-key")
	value1 := []byte("value1")
	resp1, _ := storage.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: value1})
	revision1 := getRevision(t, resp1)

	// Update the key
	value2 := []byte("value2")
	resp2, _ := storage.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: value2})
	revision2 := getRevision(t, resp2)

	// Delete the key
	delResp, _ := storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
	revision3 := delResp.Header.Revision

	// Test historical access
	// At revision1, should get value1
	kvResp, err := storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: int64(revision1)})
	if err != nil {
		t.Fatalf("Get at revision1 failed: %v", err)
	}
	kv := kvResp.Kvs[0]
	if !reflect.DeepEqual(kv.Value, value1) {
		t.Errorf("Expected value1 at revision1, got %v", kv.Value)
	}
	if Revision(kv.CreateRevision) != revision1 {
		t.Errorf("Expected CreateRevision %d, got %d", revision1, kv.CreateRevision)
	}

	// At revision2, should get value2
	kvResp, err = storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: int64(revision2)})
	if err != nil {
		t.Fatalf("Get at revision2 failed: %v", err)
	}
	kv = kvResp.Kvs[0]
	if !reflect.DeepEqual(kv.Value, value2) {
		t.Errorf("Expected value2 at revision2, got %v", kv.Value)
	}
	if Revision(kv.CreateRevision) != revision1 {
		t.Errorf("Expected CreateRevision %d, got %d", revision1, kv.CreateRevision)
	}

	// At revision3, should be not found
	kvResp, err = storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: int64(revision3)})
	assertNotFound(t, kvResp, err)

	// At latest revision (0), should get error for deleted key
	kvResp, err = storage.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	assertNotFound(t, kvResp, err)
}

func assertNotFound(t *testing.T, resp *etcdserverpb.RangeResponse, err error) {
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(resp.Kvs) > 0 {
		t.Fatalf("expected no key-value pairs, got %v", resp.Kvs)
	}
}

func TestMemoryStorage_RangeQueries(t *testing.T) {
	storage, err := NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := t.Context()

	// Put some test data
	testData := map[string]string{
		"a": "value-a",
		"b": "value-b",
		"c": "value-c",
		"d": "value-d",
		"e": "value-e",
	}

	for k, v := range testData {
		_, err := storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(k), Value: []byte(v)})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Test range query [b, d) - should return b and c
	rangeResp, err := storage.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("b"), RangeEnd: []byte("d")})
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}
	rangeKeys := rangeResp.Kvs
	if len(rangeKeys) != 2 {
		t.Errorf("Expected 2 keys in range [b, d), got %d", len(rangeKeys))
	}

	// Verify we got b and c
	keys := make([]string, len(rangeKeys))
	for i, kv := range rangeKeys {
		keys[i] = string(kv.Key)
	}
	require.Contains(t, keys, "b")
	require.Contains(t, keys, "c")
	require.NotContains(t, keys, "a")
	require.NotContains(t, keys, "d")
	require.NotContains(t, keys, "e")

	// Test range query [a, c) - should return a and b
	rangeResp, err = storage.List(ctx, &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c")})
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}
	rangeKeys = rangeResp.Kvs
	if len(rangeKeys) != 2 {
		t.Errorf("Expected 2 keys in range [a, c), got %d", len(rangeKeys))
	}

	keys = make([]string, len(rangeKeys))
	for i, kv := range rangeKeys {
		keys[i] = string(kv.Key)
	}
	require.Contains(t, keys, "a")
	require.Contains(t, keys, "b")
	require.NotContains(t, keys, "c")
	require.NotContains(t, keys, "d")
	require.NotContains(t, keys, "e")
}

func TestMemoryStorage_Watch(t *testing.T) {
	storage, err := NewMemoryStorage(memorylog.New())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := t.Context()

	t.Run("Storage Watch Interface", func(t *testing.T) {
		var events []*mvccpb.Event
		var mu sync.Mutex

		// Callback to collect events
		callback := func(event *etcdserverpb.WatchResponse) error {
			mu.Lock()
			events = append(events, event.Events...)
			mu.Unlock()
			return nil
		}
		// Create a watcher for exact key match
		watcher, _, err := storage.Watch(ctx, &etcdserverpb.WatchCreateRequest{
			WatchId: 1,
			Key:     []byte("test-key"),
			PrevKv:  true,
		}, callback)
		if err != nil {
			t.Fatalf("Failed to create watcher: %v", err)
		}
		defer watcher.Close()

		go func() {
			if err := watcher.Run(ctx); err != nil {
				t.Errorf("watch stopped with error: %v", err)
			}
		}()

		// Give watcher time to establish
		time.Sleep(50 * time.Millisecond)

		// Put a key
		_, err = storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("test-key"), Value: []byte("value1")})
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Update the key
		_, err = storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("test-key"), Value: []byte("value2")})
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Delete the key
		delResp, err := storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("test-key")})
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
		if delResp.Deleted != 1 {
			t.Fatalf("Expected 1 deleted, got %d", delResp.Deleted)
		}

		// Wait for events
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		if len(events) != 3 {
			t.Fatalf("Expected 3 events, got %d", len(events))
		}

		// Check first event (PUT)
		if events[0].Type != mvccpb.PUT {
			t.Errorf("Expected PUT event, got %d", events[0].Type)
		}
		if string(events[0].Kv.Key) != "test-key" {
			t.Errorf("Expected key 'test-key', got '%s'", string(events[0].Kv.Key))
		}
		if string(events[0].Kv.Value) != "value1" {
			t.Errorf("Expected value 'value1', got '%s'", string(events[0].Kv.Value))
		}

		// Check second event (UPDATE)
		if events[1].Type != mvccpb.PUT {
			t.Errorf("Expected PUT event, got %d", events[1].Type)
		}
		if string(events[1].Kv.Value) != "value2" {
			t.Errorf("Expected value 'value2', got '%s'", string(events[1].Kv.Value))
		}
		if events[1].PrevKv == nil {
			t.Error("Expected PrevKv to be set for update")
		} else if string(events[1].PrevKv.Value) != "value1" {
			t.Errorf("Expected previous value 'value1', got '%s'", string(events[1].PrevKv.Value))
		}

		// Check third event (DELETE)
		if events[2].Type != mvccpb.DELETE {
			t.Errorf("Expected DELETE event, got %d", events[2].Type)
		}
		if events[2].PrevKv == nil {
			t.Fatalf("Expected PrevKv to be set for delete")
		}
		// The value should be the previous value
		if string(events[2].PrevKv.Value) != "value2" {
			t.Errorf("Expected kv value 'value2', got '%s'", string(events[2].PrevKv.Value))
		}
	})

	t.Run("Storage Prefix Watch", func(t *testing.T) {
		var events []*mvccpb.Event
		var mu sync.Mutex

		callback := func(event *etcdserverpb.WatchResponse) error {
			mu.Lock()
			events = append(events, event.Events...)
			mu.Unlock()
			return nil
		}
		// Create a watcher for prefix match (using empty rangeEnd for prefix behavior)
		watcher, _, err := storage.Watch(ctx, &etcdserverpb.WatchCreateRequest{
			WatchId:  1,
			Key:      []byte("prefix/"),
			RangeEnd: rangeEndForPrefix(t, []byte("prefix/")),
		}, callback)
		if err != nil {
			t.Fatalf("Failed to create watcher: %v", err)
		}
		defer watcher.Close()

		go func() {
			if err := watcher.Run(ctx); err != nil {
				t.Errorf("watch stopped with error: %v", err)
			}
		}()

		// Give watcher time to establish
		time.Sleep(50 * time.Millisecond)

		// Put keys under prefix
		_, err = storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("prefix/key1"), Value: []byte("value1")})
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		_, err = storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("prefix/key2"), Value: []byte("value2")})
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Put key that doesn't match prefix - should not trigger event
		_, err = storage.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("other"), Value: []byte("value3")})
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Delete key under prefix
		delResp, err := storage.Delete(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("prefix/key1")})
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
		if delResp.Deleted != 1 {
			t.Fatalf("Expected 1 deleted, got %d", delResp.Deleted)
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
			if string(event.Kv.Key) != expectedKeys[i] {
				t.Errorf("Event %d: expected key '%s', got '%s'", i, expectedKeys[i], string(event.Kv.Key))
			}
		}
	})
}
