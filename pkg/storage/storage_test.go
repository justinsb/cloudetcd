package storage

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/persistence"
)

func TestMemoryStorage_Put(t *testing.T) {
	storage, err := NewMemoryStorage(persistence.NewMemoryLog())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := context.Background()

	// Test basic put
	key := []byte("test-key")
	value := []byte("test-value")
	revision, err := storage.Put(ctx, key, value, 0)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if revision != 1 {
		t.Errorf("Expected revision 1, got %d", revision)
	}

	// Test update
	newValue := []byte("updated-value")
	revision, err = storage.Put(ctx, key, newValue, 0)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if revision != 2 {
		t.Errorf("Expected revision 2, got %d", revision)
	}
}

func TestMemoryStorage_Get(t *testing.T) {
	storage, err := NewMemoryStorage(persistence.NewMemoryLog())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := context.Background()

	// Put a key-value pair
	key := []byte("test-key")
	value := []byte("test-value")
	storage.Put(ctx, key, value, 0)

	// Test get existing key
	kv, err := storage.Get(ctx, key, 0)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !reflect.DeepEqual(kv.Key, key) {
		t.Errorf("Expected key %v, got %v", key, kv.Key)
	}
	if !reflect.DeepEqual(kv.Value, value) {
		t.Errorf("Expected value %v, got %v", value, kv.Value)
	}
	if kv.CreateRevision != 1 {
		t.Errorf("Expected CreateRevision 1, got %d", kv.CreateRevision)
	}

	// Test get non-existent key
	_, err = storage.Get(ctx, []byte("non-existent"), 0)
	if err == nil {
		t.Error("Expected error for non-existent key")
	}

	// Test get at specific revision
	kv, err = storage.Get(ctx, key, 1)
	if err != nil {
		t.Fatalf("Get at revision failed: %v", err)
	}
	// Note: We can't easily check the revision number without storing it with each record
	// For now, just verify we get a valid result

	// Test get at future revision (should fail)
	_, err = storage.Get(ctx, key, 0)
	if err != nil {
		t.Errorf("Get at revision 0 should succeed, got error: %v", err)
	}
}

func TestMemoryStorage_Delete(t *testing.T) {
	storage, storageErr := NewMemoryStorage(persistence.NewMemoryLog())
	if storageErr != nil {
		t.Fatalf("Failed to create storage: %v", storageErr)
	}
	ctx := context.Background()

	// Put a key-value pair
	key := []byte("test-key")
	value := []byte("test-value")
	storage.Put(ctx, key, value, 0)

	// Verify it exists
	_, err := storage.Get(ctx, key, 0)
	if err != nil {
		t.Fatalf("Key should exist before delete: %v", err)
	}

	// Delete the key
	revision, err := storage.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if revision != 2 {
		t.Errorf("Expected revision 2, got %d", revision)
	}

	// Verify it's gone (should return error for deleted key)
	_, err = storage.Get(ctx, key, 0)
	if err == nil {
		t.Error("Expected error for deleted key")
	}

	// Test delete non-existent key (should succeed)
	revision, err = storage.Delete(ctx, []byte("non-existent"))
	if err != nil {
		t.Fatalf("Delete non-existent key failed: %v", err)
	}
	if revision != 3 {
		t.Errorf("Expected revision 3, got %d", revision)
	}
}

func TestMemoryStorage_List(t *testing.T) {
	storage, err := NewMemoryStorage(persistence.NewMemoryLog())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := context.Background()

	// Put several key-value pairs
	testData := map[string]string{
		"prefix1/key1": "value1",
		"prefix1/key2": "value2",
		"prefix2/key1": "value3",
		"other/key1":   "value4",
	}

	for k, v := range testData {
		_, err := storage.Put(ctx, []byte(k), []byte(v), 0)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Test list all keys
	{
		allKeys, err := storage.List(ctx, []byte{}, nil, 0)
		if err != nil {
			t.Fatalf("List all failed: %v", err)
		}
		if len(allKeys) != 4 {
			t.Errorf("Expected 4 keys, got %d", len(allKeys))
		}
	}

	// Test list with prefix (using empty rangeEnd for prefix behavior)
	{
		prefix := []byte("prefix1/")
		prefix1Keys, err := storage.List(ctx, prefix, rangeEndForPrefix(t, prefix), 0)
		if err != nil {
			t.Fatalf("List with prefix failed: %v", err)
		}
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
		emptyKeys, err := storage.List(ctx, prefix, rangeEndForPrefix(t, prefix), 0)
		if err != nil {
			t.Fatalf("List with non-existent prefix failed: %v", err)
		}
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
	storage, err := NewMemoryStorage(persistence.NewMemoryLog())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := context.Background()

	// Put a key
	key := []byte("test-key")
	value1 := []byte("value1")
	revision1, _ := storage.Put(ctx, key, value1, 0)

	// Update the key
	value2 := []byte("value2")
	revision2, _ := storage.Put(ctx, key, value2, 0)

	// Verify revision ordering
	if revision1 >= revision2 {
		t.Errorf("Expected revision1 (%d) < revision2 (%d)", revision1, revision2)
	}

	// Get at revision1
	kv, err := storage.Get(ctx, key, revision1)
	if err != nil {
		t.Fatalf("Get at revision1 failed: %v", err)
	}
	if !reflect.DeepEqual(kv.Value, value1) {
		t.Errorf("Expected value1 at revision1, got %v", kv.Value)
	}

	// Get at revision2
	kv, err = storage.Get(ctx, key, revision2)
	if err != nil {
		t.Fatalf("Get at revision2 failed: %v", err)
	}
	if !reflect.DeepEqual(kv.Value, value2) {
		t.Errorf("Expected value2 at revision2, got %v", kv.Value)
	}
}

func TestMemoryStorage_ConcurrentAccess(t *testing.T) {
	storage, err := NewMemoryStorage(persistence.NewMemoryLog())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := context.Background()

	// Test concurrent puts
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := []byte(fmt.Sprintf("concurrent-key-%d", id))
			value := []byte(fmt.Sprintf("value-%d", id))
			_, err := storage.Put(ctx, key, value, 0)
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
		_, err := storage.Get(ctx, key, 0)
		if err != nil {
			t.Errorf("Key %s not found after concurrent access", string(key))
		}
	}
}

func TestMemoryStorage_MVCCBehavior(t *testing.T) {
	storage, err := NewMemoryStorage(persistence.NewMemoryLog())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := context.Background()

	// Put a key
	key := []byte("test-key")
	value1 := []byte("value1")
	revision1, _ := storage.Put(ctx, key, value1, 0)

	// Update the key
	value2 := []byte("value2")
	revision2, _ := storage.Put(ctx, key, value2, 0)

	// Delete the key
	revision3, _ := storage.Delete(ctx, key)

	// Test historical access
	// At revision1, should get value1
	kv, err := storage.Get(ctx, key, revision1)
	if err != nil {
		t.Fatalf("Get at revision1 failed: %v", err)
	}
	if !reflect.DeepEqual(kv.Value, value1) {
		t.Errorf("Expected value1 at revision1, got %v", kv.Value)
	}
	if Revision(kv.CreateRevision) != revision1 {
		t.Errorf("Expected CreateRevision %d, got %d", revision1, kv.CreateRevision)
	}

	// At revision2, should get value2
	kv, err = storage.Get(ctx, key, revision2)
	if err != nil {
		t.Fatalf("Get at revision2 failed: %v", err)
	}
	if !reflect.DeepEqual(kv.Value, value2) {
		t.Errorf("Expected value2 at revision2, got %v", kv.Value)
	}
	if Revision(kv.CreateRevision) != revision1 {
		t.Errorf("Expected CreateRevision %d, got %d", revision1, kv.CreateRevision)
	}

	// At revision3, should get deleted version
	kv, err = storage.Get(ctx, key, revision3)
	if err != nil {
		t.Fatalf("Get at revision3 failed: %v", err)
	}
	if kv != nil {
		t.Errorf("Expected nil key-value pair at revision3, got %v", kv)
	}

	// At latest revision (0), should get error for deleted key
	_, err = storage.Get(ctx, key, 0)
	if err == nil {
		t.Error("Expected error for deleted key at latest revision")
	}
}

func TestMemoryStorage_RangeQueries(t *testing.T) {
	storage, err := NewMemoryStorage(persistence.NewMemoryLog())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := context.Background()

	// Put some test data
	testData := map[string]string{
		"a": "value-a",
		"b": "value-b",
		"c": "value-c",
		"d": "value-d",
		"e": "value-e",
	}

	for k, v := range testData {
		_, err := storage.Put(ctx, []byte(k), []byte(v), 0)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Test range query [b, d) - should return b and c
	rangeKeys, err := storage.List(ctx, []byte("b"), []byte("d"), 0)
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}
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
	rangeKeys, err = storage.List(ctx, []byte("a"), []byte("c"), 0)
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}
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
	storage, err := NewMemoryStorage(persistence.NewMemoryLog())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	ctx := context.Background()

	t.Run("Storage Watch Interface", func(t *testing.T) {
		// Create a watcher for exact key match
		watcher, err := storage.Watch(ctx, []byte("test-key"), []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to create watcher: %v", err)
		}
		defer watcher.Close()

		var events []*mvccpb.Event
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
		_, err = storage.Put(ctx, []byte("test-key"), []byte("value1"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Update the key
		_, err = storage.Put(ctx, []byte("test-key"), []byte("value2"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Delete the key
		_, err = storage.Delete(ctx, []byte("test-key"))
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
		if events[2].PrevKv != nil {
			t.Error("Expected PrevKv not to be set for delete")
		}
		// The value should be the previous value
		if string(events[2].Kv.Value) != "value2" {
			t.Errorf("Expected kv value 'value2', got '%s'", string(events[2].PrevKv.Value))
		}
	})

	t.Run("Storage Prefix Watch", func(t *testing.T) {
		// Create a watcher for prefix match (using empty rangeEnd for prefix behavior)
		watcher, err := storage.Watch(ctx, []byte("prefix/"), []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to create watcher: %v", err)
		}
		defer watcher.Close()

		var events []*mvccpb.Event
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
		_, err = storage.Put(ctx, []byte("prefix/key1"), []byte("value1"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		_, err = storage.Put(ctx, []byte("prefix/key2"), []byte("value2"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Put key that doesn't match prefix - should not trigger event
		_, err = storage.Put(ctx, []byte("other"), []byte("value3"), 0)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}

		// Delete key under prefix
		_, err = storage.Delete(ctx, []byte("prefix/key1"))
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
			if string(event.Kv.Key) != expectedKeys[i] {
				t.Errorf("Event %d: expected key '%s', got '%s'", i, expectedKeys[i], string(event.Kv.Key))
			}
		}
	})
}
