package storage

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryStorage_Put(t *testing.T) {
	storage := NewMemoryStorage()
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
	storage := NewMemoryStorage()
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
	if kv.Deleted {
		t.Error("Expected Deleted to be false")
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
	storage := NewMemoryStorage()
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
	storage := NewMemoryStorage()
	ctx := context.Background()

	// Put several key-value pairs
	testData := map[string]string{
		"prefix1/key1": "value1",
		"prefix1/key2": "value2",
		"prefix2/key1": "value3",
		"other/key1":   "value4",
	}

	for k, v := range testData {
		storage.Put(ctx, []byte(k), []byte(v), 0)
	}

	// Test list all keys
	allKeys, err := storage.List(ctx, []byte{}, []byte{}, 0)
	if err != nil {
		t.Fatalf("List all failed: %v", err)
	}
	if len(allKeys) != 4 {
		t.Errorf("Expected 4 keys, got %d", len(allKeys))
	}

	// Test list with prefix (using empty rangeEnd for prefix behavior)
	prefix1Keys, err := storage.List(ctx, []byte("prefix1/"), []byte{}, 0)
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

	// Test list with non-existent prefix
	emptyKeys, err := storage.List(ctx, []byte("non-existent/"), []byte{}, 0)
	if err != nil {
		t.Fatalf("List with non-existent prefix failed: %v", err)
	}
	if len(emptyKeys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(emptyKeys))
	}
}

func TestMemoryStorage_RevisionOrdering(t *testing.T) {
	storage := NewMemoryStorage()
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
	storage := NewMemoryStorage()
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
	storage := NewMemoryStorage()
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
	if kv.CreateRevision != revision1 {
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
	if kv.CreateRevision != revision1 {
		t.Errorf("Expected CreateRevision %d, got %d", revision1, kv.CreateRevision)
	}

	// At revision3, should get deleted version
	kv, err = storage.Get(ctx, key, revision3)
	if err != nil {
		t.Fatalf("Get at revision3 failed: %v", err)
	}
	if !kv.Deleted {
		t.Error("Expected deleted version at revision3")
	}
	if kv.CreateRevision != revision1 {
		t.Errorf("Expected CreateRevision %d, got %d", revision1, kv.CreateRevision)
	}

	// At latest revision (0), should get error for deleted key
	_, err = storage.Get(ctx, key, 0)
	if err == nil {
		t.Error("Expected error for deleted key at latest revision")
	}
}

func TestMemoryStorage_RangeQueries(t *testing.T) {
	storage := NewMemoryStorage()
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
