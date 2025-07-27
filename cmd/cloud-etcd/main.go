package main

import (
	"context"
	"fmt"
	"log"

	"justinsb.com/cloudetcd/pkg/storage"
)

func main() {
	fmt.Println("cloud-etcd starting...")

	// Create a new storage instance
	store := storage.NewMemoryStorage()
	ctx := context.Background()

	// Demo basic operations
	fmt.Println("\n=== Basic Operations Demo ===")

	// Put a key
	key := []byte("demo-key")
	value := []byte("demo-value")
	revision, err := store.Put(ctx, key, value, 0)
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Printf("Put key '%s' with value '%s' at revision %d\n", string(key), string(value), revision)

	// Get the key
	kv, err := store.Get(ctx, key, 0)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Got key '%s' with value '%s' at revision %d\n", string(kv.Key), string(kv.Value), kv.Revision)

	// Update the key
	newValue := []byte("updated-value")
	revision2, err := store.Put(ctx, key, newValue, 0)
	if err != nil {
		log.Fatalf("Update failed: %v", err)
	}
	fmt.Printf("Updated key '%s' with value '%s' at revision %d\n", string(key), string(newValue), revision2)

	// Get at specific revision
	kv, err = store.Get(ctx, key, revision)
	if err != nil {
		log.Fatalf("Get at revision failed: %v", err)
	}
	fmt.Printf("Got key '%s' at revision %d with value '%s'\n", string(kv.Key), revision, string(kv.Value))

	// Demo list operations
	fmt.Println("\n=== List Operations Demo ===")

	// Put several keys with a prefix
	prefixKeys := []string{"prefix/key1", "prefix/key2", "prefix/key3"}
	for i, k := range prefixKeys {
		v := fmt.Sprintf("value-%d", i+1)
		_, err := store.Put(ctx, []byte(k), []byte(v), 0)
		if err != nil {
			log.Fatalf("Put failed: %v", err)
		}
		fmt.Printf("Put key '%s' with value '%s'\n", k, v)
	}

	// List all keys with prefix
	keys, err := store.List(ctx, []byte("prefix/"), 0)
	if err != nil {
		log.Fatalf("List failed: %v", err)
	}
	fmt.Printf("Listed %d keys with prefix 'prefix/':\n", len(keys))
	for _, kv := range keys {
		fmt.Printf("  %s = %s (revision %d)\n", string(kv.Key), string(kv.Value), kv.Revision)
	}

	fmt.Println("\ncloud-etcd demo completed successfully!")
}
