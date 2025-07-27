package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"justinsb.com/cloudetcd/pkg/api"
	"justinsb.com/cloudetcd/pkg/storage"
)

func main() {
	// Parse command line flags
	var (
		addr = flag.String("addr", ":2379", "Address to listen on")
		demo = flag.Bool("demo", false, "Run demo instead of server")
	)
	flag.Parse()

	if *demo {
		runDemo()
		return
	}

	fmt.Println("Starting cloud-etcd server...")

	// Create storage instance
	store := storage.NewMemoryStorage()

	// Create and start the etcd API server
	server := api.NewServer(store)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down server...")
		server.Stop()
	}()

	// Start the server
	fmt.Printf("Starting etcd API server on %s\n", *addr)
	if err := server.Start(*addr); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func runDemo() {
	fmt.Println("cloud-etcd demo starting...")

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
	fmt.Printf("Got key '%s' with value '%s' (created at revision %d)\n", string(kv.Key), string(kv.Value), kv.CreateRevision)

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
	keys, err := store.List(ctx, []byte("prefix/"), []byte{}, 0)
	if err != nil {
		log.Fatalf("List failed: %v", err)
	}
	fmt.Printf("Listed %d keys with prefix 'prefix/':\n", len(keys))
	for _, kv := range keys {
		fmt.Printf("  %s = %s (created at revision %d)\n", string(kv.Key), string(kv.Value), kv.CreateRevision)
	}

	fmt.Println("\ncloud-etcd demo completed successfully!")
}
