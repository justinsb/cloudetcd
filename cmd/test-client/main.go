package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	// Create etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("Testing etcd API...")

	// Test PUT operation
	fmt.Println("\n=== Testing PUT ===")
	putResp, err := cli.Put(ctx, "test-key", "test-value")
	if err != nil {
		log.Fatalf("PUT failed: %v", err)
	}
	fmt.Printf("PUT successful: revision=%d\n", putResp.Header.Revision)

	// Test GET operation
	fmt.Println("\n=== Testing GET ===")
	getResp, err := cli.Get(ctx, "test-key")
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	if len(getResp.Kvs) > 0 {
		kv := getResp.Kvs[0]
		fmt.Printf("GET successful: key=%s, value=%s, revision=%d\n",
			string(kv.Key), string(kv.Value), kv.CreateRevision)
	} else {
		fmt.Println("GET returned no key-value pairs")
	}

	// Test PUT with previous value
	fmt.Println("\n=== Testing PUT with PrevKv ===")
	putResp2, err := cli.Put(ctx, "test-key", "updated-value", clientv3.WithPrevKV())
	if err != nil {
		log.Fatalf("PUT with PrevKv failed: %v", err)
	}
	fmt.Printf("PUT with PrevKv successful: revision=%d\n", putResp2.Header.Revision)
	if putResp2.PrevKv != nil {
		fmt.Printf("Previous value: %s\n", string(putResp2.PrevKv.Value))
	}

	// Test range query
	fmt.Println("\n=== Testing Range Query ===")

	// Put some keys with a prefix
	cli.Put(ctx, "prefix/key1", "value1")
	cli.Put(ctx, "prefix/key2", "value2")
	cli.Put(ctx, "prefix/key3", "value3")

	// Get all keys with prefix
	rangeResp, err := cli.Get(ctx, "prefix/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Range query failed: %v", err)
	}
	fmt.Printf("Range query returned %d keys:\n", len(rangeResp.Kvs))
	for _, kv := range rangeResp.Kvs {
		fmt.Printf("  %s = %s\n", string(kv.Key), string(kv.Value))
	}

	// Test DELETE operation
	fmt.Println("\n=== Testing DELETE ===")
	delResp, err := cli.Delete(ctx, "test-key")
	if err != nil {
		log.Fatalf("DELETE failed: %v", err)
	}
	fmt.Printf("DELETE successful: deleted=%d, revision=%d\n",
		delResp.Deleted, delResp.Header.Revision)

	// Test GET after DELETE
	getResp2, err := cli.Get(ctx, "test-key")
	if err != nil {
		log.Fatalf("GET after DELETE failed: %v", err)
	}
	if len(getResp2.Kvs) == 0 {
		fmt.Println("GET after DELETE: key not found (as expected)")
	} else {
		fmt.Println("GET after DELETE: key still exists (unexpected)")
	}

	// Test transaction
	fmt.Println("\n=== Testing Transaction ===")
	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value("txn-key"), "=", "")).
		Then(clientv3.OpPut("txn-key", "txn-value")).
		Else(clientv3.OpGet("txn-key")).
		Commit()
	if err != nil {
		log.Fatalf("Transaction failed: %v", err)
	}
	fmt.Printf("Transaction successful: succeeded=%t\n", txnResp.Succeeded)

	fmt.Println("\nAll tests completed successfully!")
}
