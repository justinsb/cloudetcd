# Test Client

This is a simple test client for the cloud-etcd server that uses the official etcd client library to verify our etcd API implementation.

## Usage

1. First, start the cloud-etcd server:
   ```bash
   go run cmd/cloud-etcd/main.go
   ```

2. In another terminal, run the test client:
   ```bash
   go run cmd/test-client/main.go
   ```

## What it tests

The test client verifies:
- PUT operations
- GET operations  
- PUT with previous value retrieval
- Range queries with prefixes
- DELETE operations
- Transactions

This helps ensure our etcd API implementation is compatible with the official etcd client library. 