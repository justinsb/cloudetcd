# cloud-etcd

A custom implementation of the etcd API designed to be backed by cloud storage services, with Google Cloud Storage (GCS) as the target.

## Overview

cloud-etcd provides an etcd-compatible API endpoint that can be used as a backend for Kubernetes (specifically `kube-apiserver`), while leveraging cloud-native storage for persistence and durability.

The goal is not raw performance — it is to be **simple and low cost**. By treating an object store like GCS as the source of truth, cloud-etcd avoids running and operating a stateful etcd cluster, and instead relies on the durability, availability, and low cost of managed object storage.

The core design principle is to treat the cloud storage as a write-ahead log (or change log). A local, on-disk cache stores the materialized view of the data for fast read access. This approach avoids the need for a traditional distributed consensus protocol like Raft, as it relies on the consistency guarantees of the underlying cloud storage service.

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

## Current Status

The project is in early development. Currently implemented:

- **Storage Interface**: A clean abstraction for the storage layer
- **Memory Storage**: An in-memory implementation for testing and development
- **Core Operations**: Create, update, get, delete, and list operations
- **MVCC Support**: Full Multi-Version Concurrency Control with revision ranges
  - `CreateRevision`: Tracks when a key was first created
  - `ModRevision`: Tracks when a key was last modified
  - `Deleted`: Tombstone flag for deleted entries
- **Historical Access**: Ability to query data at specific revisions
- **Comprehensive Tests**: Test coverage for all core operations including concurrent access
- **etcd v3 API**: Full gRPC server implementing the etcd v3 API
  - Key-Value operations (PUT, GET, DELETE, RANGE)
  - Transactions
  - Lease management (basic implementation)
  - Compatible with official etcd client library

## Building and Testing

### Prerequisites

- Go 1.24 or later

### Running Tests

```bash
# Run all tests
go test ./...

# Run storage tests with verbose output
go test ./pkg/storage -v

# Run tests with race detection
go test ./pkg/storage -race
```

### Running the Server

```bash
# Start the etcd API server (default port 2379)
go run cmd/cloud-etcd/main.go

# Start on a different port
go run cmd/cloud-etcd/main.go -addr :2380

# Run the demo instead
go run cmd/cloud-etcd/main.go -demo
```

### Testing with etcd Client

```bash
# Start the server
go run cmd/cloud-etcd/main.go

# In another terminal, run the test client
go run cmd/test-client/main.go
```

## Project Structure

```
cloudetcd/
├── cmd/
│   ├── cloud-etcd/     # Main application entry point
│   └── test-client/    # Test client using etcd client library
├── docs/               # Documentation
├── pkg/
│   ├── api/           # etcd API layer
│   ├── replicator/    # Replicator component (future)
│   └── storage/       # Storage interface and implementations
└── README.md
```

## Next Steps

1. **GCS Implementation**: Implement the storage interface using Google Cloud Storage (GCS)
2. **Local Cache**: Implement persistent local cache (BoltDB/LevelDB)
3. **Replicator**: Implement the component that keeps local cache in sync
4. **Watch API**: Implement etcd's watch functionality
5. **Lease Management**: Implement full TTL and lease management
6. **Authentication**: Add authentication and authorization
7. **TLS**: Add TLS support for secure communication

## Contributing

This is an experimental project. Contributions are welcome!

## License

[Add your license here] 