# cloud-etcd

A custom implementation of the etcd API designed to be backed by cloud storage services, initially targeting AWS DynamoDB.

## Overview

cloud-etcd provides an etcd-compatible API endpoint that can be used as a backend for Kubernetes (specifically `kube-apiserver`), while leveraging cloud-native storage for persistence and scalability.

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

### Running the Demo

```bash
# Run the demo program
go run cmd/cloud-etcd/main.go
```

## Project Structure

```
cloudetcd/
├── cmd/cloud-etcd/     # Main application entry point
├── docs/               # Documentation
├── pkg/
│   ├── api/           # etcd API layer (future)
│   ├── replicator/    # Replicator component (future)
│   └── storage/       # Storage interface and implementations
└── README.md
```

## Next Steps

1. **DynamoDB Implementation**: Implement the storage interface using DynamoDB
2. **etcd API Layer**: Implement the gRPC server with etcd v3 API
3. **Local Cache**: Implement persistent local cache (BoltDB/LevelDB)
4. **Replicator**: Implement the component that keeps local cache in sync
5. **Watch API**: Implement etcd's watch functionality
6. **Lease Management**: Implement TTL and lease management

## Contributing

This is an experimental project. Contributions are welcome!

## License

[Add your license here] 