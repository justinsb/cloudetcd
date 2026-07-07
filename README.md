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
  - Watch (create/cancel, streaming events)
  - Lease management (grant, revoke, keepalive, TTL expiry)
  - Compatible with official etcd client library
- **Persistence Log**: Pluggable change log backends selected by URI
  - `memory://` — in-memory log for testing
  - `filesystem://` — local on-disk log
  - `gs://` — Google Cloud Storage log using conditional (generation-based) writes
  - `tiered:` — tiered log combining a fast tier with async archival to GCS

## Building and Testing

### Prerequisites

- Go 1.24 or later

### Running Tests

```bash
# Run all tests
go test ./...

# Run storage tests with verbose output
go test ./pkg/storage/... -v

# Run tests with race detection
go test ./pkg/storage/... -race
```

### Running the Server

```bash
# Start the etcd API server (default port 2379, in-memory log)
go run cmd/cloud-etcd/main.go

# Start on a different port
go run cmd/cloud-etcd/main.go -addr :2380

# Persist the change log to the local filesystem
go run cmd/cloud-etcd/main.go -log filesystem:///var/lib/cloudetcd/log

# Persist the change log to a GCS bucket
go run cmd/cloud-etcd/main.go -log gs://my-bucket/logs/

# Use a tiered log: fast local tier with async archival to GCS
go run cmd/cloud-etcd/main.go -log 'tiered:?fast=filesystem:///var/lib/cloudetcd/log&archive=gs://my-bucket/logs/&flushInterval=5m'
```

### Testing with etcd Client

The server is compatible with the official etcd v3 client and tools:

```bash
# Start the server
go run cmd/cloud-etcd/main.go

# In another terminal, use etcdctl
etcdctl --endpoints=localhost:2379 put foo bar
etcdctl --endpoints=localhost:2379 get foo
```

See [docs/start-kube.md](docs/start-kube.md) for running `kube-apiserver` against cloud-etcd.

## Project Structure

```
cloudetcd/
├── cmd/
│   └── cloud-etcd/         # Main application entry point
├── docs/                   # Documentation
├── pkg/
│   ├── api/                # etcd v3 gRPC API layer (KV, Watch, Lease, Txn, Maintenance)
│   ├── bptree/             # In-memory B+ tree index (key → revisions)
│   ├── lease/              # Lease manager (TTL expiry, keepalive)
│   ├── persistence/        # Change log and snapshot interfaces
│   │   ├── batch/          # Batched commits with conflict detection
│   │   ├── filesystemlog/  # Local filesystem log
│   │   ├── gcslog/         # Google Cloud Storage log
│   │   ├── logfactory/     # Constructs a log from a URI
│   │   ├── logtests/       # Shared conformance tests for log implementations
│   │   ├── memorylog/      # In-memory log
│   │   └── tieredlog/      # Fast tier + async GCS archival
│   └── storage/            # Storage interface
│       └── memorystorage/  # In-memory MVCC storage backed by a persistence log
├── tests/
│   └── e2e/                # End-to-end tests
└── README.md
```

## Next Steps

1. **Snapshots & Compaction**: Snapshot the state so startup doesn't replay the full log
2. **Persistent Local Cache**: Persist the materialized view locally for fast restarts
3. **Range-read Conflict Detection**: Extend batch conflict detection to cover range reads
4. **Authentication**: Add authentication and authorization
5. **TLS**: Add TLS support for secure communication

## Contributing

This is an experimental project. Contributions are welcome!

## License

Apache License 2.0 — see [LICENSE.md](LICENSE.md).
