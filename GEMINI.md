## Project Summary: cloud-etcd

`cloud-etcd` is a custom implementation of the etcd API designed to be backed by cloud storage services, with Google Cloud Storage (GCS) as the target.

The goal is to be simple and low cost rather than high performance: by using a managed object store like GCS as the source of truth, `cloud-etcd` avoids operating a stateful etcd cluster.

The core architectural principle is to use the cloud storage as a write-ahead log, complemented by a local on-disk cache for fast read operations. This design avoids the need for a consensus protocol like Raft, relying instead on the consistency guarantees of the cloud provider's storage service.

### Current Status

The project is in its early stages but has already implemented several key features:
- A clean storage interface with an in-memory implementation for testing.
- Core CRUDL (Create, Read, Update, Delete, List) operations.
- Full Multi-Version Concurrency Control (MVCC) for tracking data history.
- A gRPC server that implements the etcd v3 API, including key-value operations, transactions, and basic lease management.

### Next Steps

The immediate goals for the project are:
1.  Implement the storage interface for Google Cloud Storage (GCS).
2.  Develop a persistent local cache using a tool like BoltDB or LevelDB.
3.  Build the replicator component to synchronize the local cache with the cloud storage.
4.  Implement the `Watch` API for monitoring changes.
5.  Flesh out lease management, authentication, and TLS support.