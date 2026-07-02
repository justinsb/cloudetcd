# Architecture: cloud-etcd

This document outlines the architecture for `cloud-etcd`, a custom implementation of the etcd API, designed to be backed by cloud storage services. The implementation targets Google Cloud Storage (GCS).

## 1. Overview

The primary goal of `cloud-etcd` is to provide an etcd-compatible API endpoint that can be used as a backend for Kubernetes (specifically `kube-apiserver`), while leveraging cloud-native storage for persistence and durability.

The design optimizes for **simplicity and low cost** rather than high performance. Using a managed object store such as GCS as the source of truth means there is no stateful etcd cluster to run, operate, or scale — durability and availability are inherited from the object store, which is inexpensive and requires no operational overhead.

The core design principle is to treat the cloud storage as a write-ahead log (or change log). A local, on-disk cache will store the materialized view of the data for fast read access. This approach avoids the need for a traditional distributed consensus protocol like Raft, as it relies on the consistency guarantees of the underlying cloud storage service.

## 2. Components

The system is composed of four main components:

### 2.1. etcd API Layer

This layer is responsible for handling incoming gRPC requests from clients like `kube-apiserver`. It will implement the necessary parts of the etcd v3 API, focusing on the subset required by Kubernetes.

- **API Implemented**: Key-Value, Watch, Lease.
- **Functionality**: It will parse incoming requests, delegate write operations to the change log, and serve read requests from the local cache. For watch operations, it will tail the change log.

### 2.2. Change Log (GCS)

The change log is the source of truth for all data modifications. It is stored as a sequence of objects in a Google Cloud Storage (GCS) bucket.

- **Object Layout**:
    - Each entry (or batch of entries) is written as an object whose name encodes the `revision`, a monotonically increasing integer (e.g., zero-padded so that lexical listing order matches revision order).
    - **Object Contents** (per change):
        - `operation_type`: (e.g., `PUT`, `DELETE`)
        - `key`: The key of the data being modified.
        - `value`: The value being written (for `PUT` operations).
        - `lease_id`: The ID of the associated lease.
        - `timestamp`: The time of the operation.

Ordering the change log by revision in the object name allows changes to be listed and replayed in chronological order. GCS's strong consistency and atomic, conditional (generation-based) writes are used to append entries safely and to avoid two writers claiming the same revision.

### 2.3. Local Cache

To provide low-latency read access, `cloud-etcd` will maintain a local cache of the key-value store. This cache can be stored on a local disk (e.g., using a key-value store library like BoltDB or LevelDB) or even in memory on a `tmpfs` volume for maximum performance.

- **Structure**: A simple key-value map.
- **Consistency**: The cache is updated *after* a write has been successfully committed to the GCS change log.

### 2.4. Replicator

The Replicator is responsible for keeping the Local Cache in sync with the Change Log.

- **On Startup**: During startup, the replicator reads the change log from GCS from the last known revision and applies the changes to the local cache to bring it up to date. If the cache is empty, it replays the entire log.
- **In Background**: It can run in the background to apply changes continuously, though for the first implementation, we will focus on startup replication.

## 3. Workflows

### 3.1. Startup Sequence

1.  Initialize the etcd API layer.
2.  Check for the existence of a local cache.
3.  If the cache exists, read the last applied revision number.
4.  The Replicator lists the GCS change log for all changes since the last applied revision.
5.  The Replicator applies these changes in order to the local cache.
6.  The server begins accepting traffic.

### 3.2. Write Operations (e.g., PUT)

1.  A write request is received by the etcd API Layer.
2.  A new revision number is generated.
3.  The change (operation type, key, value) is written to the GCS change log with the new revision number.
4.  Once the write to GCS is confirmed, the change is applied to the local cache.
5.  A success response is sent to the client.

### 3.3. Read Operations (e.g., GET)

1.  A read request is received by the etcd API Layer.
2.  The key is looked up directly in the local cache.
3.  The value from the cache is returned to the client. This ensures reads are fast and do not require a round-trip to GCS.

## 4. Future Considerations

- **Snapshots**: To speed up startup time for very large change logs, a snapshotting mechanism can be introduced. A background process would periodically compact the log up to a certain revision and store a full snapshot of the data as an object in GCS. The Replicator would then restore from the latest snapshot and replay only the changes that occurred after the snapshot was taken.
- **Watch API**: The Watch API will be implemented by tailing the GCS change log. When a client creates a watch, the server will query the change log for any changes after the requested revision and stream them back.
- **Leases**: Leases will be managed by a separate process that periodically checks for expired leases and writes corresponding `DELETE` operations to the change log.
- **Multi-node**: While the initial design is single-node, a multi-node setup for high availability could be achieved by having multiple `cloud-etcd` instances running, each with its own local cache, all reading from the same GCS change log. A mechanism for leader election would be needed for write operations to avoid conflicting revision numbers. 