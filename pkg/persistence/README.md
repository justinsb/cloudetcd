# Persistence Package

The persistence package provides interfaces and implementations for persisting etcd operations to durable storage.

## Overview

The persistence layer consists of two main interfaces:

- **Log**: Appends individual operations to a durable log
- **Snapshotter**: Creates and manages snapshots of the entire state

## Log Interface

The `Log` interface provides methods for appending operations and reading from the log:

```go
type Log interface {
    // Append adds a new record to the log and returns the revision number
    Append(ctx context.Context, operation string, key []byte, value []byte, leaseID int64) (int64, error)
    
    // GetCurrentRevision returns the current revision number
    GetCurrentRevision(ctx context.Context) (int64, error)
    
    // Read reads records from the log starting from the given revision
    Read(ctx context.Context, fromRevision int64, limit int) ([]*LogRecord, error)
    
    // Close closes the log and releases any resources
    Close() error
}
```

### LogRecord Structure

Each log record contains:

- `Revision`: The revision number for this record
- `Timestamp`: When this record was created
- `Operation`: The type of operation (PUT, DELETE, etc.)
- `Key`: The key being operated on
- `Value`: The value (for PUT operations)
- `LeaseID`: Associated lease ID (if any)

## Implementations

### MemoryLog

A simple in-memory implementation of the Log interface. This is useful for testing and development, but data is lost when the process terminates.

```go
log := persistence.NewMemoryLog()
revision, err := log.Append(ctx, "PUT", []byte("key"), []byte("value"), 0)
```

## Snapshotter Interface

The `Snapshotter` interface provides methods for creating and managing snapshots:

```go
type Snapshotter interface {
    // CreateSnapshot creates a snapshot of the current state
    CreateSnapshot(ctx context.Context, data []byte) (*Snapshot, error)
    
    // GetLatestSnapshot returns the most recent snapshot
    GetLatestSnapshot(ctx context.Context) (*Snapshot, error)
    
    // GetSnapshotAtRevision returns the snapshot at or before the given revision
    GetSnapshotAtRevision(ctx context.Context, revision int64) (*Snapshot, error)
    
    // Close closes the snapshotter and releases any resources
    Close() error
}
```

## Integration with Storage

The storage layer has been updated to integrate with the persistence layer. When you create a new `MemoryStorage`, you must provide a log implementation:

```go
// Create storage with memory log
log := persistence.NewMemoryLog()
store, err := storage.NewMemoryStorage(log)
if err != nil {
    // Handle error
}

// Or create storage with a custom log
customLog := persistence.NewMemoryLog()
store, err := storage.NewMemoryStorage(customLog)
if err != nil {
    // Handle error
}
```

Every `Put` and `Delete` operation is automatically appended to the log before being applied to the in-memory storage.

## Future Implementations

Planned implementations include:

- **DynamoDBLog**: A DynamoDB-backed implementation for production use
- **FileLog**: A file-based implementation for local development
- **DynamoDBSnapshotter**: A DynamoDB-backed snapshot implementation

## Usage Example

See `examples/persistence-example.go` for a complete example demonstrating how to use the persistence layer with the storage system. 