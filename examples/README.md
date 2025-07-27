# Cloud etcd Examples

This directory contains examples demonstrating various features of Cloud etcd.

## Examples

### etcdctl Example (`etcdctl-example.sh`)

A shell script demonstrating how to use the standard etcdctl tool with Cloud etcd.

### Kubernetes Example (`k8s-example.yaml`)

A Kubernetes deployment example showing how to run Cloud etcd in a containerized environment.

## Log Replay Feature

The log replay feature allows Cloud etcd to recover its state from a persistent log when restarting. This is particularly useful for:

- **High Availability**: Ensuring data persistence across restarts
- **Disaster Recovery**: Recovering from system failures
- **Development/Testing**: Maintaining state across development sessions

### How it works:

1. **Write-Ahead Logging**: All operations (PUT, DELETE) are first written to a persistent log
2. **Automatic Replay**: When a storage instance is created, it automatically replays the log to restore state
3. **State Recovery**: The storage state is fully recovered, including all key-value pairs and their revision history

### Supported Log Types:

- **Memory Log**: In-memory log for testing (not persistent)
- **Filesystem Log**: Persistent log stored on disk

### Usage:

```go
// Create a filesystem log
log, err := persistence.NewFilesystemLog("/path/to/log/dir")
if err != nil {
    log.Fatal(err)
}

// Create storage with the log (will automatically replay)
storage := storage.NewMemoryStorageWithLog(log)

// The storage is now ready with all previous state restored
```

The replay process is automatic and transparent - you don't need to manually trigger it. The storage will be fully functional immediately after creation.

### Testing

The log replay functionality is thoroughly tested in the test suite (`pkg/storage/memory_replay_test.go`), including:

- Basic log replay with memory logs
- Filesystem log replay (persistent storage)
- Empty log handling
- Force replay capability
- State recovery verification 