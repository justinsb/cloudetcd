# Google Cloud Storage Log Implementation

This document describes the Google Cloud Storage (GCS) backed implementation of the Log interface.

## Overview

The `GCSLog` provides a durable, scalable implementation of the Log interface using Google Cloud Storage as the backend. Each log entry is stored as a separate object in a GCS bucket, with the revision number encoded in the object name.

## Features

- **Durability**: Log entries are persisted to Google Cloud Storage
- **Scalability**: Leverages GCS's scalability and reliability
- **Consistency**: Uses atomic operations for log appends
- **Replay**: Automatically discovers and replays existing log entries on startup
- **Concurrent Access**: Thread-safe implementation with proper locking

## Usage

### Basic Setup

```go
import (
    "context"
    "justinsb.com/cloudetcd/pkg/persistence"
)

func main() {
    ctx := context.Background()
    
    // Create a new GCS-backed log
    log, err := persistence.NewGCSLog(ctx, "my-etcd-bucket", "etcd-log-")
    if err != nil {
        panic(err)
    }
    defer log.Close()
    
    // Use the log...
}
```

### Configuration

The `NewGCSLog` function requires:
- `ctx`: Context for the GCS client operations
- `bucketName`: Name of the GCS bucket to store log entries
- `prefix`: Prefix for log object names (helps organize objects in the bucket)

### Authentication

The GCS client uses the standard Google Cloud authentication methods:

1. **Service Account Key**: Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to a service account key file
2. **Default Credentials**: When running on Google Cloud (GKE, Compute Engine, etc.), default service account credentials are used automatically
3. **gcloud CLI**: When running locally, `gcloud auth application-default login` can be used

### Required Permissions

The service account or user needs the following IAM permissions on the GCS bucket:

- `storage.objects.create` - To create new log entries
- `storage.objects.get` - To read existing log entries
- `storage.objects.list` - To discover existing log entries during replay
- `storage.buckets.get` - To verify bucket access

## Object Naming Convention

Log entries are stored as objects with names following this pattern:
```
{prefix}{hex-encoded-revision}.log
```

For example, with prefix `"etcd-log-"` and revision `12345`, the object name would be:
```
etcd-log-0000000000003039.log
```

The revision number is encoded as a 64-bit big-endian integer, then hex-encoded to ensure proper lexicographic ordering.

## Performance Considerations

- **Latency**: GCS operations have higher latency than local storage. Consider this for high-throughput applications.
- **Costs**: Each log entry creates a separate GCS object, which may impact storage costs for high-frequency operations.
- **Consistency**: GCS provides strong consistency for object operations, ensuring reliable log ordering.

## Example Usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "justinsb.com/cloudetcd/pkg/persistence"
    "go.etcd.io/etcd/api/v3/mvccpb"
)

func main() {
    ctx := context.Background()
    
    // Create GCS log
    gcsLog, err := persistence.NewGCSLog(ctx, "my-bucket", "etcd-log-")
    if err != nil {
        log.Fatal(err)
    }
    defer gcsLog.Close()
    
    // Create a log record
    record := &persistence.LogRecord{
        Events: []*mvccpb.Event{
            {
                Type: mvccpb.PUT,
                Kv: &mvccpb.KeyValue{
                    Key:   []byte("my-key"),
                    Value: []byte("my-value"),
                },
            },
        },
    }
    
    // Append to log
    revision, success, err := gcsLog.Append(ctx, 0, record)
    if err != nil {
        log.Fatal(err)
    }
    if !success {
        log.Fatal("Failed to append due to condition mismatch")
    }
    
    fmt.Printf("Appended record at revision %d\n", revision)
    
    // Read from log
    err = gcsLog.Read(ctx, 1, func(rev persistence.Revision, rec *persistence.LogRecord) bool {
        fmt.Printf("Read revision %d with %d events\n", rev, len(rec.Events))
        return true // Continue reading
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

## Testing

To run the full GCS integration tests:

1. Set up a GCS bucket for testing
2. Set environment variables:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   export TEST_GCS_BUCKET=my-test-bucket
   ```
3. Run the tests:
   ```bash
   go test ./pkg/persistence -v -run TestGCSLog
   ```

The unit tests (object name conversion) can be run without GCS credentials:
```bash
go test ./pkg/persistence -v -run TestGCSLogObjectNameConversion
```

## Error Handling

The implementation handles various error conditions:

- **Network failures**: Retries are handled by the GCS client library
- **Authentication errors**: Returned during log creation
- **Bucket access errors**: Detected during initialization
- **Concurrent access**: Handled through proper locking mechanisms

## Limitations

- **Sequential reads**: The `Read` method lists all objects to find relevant revisions, which may be slow for very large logs
- **No compression**: Log entries are stored as individual JSON objects without compression
- **No cleanup**: Old log entries are not automatically deleted (implement retention policies as needed)

## Future Enhancements

Potential improvements for production use:

1. **Batching**: Group multiple log entries into single objects for better performance
2. **Compression**: Compress log entries to reduce storage costs
3. **Indexing**: Use additional metadata or separate index objects for faster reads
4. **Retention**: Implement automatic cleanup of old log entries
5. **Caching**: Add local caching for frequently accessed log entries
