# Filesystem Log Implementation

This directory contains a filesystem-backed implementation of the persistence log interface. Each log entry is stored as a separate file in a directory, with the revision number encoded in the filename.

## Features

- **Persistent Storage**: Each log entry is stored as a separate file
- **Restart Recovery**: Automatically replays existing log entries on startup
- **Atomic Writes**: Each log entry is written atomically to prevent corruption
- **Revision Tracking**: Maintains sequential revision numbers across restarts
- **JSON Serialization**: Log entries are stored in human-readable JSON format

## File Format

Each log entry is stored as a separate file with the following naming convention:
- Filename: `{hex-encoded-revision}.log`
- Content: JSON-serialized `LogRecord` structure

Example:
```
0000000000000001.log  # Revision 1
0000000000000002.log  # Revision 2
0000000000000003.log  # Revision 3
```

## Usage

```go
import "justinsb.com/cloudetcd/pkg/persistence"

// Create a new filesystem log
log, err := persistence.NewFilesystemLog("/path/to/log/directory")
if err != nil {
    log.Fatalf("Failed to create log: %v", err)
}
defer log.Close()

ctx := context.Background()

// Append a record
revision, err := log.Append(ctx, "PUT", []byte("key"), []byte("value"), 0)
if err != nil {
    log.Fatalf("Failed to append: %v", err)
}

// Read records
records, err := log.Read(ctx, 1, 10)
if err != nil {
    log.Fatalf("Failed to read: %v", err)
}

// Get current revision
currentRev, err := log.GetCurrentRevision(ctx)
if err != nil {
    log.Fatalf("Failed to get revision: %v", err)
}
```

## Design Considerations

### Advantages
- **Simple and Reliable**: Each entry is a separate file, making corruption unlikely
- **Easy to Debug**: Files can be inspected manually
- **Restart Safe**: Automatically recovers state on restart
- **Cloud Ready**: Similar pattern can be used with DynamoDB or other cloud storage

### Disadvantages
- **Not Efficient**: Each entry requires a separate file operation
- **File System Limits**: May hit file system limits with many entries
- **No Compression**: No built-in compression or compaction

### Future Improvements
- **Compaction**: Periodically merge old entries into larger files
- **Compression**: Compress individual log files
- **Indexing**: Create an index file for faster lookups
- **Cloud Storage**: Implement similar pattern with DynamoDB or S3

## Testing

Run the tests with:
```bash
go test ./pkg/persistence -v
```

The tests cover:
- Basic append and read operations
- Restart scenarios
- Reading with limits
- Reading from specific revisions
- Error handling
- Empty directory handling 