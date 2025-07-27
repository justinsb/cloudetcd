package persistence

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// FilesystemLog is a filesystem-backed implementation of the Log interface
type FilesystemLog struct {
	mu       sync.RWMutex
	dir      string
	revision int64 // Current revision number
}

// NewFilesystemLog creates a new filesystem-backed log
func NewFilesystemLog(dir string) (*FilesystemLog, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	log := &FilesystemLog{
		dir: dir,
	}

	// Replay existing log entries to determine current revision
	if err := log.replay(); err != nil {
		return nil, fmt.Errorf("failed to replay existing log: %w", err)
	}

	return log, nil
}

// replay reads all existing log files to determine the current revision
func (f *FilesystemLog) replay() error {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	var revisions []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse revision from filename
		filename := entry.Name()
		revision, err := filenameToRevision(filename)
		if err != nil {
			// Skip invalid filenames
			continue
		}

		revisions = append(revisions, revision)
	}

	// Find the highest revision
	if len(revisions) > 0 {
		sort.Slice(revisions, func(i, j int) bool {
			return revisions[i] < revisions[j]
		})
		f.revision = revisions[len(revisions)-1]
	}

	return nil
}

// Append adds a new record to the log and returns the revision number
func (f *FilesystemLog) Append(ctx context.Context, operation string, key []byte, value []byte, leaseID int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Increment revision number
	f.revision++

	record := &LogRecord{
		Revision:  f.revision,
		Timestamp: time.Now(),
		Operation: operation,
		Key:       key,
		Value:     value,
		LeaseID:   leaseID,
	}

	// Create filename with hex-encoded revision
	filename := revisionToFilename(f.revision)
	filepath := filepath.Join(f.dir, filename)

	// Serialize record to JSON
	data, err := json.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal log record: %w", err)
	}

	// Write to file atomically
	if err := os.WriteFile(filepath, data, 0644); err != nil {
		return 0, fmt.Errorf("failed to write log file: %w", err)
	}

	return f.revision, nil
}

// GetCurrentRevision returns the current revision number
func (f *FilesystemLog) GetCurrentRevision(ctx context.Context) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.revision, nil
}

// Read reads records from the log starting from the given revision
func (f *FilesystemLog) Read(ctx context.Context, fromRevision int64, limit int) ([]*LogRecord, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if fromRevision < 0 {
		return nil, fmt.Errorf("invalid fromRevision: %d", fromRevision)
	}

	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read log directory: %w", err)
	}

	var records []*LogRecord
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !strings.HasSuffix(filename, ".log") {
			continue
		}

		// Parse revision from filename
		revision, err := filenameToRevision(filename)
		if err != nil {
			continue
		}

		if revision < fromRevision {
			continue
		}

		// Read and parse the log file
		filepath := filepath.Join(f.dir, filename)
		data, err := os.ReadFile(filepath)
		if err != nil {
			continue
		}

		var record LogRecord
		if err := json.Unmarshal(data, &record); err != nil {
			continue
		}

		records = append(records, &record)
	}

	// Sort by revision
	sort.Slice(records, func(i, j int) bool {
		return records[i].Revision < records[j].Revision
	})

	// Apply limit
	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}

	return records, nil
}

// Close closes the log and releases any resources
func (f *FilesystemLog) Close() error {
	// For filesystem implementation, there's nothing to clean up
	return nil
}

// revisionToFilename converts a revision number to a filename
func revisionToFilename(revision int64) string {
	revisionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(revisionBytes, uint64(revision))
	return hex.EncodeToString(revisionBytes) + ".log"
}

// filenameToRevision converts a filename to a revision number
func filenameToRevision(filename string) (int64, error) {
	if !strings.HasSuffix(filename, ".log") {
		return 0, fmt.Errorf("invalid filename format: %s", filename)
	}

	hexRevision := strings.TrimSuffix(filename, ".log")
	revisionBytes, err := hex.DecodeString(hexRevision)
	if err != nil {
		return 0, fmt.Errorf("failed to decode hex revision: %w", err)
	}

	if len(revisionBytes) != 8 {
		return 0, fmt.Errorf("invalid revision bytes length: %d", len(revisionBytes))
	}

	revision := int64(binary.BigEndian.Uint64(revisionBytes))
	return revision, nil
}
