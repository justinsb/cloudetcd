package persistence

import (
	"context"
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

		// Parse revision from filename (hex encoded)
		filename := entry.Name()
		if !strings.HasSuffix(filename, ".log") {
			continue
		}

		hexRevision := strings.TrimSuffix(filename, ".log")
		revisionBytes, err := hex.DecodeString(hexRevision)
		if err != nil {
			// Skip invalid filenames
			continue
		}

		if len(revisionBytes) != 8 {
			continue
		}

		// Convert bytes to int64
		revision := int64(revisionBytes[0])<<56 |
			int64(revisionBytes[1])<<48 |
			int64(revisionBytes[2])<<40 |
			int64(revisionBytes[3])<<32 |
			int64(revisionBytes[4])<<24 |
			int64(revisionBytes[5])<<16 |
			int64(revisionBytes[6])<<8 |
			int64(revisionBytes[7])

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
	revisionBytes := make([]byte, 8)
	revisionBytes[0] = byte(f.revision >> 56)
	revisionBytes[1] = byte(f.revision >> 48)
	revisionBytes[2] = byte(f.revision >> 40)
	revisionBytes[3] = byte(f.revision >> 32)
	revisionBytes[4] = byte(f.revision >> 24)
	revisionBytes[5] = byte(f.revision >> 16)
	revisionBytes[6] = byte(f.revision >> 8)
	revisionBytes[7] = byte(f.revision)

	filename := hex.EncodeToString(revisionBytes) + ".log"
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
		hexRevision := strings.TrimSuffix(filename, ".log")
		revisionBytes, err := hex.DecodeString(hexRevision)
		if err != nil {
			continue
		}

		if len(revisionBytes) != 8 {
			continue
		}

		revision := int64(revisionBytes[0])<<56 |
			int64(revisionBytes[1])<<48 |
			int64(revisionBytes[2])<<40 |
			int64(revisionBytes[3])<<32 |
			int64(revisionBytes[4])<<24 |
			int64(revisionBytes[5])<<16 |
			int64(revisionBytes[6])<<8 |
			int64(revisionBytes[7])

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
