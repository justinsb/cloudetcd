// Copyright 2026 Justin Santa Barbara
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filesystemlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/logtests"
)

func makeTestFilesystemLog(t *testing.T) *FilesystemLog {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "filesystem_log_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	// Create a new filesystem log
	log, err := NewFilesystemLog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}

	return log
}

func TestFilesystemLog_All(t *testing.T) {
	logtests.RunAll(t, func(t *testing.T) persistence.Log {
		return makeTestFilesystemLog(t)
	})
}

func TestFilesystemLog_Restart(t *testing.T) {

	ctx := t.Context()

	log1 := makeTestFilesystemLog(t)

	// Add some records
	revision1, ok, err := log1.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
			},
		},
	}, persistence.NewTxnMeta(0))
	if err != nil || !ok {
		t.Fatalf("Failed to append first record: %v", err)
	}

	revision2, ok, err := log1.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
			},
		},
	}, persistence.NewTxnMeta(1))
	if err != nil || !ok {
		t.Fatalf("Failed to append second record: %v", err)
	}
	if revision2 != 2 {
		t.Errorf("Expected revision 2, got %d", revision2)
	}

	log1.Close()

	// Create second log instance (simulating restart)
	log2, err := NewFilesystemLog(log1.dir)
	if err != nil {
		t.Fatalf("Failed to create second filesystem log: %v", err)
	}
	defer log2.Close()

	// Check that the revision was properly restored
	currentRevision, err := log2.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRevision != revision2 {
		t.Errorf("Expected revision %d after restart, got %d", revision2, currentRevision)
	}

	// Read all records to verify they were preserved
	records := make(map[Revision]*LogRecord)
	if err := log2.Read(ctx, 1, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("Expected 2 records after restart, got %d", len(records))
	}

	// Verify first record
	if records[revision1] == nil {
		t.Errorf("Expected first record revision %d, got %v", revision1, records[revision1])
	}
	if string(records[revision1].Events[0].Kv.Key) != "key1" {
		t.Errorf("Expected first record key key1, got %s", string(records[revision1].Events[0].Kv.Key))
	}

	// Verify second record
	if records[revision2] == nil {
		t.Errorf("Expected second record revision %d, got %v", revision2, records[revision2])
	}
	if string(records[revision2].Events[0].Kv.Key) != "key2" {
		t.Errorf("Expected second record key key2, got %s", string(records[revision2].Events[0].Kv.Key))
	}

	// Add a new record after restart
	revision3, ok, err := log2.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("key3"),
					Value: []byte("value3"),
				},
			},
		},
	}, persistence.NewTxnMeta(2))
	if err != nil || !ok {
		t.Fatalf("Failed to append record after restart: %v", err)
	}
	if revision3 != revision2+1 {
		t.Errorf("Expected revision %d, got %d", revision2+1, revision3)
	}
}

func TestFilesystemLog_ExampleUsage(t *testing.T) {
	ctx := t.Context()

	log := makeTestFilesystemLog(t)

	// Add some sample records
	logRevision1, ok, err := log.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("user:1"),
					Value: []byte("Alice"),
				},
			},
		},
	}, persistence.NewTxnMeta(0))
	if err != nil || !ok {
		t.Fatalf("Failed to append record: %v", err)
	}
	if logRevision1 != 1 {
		t.Errorf("Expected revision 1, got %d", logRevision1)
	}

	logRevision2, ok, err := log.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("user:2"),
					Value: []byte("Bob"),
				},
			},
		},
	}, persistence.NewTxnMeta(1))
	if err != nil || !ok {
		t.Fatalf("Failed to append record: %v", err)
	}
	if logRevision2 != 2 {
		t.Errorf("Expected revision 2, got %d", logRevision2)
	}

	logRevision3, ok, err := log.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.DELETE,
				Kv: &mvccpb.KeyValue{
					Key: []byte("user:1"),
				},
			},
		},
	}, persistence.NewTxnMeta(2))
	if err != nil || !ok {
		t.Fatalf("Failed to append record: %v", err)
	}
	if logRevision3 != 3 {
		t.Errorf("Expected revision 3, got %d", logRevision3)
	}

	// Read all records
	records := make(map[Revision]*LogRecord)
	if err := log.Read(ctx, 1, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// Verify first record
	if records[1] == nil || records[1].Events[0].Type != mvccpb.PUT || string(records[1].Events[0].Kv.Key) != "user:1" || string(records[1].Events[0].Kv.Value) != "Alice" {
		t.Errorf("First record mismatch: %+v", records[1])
	}

	// Verify second record
	if records[2] == nil || records[2].Events[0].Type != mvccpb.PUT || string(records[2].Events[0].Kv.Key) != "user:2" || string(records[2].Events[0].Kv.Value) != "Bob" {
		t.Errorf("Second record mismatch: %+v", records[2])
	}

	// Verify third record
	if records[3] == nil || records[3].Events[0].Type != mvccpb.DELETE || string(records[3].Events[0].Kv.Key) != "user:1" {
		t.Errorf("Third record mismatch: %+v", records[3])
	}

	// Simulate a restart by creating a new log instance
	log.Close()

	newLog, err := NewFilesystemLog(log.dir)
	if err != nil {
		t.Fatalf("Failed to create new log after restart: %v", err)
	}
	defer newLog.Close()

	// Check current revision after restart
	currentRevision, err := newLog.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRevision != 3 {
		t.Errorf("Expected revision 3 after restart, got %d", currentRevision)
	}

	// Add a new record after restart
	logRevision4, ok, err := newLog.Append(ctx, &LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("user:3"),
					Value: []byte("Charlie"),
				},
			},
		},
	}, persistence.NewTxnMeta(3))
	if err != nil || !ok {
		t.Fatalf("Failed to append record after restart: %v", err)
	}
	if logRevision4 != 4 {
		t.Errorf("Expected revision 4, got %d", logRevision4)
	}

	// Read all records again
	records = make(map[Revision]*LogRecord)
	if err := newLog.Read(ctx, 1, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Fatalf("Failed to read records after restart: %v", err)
	}
	if len(records) != 4 {
		t.Errorf("Expected 4 records after restart, got %d", len(records))
	}

	// Verify the new record
	if records[4] == nil || records[4].Events[0].Type != mvccpb.PUT || string(records[4].Events[0].Kv.Key) != "user:3" || string(records[4].Events[0].Kv.Value) != "Charlie" {
		t.Errorf("Fourth record mismatch: %+v", records[4])
	}
}

// TestFilesystemLog_TinyCache runs the whole log suite with a record cache
// too small to hold anything, so every GetLogEntry is a positioned read of
// one record from disk.
func TestFilesystemLog_TinyCache(t *testing.T) {
	logtests.RunAll(t, func(t *testing.T) persistence.Log {
		log, err := NewFilesystemLogWithOptions(t.TempDir(), Options{CacheBytes: 1})
		if err != nil {
			t.Fatalf("Failed to create filesystem log: %v", err)
		}
		return log
	})
}

// TestFilesystemLog_ReadsAfterRestart checks that a log reopened on an
// existing directory reads individual records from files it did not write
// (rebuilding their record offsets on first use), with the cache disabled.
func TestFilesystemLog_ReadsAfterRestart(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	log1, err := NewFilesystemLogWithOptions(dir, Options{CacheBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	const n = 40
	for i := 0; i < n; i++ {
		rec := &persistence.LogRecord{Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(fmt.Sprintf("k%03d", i)), Value: make([]byte, 100+i)}}}}
		if _, ok, err := log1.Append(ctx, rec, persistence.NewTxnMeta(0)); err != nil || !ok {
			t.Fatalf("append %d: ok=%v err=%v", i, ok, err)
		}
	}
	if err := log1.Close(); err != nil {
		t.Fatal(err)
	}

	log2, err := NewFilesystemLogWithOptions(dir, Options{CacheBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer log2.Close()
	for r := persistence.Revision(1); r <= n; r++ {
		rec, err := log2.GetLogEntry(r)
		if err != nil {
			t.Fatalf("GetLogEntry(%d): %v", r, err)
		}
		if want := fmt.Sprintf("k%03d", r-1); string(rec.Events[0].Kv.Key) != want || len(rec.Events[0].Kv.Value) != 100+int(r-1) {
			t.Fatalf("GetLogEntry(%d) = %s (%d bytes), want %s (%d bytes)", r, rec.Events[0].Kv.Key, len(rec.Events[0].Kv.Value), want, 100+int(r-1))
		}
	}
}

// TestRotation checks that the active file rotates by size, that reads span
// files, and that a reopened log continues from the right revision.
func TestRotation(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	opts := Options{RotateBytes: 300, CacheBytes: 1}
	log, err := NewFilesystemLogWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 30
	for i := 0; i < n; i++ {
		rec := &persistence.LogRecord{Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(fmt.Sprintf("k%03d", i)), Value: make([]byte, 40)}}}}
		if _, ok, err := log.Append(ctx, rec, persistence.NewTxnMeta(0)); err != nil || !ok {
			t.Fatalf("append %d: ok=%v err=%v", i, ok, err)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) < 3 {
		t.Fatalf("expected several files after %d records with RotateBytes=300, got %d", n, len(entries))
	}
	for r := persistence.Revision(1); r <= n; r++ {
		rec, err := log.GetLogEntry(r)
		if err != nil {
			t.Fatalf("GetLogEntry(%d): %v", r, err)
		}
		if want := fmt.Sprintf("k%03d", r-1); string(rec.Events[0].Kv.Key) != want {
			t.Fatalf("GetLogEntry(%d) = %s, want %s", r, rec.Events[0].Kv.Key, want)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	log2, err := NewFilesystemLogWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer log2.Close()
	if rev, _ := log2.GetCurrentRevision(ctx); rev != n {
		t.Fatalf("reopened log at revision %d, want %d", rev, n)
	}
	rec := &persistence.LogRecord{Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("after"), Value: []byte("v")}}}}
	if rev, ok, err := log2.Append(ctx, rec, persistence.NewTxnMeta(0)); err != nil || !ok || rev != n+1 {
		t.Fatalf("append after reopen: rev=%d ok=%v err=%v", rev, ok, err)
	}
}

// TestTornTail checks that a write cut short by a crash is dropped on the
// next open: the file is truncated to its last complete record, the revision
// is what was acknowledged, and appending continues.
func TestTornTail(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	log, err := NewFilesystemLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		rec := &persistence.LogRecord{Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}}}
		if _, ok, err := log.Append(ctx, rec, persistence.NewTxnMeta(0)); err != nil || !ok {
			t.Fatalf("append %d: ok=%v err=%v", i, ok, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: a partial record at the end of the file.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("expected one file, got %d", len(entries))
	}
	path := filepath.Join(dir, entries[0].Name())
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x80, 0x01, 0xde, 0xad}) // a 128-byte length prefix followed by 2 bytes
	f.Close()

	log2, err := NewFilesystemLog(dir)
	if err != nil {
		t.Fatalf("reopening a log with a torn tail: %v", err)
	}
	defer log2.Close()
	if rev, _ := log2.GetCurrentRevision(ctx); rev != 3 {
		t.Fatalf("revision after torn tail = %d, want 3", rev)
	}
	rec := &persistence.LogRecord{Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("k3"), Value: []byte("v")}}}}
	if rev, ok, err := log2.Append(ctx, rec, persistence.NewTxnMeta(0)); err != nil || !ok || rev != 4 {
		t.Fatalf("append after repair: rev=%d ok=%v err=%v", rev, ok, err)
	}
	for r := persistence.Revision(1); r <= 4; r++ {
		if _, err := log2.GetLogEntry(r); err != nil {
			t.Fatalf("GetLogEntry(%d) after repair: %v", r, err)
		}
	}
}
