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

package tieredlog

import (
	"fmt"
	"os"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/filesystemlog"
	"justinsb.com/cloudetcd/pkg/persistence/logtests"
	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
)

func makeTieredLog(t *testing.T, fast persistence.Log, archive persistence.Log, options Options) *TieredLog {
	log, err := NewTieredLog(t.Context(), fast, archive, options)
	if err != nil {
		t.Fatalf("Failed to create tiered log: %v", err)
	}
	t.Cleanup(func() {
		log.Close()
	})
	return log
}

func makeFilesystemLog(t *testing.T) (*filesystemlog.FilesystemLog, string) {
	dir := t.TempDir()
	log, err := filesystemlog.NewFilesystemLog(dir)
	if err != nil {
		t.Fatalf("Failed to create filesystem log: %v", err)
	}
	return log, dir
}

func TestTieredLog_All_Memory(t *testing.T) {
	logtests.RunAll(t, func(t *testing.T) persistence.Log {
		// A short flush interval so drains run concurrently with the tests.
		return makeTieredLog(t, memorylog.New(), memorylog.New(), Options{FlushInterval: 20 * time.Millisecond})
	})
}

func TestTieredLog_All_Filesystem(t *testing.T) {
	logtests.RunAll(t, func(t *testing.T) persistence.Log {
		fast, _ := makeFilesystemLog(t)
		archive, _ := makeFilesystemLog(t)
		// A short flush interval so drains and pruning run concurrently with the tests.
		return makeTieredLog(t, fast, archive, Options{FlushInterval: 20 * time.Millisecond})
	})
}

func appendRecords(t *testing.T, log persistence.Log, startRevision Revision, count int) {
	t.Helper()
	ctx := t.Context()
	for i := 0; i < count; i++ {
		snapshotRevision := startRevision + Revision(i)
		record := &LogRecord{
			Events: []*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Key:   fmt.Appendf(nil, "key%d", snapshotRevision+1),
						Value: fmt.Appendf(nil, "value%d", snapshotRevision+1),
					},
				},
			},
		}
		revision, ok, err := log.Append(ctx, record, persistence.NewTxnMeta(snapshotRevision))
		if err != nil || !ok {
			t.Fatalf("Failed to append record: ok=%v, err=%v", ok, err)
		}
		if revision != snapshotRevision+1 {
			t.Fatalf("Expected revision %d, got %d", snapshotRevision+1, revision)
		}
	}
}

func readAll(t *testing.T, log persistence.Log, fromRevision Revision) map[Revision]*LogRecord {
	t.Helper()
	records := make(map[Revision]*LogRecord)
	if err := log.Read(t.Context(), fromRevision, func(revision Revision, record *LogRecord) bool {
		records[revision] = record
		return true
	}); err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}
	return records
}

func TestTieredLog_DrainAndPrune(t *testing.T) {
	ctx := t.Context()

	fast, fastDir := makeFilesystemLog(t)
	archive := memorylog.New()

	// A long flush interval so only explicit Flush calls drain.
	log := makeTieredLog(t, fast, archive, Options{FlushInterval: time.Hour})

	appendRecords(t, log, 0, 5)

	if err := log.Flush(ctx); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// The archive should now have all the records
	archiveRevision, err := archive.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get archive revision: %v", err)
	}
	if archiveRevision != 5 {
		t.Errorf("Expected archive revision 5, got %d", archiveRevision)
	}

	// The fast tier should have been pruned to just the newest file
	entries, err := os.ReadDir(fastDir)
	if err != nil {
		t.Fatalf("Failed to read fast tier directory: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 file in fast tier after pruning, got %d", len(entries))
	}

	// Reads must still see everything, merged across the tiers
	records := readAll(t, log, 1)
	if len(records) != 5 {
		t.Errorf("Expected 5 records from merged read, got %d", len(records))
	}
	for revision := Revision(1); revision <= 5; revision++ {
		record := records[revision]
		if record == nil {
			t.Fatalf("Missing record for revision %d", revision)
		}
		expectedKey := fmt.Sprintf("key%d", revision)
		if string(record.Events[0].Kv.Key) != expectedKey {
			t.Errorf("Expected key %q at revision %d, got %q", expectedKey, revision, record.Events[0].Kv.Key)
		}
	}

	// GetLogEntry for a pruned revision should fall back to the archive
	record, err := log.GetLogEntry(1)
	if err != nil {
		t.Fatalf("Failed to get pruned log entry: %v", err)
	}
	if string(record.Events[0].Kv.Key) != "key1" {
		t.Errorf("Expected key1, got %q", record.Events[0].Kv.Key)
	}

	// Appends must continue with the correct revision numbering
	appendRecords(t, log, 5, 2)
	records = readAll(t, log, 1)
	if len(records) != 7 {
		t.Errorf("Expected 7 records after more appends, got %d", len(records))
	}

	// A second flush drains only the new records
	if err := log.Flush(ctx); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	archiveRevision, err = archive.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get archive revision: %v", err)
	}
	if archiveRevision != 7 {
		t.Errorf("Expected archive revision 7 after second flush, got %d", archiveRevision)
	}
}

func TestTieredLog_BootstrapFromArchive(t *testing.T) {
	ctx := t.Context()

	archiveDir := t.TempDir()

	// First instance: append some records and drain them to the archive
	{
		fast, _ := makeFilesystemLog(t)
		archive, err := filesystemlog.NewFilesystemLog(archiveDir)
		if err != nil {
			t.Fatalf("Failed to create archive log: %v", err)
		}
		log, err := NewTieredLog(ctx, fast, archive, Options{FlushInterval: time.Hour})
		if err != nil {
			t.Fatalf("Failed to create tiered log: %v", err)
		}

		appendRecords(t, log, 0, 3)

		// Close flushes to the archive
		if err := log.Close(); err != nil {
			t.Fatalf("Failed to close tiered log: %v", err)
		}
	}

	// Second instance: an empty fast tier (simulating a replacement machine)
	// must be backfilled from the archive
	fast, _ := makeFilesystemLog(t)
	archive, err := filesystemlog.NewFilesystemLog(archiveDir)
	if err != nil {
		t.Fatalf("Failed to recreate archive log: %v", err)
	}
	log := makeTieredLog(t, fast, archive, Options{FlushInterval: time.Hour})

	currentRevision, err := log.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get current revision: %v", err)
	}
	if currentRevision != 3 {
		t.Errorf("Expected revision 3 after bootstrap, got %d", currentRevision)
	}

	records := readAll(t, log, 1)
	if len(records) != 3 {
		t.Errorf("Expected 3 records after bootstrap, got %d", len(records))
	}

	// New appends continue the revision numbering
	appendRecords(t, log, 3, 1)
}

// logWithoutBatchAppend hides the optional BatchAppender/Truncater interfaces,
// to exercise the sequential-append fallback path.
type logWithoutBatchAppend struct {
	persistence.Log
}

func TestTieredLog_SequentialAppendFallback(t *testing.T) {
	ctx := t.Context()

	fast := memorylog.New()
	archive := memorylog.New()

	log := makeTieredLog(t, fast, &logWithoutBatchAppend{Log: archive}, Options{FlushInterval: time.Hour})

	appendRecords(t, log, 0, 4)

	if err := log.Flush(ctx); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	archiveRevision, err := archive.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("Failed to get archive revision: %v", err)
	}
	if archiveRevision != 4 {
		t.Errorf("Expected archive revision 4, got %d", archiveRevision)
	}

	records := readAll(t, archive, 1)
	for revision := Revision(1); revision <= 4; revision++ {
		record := records[revision]
		if record == nil {
			t.Fatalf("Missing record for revision %d in archive", revision)
		}
		expectedKey := fmt.Sprintf("key%d", revision)
		if string(record.Events[0].Kv.Key) != expectedKey {
			t.Errorf("Expected key %q at revision %d, got %q", expectedKey, revision, record.Events[0].Kv.Key)
		}
	}
}
