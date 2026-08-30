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

package gcsarchive

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/justinsb/objectstorage/pkg/gcs"
	"github.com/justinsb/objectstorage/pkg/store"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/filesystemlog"
)

// startEmulator runs github.com/justinsb/objectstorage in-process and points
// the GCS client at it via STORAGE_EMULATOR_HOST, so the test is hermetic and
// needs no GCS credentials. It creates the given bucket before returning.
func startEmulator(t *testing.T, bucketName string) {
	t.Helper()

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("opening object store: %v", err)
	}
	server := httptest.NewServer(gcs.NewServer(st))
	t.Cleanup(func() {
		server.Close()
		st.Close()
	})

	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(server.URL, "http://"))

	ctx := t.Context()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("creating storage client: %v", err)
	}
	defer client.Close()

	if err := client.Bucket(bucketName).Create(ctx, "test-project", nil); err != nil {
		t.Fatalf("creating bucket %q: %v", bucketName, err)
	}
}

func writeTemp(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestArchive covers List/Upload/Download, that re-uploading an identical
// file is fine (an upload interrupted by a crash is retried after restart),
// and that a different file under the same name is a conflict.
func TestArchive(t *testing.T) {
	startEmulator(t, "cloudetcd-test")
	ctx := t.Context()
	a, err := New(ctx, "cloudetcd-test", "logs")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	dir := t.TempDir()
	one := writeTemp(t, dir, "0000000000000001.log", "one")
	two := writeTemp(t, dir, "0000000000000002.log", "two")
	for _, p := range []string{one, two} {
		if err := a.Upload(ctx, filepath.Base(p), p); err != nil {
			t.Fatalf("upload %s: %v", p, err)
		}
	}
	names, err := a.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(names) != "[0000000000000001.log 0000000000000002.log]" {
		t.Fatalf("list = %v", names)
	}

	// Identical re-upload: success. Different content: conflict.
	if err := a.Upload(ctx, "0000000000000001.log", one); err != nil {
		t.Fatalf("re-upload of an identical file: %v", err)
	}
	other := writeTemp(t, dir, "other.log", "ONE")
	if err := a.Upload(ctx, "0000000000000001.log", other); !errors.Is(err, persistence.ErrRevisionConflict) {
		t.Fatalf("upload of a different file under an existing name: got %v, want ErrRevisionConflict", err)
	}

	if err := a.Delete(ctx, "0000000000000002.log"); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(ctx, "0000000000000002.log"); err != nil {
		t.Fatalf("deleting a missing object should succeed: %v", err)
	}
	if names, _ := a.List(ctx); fmt.Sprint(names) != "[0000000000000001.log]" {
		t.Fatalf("after delete: %v", names)
	}
	if err := a.Upload(ctx, "0000000000000002.log", two); err != nil {
		t.Fatal(err)
	}

	got := filepath.Join(dir, "restored.log")
	if err := a.Download(ctx, "0000000000000002.log", got); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(got); string(data) != "two" {
		t.Fatalf("downloaded %q, want %q", data, "two")
	}
}

func put(key string) *persistence.LogRecord {
	return &persistence.LogRecord{Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(key), Value: []byte("v")}}}}
}

// TestLogArchivesAndRestores runs a file log with the archive attached,
// forces rotations, and checks the closed files reach the archive; then
// opens a log on an empty directory with the same archive and checks it
// restores and replays everything, continuing at the right revision.
func TestLogArchivesAndRestores(t *testing.T) {
	startEmulator(t, "cloudetcd-test")
	ctx := t.Context()
	a, err := New(ctx, "cloudetcd-test", "logs")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	dir1 := t.TempDir()
	opts := filesystemlog.Options{Archive: a, RotateBytes: 64} // a few (~16-byte) records per file
	log1, err := filesystemlog.NewFilesystemLogWithOptions(dir1, opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 20
	for i := 0; i < n; i++ {
		meta := persistence.NewTxnMeta(0)
		meta.AddWrite(fmt.Sprintf("k%d", i))
		if _, ok, err := log1.Append(ctx, put(fmt.Sprintf("k%d", i)), meta); err != nil || !ok {
			t.Fatalf("append %d: ok=%v err=%v", i, ok, err)
		}
	}
	if err := log1.Close(); err != nil { // rotates the active file and waits for uploads
		t.Fatal(err)
	}
	names, err := a.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) < 3 {
		t.Fatalf("expected several archived files after %d records with RotateBytes=64, got %v", n, names)
	}

	// A fresh machine: empty directory, same archive.
	dir2 := t.TempDir()
	log2, err := filesystemlog.NewFilesystemLogWithOptions(dir2, opts)
	if err != nil {
		t.Fatalf("opening a log on an empty directory with an archive: %v", err)
	}
	defer log2.Close()
	rev, err := log2.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rev != n {
		t.Fatalf("restored log is at revision %d, want %d", rev, n)
	}
	var seen int
	if err := log2.Read(ctx, 1, func(r persistence.Revision, rec *persistence.LogRecord) bool {
		if want := fmt.Sprintf("k%d", r-1); string(rec.Events[0].Kv.Key) != want {
			t.Fatalf("revision %d: key %s, want %s", r, rec.Events[0].Kv.Key, want)
		}
		seen++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if seen != n {
		t.Fatalf("replayed %d records, want %d", seen, n)
	}
	if rec, err := log2.GetLogEntry(7); err != nil || string(rec.Events[0].Kv.Key) != "k6" {
		t.Fatalf("GetLogEntry(7) = %v, %v", rec, err)
	}
	// Writing continues at n+1.
	meta := persistence.NewTxnMeta(0)
	meta.AddWrite("more")
	if rev, ok, err := log2.Append(ctx, put("more"), meta); err != nil || !ok || rev != n+1 {
		t.Fatalf("append after restore: rev=%d ok=%v err=%v", rev, ok, err)
	}

	// A third instance pointed at the same archive from another directory
	// that has its own files is refused: the archive has files it lacks.
	dir3 := t.TempDir()
	log3, err := filesystemlog.NewFilesystemLogWithOptions(dir3, filesystemlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	meta = persistence.NewTxnMeta(0)
	meta.AddWrite("x")
	if _, _, err := log3.Append(ctx, put("x"), meta); err != nil {
		t.Fatal(err)
	}
	log3.Close()
	if _, err := filesystemlog.NewFilesystemLogWithOptions(dir3, opts); err == nil {
		t.Fatal("a log with its own files opened against an archive holding other files")
	}
	_ = time.Second
}

// TestCompactionReplacesArchivedFiles checks that compacting a log with an
// archive uploads the compacted file and removes the originals from the
// archive, and that a fresh directory restores from the compacted archive.
func TestCompactionReplacesArchivedFiles(t *testing.T) {
	startEmulator(t, "cloudetcd-test")
	ctx := t.Context()
	a, err := New(ctx, "cloudetcd-test", "logs")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	opts := filesystemlog.Options{Archive: a, RotateBytes: 300, CacheBytes: 1}
	log1, err := filesystemlog.NewFilesystemLogWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	latest := map[string]persistence.Revision{}
	for round := 0; round < 10; round++ {
		for k := 0; k < 5; k++ {
			key := fmt.Sprintf("k%d", k)
			snapshot, _ := log1.GetCurrentRevision(ctx)
			meta := persistence.NewTxnMeta(snapshot)
			meta.AddWrite(key)
			rev, ok, err := log1.Append(ctx, put(key), meta)
			if err != nil || !ok {
				t.Fatalf("append %s round %d: ok=%v err=%v", key, round, ok, err)
			}
			latest[key] = rev
		}
	}
	head, _ := log1.GetCurrentRevision(ctx)
	// Rotate so everything is closed and archived before compacting.
	if err := log1.Close(); err != nil {
		t.Fatal(err)
	}
	dir := log1.Dir()
	log1, err = filesystemlog.NewFilesystemLogWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := a.List(ctx)
	if err := log1.Compact(ctx, head, func(key []byte, rev persistence.Revision) bool { return latest[string(key)] == rev }); err != nil {
		t.Fatal(err)
	}
	if err := log1.Close(); err != nil { // waits for archive operations
		t.Fatal(err)
	}
	after, _ := a.List(ctx)
	if len(after) >= len(before) {
		t.Fatalf("archive has %d files after compaction (was %d); expected the originals replaced", len(after), len(before))
	}
	var sparse int
	for _, n := range after {
		if strings.Contains(n, "-") {
			sparse++
		}
	}
	if sparse == 0 {
		t.Fatalf("no compacted file in the archive: %v", after)
	}

	// A fresh directory restores the compacted archive and has the live state.
	log2, err := filesystemlog.NewFilesystemLogWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("restoring a compacted archive: %v", err)
	}
	defer log2.Close()
	if rev, _ := log2.GetCurrentRevision(ctx); rev != head {
		t.Fatalf("restored log at revision %d, want %d", rev, head)
	}
	for key, rev := range latest {
		rec, err := log2.GetLogEntry(rev)
		if err != nil || string(rec.Events[0].Kv.Key) != key {
			t.Fatalf("GetLogEntry(%d) for %s after restore: %v, %v", rev, key, rec, err)
		}
	}
}
