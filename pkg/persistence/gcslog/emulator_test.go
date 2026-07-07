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

package gcslog

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/justinsb/objectstorage/pkg/gcs"
	"github.com/justinsb/objectstorage/pkg/store"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/logtests"
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

// TestGCSLogWithEmulator runs the full log test suite against GCSLog backed
// by the objectstorage emulator, with no cloud credentials required.
func TestGCSLogWithEmulator(t *testing.T) {
	bucketName := "cloudetcd-test"
	startEmulator(t, bucketName)

	ctx := t.Context()

	logFactory := func(t *testing.T) persistence.Log {
		prefix := fmt.Sprintf("test-log-%d/", time.Now().UnixNano())

		log, err := NewGCSLog(ctx, bucketName, prefix)
		if err != nil {
			t.Fatalf("Failed to create GCS log: %v", err)
		}
		return log
	}

	logtests.RunAll(t, logFactory)
}
