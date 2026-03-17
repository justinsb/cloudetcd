// Copyright 2026 Google LLC
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
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/logtests"
)

func TestGCSLog(t *testing.T) {
	// Skip test if GCS credentials are not available
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" && os.Getenv("GCLOUD_PROJECT") == "" {
		t.Skip("Skipping GCS test: no GCS credentials found")
	}

	bucketName := os.Getenv("TEST_GCS_BUCKET")
	if bucketName == "" {
		t.Skip("Skipping GCS test: TEST_GCS_BUCKET environment variable not set")
	}

	ctx := context.Background()
	prefix := "test-log-"

	// Create GCS log
	logFactory := func(t *testing.T) persistence.Log {
		log, err := NewGCSLog(ctx, bucketName, prefix)
		if err != nil {
			t.Fatalf("Failed to create GCS log: %v", err)
		}
		return log
	}

	logtests.RunAll(t, logFactory)
}

func TestGCSLogObjectNameConversion(t *testing.T) {
	// Create a mock GCS log (we don't need actual GCS for this test)
	log := &GCSLog{
		prefix: "test-prefix-",
	}

	// Test revision to object name conversion
	revision := Revision(12345)
	objectName := log.revisionToObjectName(revision)
	assert.Contains(t, objectName, "test-prefix-")
	assert.Contains(t, objectName, ".log")

	// Test object name to revision conversion
	convertedRevision, err := log.objectNameToRevision(objectName)
	require.NoError(t, err)
	assert.Equal(t, revision, convertedRevision)

	// Test invalid object name
	_, err = log.objectNameToRevision("invalid-name")
	assert.Error(t, err)

	// Test object name without prefix
	_, err = log.objectNameToRevision("wrong-prefix-0000000000003039.log")
	assert.Error(t, err)

	// Test object name without .log suffix
	_, err = log.objectNameToRevision("test-prefix-0000000000003039.txt")
	assert.Error(t, err)
}
