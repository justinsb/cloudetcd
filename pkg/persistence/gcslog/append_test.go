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
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"k8s.io/klog/v2"
)

func TestAppend(t *testing.T) {
	ctx := t.Context()

	log := klog.FromContext(ctx)

	// Test append functionality
	bucketName := os.Getenv("TEST_GCS_BUCKET")
	if bucketName == "" {
		t.Skip("Skipping test: TEST_GCS_BUCKET environment variable not set")
	}

	// Can't get this to work yet, get unimplemented error
	// append_test.go:54: failed to close GCS writer: rpc error: code = Unimplemented desc = Appendable objects are not supported for this request.

	client, err := storage.NewGRPCClient(ctx, experimental.WithZonalBucketAPIs())
	if err != nil {
		t.Fatalf("failed to create GCS client: %v", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)

	prefix := "append/"
	objectName := prefix + "test.log"

	obj := bucket.Object(objectName)

	{
		b1 := []byte("test1\n")

		// Write to GCS
		log.Info("Writing log entry to GCS object", "objectName", objectName)
		writer := obj.NewWriter(ctx)
		writer.ContentType = "application/json"

		writer.Append = true

		if _, err := writer.Write(b1); err != nil {
			writer.Close()
			t.Fatalf("failed to write log record to GCS: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close GCS writer: %v", err)
		}
	}

	{
		b2 := []byte("test2\n")
		// Write to GCS
		log.Info("Writing log entry to GCS object", "objectName", objectName)
		writer := obj.NewWriter(ctx)
		writer.ContentType = "application/json"

		writer.Append = true

		if _, err := writer.Write(b2); err != nil {
			writer.Close()
			t.Fatalf("failed to write log record to GCS: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close GCS writer: %v", err)
		}
	}
}
