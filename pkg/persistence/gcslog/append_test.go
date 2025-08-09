package gcslog

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"k8s.io/klog/v2"
)

func TestAppend(t *testing.T) {
	ctx := context.Background()

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
