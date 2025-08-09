package gcslog

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"justinsb.com/cloudetcd/pkg/persistence"
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
	log, err := NewGCSLog(ctx, bucketName, prefix)
	require.NoError(t, err)
	defer log.Close()

	// Test initial state
	revision, err := log.GetCurrentRevision(ctx)
	require.NoError(t, err)
	assert.Equal(t, Revision(0), revision)

	// Test appending a record
	record := &persistence.LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
			},
		},
	}

	newRevision, success, err := log.Append(ctx, 0, record)
	require.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, Revision(1), newRevision)

	// Test getting current revision
	currentRevision, err := log.GetCurrentRevision(ctx)
	require.NoError(t, err)
	assert.Equal(t, Revision(1), currentRevision)

	// Test getting log entry
	retrievedRecord, err := log.GetLogEntry(1)
	require.NoError(t, err)
	require.NotNil(t, retrievedRecord)
	assert.Equal(t, len(record.Events), len(retrievedRecord.Events))
	assert.Equal(t, record.Events[0].Type, retrievedRecord.Events[0].Type)
	assert.Equal(t, record.Events[0].Kv.Key, retrievedRecord.Events[0].Kv.Key)
	assert.Equal(t, record.Events[0].Kv.Value, retrievedRecord.Events[0].Kv.Value)

	// Test conditional append with wrong condition
	_, success, err = log.Append(ctx, 0, record) // Wrong condition position
	require.NoError(t, err)
	assert.False(t, success)

	// Test conditional append with correct condition
	newRevision2, success, err := log.Append(ctx, 1, record)
	require.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, Revision(2), newRevision2)

	// Test reading from log
	var readRecords []Revision
	err = log.Read(ctx, 1, func(revision Revision, record *persistence.LogRecord) bool {
		readRecords = append(readRecords, revision)
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, []Revision{1, 2}, readRecords)

	// Test reading with callback that returns false
	readRecords = nil
	err = log.Read(ctx, 1, func(revision Revision, record *persistence.LogRecord) bool {
		readRecords = append(readRecords, revision)
		return false // Stop after first record
	})
	require.NoError(t, err)
	assert.Equal(t, []Revision{1}, readRecords)
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
