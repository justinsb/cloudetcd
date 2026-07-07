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
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/logtests"
)

func putRecord(key, value string) *persistence.LogRecord {
	return &persistence.LogRecord{
		Events: []*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte(key),
					Value: []byte(value),
				},
			},
		},
	}
}

// TestGCSLogFencing verifies that log object creation is the commit point:
// when two writers share a bucket/prefix, the second writer to claim a
// revision must fail with ErrRevisionConflict rather than silently
// overwriting the first writer's committed batch.
func TestGCSLogFencing(t *testing.T) {
	bucketName := "cloudetcd-test"
	startEmulator(t, bucketName)

	ctx := t.Context()
	prefix := "fencing/"

	// Two instances sharing the same bucket/prefix, both starting at revision 0.
	log1, err := NewGCSLog(ctx, bucketName, prefix)
	if err != nil {
		t.Fatalf("creating log1: %v", err)
	}
	defer log1.Close()

	log2, err := NewGCSLog(ctx, bucketName, prefix)
	if err != nil {
		t.Fatalf("creating log2: %v", err)
	}
	defer log2.Close()

	// log2 wins the race for revision 1.
	winnerMeta := logtests.NewTxnMeta(0)
	winnerMeta.AddWrite("contested")
	rev, ok, err := log2.Append(ctx, putRecord("contested", "winner"), winnerMeta)
	if err != nil || !ok {
		t.Fatalf("winner append failed: ok=%v, err=%v", ok, err)
	}
	if rev != 1 {
		t.Fatalf("winner append: got revision %d, want 1", rev)
	}

	// log1 doesn't know, and tries to claim revision 1 as well.
	loserMeta := logtests.NewTxnMeta(0)
	loserMeta.AddWrite("contested")
	_, ok, err = log1.Append(ctx, putRecord("contested", "loser"), loserMeta)
	if ok {
		t.Fatalf("loser append reported success; it should have lost the race")
	}
	if !errors.Is(err, persistence.ErrRevisionConflict) {
		t.Fatalf("loser append: got error %v, want persistence.ErrRevisionConflict", err)
	}

	// The loser's local state must not have advanced.
	currentRev, err := log1.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatalf("GetCurrentRevision: %v", err)
	}
	if currentRev != 0 {
		t.Errorf("loser's current revision advanced to %d, want 0", currentRev)
	}

	// The winner's committed batch must be intact: a fresh replay of the log
	// sees the winner's record at revision 1.
	log3, err := NewGCSLog(ctx, bucketName, prefix)
	if err != nil {
		t.Fatalf("creating log3: %v", err)
	}
	defer log3.Close()

	record, err := log3.GetLogEntry(1)
	if err != nil {
		t.Fatalf("reading revision 1 after replay: %v", err)
	}
	if len(record.Events) != 1 || string(record.Events[0].Kv.Value) != "winner" {
		t.Errorf("revision 1 after replay: got %+v, want the winner's record", record)
	}
}
