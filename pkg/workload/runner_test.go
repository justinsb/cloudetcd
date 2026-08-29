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

package workload_test

import (
	"testing"
	"time"

	"justinsb.com/cloudetcd/pkg/workload"
	"justinsb.com/cloudetcd/pkg/workload/inprocess"
)

// TestRunnerSmoke runs every phase of a tiny cluster against an in-process
// cloudetcd with all cadences shortened, and checks that every kind of
// operation the model generates succeeds and that watches see the writes.
func TestRunnerSmoke(t *testing.T) {
	ctx := t.Context()
	srv, err := inprocess.Start(ctx, "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	client, err := srv.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	cfg := workload.DefaultConfig()
	cfg.Nodes = 4
	cfg.PodsPerNode = 3
	cfg.Namespaces = 2
	cfg.NodeLeaseInterval = 500 * time.Millisecond
	cfg.NodeStatusInterval = time.Second
	cfg.PodChurnPerNodePerHour = 3600 * 2 // 2 per node per second
	cfg.MasterLeaseInterval = 500 * time.Millisecond
	cfg.CountInterval = time.Second
	cfg.CompactInterval = 700 * time.Millisecond
	cfg.HealthCheckInterval = time.Second
	cfg.ConsistentReadsPerSecond = 4

	runner, err := workload.NewRunner(cfg, client, 8)
	if err != nil {
		t.Fatal(err)
	}

	rep, err := runner.Populate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + rep.Text())
	if rep.Errors != 0 {
		t.Fatalf("populate: %d errors", rep.Errors)
	}
	wantCreates := map[string]int64{
		workload.OpNodeCreate:      4,
		workload.OpNodeLeaseCreate: 4,
		workload.OpPodCreate:       12,
		workload.OpPodUpdate:       24,
		workload.OpEventCreate:     12 * 3, // all but the "Killing" event
	}
	for op, want := range wantCreates {
		if got := rep.Ops[op].Count; got != want {
			t.Errorf("populate %s count = %d, want %d", op, got, want)
		}
	}

	rep, err = runner.Steady(ctx, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + rep.Text())
	if rep.Errors != 0 {
		t.Fatalf("steady: %d errors", rep.Errors)
	}
	for _, op := range []string{
		workload.OpNodeLeaseUpdate, workload.OpNodeStatusUpdate, workload.OpPodCreate, workload.OpPodUpdate,
		workload.OpPodDelete, workload.OpEventCreate, workload.OpMasterLease, workload.OpPeerServerLease,
		workload.OpAPIServerLease, workload.OpLeaseGrant, workload.OpCount, workload.OpHealthCheck,
		workload.OpCompact, workload.OpConsistentRead, workload.OpProgressRequest,
	} {
		if rep.Ops[op] == nil || rep.Ops[op].Count == 0 {
			t.Errorf("steady: no %s operations were performed", op)
		}
	}
	// Compaction ran at least twice, so a real Compact RPC was issued.
	if rep.Ops[workload.OpCompact].Count < 2 {
		t.Errorf("steady: compact ran %d times, want >= 2", rep.Ops[workload.OpCompact].Count)
	}
	for _, w := range []string{"minions", "leases", "pods", "events"} {
		ws := rep.Watches[w]
		if ws == nil || ws.Events == 0 {
			t.Errorf("steady: watch on %s saw no events", w)
			continue
		}
		if ws.Lag.Count == 0 {
			t.Errorf("steady: watch on %s measured no lag samples", w)
		}
	}
	if rep.Ops[workload.OpNodeLeaseUpdate].Count < 4*4 {
		t.Errorf("steady: only %d lease updates in 3s at 500ms for 4 nodes", rep.Ops[workload.OpNodeLeaseUpdate].Count)
	}

	rep, err = runner.ListStorm(ctx, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + rep.Text())
	if rep.Errors != 0 {
		t.Fatalf("list-storm: %d errors", rep.Errors)
	}
	if got := rep.Ops[workload.OpList].Count; got != 12 {
		t.Errorf("list-storm: %d lists, want 12", got)
	}
}
