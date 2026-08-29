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

// Package stress runs the pkg/workload cluster model against an in-process
// cloudetcd. It is gated on RUN_STRESS so that the default test run stays
// fast; the cluster size and duration come from the environment:
//
//	RUN_STRESS=1 STRESS_NODES=1000 STRESS_PODS_PER_NODE=30 STRESS_DURATION=1m go test ./tests/stress -v
//
// For interactive runs with more control, use cmd/cloud-etcd-bench.
package stress

import (
	"os"
	"strconv"
	"testing"
	"time"

	"justinsb.com/cloudetcd/pkg/workload"
	"justinsb.com/cloudetcd/pkg/workload/inprocess"
)

func envInt(t *testing.T, name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return n
}

func envDuration(t *testing.T, name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return d
}

// TestClusterWorkload brings up a cluster of STRESS_NODES nodes (populate),
// runs the steady-state traffic for STRESS_DURATION, then a list storm. It
// fails if any operation errored, or if the generator fell so far behind the
// model's schedule that the run no longer represents the requested cluster.
func TestClusterWorkload(t *testing.T) {
	if os.Getenv("RUN_STRESS") == "" {
		t.Skip("Skipping stress test; set RUN_STRESS=1 to run")
	}

	cfg := workload.DefaultConfig()
	cfg.Nodes = envInt(t, "STRESS_NODES", 50)
	cfg.PodsPerNode = envInt(t, "STRESS_PODS_PER_NODE", 5)
	duration := envDuration(t, "STRESS_DURATION", 15*time.Second)
	logURI := os.Getenv("STRESS_LOG")
	if logURI == "" {
		logURI = "memory://"
	}
	if dir := os.Getenv("STRESS_BLOBS"); dir != "" {
		b, err := workload.LoadBlobs(dir)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Blobs = b
	}

	ctx := t.Context()
	srv, err := inprocess.Start(ctx, logURI)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	client, err := srv.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	runner, err := workload.NewRunner(cfg, client, envInt(t, "STRESS_WORKERS", 64))
	if err != nil {
		t.Fatal(err)
	}

	check := func(rep *workload.Report, err error) {
		t.Helper()
		if rep != nil {
			t.Log("\n" + rep.Text())
		}
		if err != nil {
			t.Fatalf("%s: %v", rep.Phase, err)
		}
		if rep.Errors > 0 {
			t.Errorf("%s: %d operations failed", rep.Phase, rep.Errors)
		}
		if lag := rep.SchedulingLag; lag != nil && lag.P99 > duration/2 {
			t.Errorf("%s: scheduling lag p99 %s: the server is not keeping up with the model", rep.Phase, lag.P99)
		}
	}

	check(runner.Populate(ctx))
	check(runner.Steady(ctx, duration))
	check(runner.ListStorm(ctx, envInt(t, "STRESS_LIST_CONCURRENCY", 4), 1))
}
