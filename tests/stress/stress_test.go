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
// cloudetcd as both a benchmark and a correctness test.
//
// It drives the etcd traffic of a Kubernetes cluster (only the request
// shapes kube-apiserver makes) through every phase of pkg/workload, with
// every response checked against the workload's own record of what it wrote
// (see workload.Checker): lost or phantom writes, stale reads, wrong
// versions, spurious conflicts, missing or misordered watch events, bad
// prev_kv, lease expiry deleting a re-attached key, and state that does not
// survive a restart all fail the test. The headline number is the
// throughput of the saturate phase, a closed loop of kube operations with
// deliberate same-key contention.
//
// It is gated on RUN_STRESS so that the default test run stays fast. The
// cluster size, durations and seed come from the environment:
//
//	RUN_STRESS=1 go test ./tests/stress -v
//	RUN_STRESS=1 STRESS_NODES=200 STRESS_PODS_PER_NODE=10 STRESS_SATURATE=1m go test ./tests/stress -v
//
// Every run prints its seed and the command to re-run it. A failing run is
// re-run in diagnostic mode (STRESS_DIAG=1), which stops at the first
// violation and records every RPC the server handled to a JSONL file for
// building a repro (see docs/stress-testing.md).
package stress

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/klog/v2"

	"justinsb.com/cloudetcd/pkg/api"
	"justinsb.com/cloudetcd/pkg/recording"
	"justinsb.com/cloudetcd/pkg/workload"
	"justinsb.com/cloudetcd/pkg/workload/inprocess"
	"justinsb.com/cloudetcd/pkg/workload/kubeblobs"
)

// params is everything that shapes a run, all settable from the environment
// so that a run can be reproduced from the line the test prints.
type params struct {
	seed         uint64
	nodes        int
	podsPerNode  int
	workers      int
	steady       time.Duration
	saturate     time.Duration
	speedup      int
	listClients  int
	logURI       string
	blobsDir     string
	diag         bool
	artifactsDir string
}

func (p params) repro() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "RUN_STRESS=1 STRESS_SEED=%d STRESS_NODES=%d STRESS_PODS_PER_NODE=%d STRESS_WORKERS=%d STRESS_DURATION=%s STRESS_SATURATE=%s STRESS_SPEEDUP=%d",
		p.seed, p.nodes, p.podsPerNode, p.workers, p.steady, p.saturate, p.speedup)
	if p.blobsDir != "" {
		fmt.Fprintf(&sb, " STRESS_BLOBS=%s", p.blobsDir)
	}
	sb.WriteString(" STRESS_DIAG=1 go test ./tests/stress -run TestStress -v")
	return sb.String()
}

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

func loadParams(t *testing.T) params {
	p := params{
		nodes:       envInt(t, "STRESS_NODES", 50),
		podsPerNode: envInt(t, "STRESS_PODS_PER_NODE", 5),
		workers:     envInt(t, "STRESS_WORKERS", 64),
		steady:      envDuration(t, "STRESS_DURATION", 15*time.Second),
		saturate:    envDuration(t, "STRESS_SATURATE", 30*time.Second),
		speedup:     envInt(t, "STRESS_SPEEDUP", 10),
		listClients: envInt(t, "STRESS_LIST_CONCURRENCY", 4),
		logURI:      os.Getenv("STRESS_LOG"),
		blobsDir:    os.Getenv("STRESS_BLOBS"),
		diag:        os.Getenv("STRESS_DIAG") != "",
	}
	if v := os.Getenv("STRESS_SEED"); v != "" {
		seed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("STRESS_SEED=%q: %v", v, err)
		}
		p.seed = seed
	} else {
		p.seed = uint64(time.Now().UnixNano())
	}
	if p.logURI == "" {
		// A real directory, so that the run ends with a restart from the log.
		p.logURI = "filesystem://" + filepath.Join(t.TempDir(), "log")
	}
	p.artifactsDir = os.Getenv("ARTIFACTS")
	if p.artifactsDir == "" {
		p.artifactsDir = filepath.Join(moduleRoot(t), ".build")
	}
	p.artifactsDir = filepath.Join(p.artifactsDir, "stress", fmt.Sprintf("seed-%d", p.seed))
	return p
}

// stressConfig is the cluster model with its cadences compressed by speedup,
// so that a small cluster produces the traffic of a larger one and the
// infrequent paths (node status, compaction, lease expiry) all happen within
// a short run; and with the paths kube-apiserver depends on stressed harder
// than a real cluster would:
//
//   - the masterlease is refreshed under a new lease every second with a 2s
//     TTL, so old leases expire constantly while the key is re-attached;
//   - Events get a short TTL and lease reuse, so Events expire mid-run and
//     the server's lease-expiry deletes are checked;
//   - compaction runs every few seconds against the live traffic;
//   - conditional updates retry as many times as the apiserver's
//     GuaranteedUpdate does, since the saturate phase contends keys.
func stressConfig(t *testing.T, p params) workload.Config {
	cfg := workload.DefaultConfig()
	cfg.Nodes = p.nodes
	cfg.PodsPerNode = p.podsPerNode
	speedup := time.Duration(max(p.speedup, 1))
	cfg.NodeLeaseInterval = max(cfg.NodeLeaseInterval/speedup, 100*time.Millisecond)
	cfg.NodeStatusInterval = max(cfg.NodeStatusInterval/speedup, time.Second)
	cfg.PodChurnPerNodePerHour *= float64(speedup) * 10 // a busy cluster: ~20 lifecycles/node/hour before speedup
	cfg.MasterLeaseInterval = time.Second
	cfg.MasterLeaseTTL = 2 * time.Second
	cfg.EventTTL = 10 * time.Second
	cfg.EventLeaseReuse = 5 * time.Second
	cfg.CountInterval = max(cfg.CountInterval/speedup, time.Second)
	cfg.HealthCheckInterval = max(cfg.HealthCheckInterval/speedup, time.Second)
	cfg.CompactInterval = 5 * time.Second
	cfg.ConsistentReadsPerSecond *= float64(speedup)
	cfg.ProgressRequestsPerSecond = 2
	cfg.UpdateRetries = 64

	// Values are generated Kubernetes objects encoded as the apiserver stores
	// them; STRESS_BLOBS points at a directory of captured values instead.
	if p.blobsDir != "" {
		b, err := workload.LoadBlobs(p.blobsDir)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Blobs = b
	} else {
		b, err := kubeblobs.Generate()
		if err != nil {
			t.Fatal(err)
		}
		cfg.Blobs = b
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestStress runs the whole model against an in-process cloudetcd and fails
// on any operation error, any correctness violation, or a server that
// cannot keep up with the steady-state schedule. It prints a HEADLINE line
// with the saturate phase's throughput.
func TestStress(t *testing.T) {
	if os.Getenv("RUN_STRESS") == "" {
		t.Skip("Skipping stress test; set RUN_STRESS=1 to run")
	}
	p := loadParams(t)
	t.Logf("seed %d; re-run with: %s", p.seed, p.repro())
	cfg := stressConfig(t, p)
	if err := os.MkdirAll(p.artifactsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// The server's own logging goes to a file next to the summary: at this
	// load it is voluminous, and the test's output should be the results.
	serverLog := filepath.Join(p.artifactsDir, "server.log")
	redirectKlog(t, serverLog)
	t.Logf("server log: %s", serverLog)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var serverOpts []api.Option
	if p.diag {
		path := filepath.Join(p.artifactsDir, "recording.jsonl")
		rec, err := recording.NewFileRecorder(path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := rec.Close(); err != nil {
				t.Errorf("closing recording: %v", err)
			}
			t.Logf("diagnostic mode: %d RPCs recorded to %s", rec.Entries(), path)
		}()
		serverOpts = append(serverOpts, api.WithRecorder(rec))
		t.Logf("diagnostic mode: recording every RPC to %s; the run stops at the first violation", path)
	}

	srv, err := inprocess.Start(ctx, p.logURI, serverOpts...)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			srv.Stop()
		}
	}
	defer stop()

	client, err := srv.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	runner, err := workload.NewRunner(cfg, client, p.workers)
	if err != nil {
		t.Fatal(err)
	}
	runner.WatcherStatus = srv.Store.Watchers
	runner.LogBytes = func() int64 { return dirBytes(srv.LogDir) }
	check := runner.Checker()

	// The first violation is logged as soon as it happens, so that it is next
	// to the server's own logging of that moment; in diagnostic mode it also
	// ends the run, so the recording stops there.
	var once sync.Once
	check.OnViolation(func(v workload.Violation) {
		once.Do(func() {
			t.Logf("FIRST VIOLATION at %s: %v", v.At.Format(time.RFC3339Nano), v)
			if p.diag {
				cancel()
			}
		})
	})

	// A violation makes the run worthless as a benchmark but every phase's
	// report is still logged, and the checks that remain (state, restart)
	// still run: they often explain the first violation.
	var reports []*workload.Report
	phase := func(name string, run func() (*workload.Report, error)) bool {
		t.Helper()
		if ctx.Err() != nil {
			t.Logf("skipping %s: run was stopped", name)
			return false
		}
		t.Logf("phase %s...", name)
		rep, err := run()
		if rep != nil {
			reports = append(reports, rep)
			t.Log("\n" + rep.Text())
		}
		if err != nil {
			if ctx.Err() != nil && p.diag {
				return false // stopped at the first violation
			}
			t.Errorf("%s: %v", name, err)
			return false
		}
		if rep.Errors > 0 {
			t.Errorf("%s: %d operations failed", name, rep.Errors)
		}
		if lag := rep.SchedulingLag; lag != nil && lag.P99 > p.steady/2 {
			t.Errorf("%s: scheduling lag p99 %s: the server is not keeping up with the model", name, lag.P99)
		}
		return true
	}

	phase("populate", func() (*workload.Report, error) { return runner.Populate(ctx) })
	phase("steady", func() (*workload.Report, error) { return runner.Steady(ctx, p.steady) })
	var headline *workload.Report
	phase("saturate", func() (*workload.Report, error) {
		rep, err := runner.Saturate(ctx, workload.SaturateOptions{Duration: p.saturate, Seed: p.seed})
		headline = rep
		return rep, err
	})
	phase("list-storm", func() (*workload.Report, error) { return runner.ListStorm(ctx, p.listClients, 1) })

	// With nothing in flight, the server's state must be exactly what was
	// written.
	verify := func(what string) {
		t.Helper()
		if ctx.Err() != nil {
			return
		}
		t.Logf("verifying state %s...", what)
		n, err := check.VerifyState(ctx, client, cfg.Prefix, cfg.ListPageSize)
		if err != nil {
			t.Errorf("verifying state %s: %v", what, err)
			return
		}
		live, expiring := check.LiveKeys()
		t.Logf("state %s: %d keys match the oracle (%d live, of which %d on leases that may have expired)", what, n, live, expiring)
	}
	verify("after the run")
	finalRev := check.Floor()

	// Restart: a new server on the same log must come back with the same
	// state, and serve the run's history to a watch exactly as it happened.
	if ctx.Err() == nil && strings.HasPrefix(p.logURI, "filesystem://") {
		t.Logf("restarting the server from %s...", p.logURI)
		client.Close()
		stop()
		srv, err = inprocess.Start(ctx, p.logURI, serverOpts...)
		if err != nil {
			t.Fatalf("restarting server: %v", err)
		}
		stopped = false
		client, err = srv.NewClient()
		if err != nil {
			t.Fatal(err)
		}
		verify("after restart")
		// The history since the last compaction the workload asked for must
		// be readable, since the server's compaction point is at or below it.
		from := runner.LastCompaction()
		for _, res := range []string{"minions", "leases", "pods"} {
			startedFrom, events, err := check.VerifyHistory(ctx, client, "replay/"+res, cfg.Prefix+"/"+res+"/", from, finalRev)
			if err != nil {
				t.Errorf("replaying %s history after restart from revision %d: %v", res, from+1, err)
				continue
			}
			t.Logf("replayed %s history after restart: %d events from revision %d to %d match the oracle", res, events, startedFrom+1, finalRev)
		}
		// A watch from before the compaction point is what a watch cache
		// reconnecting with an old resourceVersion does; it must either be
		// served or be cancelled as compacted at once, never left hanging.
		if from > 0 {
			startedFrom, events, err := check.VerifyHistory(ctx, client, "replay-compacted/minions", cfg.Prefix+"/minions/", 0, finalRev)
			switch {
			case err != nil:
				t.Errorf("watch from revision 1 after restart (compaction was requested through %d): %v; a compacted watch must be cancelled with CompactRevision", from, err)
			case startedFrom > 0:
				t.Logf("watch from revision 1 after restart was cancelled as compacted at %d, then replayed %d events to %d", startedFrom, events, finalRev)
			default:
				t.Logf("watch from revision 1 after restart replayed the whole history (%d events)", events)
			}
		}
	}

	// Results.
	if headline != nil {
		t.Logf("HEADLINE %s", headline.Headline())
	}
	violations := check.Violations()
	if n := check.ViolationCount(); n > 0 {
		t.Errorf("%d correctness violations (seed %d); re-run with: %s", n, p.seed, p.repro())
		shown := map[string]bool{}
		for i, v := range violations {
			if i >= 20 {
				t.Logf("... %d more violations", len(violations)-i)
				break
			}
			t.Errorf("violation %d: %v", i+1, v)
			if v.Key != "" && !shown[v.Key] {
				shown[v.Key] = true
				t.Log(check.Dump(v.Key))
			}
		}
		if p.diag {
			t.Logf("server watchers at the end: %+v", srv.Store.Watchers())
		}
	}
	if err := writeSummary(p, reports, violations); err != nil {
		t.Logf("writing summary: %v", err)
	}
}

// moduleRoot is the directory holding go.mod, which the test runs two levels
// below.
func moduleRoot(t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod above %s", dir)
		}
		dir = parent
	}
}

// redirectKlog sends klog (the server's logging) to path for the rest of the
// test binary's life.
func redirectKlog(t *testing.T, path string) {
	t.Helper()
	fs := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(fs)
	// stderrthreshold: klog copies ERROR and above to stderr even when it is
	// writing to a file; FATAL keeps everything in the file.
	for name, value := range map[string]string{"logtostderr": "false", "alsologtostderr": "false", "stderrthreshold": "FATAL", "log_file": path, "log_file_max_size": "0"} {
		if err := fs.Set(name, value); err != nil {
			t.Fatalf("setting klog flag %s: %v", name, err)
		}
	}
	t.Cleanup(klog.Flush)
}

// writeSummary saves the reports and violations as JSON under the artifacts
// directory, so that CI keeps the numbers.
func writeSummary(p params, reports []*workload.Report, violations []workload.Violation) error {
	if len(reports) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("{\n")
	fmt.Fprintf(&sb, "  \"seed\": %d,\n  \"repro\": %q,\n", p.seed, p.repro())
	sb.WriteString("  \"violations\": [")
	for i, v := range violations {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "\n    %q", v.Error())
	}
	if len(violations) > 0 {
		sb.WriteString("\n  ")
	}
	sb.WriteString("],\n  \"reports\": ")
	b, err := json.MarshalIndent(reports, "  ", "  ")
	if err != nil {
		return err
	}
	sb.Write(b)
	sb.WriteString("\n}\n")
	return os.WriteFile(filepath.Join(p.artifactsDir, "summary.json"), []byte(sb.String()), 0o644)
}

// dirBytes is the total size of the files in dir.
func dirBytes(dir string) int64 {
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if info, err := e.Info(); err == nil && !e.IsDir() {
			total += info.Size()
		}
	}
	return total
}
