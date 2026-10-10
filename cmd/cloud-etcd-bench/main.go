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

// cloud-etcd-bench generates the etcd traffic of a Kubernetes cluster with N
// nodes against cloudetcd, without running kube-apiserver or any nodes.
//
//	cloud-etcd-bench run -nodes 1000 -pods-per-node 30 -duration 2m
//	cloud-etcd-bench run -nodes 100 -phases populate,saturate,verify -saturate-duration 1m
//	cloud-etcd-bench run -nodes 100 -log filesystem:///var/tmp/bench -phases populate,steady,list-storm
//	cloud-etcd-bench run -endpoint 127.0.0.1:2379 -nodes 100
//	cloud-etcd-bench run -nodes 100 -node-images 50 -pod-containers 3
//	cloud-etcd-bench analyze recording.jsonl
//	cloud-etcd-bench extract-blobs recording.jsonl pkg/workload/blobs
//
// See pkg/workload for the traffic model.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"k8s.io/klog/v2"

	"justinsb.com/cloudetcd/pkg/workload"
	"justinsb.com/cloudetcd/pkg/workload/inprocess"
	"justinsb.com/cloudetcd/pkg/workload/kubeblobs"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() error {
	return fmt.Errorf("usage: cloud-etcd-bench run|analyze|extract-blobs [flags]")
}

func run(ctx context.Context, args []string) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "run":
		return runBench(ctx, args[1:])
	case "analyze":
		return runAnalyze(args[1:])
	case "extract-blobs":
		return runExtractBlobs(args[1:])
	default:
		return usage()
	}
}

func runBench(ctx context.Context, args []string) error {
	cfg := workload.DefaultConfig()
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.IntVar(&cfg.Nodes, "nodes", 100, "Number of nodes to simulate")
	fs.IntVar(&cfg.PodsPerNode, "pods-per-node", 10, "Pods per node")
	fs.IntVar(&cfg.Namespaces, "namespaces", cfg.Namespaces, "Namespaces pods are spread across")
	fs.DurationVar(&cfg.NodeLeaseInterval, "node-lease-interval", cfg.NodeLeaseInterval, "Kubelet node lease renewal interval")
	fs.DurationVar(&cfg.NodeStatusInterval, "node-status-interval", cfg.NodeStatusInterval, "Kubelet node status report interval")
	fs.Float64Var(&cfg.PodChurnPerNodePerHour, "pod-churn", cfg.PodChurnPerNodePerHour, "Pods replaced per node per hour")
	fs.IntVar(&cfg.EventsPerPodLifecycle, "events-per-pod", cfg.EventsPerPodLifecycle, "Events written per pod lifecycle")
	fs.DurationVar(&cfg.CountInterval, "count-interval", cfg.CountInterval, "Interval between apiserver object counts per resource")
	fs.DurationVar(&cfg.CompactInterval, "compact-interval", cfg.CompactInterval, "apiserver compaction interval")
	fs.Float64Var(&cfg.ConsistentReadsPerSecond, "consistent-reads", cfg.ConsistentReadsPerSecond, "Consistent reads per second")
	fs.Int64Var(&cfg.ListPageSize, "list-page-size", cfg.ListPageSize, "Page size for lists")
	fs.StringVar(&cfg.Prefix, "prefix", cfg.Prefix, "etcd key prefix")

	duration := fs.Duration("duration", time.Minute, "Duration of the steady-state phase")
	saturateDuration := fs.Duration("saturate-duration", 30*time.Second, "Duration of the saturate phase")
	seed := fs.Uint64("seed", 0, "Seed for the saturate phase's random choices (0: from the clock; the report prints it)")
	contention := fs.Float64("contention", 0, "Share of saturate-phase operations that contend a key another writer owns (0: default 0.15; negative: none)")
	verify := fs.Bool("verify", true, "Check every response against the workload's own record of what it wrote, and report violations (costs one record per write in memory)")
	phases := fs.String("phases", "populate,steady", "Comma-separated phases to run: populate, steady, saturate, list-storm, verify")
	logURI := fs.String("log", "memory://", "Log URI for the in-process server (memory:// is a temporary directory; filesystem:///path; gs://bucket/prefix)")
	endpoint := fs.String("endpoint", "", "Run against this etcd endpoint instead of starting an in-process server")
	workers := fs.Int("workers", 64, "Concurrent requests in flight")
	blobs := fs.String("blobs", "kube", "Values to write: 'kube' (generated Kubernetes objects encoded as the apiserver stores them), 'synthetic' (random bytes of captured sizes), or a directory of captured blobs (see extract-blobs)")
	nodeImages := fs.Int("node-images", kubeblobs.DefaultOptions().Images, "Images reported by each generated Node (sizes the Node object; -blobs kube)")
	podContainers := fs.Int("pod-containers", kubeblobs.DefaultOptions().Containers, "Containers in each generated Pod (-blobs kube)")
	output := fs.String("o", "text", "Output format: text or json")
	stormConcurrency := fs.Int("list-storm-concurrency", 10, "Concurrent listers in the list-storm phase")
	stormRounds := fs.Int("list-storm-rounds", 1, "Full lists per lister in the list-storm phase")
	cpuProfile := fs.String("cpuprofile", "", "Write a CPU profile to this file")
	memProfile := fs.String("memprofile", "", "Write a heap profile to this file at the end")
	pprofAddr := fs.String("pprof-addr", "", "Serve net/http/pprof on this address during the run (e.g. 127.0.0.1:6060)")
	klog.InitFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	switch *blobs {
	case "kube":
		b, err := kubeblobs.GenerateWithOptions(kubeblobs.Options{Images: *nodeImages, Containers: *podContainers})
		if err != nil {
			return err
		}
		cfg.Blobs = b
	case "synthetic":
		cfg.Blobs = workload.SyntheticBlobs()
	default:
		b, err := workload.LoadBlobs(*blobs)
		if err != nil {
			return err
		}
		cfg.Blobs = b
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	if *pprofAddr != "" {
		go func() {
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				fmt.Fprintf(os.Stderr, "pprof server: %v\n", err)
			}
		}()
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}

	addr := *endpoint
	var srv *inprocess.Server
	if addr == "" {
		var err error
		srv, err = inprocess.Start(ctx, *logURI)
		if err != nil {
			return err
		}
		defer srv.Stop()
		addr = srv.Addr
		fmt.Fprintf(os.Stderr, "started in-process cloudetcd at %s (log %s)\n", addr, *logURI)
	}

	client, err := inprocess.NewClient(addr)
	if err != nil {
		return err
	}
	defer client.Close()

	runner, err := workload.NewRunner(cfg, client, *workers)
	if err != nil {
		return err
	}
	if srv != nil {
		runner.WatcherStatus = srv.Store.Watchers
		runner.LogBytes = func() int64 { return dirBytes(srv.LogDir) }
	}
	runner.SetVerify(*verify)

	fmt.Fprintf(os.Stderr, "model: %d nodes, %d pods, %d keys; blobs %v\n", cfg.Nodes, cfg.Pods(), cfg.Keys(), cfg.Blobs.Sizes())
	var reports []*workload.Report
	emit := func(rep *workload.Report) {
		reports = append(reports, rep)
		if *output != "json" {
			fmt.Print(rep.Text())
		}
	}
	for _, phase := range strings.Split(*phases, ",") {
		phase = strings.TrimSpace(phase)
		var rep *workload.Report
		var err error
		switch phase {
		case "populate":
			fmt.Fprintf(os.Stderr, "phase populate...\n")
			rep, err = runner.Populate(ctx)
		case "steady":
			fmt.Fprintf(os.Stderr, "phase steady for %s...\n", *duration)
			rep, err = runner.Steady(ctx, *duration)
		case "saturate":
			fmt.Fprintf(os.Stderr, "phase saturate for %s...\n", *saturateDuration)
			rep, err = runner.Saturate(ctx, workload.SaturateOptions{Duration: *saturateDuration, Seed: *seed, Contention: *contention})
			if rep != nil && *output != "json" {
				defer fmt.Println("HEADLINE " + rep.Headline())
			}
		case "list-storm":
			fmt.Fprintf(os.Stderr, "phase list-storm (%d x %d)...\n", *stormConcurrency, *stormRounds)
			rep, err = runner.ListStorm(ctx, *stormConcurrency, *stormRounds)
		case "verify":
			// With nothing in flight, the server's state must be exactly
			// what was written.
			if check := runner.Checker(); check != nil {
				fmt.Fprintf(os.Stderr, "verifying state...\n")
				n, verr := check.VerifyState(ctx, client, cfg.Prefix, cfg.ListPageSize)
				if verr != nil {
					return verr
				}
				live, expiring := check.LiveKeys()
				fmt.Fprintf(os.Stderr, "state: %d keys match the oracle (%d live, %d of them on leases that may have expired)\n", n, live, expiring)
			}
		case "":
			continue
		default:
			return fmt.Errorf("unknown phase %q", phase)
		}
		if rep != nil {
			emit(rep)
		}
		if err != nil {
			return err
		}
	}

	if *output == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(reports); err != nil {
			return err
		}
	}
	if check := runner.Checker(); check != nil {
		if n := check.ViolationCount(); n > 0 {
			for i, v := range check.Violations() {
				if i >= 20 {
					fmt.Fprintf(os.Stderr, "... %d more violations\n", n-int64(i))
					break
				}
				fmt.Fprintf(os.Stderr, "violation: %v\n", v)
			}
			return fmt.Errorf("%d correctness violations", n)
		}
	}
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			return err
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			return err
		}
	}
	return nil
}

func runAnalyze(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: cloud-etcd-bench analyze <recording.jsonl>")
	}
	a, err := workload.Analyze(args[0])
	if err != nil {
		return err
	}
	fmt.Print(a.Text())
	return nil
}

func runExtractBlobs(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: cloud-etcd-bench extract-blobs <recording.jsonl> <dir>")
	}
	a, err := workload.Analyze(args[0])
	if err != nil {
		return err
	}
	b := a.ExtractBlobs()
	if err := b.SaveBlobs(args[1]); err != nil {
		return err
	}
	fmt.Printf("wrote blobs to %s: %v\n", args[1], b.Sizes())
	return nil
}

// dirBytes is the total size of the files in dir.
func dirBytes(dir string) int64 {
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		// The log directory should always be readable; a size of 0 in the
		// report would be quietly wrong.
		klog.Warningf("reading log directory %s: %v", dir, err)
		return 0
	}
	for _, e := range entries {
		if e.IsDir() {
			// The log keeps a flat directory of files; a subdirectory is
			// not ours and is not counted.
			klog.Warningf("unexpected directory %s in log directory %s", e.Name(), dir)
			continue
		}
		info, err := e.Info()
		if err != nil {
			klog.Warningf("sizing log file %s: %v", e.Name(), err)
			continue
		}
		total += info.Size()
	}
	return total
}
