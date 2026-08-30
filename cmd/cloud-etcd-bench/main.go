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
	phases := fs.String("phases", "populate,steady", "Comma-separated phases to run: populate, steady, list-storm")
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
		case "list-storm":
			fmt.Fprintf(os.Stderr, "phase list-storm (%d x %d)...\n", *stormConcurrency, *stormRounds)
			rep, err = runner.ListStorm(ctx, *stormConcurrency, *stormRounds)
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
