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

package workload

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"justinsb.com/cloudetcd/pkg/storage"
)

// Runner drives a cluster's worth of traffic at an etcd endpoint. Its phases
// are run in order on the same state:
//
//   - Populate: the cluster comes up — every node registers, its lease is
//     created, and its pods are created, bound and reported running. This is
//     the burst a control plane sees after a restart.
//   - Steady: the cluster hums — node lease renewals, node status reports,
//     pod churn with events, apiserver housekeeping (masterlease, counts,
//     compaction) and consistent reads, with the apiserver's watches attached.
//   - Saturate: the same kinds of operations, but closed-loop and in a
//     seeded random mix with deliberate contention, as fast as the server
//     will take them. Its throughput is the benchmark's headline.
//   - ListStorm: many clients relist everything at once, which is what
//     happens when watch caches fall behind and controllers restart.
//
// Every response is checked against a Checker (the correctness oracle)
// unless verification is turned off; violations are counted in each phase's
// Report and available from Checker.
type Runner struct {
	cfg    Config
	client *clientv3.Client
	// Workers is the number of concurrent RPCs in flight. kube-apiserver's
	// concurrency to etcd is bounded only by its request in-flight limits
	// (hundreds), all multiplexed on one connection, which is what this
	// reproduces.
	Workers int
	// WatcherStatus, if set, reports the server's watchers; the steady phase
	// samples it to report how far behind the log head the server-side
	// watchers get, separately from what the client receives.
	WatcherStatus func() []storage.WatcherStatus
	// LogBytes, if set, reports what the log uses on disk; reported per
	// phase.
	LogBytes func() int64

	exec   *executor
	state  *stateMap
	events *leaseCache

	// podGen is the generation of each pod slot (node*PodsPerNode+slot),
	// incremented on every churn so that names are never reused.
	podGen []atomic.Uint64
	// slotBusy marks pod slots with a lifecycle in progress, so that two
	// workers do not churn the same slot at once (one pod has one kubelet).
	slotBusy []atomic.Bool
	// extraEvents numbers the events Saturate emits outside pod lifecycles.
	extraEvents atomic.Int64
}

// NewRunner creates a Runner for cfg against client, with verification on.
func NewRunner(cfg Config, client *clientv3.Client, workers int) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if workers <= 0 {
		workers = 64
	}
	state := newStateMap()
	r := &Runner{
		cfg:      cfg,
		client:   client,
		Workers:  workers,
		state:    state,
		exec:     &executor{kv: client.KV, lease: client.Lease, state: state, stats: NewStats(), check: NewChecker()},
		events:   newLeaseCache(cfg.EventTTL, cfg.EventLeaseReuse),
		podGen:   make([]atomic.Uint64, cfg.Pods()),
		slotBusy: make([]atomic.Bool, cfg.Pods()),
	}
	r.exec.cfg = &r.cfg
	return r, nil
}

// Config returns the runner's configuration.
func (r *Runner) Config() *Config { return &r.cfg }

// Checker returns the correctness oracle, or nil if verification is off.
func (r *Runner) Checker() *Checker { return r.exec.check }

// LastCompaction returns the revision the workload most recently asked the
// server to compact to (0 if it has not). The server's compaction point is
// at or below it, so history above it must be readable.
func (r *Runner) LastCompaction() int64 { return r.exec.lastCompacted.Load() }

// SetVerify turns response verification on or off. It must be called before
// the first phase; turning it off saves the oracle's memory (one record per
// write) on long benchmark runs.
func (r *Runner) SetVerify(on bool) {
	switch {
	case on && r.exec.check == nil:
		r.exec.check = NewChecker()
	case !on:
		r.exec.check = nil
	}
}

func (r *Runner) beginPhase() *Stats {
	stats := NewStats()
	if r.exec.check != nil {
		stats.violationsBefore = r.exec.check.ViolationCount()
	}
	r.exec.setStats(stats)
	return stats
}

// endPhase settles the oracle's parked checks once no write is in flight.
func (r *Runner) endPhase() {
	if r.exec.check != nil {
		r.exec.check.Drain()
	}
}

// Populate brings the cluster up: nodes, node leases and pods are created as
// fast as Workers allows. Returns the report for the phase.
func (r *Runner) Populate(ctx context.Context) (*Report, error) {
	stats := r.beginPhase()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p := newPool(ctx, ctx, r.Workers, stats)
	p.submit(job{fn: r.seedAPIServer})
	for i := 0; i < r.cfg.Nodes; i++ {
		if !p.submit(job{fn: func(ctx context.Context) { r.registerNode(ctx, i) }}) {
			break
		}
	}
	p.close()
	r.endPhase()
	return r.report("populate", stats, nil), ctx.Err()
}

// seedAPIServer creates the apiserver's own keys (masterlease, peer-server
// lease, identity lease), which an apiserver writes when it starts.
func (r *Runner) seedAPIServer(ctx context.Context) {
	c := &r.cfg
	if leaseID, err := r.exec.leaseGrant(ctx, OpLeaseGrant, c.MasterLeaseTTL); err == nil {
		_ = r.exec.create(ctx, OpMasterLease, c.masterLeaseKey(), c.Blobs.MasterLease, leaseID)
	}
	if leaseID, err := r.exec.leaseGrant(ctx, OpLeaseGrant, c.MasterLeaseTTL); err == nil {
		_ = r.exec.create(ctx, OpPeerServerLease, c.peerServerLeaseKey(), c.Blobs.MasterLease, leaseID)
	}
	_ = r.exec.create(ctx, OpAPIServerLease, c.apiserverLeaseKey(), c.Blobs.NodeLease, 0)
}

// housekeeping is one round of the apiserver's own 10s traffic: the
// masterlease reconciler reads and lists the masterleases then refreshes its
// own under a fresh lease; the peer-server lease and the apiserver's identity
// Lease are refreshed on the same cadence.
func (r *Runner) housekeeping(ctx context.Context) {
	c := &r.cfg
	_ = r.exec.get(ctx, OpMasterLease, c.masterLeaseKey())
	_ = r.exec.listAll(ctx, OpMasterLease, c.resourcePrefix(resourceMasterLeases))
	if leaseID, err := r.exec.leaseGrant(ctx, OpLeaseGrant, c.MasterLeaseTTL); err == nil {
		_ = r.exec.upsertWithLease(ctx, OpMasterLease, c.masterLeaseKey(), c.Blobs.MasterLease, leaseID)
	}
	if leaseID, err := r.exec.leaseGrant(ctx, OpLeaseGrant, c.MasterLeaseTTL); err == nil {
		_ = r.exec.upsertWithLease(ctx, OpPeerServerLease, c.peerServerLeaseKey(), c.Blobs.MasterLease, leaseID)
	}
	_ = r.exec.update(ctx, OpAPIServerLease, c.apiserverLeaseKey(), c.Blobs.NodeLease)
}

func (r *Runner) registerNode(ctx context.Context, i int) {
	c := &r.cfg
	if err := r.exec.create(ctx, OpNodeCreate, c.nodeKey(i), c.Blobs.Node, 0); err != nil {
		return
	}
	if err := r.exec.create(ctx, OpNodeLeaseCreate, c.nodeLeaseKey(i), c.Blobs.NodeLease, 0); err != nil {
		return
	}
	for slot := 0; slot < c.PodsPerNode; slot++ {
		r.startPod(ctx, i, slot, r.podGen[i*c.PodsPerNode+slot].Load())
	}
}

// startPod is a pod's birth: created (pending), bound to its node, and
// reported running by the kubelet; events are emitted along the way.
func (r *Runner) startPod(ctx context.Context, node, slot int, gen uint64) {
	c := &r.cfg
	key := c.podKey(node, slot, gen)
	if err := r.exec.create(ctx, OpPodCreate, key, c.Blobs.Pod, 0); err != nil {
		return
	}
	r.emitEvent(ctx, node, slot, gen, 0)
	if err := r.exec.update(ctx, OpPodUpdate, key, c.Blobs.Pod); err != nil { // bind
		return
	}
	if err := r.exec.update(ctx, OpPodUpdate, key, c.Blobs.Pod); err != nil { // status: Running
		return
	}
	for n := 1; n < c.EventsPerPodLifecycle-1; n++ {
		r.emitEvent(ctx, node, slot, gen, n)
	}
}

// stopPod is a pod's death: graceful deletion sets deletionTimestamp, the
// kubelet reports it terminated, then removes it.
func (r *Runner) stopPod(ctx context.Context, node, slot int, gen uint64) {
	c := &r.cfg
	key := c.podKey(node, slot, gen)
	if err := r.exec.update(ctx, OpPodUpdate, key, c.Blobs.Pod); err != nil { // deletionTimestamp
		return
	}
	if c.EventsPerPodLifecycle > 1 {
		r.emitEvent(ctx, node, slot, gen, c.EventsPerPodLifecycle-1)
	}
	if err := r.exec.update(ctx, OpPodUpdate, key, c.Blobs.Pod); err != nil { // status: Terminated
		return
	}
	_ = r.exec.delete(ctx, OpPodDelete, key)
}

// emitEvent writes event n of a pod's generation; lifecycle events are
// numbered below EventsPerPodLifecycle.
func (r *Runner) emitEvent(ctx context.Context, node, slot int, gen uint64, n int) {
	c := &r.cfg
	leaseID, err := r.events.get(ctx, r.exec)
	if err != nil {
		return
	}
	_ = r.exec.create(ctx, OpEventCreate, c.eventKey(node, slot, gen, n), c.Blobs.Event, leaseID)
}

// churnPod replaces the pod in a slot with a new generation. It returns
// false, doing nothing, if the slot's previous lifecycle is still running.
func (r *Runner) churnPod(ctx context.Context, node, slot int) bool {
	i := node*r.cfg.PodsPerNode + slot
	if !r.slotBusy[i].CompareAndSwap(false, true) {
		return false
	}
	defer r.slotBusy[i].Store(false)
	g := &r.podGen[i]
	gen := g.Load()
	r.stopPod(ctx, node, slot, gen)
	g.Store(gen + 1)
	r.startPod(ctx, node, slot, gen+1)
	return true
}

// currentPodKey is the key of the pod currently in a slot (which may be mid
// lifecycle).
func (r *Runner) currentPodKey(node, slot int) string {
	return r.cfg.podKey(node, slot, r.podGen[node*r.cfg.PodsPerNode+slot].Load())
}

// Steady runs the steady-state workload for d and returns its report.
func (r *Runner) Steady(ctx context.Context, d time.Duration) (*Report, error) {
	c := &r.cfg
	stats := r.beginPhase()

	ws, err := r.startWatches(ctx, stats)
	if err != nil {
		return nil, err
	}

	// schedCtx bounds the phase; ops still in flight at the deadline finish
	// under the parent ctx.
	schedCtx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	p := newPool(schedCtx, ctx, r.Workers, stats)
	var loops sync.WaitGroup
	loop := func(fn func()) {
		loops.Add(1)
		go func() {
			defer loops.Done()
			fn()
		}()
	}

	loop(func() {
		p.spread(c.Nodes, c.NodeLeaseInterval, func(ctx context.Context, i int) {
			_ = r.exec.update(ctx, OpNodeLeaseUpdate, c.nodeLeaseKey(i), c.Blobs.NodeLease)
		})
	})
	loop(func() {
		p.spread(c.Nodes, c.NodeStatusInterval, func(ctx context.Context, i int) {
			_ = r.exec.update(ctx, OpNodeStatusUpdate, c.nodeKey(i), c.Blobs.Node)
		})
	})
	if c.PodsPerNode > 0 && c.PodChurnPerNodePerHour > 0 {
		// Each slot is replaced once every PodsPerNode/churn hours; spread over
		// all slots that gives Nodes*churn replacements per hour.
		slotInterval := time.Duration(float64(c.PodsPerNode) / c.PodChurnPerNodePerHour * float64(time.Hour))
		loop(func() {
			p.spread(c.Pods(), slotInterval, func(ctx context.Context, i int) {
				r.churnPod(ctx, i/c.PodsPerNode, i%c.PodsPerNode)
			})
		})
	}
	loop(func() {
		p.every(c.MasterLeaseInterval, r.housekeeping)
	})
	loop(func() {
		p.spread(len(countedResources), c.CountInterval, func(ctx context.Context, i int) {
			_ = r.exec.count(ctx, OpCount, c.resourcePrefix(countedResources[i]))
		})
	})
	loop(func() {
		p.every(c.HealthCheckInterval, func(ctx context.Context) {
			_ = r.exec.get(ctx, OpHealthCheck, c.Prefix+healthKey)
		})
	})
	if c.ProgressRequestsPerSecond > 0 {
		interval := time.Duration(float64(time.Second) / c.ProgressRequestsPerSecond)
		loop(func() {
			p.every(interval, func(ctx context.Context) {
				start := time.Now()
				err := ws.watcher.RequestProgress(ctx)
				_ = r.exec.observe(OpProgressRequest, start, err)
			})
		})
	}
	loop(func() {
		p.every(c.CompactInterval, func(ctx context.Context) {
			_ = r.exec.compact(ctx, OpCompact)
		})
	})
	if c.ConsistentReadsPerSecond > 0 {
		interval := time.Duration(float64(time.Second) / c.ConsistentReadsPerSecond)
		loop(func() {
			var n atomic.Int64 // jobs run on several workers
			p.every(interval, func(ctx context.Context) {
				res := readResources[int(n.Add(1)-1)%len(readResources)]
				_ = r.exec.get(ctx, OpConsistentRead, c.resourceKey(res))
			})
		})
	}

	loops.Wait()
	p.close() // waits for in-flight ops, so the watches still see their events
	ws.stop(ctx, r.lastWrittenRev())
	r.endPhase()

	return r.report("steady", stats, c.ExpectedRates()), nil
}

// lastWrittenRev is the highest revision any response has reported, which
// with no write in flight is at or above every acknowledged write.
func (r *Runner) lastWrittenRev() int64 {
	if r.exec.check == nil {
		return 0
	}
	return r.exec.check.Floor()
}

// ListStorm has concurrency clients each perform rounds full paginated lists
// of the pods and nodes prefixes, all at once. Each list is checked to be
// complete and consistent at its revision.
func (r *Runner) ListStorm(ctx context.Context, concurrency, rounds int) (*Report, error) {
	c := &r.cfg
	stats := r.beginPhase()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < rounds && ctx.Err() == nil; n++ {
				_, _ = r.exec.list(ctx, OpList, c.resourcePrefix(resourcePods), c.ListPageSize, true)
				_, _ = r.exec.list(ctx, OpList, c.resourcePrefix(resourceNodes), c.ListPageSize, true)
			}
		}()
	}
	wg.Wait()
	r.endPhase()
	return r.report("list-storm", stats, nil), ctx.Err()
}

// sampleWatchers records, twice a second, how far behind the log head each
// server-side watcher is.
func (r *Runner) sampleWatchers(ctx context.Context, stats *Stats) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, w := range r.WatcherStatus() {
			lag := int64(w.Head) - int64(w.Delivered)
			if lag < 0 {
				lag = 0
			}
			stats.ServerWatchLag.Observe(time.Duration(lag)) // in revisions, not time
		}
	}
}
