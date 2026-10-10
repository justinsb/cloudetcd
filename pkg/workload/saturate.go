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
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// SaturateOptions configures the Saturate phase.
type SaturateOptions struct {
	// Duration is how long to run.
	Duration time.Duration
	// Seed seeds the per-worker random streams that choose operations and
	// keys. The same seed on the same cluster size replays the same
	// sequence of choices per worker (though not the same interleaving).
	Seed uint64
	// Contention is the share of operations, in [0,1], that are a second
	// writer's GuaranteedUpdate on a pod or node another writer owns; the
	// default (0 means default) is 0.15. Set it to a negative value for none.
	// Since contended keys conflict by design, the phase retries conditional
	// updates at least saturateUpdateRetries times whatever
	// Config.UpdateRetries says.
	Contention float64
}

// saturateUpdateRetries is the retry budget of a conditional update during
// Saturate: the apiserver's GuaranteedUpdate retries without limit, and a
// key several workers contend is conflicted a few times in a row.
const saturateUpdateRetries = 64

// Saturate runs a closed loop of Workers goroutines, each repeatedly picking
// an operation from the steady-state mix at random and issuing it as soon as
// the previous one completes, so the server sees as many requests as it can
// take. The mix is what kube-apiserver sends weighted towards what dominates
// at scale (node lease renewals), plus deliberate contention: a share of the
// operations are a second writer updating a pod or node its kubelet is also
// writing, with the conflict-and-retry that follows, as the scheduler and
// controllers do. Housekeeping, compaction and the apiserver's prefix
// watches run alongside as in Steady.
//
// Every response is checked against the oracle; the report's TotalRate is
// the benchmark headline.
func (r *Runner) Saturate(ctx context.Context, opts SaturateOptions) (*Report, error) {
	c := &r.cfg
	if opts.Duration <= 0 {
		return nil, fmt.Errorf("Saturate: Duration must be positive")
	}
	if opts.Seed == 0 {
		opts.Seed = uint64(time.Now().UnixNano())
	}
	contention := opts.Contention
	switch {
	case contention == 0:
		contention = 0.15
	case contention < 0:
		contention = 0
	}
	stats := r.beginPhase()
	if r.cfg.UpdateRetries < saturateUpdateRetries {
		retries := r.cfg.UpdateRetries
		r.cfg.UpdateRetries = saturateUpdateRetries
		defer func() { r.cfg.UpdateRetries = retries }()
	}

	ws, err := r.startWatches(ctx, stats)
	if err != nil {
		return nil, err
	}

	// runCtx bounds the phase; ops in flight at the deadline finish under
	// the parent ctx.
	runCtx, cancel := context.WithTimeout(ctx, opts.Duration)
	defer cancel()

	// Background cadences, as in Steady but with their own tiny pool so
	// that saturation of the workers does not starve them.
	bg := newPool(runCtx, ctx, 4, stats)
	var loops sync.WaitGroup
	loop := func(fn func()) {
		loops.Add(1)
		go func() {
			defer loops.Done()
			fn()
		}()
	}
	loop(func() { bg.every(c.MasterLeaseInterval, r.housekeeping) })
	loop(func() {
		bg.every(c.CompactInterval, func(ctx context.Context) { _ = r.exec.compact(ctx, OpCompact) })
	})
	loop(func() {
		bg.every(c.HealthCheckInterval, func(ctx context.Context) { _ = r.exec.get(ctx, OpHealthCheck, c.Prefix+healthKey) })
	})
	if c.ProgressRequestsPerSecond > 0 {
		interval := time.Duration(float64(time.Second) / c.ProgressRequestsPerSecond)
		loop(func() {
			bg.every(interval, func(ctx context.Context) {
				start := time.Now()
				r.exec.record(OpProgressRequest, start, ws.watcher.RequestProgress(ctx))
			})
		})
	}

	var workers sync.WaitGroup
	for w := 0; w < r.Workers; w++ {
		workers.Add(1)
		go func(w int) {
			defer workers.Done()
			rng := rand.New(rand.NewPCG(opts.Seed, uint64(w)+1))
			for runCtx.Err() == nil {
				r.saturateOp(ctx, rng, contention)
			}
		}(w)
	}
	workers.Wait()
	loops.Wait()
	bg.close()
	ws.stop(ctx, r.lastWrittenRev())
	r.endPhase()

	rep := r.report("saturate", stats, nil)
	rep.Seed = opts.Seed
	return rep, nil
}

// saturateOp issues one randomly chosen operation.
func (r *Runner) saturateOp(ctx context.Context, rng *rand.Rand, contention float64) {
	c := &r.cfg
	node := rng.IntN(c.Nodes)
	if contention > 0 && rng.Float64() < contention {
		r.contend(ctx, rng, node)
		return
	}
	// The weights follow the steady-state mix at scale: lease renewals
	// dominate, then node status, pod lifecycles with their events, and
	// reads of various shapes.
	x := rng.IntN(100)
	switch {
	case x < 40:
		_ = r.exec.update(ctx, OpNodeLeaseUpdate, c.nodeLeaseKey(node), c.Blobs.NodeLease)
	case x < 52:
		_ = r.exec.update(ctx, OpNodeStatusUpdate, c.nodeKey(node), c.Blobs.Node)
	case x < 70:
		if c.PodsPerNode > 0 && r.churnPod(ctx, node, rng.IntN(c.PodsPerNode)) {
			return
		}
		// The slot was busy (or there are no pods): read instead.
		_ = r.exec.get(ctx, OpGet, c.nodeKey(node))
	case x < 80:
		switch rng.IntN(3) {
		case 0:
			_ = r.exec.get(ctx, OpGet, c.nodeKey(node))
		case 1:
			_ = r.exec.get(ctx, OpGet, c.nodeLeaseKey(node))
		default:
			if c.PodsPerNode > 0 {
				_ = r.exec.get(ctx, OpGet, r.currentPodKey(node, rng.IntN(c.PodsPerNode)))
			} else {
				_ = r.exec.get(ctx, OpGet, c.nodeKey(node))
			}
		}
	case x < 86:
		_ = r.exec.get(ctx, OpConsistentRead, c.resourceKey(readResources[rng.IntN(len(readResources))]))
	case x < 92:
		if c.PodsPerNode > 0 {
			slot := rng.IntN(c.PodsPerNode)
			gen := r.podGen[node*c.PodsPerNode+slot].Load()
			// An extra event on a pod, as a controller might emit; numbered
			// past the lifecycle's own so that the name is unique.
			r.emitEvent(ctx, node, slot, gen, c.EventsPerPodLifecycle+int(r.extraEvents.Add(1)))
		} else {
			_ = r.exec.get(ctx, OpHealthCheck, c.Prefix+healthKey)
		}
	case x < 97:
		ns := c.resourcePrefix(resourcePods) + fmt.Sprintf("ns-%03d/", rng.IntN(c.Namespaces))
		_, _ = r.exec.list(ctx, OpList, ns, c.ListPageSize, false)
	default:
		_ = r.exec.count(ctx, OpCount, c.resourcePrefix(countedResources[rng.IntN(len(countedResources))]))
	}
}

// contend is a second writer's GuaranteedUpdate on a node or one of its
// pods: read, then write conditionally on what was read, retrying on
// conflict with whoever else is writing it.
func (r *Runner) contend(ctx context.Context, rng *rand.Rand, node int) {
	c := &r.cfg
	if c.PodsPerNode > 0 && rng.IntN(3) > 0 {
		_ = r.exec.contendedUpdate(ctx, OpContendedUpdate, r.currentPodKey(node, rng.IntN(c.PodsPerNode)), c.Blobs.Pod)
		return
	}
	_ = r.exec.contendedUpdate(ctx, OpContendedUpdate, c.nodeKey(node), c.Blobs.Node)
}
