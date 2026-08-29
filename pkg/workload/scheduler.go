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
	"time"
)

// job is a unit of work for the pool. due is when the model wanted it to run;
// the gap between due and when a worker picks it up is the scheduling lag.
type job struct {
	due time.Time
	fn  func(ctx context.Context)
}

// pool runs jobs on a fixed number of workers. Every job goes through a small
// bounded queue, so when the server cannot keep up, submit blocks and the
// scheduling lag of subsequent jobs grows: the generator is open-loop (the
// model decides when things are due) but measures how far behind it fell.
type pool struct {
	// ctx bounds scheduling: once it is done no more jobs are submitted.
	ctx context.Context
	// opCtx is passed to jobs; it outlives ctx so that ops in flight when a
	// phase ends complete normally instead of failing with a cancellation.
	opCtx context.Context
	jobs  chan job
	wg    sync.WaitGroup
	stats *Stats
}

func newPool(ctx, opCtx context.Context, workers int, stats *Stats) *pool {
	p := &pool{ctx: ctx, opCtx: opCtx, jobs: make(chan job, workers*4), stats: stats}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for j := range p.jobs {
				if !j.due.IsZero() {
					stats.SchedulingLag.Observe(time.Since(j.due))
				}
				j.fn(opCtx)
			}
		}()
	}
	return p
}

// submit queues a job, blocking while the queue is full. It returns false if
// the context was cancelled first.
func (p *pool) submit(j job) bool {
	select {
	case p.jobs <- j:
		return true
	case <-p.ctx.Done():
		return false
	}
}

// close stops accepting jobs and waits for queued ones to finish.
func (p *pool) close() {
	close(p.jobs)
	p.wg.Wait()
}

// spread runs fn(i) for every i in [0,n) once per interval, spaced evenly
// across the interval, until the pool's context is cancelled. Each entity has
// a fixed phase within the interval, like kubelets whose timers are unrelated.
func (p *pool) spread(n int, interval time.Duration, fn func(ctx context.Context, i int)) {
	if n <= 0 || interval <= 0 {
		return
	}
	per := float64(interval) / float64(n)
	start := time.Now()
	for cycle := 0; ; cycle++ {
		for i := 0; i < n; i++ {
			due := start.Add(time.Duration(float64(cycle)*float64(interval) + float64(i)*per))
			if !sleepUntil(p.ctx, due) {
				return
			}
			i := i
			if !p.submit(job{due: due, fn: func(ctx context.Context) { fn(ctx, i) }}) {
				return
			}
		}
	}
}

// every runs fn once per interval, starting after one interval.
func (p *pool) every(interval time.Duration, fn func(ctx context.Context)) {
	if interval <= 0 {
		return
	}
	next := time.Now().Add(interval)
	for {
		if !sleepUntil(p.ctx, next) {
			return
		}
		if !p.submit(job{due: next, fn: fn}) {
			return
		}
		next = next.Add(interval)
	}
}

// sleepUntil blocks until t or the context is cancelled, returning false in
// the latter case. Very short waits are skipped: the timer resolution is
// coarser than the spacing between ops at high rates, and a sub-millisecond
// burst is fine.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d < 200*time.Microsecond {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
