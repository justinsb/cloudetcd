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

package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"justinsb.com/cloudetcd/pkg/persistence"
)

// fakeBackend is a FlushFunc whose writes take latency and publish through a
// Sequencer, like the real logs. failAt makes the batch starting at that
// revision fail.
type fakeBackend struct {
	latency time.Duration
	seq     *Sequencer
	failAt  Revision

	mu        sync.Mutex
	published []Revision // first revision of each batch, in publish order
	maxDepth  int
	inflight  atomic.Int32
}

func (f *fakeBackend) flush(ctx context.Context, lastLogPosition Revision, commit *BatchCommit) error {
	depth := int(f.inflight.Add(1))
	defer f.inflight.Add(-1)
	f.mu.Lock()
	if depth > f.maxDepth {
		f.maxDepth = depth
	}
	f.mu.Unlock()

	// Simulate a write with variable latency, so batches complete out of order.
	jitter := time.Duration(lastLogPosition%3) * f.latency / 2
	select {
	case <-time.After(f.latency + jitter):
	case <-ctx.Done():
		return ctx.Err()
	}
	if f.failAt != 0 && lastLogPosition+1 == f.failAt {
		return errors.New("simulated write failure")
	}
	if err := f.seq.Wait(ctx, lastLogPosition); err != nil {
		return err
	}
	f.mu.Lock()
	f.published = append(f.published, lastLogPosition+1)
	f.mu.Unlock()
	f.seq.Advance(lastLogPosition + Revision(len(commit.Transactions)))
	return nil
}

func add(t *testing.T, b *Batching, key string) (Revision, bool, error) {
	t.Helper()
	meta := persistence.NewTxnMeta(0)
	meta.AddWrite(key)
	return b.Add(context.Background(), &persistence.LogRecord{}, meta)
}

// TestPipelinedFlushes checks that consecutive batches are written
// concurrently, published in order, and acknowledged with contiguous
// revisions — and that throughput is not bounded by one write per latency.
func TestPipelinedFlushes(t *testing.T) {
	const latency = 30 * time.Millisecond
	backend := &fakeBackend{latency: latency, seq: NewSequencer(0)}
	b := NewBatchingWithOptions(0, backend.flush, Options{Window: 2 * time.Millisecond, MaxInFlight: 8})
	defer b.Close()

	// 40 sequential-ish waves of adds: each wave lands in its own batch
	// (the window is short), so we get many batches in flight.
	const waves, perWave = 40, 5
	var wg sync.WaitGroup
	revs := make([]Revision, waves*perWave)
	start := time.Now()
	for w := 0; w < waves; w++ {
		for i := 0; i < perWave; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				rev, ok, err := add(t, b, fmt.Sprintf("key-%d", idx))
				if err != nil || !ok {
					t.Errorf("add %d: ok=%v err=%v", idx, ok, err)
					return
				}
				revs[idx] = rev
			}(w*perWave + i)
		}
		time.Sleep(3 * time.Millisecond)
	}
	wg.Wait()
	elapsed := time.Since(start)

	seen := map[Revision]bool{}
	var max Revision
	for _, r := range revs {
		if r == 0 || seen[r] {
			t.Fatalf("revision %d missing or duplicated", r)
		}
		seen[r] = true
		if r > max {
			max = r
		}
	}
	if int(max) != waves*perWave {
		t.Errorf("highest revision %d, want %d (contiguous)", max, waves*perWave)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	for i := 1; i < len(backend.published); i++ {
		if backend.published[i] <= backend.published[i-1] {
			t.Errorf("batches published out of order: %v", backend.published)
			break
		}
	}
	if backend.maxDepth < 2 {
		t.Errorf("max in-flight depth %d: batches were not pipelined", backend.maxDepth)
	}
	// Serial commits would take at least len(published) * latency.
	if serial := time.Duration(len(backend.published)) * latency; elapsed > serial {
		t.Errorf("took %s for %d batches; serial would be %s, so nothing was pipelined", elapsed, len(backend.published), serial)
	}
	t.Logf("%d batches, max depth %d, %s (serial would be >= %s)", len(backend.published), backend.maxDepth, elapsed, time.Duration(len(backend.published))*latency)
}

// TestPipelinedFlushFailureStopsLog checks that when a batch fails, every
// batch after it fails (none of their records are published) and the log
// refuses further writes, so no acknowledged write ever sits past a gap.
func TestPipelinedFlushFailureStopsLog(t *testing.T) {
	backend := &fakeBackend{latency: 20 * time.Millisecond, seq: NewSequencer(0)}
	b := NewBatchingWithOptions(0, backend.flush, Options{Window: 2 * time.Millisecond, MaxInFlight: 8})
	defer b.Close()

	// First batch commits normally.
	if rev, ok, err := add(t, b, "a"); err != nil || !ok || rev != 1 {
		t.Fatalf("first add: rev=%d ok=%v err=%v", rev, ok, err)
	}

	// The batch at revision 2 will fail; batches 3.. are in flight behind it.
	backend.failAt = 2
	type result struct {
		rev Revision
		ok  bool
		err error
	}
	results := make(chan result, 3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			rev, ok, err := add(t, b, fmt.Sprintf("k%d", i))
			results <- result{rev, ok, err}
		}(i)
		time.Sleep(5 * time.Millisecond) // separate batches
	}
	for i := 0; i < 3; i++ {
		r := <-results
		if r.err == nil {
			t.Errorf("write after the failure was acknowledged: rev=%d ok=%v", r.rev, r.ok)
		}
	}
	if got := backend.seq.Position(); got != 1 {
		t.Errorf("published position %d after failure, want 1", got)
	}
	if _, _, err := add(t, b, "later"); !errors.Is(err, ErrLogFailed) {
		t.Errorf("add after failure: err=%v, want ErrLogFailed", err)
	}
}
