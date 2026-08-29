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
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Histogram is a lock-free latency histogram with logarithmic buckets
// (1µs base, 10% growth), good for percentiles to ~10% precision.
type Histogram struct {
	counts [numBuckets]atomic.Int64
	n      atomic.Int64
	sum    atomic.Int64
	max    atomic.Int64
}

const (
	bucketBase   = float64(time.Microsecond)
	bucketGrowth = 1.1
	numBuckets   = 240 // 1µs * 1.1^240 ≈ 8.5e9 µs ≈ 2.4h
)

var logGrowth = math.Log(bucketGrowth)

func bucketFor(d time.Duration) int {
	if d <= time.Microsecond {
		return 0
	}
	i := int(math.Log(float64(d)/bucketBase) / logGrowth)
	if i >= numBuckets {
		return numBuckets - 1
	}
	return i
}

func bucketUpper(i int) time.Duration {
	return time.Duration(bucketBase * math.Pow(bucketGrowth, float64(i+1)))
}

// Observe records one sample.
func (h *Histogram) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.counts[bucketFor(d)].Add(1)
	h.n.Add(1)
	h.sum.Add(int64(d))
	for {
		cur := h.max.Load()
		if int64(d) <= cur || h.max.CompareAndSwap(cur, int64(d)) {
			return
		}
	}
}

// Count returns the number of samples.
func (h *Histogram) Count() int64 { return h.n.Load() }

// Max returns the largest sample.
func (h *Histogram) Max() time.Duration { return time.Duration(h.max.Load()) }

// Mean returns the mean sample.
func (h *Histogram) Mean() time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	return time.Duration(h.sum.Load() / n)
}

// Percentile returns an upper bound on the p-th percentile (0 < p <= 1).
func (h *Histogram) Percentile(p float64) time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	target := int64(math.Ceil(p * float64(n)))
	if target < 1 {
		target = 1
	}
	var seen int64
	for i := range h.counts {
		seen += h.counts[i].Load()
		if seen >= target {
			upper := bucketUpper(i)
			if max := h.Max(); upper > max {
				return max
			}
			return upper
		}
	}
	return h.Max()
}

// OpStats accumulates results for one op label.
type OpStats struct {
	Count     atomic.Int64
	Errors    atomic.Int64
	Conflicts atomic.Int64
	Latency   Histogram
}

// WatchStats accumulates results for one watched prefix.
type WatchStats struct {
	Events           atomic.Int64
	ProgressNotifies atomic.Int64
	Restarts         atomic.Int64
	// Lag is the time from a write being issued to its event arriving.
	Lag Histogram
}

// Stats accumulates everything measured in one phase of a run.
type Stats struct {
	start time.Time

	mu      sync.RWMutex
	ops     map[string]*OpStats
	watches map[string]*WatchStats
	errs    map[string]int64 // sampled error messages -> occurrences

	// SchedulingLag is how late each scheduled op started relative to when the
	// model said it was due. It grows without bound when the server cannot
	// keep up, which makes it the primary "is the server keeping up" signal.
	SchedulingLag Histogram
}

const maxErrorSamples = 32

// NewStats creates an empty Stats starting now.
func NewStats() *Stats {
	return &Stats{
		start:   time.Now(),
		ops:     map[string]*OpStats{},
		watches: map[string]*WatchStats{},
		errs:    map[string]int64{},
	}
}

// Op returns the stats for label, creating them if needed.
func (s *Stats) Op(label string) *OpStats {
	s.mu.RLock()
	st := s.ops[label]
	s.mu.RUnlock()
	if st != nil {
		return st
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if st = s.ops[label]; st == nil {
		st = &OpStats{}
		s.ops[label] = st
	}
	return st
}

// Watch returns the stats for a watched prefix, creating them if needed.
func (s *Stats) Watch(label string) *WatchStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.watches[label]
	if st == nil {
		st = &WatchStats{}
		s.watches[label] = st
	}
	return st
}

func (s *Stats) recordError(label string, err error) {
	msg := label + ": " + err.Error()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.errs[msg]; ok || len(s.errs) < maxErrorSamples {
		s.errs[msg]++
	}
}
