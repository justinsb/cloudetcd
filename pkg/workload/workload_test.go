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
	"testing"
	"time"
)

func TestKeyPattern(t *testing.T) {
	cases := map[string]string{
		"/registry/minions/node-1":                                                  "/registry/minions/*",
		"/registry/leases/kube-node-lease/node-1":                                   "/registry/leases/kube-node-lease/*",
		"/registry/leases/kube-system/kube-controller-manager":                      "/registry/leases/kube-system/*",
		"/registry/pods/app/web-abc":                                                "/registry/pods/*/*",
		"/registry/events/default/web-abc.1234":                                     "/registry/events/default/*",
		"/registry/masterleases/10.0.0.1":                                           "/registry/masterleases/*",
		"/registry/apiextensions.k8s.io/customresourcedefinitions/foos.example.com": "/registry/apiextensions.k8s.io/customresourcedefinitions/*",
		"compact_rev_key":                                                           "compact_rev_key",
	}
	for in, want := range cases {
		if got := keyPattern(in); got != want {
			t.Errorf("keyPattern(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPrefixEnd(t *testing.T) {
	if got := prefixEnd("/registry/pods/"); got != "/registry/pods0" {
		t.Errorf("prefixEnd = %q", got)
	}
	if got := prefixEnd("\xff"); got != "\x00" {
		t.Errorf("prefixEnd(0xff) = %q", got)
	}
}

func TestHistogramPercentiles(t *testing.T) {
	var h Histogram
	for i := 1; i <= 1000; i++ {
		h.Observe(time.Duration(i) * time.Millisecond)
	}
	if h.Count() != 1000 {
		t.Fatalf("count = %d", h.Count())
	}
	within := func(name string, got, want time.Duration) {
		t.Helper()
		// Buckets grow 10% per step, so the upper bound is within ~10%.
		if got < want || got > want*12/10 {
			t.Errorf("%s = %s, want within [%s, %s]", name, got, want, want*12/10)
		}
	}
	within("p50", h.Percentile(0.5), 500*time.Millisecond)
	within("p99", h.Percentile(0.99), 990*time.Millisecond)
	if h.Max() != time.Second {
		t.Errorf("max = %s", h.Max())
	}
	if h.Percentile(1) != time.Second {
		t.Errorf("p100 = %s", h.Percentile(1))
	}
}

func TestExpectedRates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Nodes = 1000
	cfg.PodsPerNode = 30
	rates := cfg.ExpectedRates()
	if got := rates[OpNodeLeaseUpdate]; got != 100 {
		t.Errorf("node lease rate = %v, want 100/s for 1000 nodes at 10s", got)
	}
	// 1000 nodes * 2 pods/node/hour = 2000 pod lifecycles/hour.
	if got, want := rates[OpPodCreate], 2000.0/3600; got < want*0.999 || got > want*1.001 {
		t.Errorf("pod create rate = %v, want %v", got, want)
	}
}

func TestPodKeysAreUniqueAcrossGenerations(t *testing.T) {
	cfg := DefaultConfig()
	seen := map[string]bool{}
	for node := 0; node < 3; node++ {
		for slot := 0; slot < cfg.PodsPerNode; slot++ {
			for gen := uint64(0); gen < 3; gen++ {
				k := cfg.podKey(node, slot, gen)
				if seen[k] {
					t.Fatalf("duplicate pod key %s", k)
				}
				seen[k] = true
			}
		}
	}
}
