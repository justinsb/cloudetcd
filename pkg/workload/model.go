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

// Package workload generates the etcd traffic that a kube-apiserver produces
// for a Kubernetes cluster of a given size, without running a kube-apiserver
// or any nodes.
//
// kube-apiserver is the only etcd client in a cluster, so "the traffic from N
// nodes" is fully described at the etcd API: a set of key patterns, request
// shapes (all writes are conditional Txns on mod_revision), and cadences that
// scale with the number of nodes and pods. This package encodes that model as
// a Config, and a Runner turns it into real etcd v3 RPCs against any endpoint
// using only the etcd client library.
//
// The model is meant to be derived from, and checked against, recordings of a
// real kube-apiserver (see pkg/recording and cmd/cloud-etcd-bench analyze).
// The shapes below follow k8s.io/apiserver/pkg/storage/etcd3:
//
//   - create:  Txn(If mod_revision(key)==0   Then Put(key, v[, lease]) Else Range(key))
//   - update:  Txn(If mod_revision(key)==rev Then Put(key, v)          Else Range(key))
//   - delete:  Range(key); Txn(If mod_revision(key)==rev Then DeleteRange(key) Else Range(key))
//   - list:    Range(prefix, prefixEnd, limit, revision) paginated on the last key
//   - count:   Range(prefix, prefixEnd, count_only) — only for resources with no watch cache (events)
//   - read:    Range(/registry/<resource>) — a single-key get whose header carries the
//     current revision, which is all a consistent read from the watch cache needs
//   - watch:   Watch(prefix, rev+1, prev_kv, progress_notify), one stream per resource,
//     plus periodic progress requests
//   - compact: Txn(If version(compact_rev_key)==v Then Put Else Range); Compact(rev)
//
// and the apiserver's own housekeeping every 10s: its masterlease (get, list,
// LeaseGrant, conditional Put with lease), peerserverlease (LeaseGrant,
// conditional Put with lease) and identity Lease in kube-system (conditional
// Put), plus an etcd health probe.
//
// Values are opaque byte blobs: cloudetcd never inspects them, so captured
// Kubernetes objects (or synthetic ones of the same size) are reused verbatim.
// These shapes and the default cadences were checked against a recording of
// kube-apiserver v1.36 with kwok nodes (tests/e2e TestCaptureKwok).
package workload

import (
	"fmt"
	"time"
)

// Config describes the cluster whose etcd traffic is generated.
type Config struct {
	// Nodes is the number of nodes in the cluster.
	Nodes int
	// PodsPerNode is the number of pods scheduled to each node.
	PodsPerNode int
	// Namespaces is the number of namespaces the pods are spread across.
	Namespaces int

	// NodeLeaseInterval is how often each kubelet renews its Lease in
	// kube-node-lease (kubelet --node-lease-duration-seconds/4; default 10s).
	// This is the dominant steady-state write in a large cluster.
	NodeLeaseInterval time.Duration
	// NodeStatusInterval is how often each kubelet writes its Node status even
	// when nothing changed (kubelet nodeStatusReportFrequency; default 5m).
	NodeStatusInterval time.Duration

	// PodChurnPerNodePerHour is the rate at which pods are replaced on each
	// node. Each replacement is a full lifecycle: graceful delete of the old
	// pod, then create/bind/status-update of the new one, plus events.
	PodChurnPerNodePerHour float64
	// EventsPerPodLifecycle is the number of Events written per pod lifecycle
	// (Scheduled, Pulled, Created, Started, Killing...).
	EventsPerPodLifecycle int
	// EventTTL is the lease TTL attached to Events (apiserver --event-ttl; default 1h).
	EventTTL time.Duration

	// MasterLeaseInterval is how often the apiserver refreshes its entry in
	// /registry/masterleases (default 10s), MasterLeaseTTL its lease (15s).
	MasterLeaseInterval time.Duration
	MasterLeaseTTL      time.Duration

	// CountInterval is how often the apiserver counts, through etcd, each
	// resource type that has no watch cache (events) for the
	// storage_object_count metric (default 1m per resource).
	CountInterval time.Duration
	// HealthCheckInterval is how often the apiserver probes etcd health with
	// a Range of /registry/health (default 25s).
	HealthCheckInterval time.Duration
	// ProgressRequestsPerSecond is the rate of watch progress requests the
	// apiserver's watch cache sends while consistent reads wait for it to
	// catch up.
	ProgressRequestsPerSecond float64
	// CompactInterval is the apiserver's --etcd-compaction-interval (default 5m).
	CompactInterval time.Duration
	// ConsistentReadsPerSecond is the rate of consistent reads that go to etcd
	// rather than the watch cache (kubectl get with resourceVersion="",
	// controllers' initial lists, ...).
	ConsistentReadsPerSecond float64
	// ListPageSize is the limit used for paginated lists (reflectors use 500).
	ListPageSize int64

	// Prefix is the etcd key prefix (apiserver --etcd-prefix; default /registry).
	Prefix string

	// Blobs provides the values written for each resource type.
	Blobs *Blobs
}

// DefaultConfig returns a Config with kube-apiserver/kubelet default cadences
// and a small cluster.
func DefaultConfig() Config {
	return Config{
		Nodes:                     10,
		PodsPerNode:               10,
		Namespaces:                10,
		NodeLeaseInterval:         10 * time.Second,
		NodeStatusInterval:        5 * time.Minute,
		PodChurnPerNodePerHour:    2,
		EventsPerPodLifecycle:     4,
		EventTTL:                  time.Hour,
		MasterLeaseInterval:       10 * time.Second,
		MasterLeaseTTL:            15 * time.Second,
		CountInterval:             time.Minute,
		HealthCheckInterval:       25 * time.Second,
		ProgressRequestsPerSecond: 1,
		CompactInterval:           5 * time.Minute,
		ConsistentReadsPerSecond:  1,
		ListPageSize:              500,
		Prefix:                    "/registry",
		Blobs:                     DefaultBlobs(),
	}
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if c.Nodes <= 0 {
		return fmt.Errorf("Nodes must be positive")
	}
	if c.PodsPerNode < 0 {
		return fmt.Errorf("PodsPerNode must not be negative")
	}
	if c.Namespaces <= 0 {
		return fmt.Errorf("Namespaces must be positive")
	}
	if c.NodeLeaseInterval <= 0 || c.NodeStatusInterval <= 0 || c.MasterLeaseInterval <= 0 || c.CountInterval <= 0 || c.CompactInterval <= 0 || c.HealthCheckInterval <= 0 {
		return fmt.Errorf("intervals must be positive")
	}
	if c.ListPageSize <= 0 {
		return fmt.Errorf("ListPageSize must be positive")
	}
	if c.Blobs == nil {
		return fmt.Errorf("Blobs must be set")
	}
	return nil
}

// Pods is the total number of pods in the cluster.
func (c *Config) Pods() int { return c.Nodes * c.PodsPerNode }

// Keys is the approximate number of live keys in steady state (nodes, node
// leases and pods; events are transient).
func (c *Config) Keys() int { return 2*c.Nodes + c.Pods() }

// ExpectedRates returns the steady-state operation rates (per second) the
// model predicts, keyed by op label. It is reported alongside the achieved
// rates so that a run that falls behind is visible.
func (c *Config) ExpectedRates() map[string]float64 {
	rates := map[string]float64{}
	rates[OpNodeLeaseUpdate] = float64(c.Nodes) / c.NodeLeaseInterval.Seconds()
	rates[OpNodeStatusUpdate] = float64(c.Nodes) / c.NodeStatusInterval.Seconds()
	churn := float64(c.Nodes) * c.PodChurnPerNodePerHour / 3600
	if c.PodsPerNode == 0 {
		churn = 0
	}
	rates[OpPodCreate] = churn
	rates[OpPodUpdate] = churn * podUpdatesPerLifecycle
	rates[OpPodDelete] = churn
	rates[OpEventCreate] = churn * float64(c.EventsPerPodLifecycle)
	housekeeping := 1 / c.MasterLeaseInterval.Seconds()
	rates[OpMasterLease] = housekeeping
	rates[OpPeerServerLease] = housekeeping
	rates[OpAPIServerLease] = housekeeping
	rates[OpLeaseGrant] = 2 * housekeeping // masterlease + peerserverlease; event leases are reused
	rates[OpCount] = float64(len(countedResources)) / c.CountInterval.Seconds()
	rates[OpHealthCheck] = 1 / c.HealthCheckInterval.Seconds()
	rates[OpCompact] = 1 / c.CompactInterval.Seconds()
	rates[OpConsistentRead] = c.ConsistentReadsPerSecond
	rates[OpProgressRequest] = c.ProgressRequestsPerSecond
	return rates
}

// podUpdatesPerLifecycle is the number of Txn updates in one pod lifecycle:
// bind (nodeName), status Running, deletionTimestamp, status Terminated.
const podUpdatesPerLifecycle = 4

// Op labels used in Stats and reports.
const (
	OpNodeCreate       = "node-create"
	OpNodeLeaseCreate  = "node-lease-create"
	OpNodeLeaseUpdate  = "node-lease-update"
	OpNodeStatusUpdate = "node-status-update"
	OpPodCreate        = "pod-create"
	OpPodUpdate        = "pod-update"
	OpPodDelete        = "pod-delete"
	OpEventCreate      = "event-create"
	OpLeaseGrant       = "lease-grant"
	OpMasterLease      = "masterlease-update"
	OpPeerServerLease  = "peerserverlease-update"
	OpAPIServerLease   = "apiserver-lease-update"
	OpCount            = "count"
	OpHealthCheck      = "health-check"
	OpCompact          = "compact"
	OpConsistentRead   = "consistent-read"
	OpProgressRequest  = "watch-progress-request"
	OpList             = "list"
)
