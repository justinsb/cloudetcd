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

import "fmt"

// Key layout, following kube-apiserver's registry:
//
//	/registry/minions/<node>
//	/registry/leases/kube-node-lease/<node>
//	/registry/pods/<namespace>/<pod>
//	/registry/events/<namespace>/<event>
//	/registry/masterleases/<ip>
//	compact_rev_key
//
// Nodes are numbered 0..Nodes-1 and pods are addressed by (node, slot) with a
// generation that increments each time the slot is churned, so that key names
// are deterministic and never reused.

// Resource prefixes (relative to Config.Prefix) for the resource types the
// model writes. These are also the prefixes the apiserver watches and counts.
const (
	resourceNodes            = "minions"
	resourceLeases           = "leases"
	resourceNodeLeases       = "leases/kube-node-lease"
	resourcePods             = "pods"
	resourceEvents           = "events"
	resourceMasterLeases     = "masterleases"
	resourcePeerServerLeases = "peerserverleases"
)

// countedResources are the resource prefixes counted every CountInterval.
// Only resources without a watch cache are counted through etcd; in a
// default apiserver that is just events.
var countedResources = []string{resourceEvents}

// watchedResources are the resource prefixes the apiserver keeps a watch on
// (one watch stream per resource type; leases of every namespace share one).
var watchedResources = []string{resourceNodes, resourceLeases, resourcePods, resourceEvents}

// readResources are the resources consistent reads are spread across.
var readResources = []string{resourceNodes, resourceNodeLeases, resourcePods, resourceEvents}

const (
	compactRevKey = "compact_rev_key"
	// healthKey is probed by the apiserver's etcd health check.
	healthKey = "/health"
)

func (c *Config) resourcePrefix(resource string) string {
	return c.Prefix + "/" + resource + "/"
}

func nodeName(i int) string { return fmt.Sprintf("node-%06d", i) }

func (c *Config) nodeKey(i int) string {
	return c.resourcePrefix(resourceNodes) + nodeName(i)
}

func (c *Config) nodeLeaseKey(i int) string {
	return c.resourcePrefix(resourceNodeLeases) + nodeName(i)
}

func (c *Config) podNamespace(node, slot int) string {
	return fmt.Sprintf("ns-%03d", (node*c.PodsPerNode+slot)%c.Namespaces)
}

func podName(node, slot int, generation uint64) string {
	return fmt.Sprintf("pod-%06d-%03d-%d", node, slot, generation)
}

func (c *Config) podKey(node, slot int, generation uint64) string {
	return c.resourcePrefix(resourcePods) + c.podNamespace(node, slot) + "/" + podName(node, slot, generation)
}

func (c *Config) eventKey(node, slot int, generation uint64, n int) string {
	return c.resourcePrefix(resourceEvents) + c.podNamespace(node, slot) + "/" + podName(node, slot, generation) + fmt.Sprintf(".%d", n)
}

func (c *Config) masterLeaseKey() string {
	return c.resourcePrefix(resourceMasterLeases) + "10.0.0.1"
}

func (c *Config) peerServerLeaseKey() string {
	return c.resourcePrefix(resourcePeerServerLeases) + "apiserver-0"
}

// apiserverLeaseKey is the apiserver's identity Lease in kube-system.
func (c *Config) apiserverLeaseKey() string {
	return c.resourcePrefix(resourceLeases) + "kube-system/apiserver-0"
}

// resourceKey is the bare resource key (no trailing slash) the apiserver
// reads to learn the current revision for a consistent read.
func (c *Config) resourceKey(resource string) string {
	return c.Prefix + "/" + resource
}

// prefixEnd returns the range end for a prefix, as clientv3.WithPrefix does.
func prefixEnd(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return "\x00"
}
