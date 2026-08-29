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

package kubeblobs

import (
	"bytes"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestGenerate(t *testing.T) {
	b, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	sizes := b.Sizes()
	t.Logf("sizes: %v", sizes)

	// Every value carries the apiserver's protobuf framing.
	for name, size := range sizes {
		if size == 0 {
			t.Errorf("%s: empty", name)
		}
	}
	for name, data := range map[string][]byte{"node": b.Node, "lease": b.NodeLease, "pod": b.Pod, "event": b.Event, "masterlease": b.MasterLease} {
		if !bytes.HasPrefix(data, []byte("k8s\x00")) {
			t.Errorf("%s: missing k8s protobuf prefix", name)
		}
	}

	// Sizes are in the range of what real clusters store: the kwok capture
	// measured Node 2093 B (without images), Lease 510 B, Pod 1755 B (a bare
	// pod), masterlease 62 B; a kubelet's Node with 30 images and a
	// Deployment pod with probes/env/volumes are a few KB larger.
	within := func(name string, got, lo, hi int) {
		t.Helper()
		if got < lo || got > hi {
			t.Errorf("%s: %d bytes, want within [%d, %d]", name, got, lo, hi)
		}
	}
	within("node", sizes["node.bin"], 4000, 20000)
	within("node-lease", sizes["node-lease.bin"], 350, 900)
	within("pod", sizes["pod.bin"], 2500, 8000)
	within("event", sizes["event.bin"], 500, 1500)
	within("masterlease", sizes["masterlease.bin"], 40, 120)

	// Round-trip: what we encoded decodes back to the same kinds with the
	// fields the generator set.
	obj, err := Decode(b.Node)
	if err != nil {
		t.Fatalf("decode Node: %v", err)
	}
	node, ok := obj.(*corev1.Node)
	if !ok {
		t.Fatalf("decoded Node is %T", obj)
	}
	if len(node.Status.Images) != DefaultOptions().Images {
		t.Errorf("Node has %d images, want %d", len(node.Status.Images), DefaultOptions().Images)
	}
	obj, err = Decode(b.NodeLease)
	if err != nil {
		t.Fatalf("decode Lease: %v", err)
	}
	if lease, ok := obj.(*coordinationv1.Lease); !ok || *lease.Spec.HolderIdentity != "node-000000" {
		t.Errorf("decoded Lease = %#v", obj)
	}
	obj, err = Decode(b.Pod)
	if err != nil {
		t.Fatalf("decode Pod: %v", err)
	}
	if pod, ok := obj.(*corev1.Pod); !ok || pod.Status.Phase != corev1.PodRunning || len(pod.Spec.Containers) != 2 {
		t.Errorf("decoded Pod = %#v", obj)
	}

	// Image count is the knob that sizes a Node.
	small, err := GenerateWithOptions(Options{Images: 0, Containers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(small.Node) >= len(b.Node) || len(small.Pod) >= len(b.Pod) {
		t.Errorf("fewer images/containers should shrink the blobs: node %d vs %d, pod %d vs %d", len(small.Node), len(b.Node), len(small.Pod), len(b.Pod))
	}
}
