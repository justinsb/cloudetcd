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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

// DefaultBlobs returns the values used when none are supplied: synthetic
// bytes sized from a capture (see SyntheticBlobs).
func DefaultBlobs() *Blobs { return SyntheticBlobs() }

// Blobs holds the values written for each resource type. cloudetcd treats
// values as opaque bytes, so these can be real protobuf-encoded Kubernetes
// objects captured from a recording (see cmd/cloud-etcd-bench extract-blobs)
// or synthetic bytes of a representative size.
type Blobs struct {
	Node        []byte
	NodeLease   []byte
	Pod         []byte
	Event       []byte
	MasterLease []byte
}

// blobFiles maps the file name used by LoadBlobs/SaveBlobs to the field.
func (b *Blobs) fields() map[string]*[]byte {
	return map[string]*[]byte{
		"node.bin":        &b.Node,
		"node-lease.bin":  &b.NodeLease,
		"pod.bin":         &b.Pod,
		"event.bin":       &b.Event,
		"masterlease.bin": &b.MasterLease,
	}
}

// Sizes returns the size in bytes of each blob, keyed by file name.
func (b *Blobs) Sizes() map[string]int {
	sizes := map[string]int{}
	for name, p := range b.fields() {
		sizes[name] = len(*p)
	}
	return sizes
}

// SyntheticBlobs returns blobs sized like the protobuf-encoded objects a
// kube-apiserver v1.36 stored for kwok nodes (tests/e2e TestCaptureKwok,
// 10 nodes x 5 pods): Node 2093 B, Lease 510 B, Pod 1755 B, masterlease
// 62 B. A real kubelet's Node is larger (it carries the node's image list);
// no Events were captured, so that size is an estimate. Only the sizes
// matter to cloudetcd, which never inspects values.
func SyntheticBlobs() *Blobs {
	return &Blobs{
		Node:        syntheticBytes(1, 2093),
		NodeLease:   syntheticBytes(2, 510),
		Pod:         syntheticBytes(3, 1755),
		Event:       syntheticBytes(4, 640),
		MasterLease: syntheticBytes(5, 62),
	}
}

// syntheticBytes returns n deterministic pseudo-random bytes. Protobuf-encoded
// objects are mostly text with some binary framing; incompressible bytes are
// a conservative stand-in.
func syntheticBytes(seed int64, n int) []byte {
	r := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	r.Read(b)
	return b
}

// LoadBlobs reads blobs from dir (node.bin, node-lease.bin, pod.bin,
// event.bin, masterlease.bin). Missing files fall back to the synthetic blob.
func LoadBlobs(dir string) (*Blobs, error) {
	b := SyntheticBlobs()
	for name, p := range b.fields() {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reading blob %q: %w", name, err)
		}
		*p = data
	}
	return b, nil
}

// SaveBlobs writes the blobs to dir.
func (b *Blobs) SaveBlobs(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for name, p := range b.fields() {
		if len(*p) == 0 {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, name), *p, 0o644); err != nil {
			return fmt.Errorf("writing blob %q: %w", name, err)
		}
	}
	return nil
}
