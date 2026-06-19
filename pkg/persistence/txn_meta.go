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

package persistence

import (
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"k8s.io/klog/v2"
)

// TxnMeta represents the read and write sets of a transaction
// to enable serializability checking for batch commits
type TxnMeta struct {
	SnapshotRevision Revision
	// ReadSet contains keys that were read during the transaction
	ReadSet map[string]bool
	// WriteSet contains keys that will be written during the transaction
	WriteSet map[string]bool
}

// NewTxnMeta creates a new TxnEffects instance
func NewTxnMeta(snapshotRevision Revision) *TxnMeta {
	return &TxnMeta{
		ReadSet:          make(map[string]bool),
		WriteSet:         make(map[string]bool),
		SnapshotRevision: snapshotRevision,
	}
}

// AddRead records a read operation on a key at a specific revision
func (t *TxnMeta) AddRead(key string) {
	t.ReadSet[key] = true
}

// AddWrite records a write operation on a key
func (t *TxnMeta) AddWrite(key string) {
	t.WriteSet[key] = true
}

// AddList records a list-read operation
func (t *TxnMeta) AddList(readRange *etcdserverpb.RangeRequest) {
	klog.Warningf("AddList is not implemented")
}
