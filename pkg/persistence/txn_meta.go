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
)

// TxnMeta represents the read and write sets of a transaction
// to enable serializability checking for batch commits
type TxnMeta struct {
	SnapshotRevision Revision
	// ReadSet maps each point key read during the transaction to the
	// mod_revision observed for that key at snapshot time. A key that did
	// not exist at snapshot time maps to revision 0. This is used for
	// per-key (backward-validation) conflict detection at commit time: a
	// read key is still valid iff it has not been written by a
	// concurrently-committed transaction since it was read.
	ReadSet map[string]Revision
	// WriteSet contains keys that will be written during the transaction
	WriteSet map[string]bool
	// HasRangeRead is true if the transaction performed a range/list read.
	// Range reads cannot be validated per-key (they are vulnerable to
	// phantoms), so a transaction that recorded one falls back to the
	// conservative whole-snapshot conflict check. Kubernetes never combines
	// a range read with a write in the same etcd transaction, so this
	// fallback is not expected to fire for the Kubernetes workload.
	HasRangeRead bool
}

// NewTxnMeta creates a new TxnEffects instance
func NewTxnMeta(snapshotRevision Revision) *TxnMeta {
	return &TxnMeta{
		ReadSet:          make(map[string]Revision),
		WriteSet:         make(map[string]bool),
		SnapshotRevision: snapshotRevision,
	}
}

// AddRead records a read of a point key, along with the mod_revision that was
// observed for that key at snapshot time (0 if the key did not exist).
func (t *TxnMeta) AddRead(key string, modRevision Revision) {
	t.ReadSet[key] = modRevision
}

// AddWrite records a write operation on a key
func (t *TxnMeta) AddWrite(key string) {
	t.WriteSet[key] = true
}

// AddList records a list-read operation. Per-key validation cannot reason
// about phantoms in a range, so we flag the transaction to fall back to the
// conservative whole-snapshot conflict check at commit time.
func (t *TxnMeta) AddList(readRange *etcdserverpb.RangeRequest) {
	t.HasRangeRead = true
}
