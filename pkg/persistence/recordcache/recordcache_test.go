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

package recordcache

import (
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"

	"justinsb.com/cloudetcd/pkg/persistence"
)

func rec(valueBytes int) *persistence.LogRecord {
	return &persistence.LogRecord{Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("k"), Value: make([]byte, valueBytes)}}}}
}

func TestLRUEviction(t *testing.T) {
	one := Size(rec(1000))
	c := New(3 * one)
	for r := Revision(1); r <= 3; r++ {
		c.Put(r, rec(1000))
	}
	if c.Len() != 3 || c.Bytes() != 3*one {
		t.Fatalf("len %d bytes %d", c.Len(), c.Bytes())
	}
	c.Get(1) // 1 is now most recent; 2 is the oldest
	c.Put(4, rec(1000))
	if c.Has(2) {
		t.Error("revision 2 should have been evicted (least recently used)")
	}
	for _, r := range []Revision{1, 3, 4} {
		if !c.Has(r) {
			t.Errorf("revision %d should be cached", r)
		}
	}
	if c.Bytes() > 3*one {
		t.Errorf("over budget: %d > %d", c.Bytes(), 3*one)
	}

	// Replacing a record adjusts the accounting.
	c.Put(4, rec(10))
	if c.Bytes() != 2*one+Size(rec(10)) {
		t.Errorf("bytes after replace = %d", c.Bytes())
	}
	// A record larger than the budget is not cached and evicts nothing.
	c.Put(5, rec(100_000))
	if c.Has(5) || c.Len() != 3 {
		t.Errorf("oversized record was cached, or evicted others: len %d", c.Len())
	}
	c.Remove(1)
	if c.Has(1) || c.Len() != 2 {
		t.Errorf("remove failed: len %d", c.Len())
	}
}

func TestUnbounded(t *testing.T) {
	c := New(0)
	for r := Revision(1); r <= 1000; r++ {
		c.Put(r, rec(100))
	}
	if c.Len() != 1000 {
		t.Fatalf("unbounded cache evicted: len %d", c.Len())
	}
}
