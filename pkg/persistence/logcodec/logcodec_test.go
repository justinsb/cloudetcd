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

package logcodec

import (
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"justinsb.com/cloudetcd/pkg/persistence"
)

func batch(n int) []*persistence.LogRecord {
	val := make([]byte, 1024)
	for i := range val {
		val[i] = byte(i)
	}
	var records []*persistence.LogRecord
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("/registry/leases/kube-node-lease/node-%06d", i))
		records = append(records, &persistence.LogRecord{Events: []*mvccpb.Event{
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: key, Value: val, CreateRevision: 100, ModRevision: int64(1000 + i), Version: 5, Lease: 7},
				PrevKv: &mvccpb.KeyValue{Key: key, Value: val[:10], CreateRevision: 100, ModRevision: int64(900 + i), Version: 4}},
			{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: append(key, '!'), ModRevision: int64(1000 + i)}},
		}})
	}
	// An empty record (the log's revision-1 placeholder) and one with no events.
	records = append(records, &persistence.LogRecord{}, &persistence.LogRecord{Events: []*mvccpb.Event{}})
	return records
}

func TestRoundTrip(t *testing.T) {
	in := batch(50)
	data, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("decoded %d records, want %d", len(out), len(in))
	}
	for i := range in {
		if len(out[i].Events) != len(in[i].Events) {
			t.Fatalf("record %d: %d events, want %d", i, len(out[i].Events), len(in[i].Events))
		}
		for j := range in[i].Events {
			if !proto.Equal(out[i].Events[j], in[i].Events[j]) {
				t.Fatalf("record %d event %d differs:\n got %v\nwant %v", i, j, out[i].Events[j], in[i].Events[j])
			}
		}
	}
	if _, err := Decode([]byte(`{"Records":[]}`)); err == nil {
		t.Error("decoded a JSON batch as valid")
	}
	if _, err := Decode(data[:len(data)-5]); err == nil {
		t.Error("decoded a truncated batch as valid")
	}
	if out, err := Decode(data[:len(magic)]); err != nil || len(out) != 0 {
		t.Errorf("empty batch: %v, %v", out, err)
	}
}

func BenchmarkEncode(b *testing.B) {
	records := batch(500)
	for i := 0; i < b.N; i++ {
		if _, err := Encode(records); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	data, _ := Encode(batch(500))
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		if _, err := Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}
