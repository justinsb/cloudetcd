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
	"unsafe"

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
	data, err := Encode(1, in)
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
	if _, err := Decode(data[:len(data)-3]); err == nil {
		t.Error("decoded a truncated batch as valid")
	}
	if out, err := Decode(data[:len(magic)]); err != nil || len(out) != 0 {
		t.Errorf("empty batch: %v, %v", out, err)
	}
}

func TestDecodeRecord(t *testing.T) {
	in := batch(50)
	data, err := Encode(1, in)
	if err != nil {
		t.Fatal(err)
	}
	for i := range in {
		got, err := DecodeRecord(data, i)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if len(got.Events) != len(in[i].Events) {
			t.Fatalf("record %d: %d events, want %d", i, len(got.Events), len(in[i].Events))
		}
		for j := range in[i].Events {
			if !proto.Equal(got.Events[j], in[i].Events[j]) {
				t.Fatalf("record %d event %d differs", i, j)
			}
		}
	}
	if _, err := DecodeRecord(data, len(in)); err == nil {
		t.Error("decoded a record past the end of the batch")
	}
	if _, err := DecodeRecord(data[:len(data)-3], len(in)-1); err == nil {
		t.Error("decoded the last record of a truncated batch")
	}
}

func TestOffsets(t *testing.T) {
	in := batch(50)
	data, err := Encode(1, in)
	if err != nil {
		t.Fatal(err)
	}
	spans, revisions, err := Offsets(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != len(in) {
		t.Fatalf("%d spans, want %d", len(spans), len(in))
	}
	for i, r := range revisions {
		if r != persistence.Revision(i+1) {
			t.Fatalf("record %d has revision %d, want %d", i, r, i+1)
		}
	}
	for i, sp := range spans {
		got, err := DecodeMessage(data[sp.Offset : sp.Offset+sp.Length])
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if len(got.Events) != len(in[i].Events) {
			t.Fatalf("record %d: %d events, want %d", i, len(got.Events), len(in[i].Events))
		}
		for j := range in[i].Events {
			if !proto.Equal(got.Events[j], in[i].Events[j]) {
				t.Fatalf("record %d event %d differs", i, j)
			}
		}
	}
	if _, _, err := Offsets(data[:len(data)-3]); err == nil {
		t.Error("offsets of a truncated batch succeeded")
	}

	// A sparse file: explicit revisions survive a scan.
	sparse, _, err := AppendRecordsAt(append([]byte(nil), Header()...), []persistence.Revision{3, 7, 500}, in[:3])
	if err != nil {
		t.Fatal(err)
	}
	if _, revs, err := Offsets(sparse); err != nil || fmt.Sprint(revs) != "[3 7 500]" {
		t.Fatalf("sparse revisions = %v, %v", revs, err)
	}
}

func TestDecodeMessageAlias(t *testing.T) {
	in := batch(50)
	data, err := Encode(1, in)
	if err != nil {
		t.Fatal(err)
	}
	spans, _, err := Offsets(data)
	if err != nil {
		t.Fatal(err)
	}
	for i, sp := range spans {
		m := data[sp.Offset : sp.Offset+sp.Length]
		got, err := DecodeMessageAlias(m)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		want, _ := DecodeMessage(m)
		if len(got.Events) != len(want.Events) {
			t.Fatalf("record %d: %d events, want %d", i, len(got.Events), len(want.Events))
		}
		for j := range want.Events {
			if !proto.Equal(got.Events[j], want.Events[j]) {
				t.Fatalf("record %d event %d:\n got %v\nwant %v", i, j, got.Events[j], want.Events[j])
			}
		}
		// The alias decoder shares the input buffer rather than copying.
		if len(got.Events) > 0 && got.Events[0].Kv != nil && len(got.Events[0].Kv.Value) > 0 {
			v := got.Events[0].Kv.Value
			start, end := uintptr(unsafe.Pointer(&m[0])), uintptr(unsafe.Pointer(&m[len(m)-1]))
			if p := uintptr(unsafe.Pointer(&v[0])); p < start || p > end {
				t.Fatalf("record %d: value was copied instead of aliased", i)
			}
		}
	}
}

func BenchmarkDecodeMessageAlias(b *testing.B) {
	data, _ := Encode(1, batch(500))
	spans, _, _ := Offsets(data)
	sp := spans[250]
	m := data[sp.Offset : sp.Offset+sp.Length]
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeMessageAlias(m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeMessage(b *testing.B) {
	data, _ := Encode(1, batch(500))
	spans, _, _ := Offsets(data)
	sp := spans[250]
	m := data[sp.Offset : sp.Offset+sp.Length]
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeMessage(m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeRecord(b *testing.B) {
	data, _ := Encode(1, batch(500))
	for i := 0; i < b.N; i++ {
		if _, err := DecodeRecord(data, 250); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	records := batch(500)
	for i := 0; i < b.N; i++ {
		if _, err := Encode(1, records); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	data, _ := Encode(1, batch(500))
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		if _, err := Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}
