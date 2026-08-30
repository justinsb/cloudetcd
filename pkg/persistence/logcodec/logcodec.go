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

// Package logcodec is the on-disk encoding of a log file.
//
// A file is a fixed header followed by one length-delimited protobuf message
// per record, appended over time. Each record is an etcdserverpb.WatchResponse
// carrying the record's revision (in its header) and events: the events are
// already mvccpb.Event protobufs, so this needs no generated code of our
// own, and values are stored as raw bytes. Because every record carries its
// revision, a file may be sparse in revision (a compacted file holds only the
// records that were still live).
package logcodec

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"justinsb.com/cloudetcd/pkg/persistence"
)

// magic identifies the format; the last byte is the version.
var magic = []byte("cloudetcd-log\x00\x01")

// Header is what a log file starts with.
func Header() []byte { return magic }

// Encode serializes records as a whole file: Header followed by the records,
// with revisions first, first+1, ...
func Encode(first persistence.Revision, records []*persistence.LogRecord) ([]byte, error) {
	buf, _, err := AppendRecords(append([]byte(nil), magic...), first, records)
	return buf, err
}

// AppendRecords appends the encoding of records to buf (for appending to a
// file that already has its header), giving them revisions first, first+1,
// ... It returns each record's Span relative to the start of buf.
func AppendRecords(buf []byte, first persistence.Revision, records []*persistence.LogRecord) ([]byte, []Span, error) {
	revisions := make([]persistence.Revision, len(records))
	for i := range revisions {
		revisions[i] = first + persistence.Revision(i)
	}
	return AppendRecordsAt(buf, revisions, records)
}

// WriteRecord writes one already-encoded record to w with its length
// prefix, as AppendRecords would, and returns the number of bytes written
// (the record's Span starts len(m) bytes before the end of them).
func WriteRecord(w io.Writer, m EncodedRecord) (int, error) {
	prefix := protowire.AppendVarint(nil, uint64(len(m)))
	n, err := w.Write(prefix)
	if err != nil {
		return n, err
	}
	k, err := w.Write(m)
	return n + k, err
}

// AppendRecordsAt is AppendRecords with an explicit revision per record, for
// files that are sparse in revision.
func AppendRecordsAt(buf []byte, revisions []persistence.Revision, records []*persistence.LogRecord) ([]byte, []Span, error) {
	if len(revisions) != len(records) {
		return nil, nil, fmt.Errorf("%d revisions for %d records", len(revisions), len(records))
	}
	opts := proto.MarshalOptions{}
	spans := make([]Span, 0, len(records))
	for i, r := range records {
		m, err := opts.MarshalAppend(nil, &etcdserverpb.WatchResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: int64(revisions[i])},
			Events: r.Events,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("encoding record %d: %w", i, err)
		}
		buf = protowire.AppendVarint(buf, uint64(len(m)))
		spans = append(spans, Span{Offset: len(buf), Length: len(m)})
		buf = append(buf, m...)
	}
	return buf, spans, nil
}

// DecodeRecord decodes only record i of a batch, skipping the others: each
// record is length-prefixed, so a skip is a varint read and a jump, and the
// cost is the size of one record rather than of the batch. This is what a
// log's GetLogEntry wants; Decode is for replay.
func DecodeRecord(data []byte, i int) (*persistence.LogRecord, error) {
	if !bytes.HasPrefix(data, magic) {
		return nil, fmt.Errorf("not a cloudetcd log batch (bad header)")
	}
	data = data[len(magic):]
	for n := 0; len(data) > 0; n++ {
		m, k := protowire.ConsumeBytes(data)
		if k < 0 {
			return nil, fmt.Errorf("decoding record %d: %w", n, protowire.ParseError(k))
		}
		if n == i {
			wr := &etcdserverpb.WatchResponse{}
			if err := proto.Unmarshal(m, wr); err != nil {
				return nil, fmt.Errorf("decoding record %d: %w", n, err)
			}
			return &persistence.LogRecord{Events: wr.Events}, nil
		}
		data = data[k:]
	}
	return nil, fmt.Errorf("record %d not in batch", i)
}

// Span locates one record's message bytes within an encoded batch.
type Span struct {
	Offset int
	Length int
}

// Bytes returns the record the span locates in data.
func (s Span) Bytes(data []byte) EncodedRecord {
	return EncodedRecord(data[s.Offset : s.Offset+s.Length])
}

// EncodedRecord is one record as stored: the bytes of its WatchResponse
// message, without the length prefix. It is what Span locates.
type EncodedRecord []byte

// Offsets returns the span and revision of every record in a file without
// decoding the records: it reads the length prefixes and each record's
// header. With a span, a record can be read from storage by itself (see
// DecodeMessage). A truncated final record is an error; see Scan for
// recovering a file with a torn tail.
func Offsets(data []byte) ([]Span, []persistence.Revision, error) {
	spans, revisions, complete, err := Scan(data)
	if err != nil {
		return nil, nil, err
	}
	if complete != len(data) {
		return nil, nil, fmt.Errorf("decoding record %d: truncated", len(spans))
	}
	return spans, revisions, nil
}

// Scan returns the spans and revisions of the complete records in a file,
// and the number of bytes they occupy including the header. If complete <
// len(data), the file ends in a torn record (a write that was cut short);
// the file's valid content is data[:complete].
func Scan(data []byte) (spans []Span, revisions []persistence.Revision, complete int, err error) {
	if !bytes.HasPrefix(data, magic) {
		return nil, nil, 0, fmt.Errorf("not a cloudetcd log file (bad header)")
	}
	pos := len(magic)
	for pos < len(data) {
		n, k := protowire.ConsumeVarint(data[pos:])
		if k < 0 {
			if perr := protowire.ParseError(k); errors.Is(perr, io.ErrUnexpectedEOF) {
				// Not enough bytes for the length prefix: a torn tail.
				return spans, revisions, pos, nil
			} else {
				return nil, nil, 0, fmt.Errorf("decoding record %d: %w", len(spans), perr)
			}
		}
		if uint64(len(data)-pos-k) < n {
			return spans, revisions, pos, nil
		}
		m := data[pos+k : pos+k+int(n)]
		rev, err := EncodedRecord(m).Revision()
		if err != nil {
			return nil, nil, 0, fmt.Errorf("decoding record %d: %w", len(spans), err)
		}
		spans = append(spans, Span{Offset: pos + k, Length: int(n)})
		revisions = append(revisions, rev)
		pos += k + int(n)
	}
	return spans, revisions, pos, nil
}

// Revision reads the record's revision from its header without decoding the
// rest (WatchResponse field 1, ResponseHeader field 3).
func (m EncodedRecord) Revision() (persistence.Revision, error) {
	for len(m) > 0 {
		num, typ, n := protowire.ConsumeTag(m)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		m = m[n:]
		if num == 1 && typ == protowire.BytesType {
			h, n := protowire.ConsumeBytes(m)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			for len(h) > 0 {
				hnum, htyp, hn := protowire.ConsumeTag(h)
				if hn < 0 {
					return 0, protowire.ParseError(hn)
				}
				h = h[hn:]
				if hnum == 3 && htyp == protowire.VarintType {
					v, vn := protowire.ConsumeVarint(h)
					if vn < 0 {
						return 0, protowire.ParseError(vn)
					}
					return persistence.Revision(v), nil
				}
				hn = protowire.ConsumeFieldValue(hnum, htyp, h)
				if hn < 0 {
					return 0, protowire.ParseError(hn)
				}
				h = h[hn:]
			}
			return 0, fmt.Errorf("record header has no revision")
		}
		n = protowire.ConsumeFieldValue(num, typ, m)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		m = m[n:]
	}
	return 0, fmt.Errorf("record has no header")
}

// Decode decodes the record.
func (m EncodedRecord) Decode() (*persistence.LogRecord, error) {
	wr := &etcdserverpb.WatchResponse{}
	if err := proto.Unmarshal(m, wr); err != nil {
		return nil, err
	}
	return &persistence.LogRecord{Events: wr.Events}, nil
}

// Decode parses a batch produced by Encode.
func Decode(data []byte) ([]*persistence.LogRecord, error) {
	if !bytes.HasPrefix(data, magic) {
		return nil, fmt.Errorf("not a cloudetcd log batch (bad header)")
	}
	data = data[len(magic):]
	var records []*persistence.LogRecord
	for len(data) > 0 {
		m, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, fmt.Errorf("decoding record %d: %w", len(records), protowire.ParseError(n))
		}
		data = data[n:]
		wr := &etcdserverpb.WatchResponse{}
		if err := proto.Unmarshal(m, wr); err != nil {
			return nil, fmt.Errorf("decoding record %d: %w", len(records), err)
		}
		records = append(records, &persistence.LogRecord{Events: wr.Events})
	}
	return records, nil
}

// DecodeAlias is Decode without copying: the returned record's keys and
// values alias m, so m must stay valid and unmodified for as long as the
// record is in use (a read-only mapping of a closed log file is). It parses
// exactly the fields of etcdserverpb.WatchResponse, mvccpb.Event and
// mvccpb.KeyValue that the log writes, and skips any others.
func (m EncodedRecord) DecodeAlias() (*persistence.LogRecord, error) {
	record := &persistence.LogRecord{}
	for len(m) > 0 {
		num, typ, n := protowire.ConsumeTag(m)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		m = m[n:]
		if num == 11 && typ == protowire.BytesType { // WatchResponse.events
			v, n := protowire.ConsumeBytes(m)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			ev, err := decodeEventAlias(v)
			if err != nil {
				return nil, err
			}
			record.Events = append(record.Events, ev)
			m = m[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, m)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		m = m[n:]
	}
	return record, nil
}

func decodeEventAlias(m []byte) (*mvccpb.Event, error) {
	ev := &mvccpb.Event{}
	for len(m) > 0 {
		num, typ, n := protowire.ConsumeTag(m)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		m = m[n:]
		switch {
		case num == 1 && typ == protowire.VarintType: // type
			v, n := protowire.ConsumeVarint(m)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			ev.Type = mvccpb.Event_EventType(v)
			m = m[n:]
		case (num == 2 || num == 3) && typ == protowire.BytesType: // kv, prev_kv
			v, n := protowire.ConsumeBytes(m)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			kv, err := decodeKeyValueAlias(v)
			if err != nil {
				return nil, err
			}
			if num == 2 {
				ev.Kv = kv
			} else {
				ev.PrevKv = kv
			}
			m = m[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, m)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			m = m[n:]
		}
	}
	return ev, nil
}

func decodeKeyValueAlias(m []byte) (*mvccpb.KeyValue, error) {
	kv := &mvccpb.KeyValue{}
	for len(m) > 0 {
		num, typ, n := protowire.ConsumeTag(m)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		m = m[n:]
		switch {
		case (num == 1 || num == 5) && typ == protowire.BytesType: // key, value
			v, n := protowire.ConsumeBytes(m)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			if num == 1 {
				kv.Key = v
			} else {
				kv.Value = v
			}
			m = m[n:]
		case typ == protowire.VarintType && num >= 2 && num <= 6: // create_revision, mod_revision, version, lease
			v, n := protowire.ConsumeVarint(m)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			switch num {
			case 2:
				kv.CreateRevision = int64(v)
			case 3:
				kv.ModRevision = int64(v)
			case 4:
				kv.Version = int64(v)
			case 6:
				kv.Lease = int64(v)
			}
			m = m[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, m)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			m = m[n:]
		}
	}
	return kv, nil
}
