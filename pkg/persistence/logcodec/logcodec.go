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

// Package logcodec is the on-disk / in-object encoding of a batch of log
// records, shared by the file and object-store logs.
//
// A batch is a fixed header followed by one length-delimited protobuf
// message per record. Each record is an etcdserverpb.WatchResponse carrying
// the record's events: the events are already mvccpb.Event protobufs, so
// this needs no generated code of its own, values are stored as raw bytes
// rather than base64, and decoding is several times faster than the JSON
// encoding it replaces.
package logcodec

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"justinsb.com/cloudetcd/pkg/persistence"
)

// magic identifies the format; the last byte is the version.
var magic = []byte("cloudetcd-log\x00\x01")

// Header is what a log file starts with.
func Header() []byte { return magic }

// Encode serializes records as a whole file: Header followed by the records.
func Encode(records []*persistence.LogRecord) ([]byte, error) {
	return EncodeRecords(append([]byte(nil), magic...), records)
}

// EncodeRecords appends the encoding of records to buf, for appending to a
// file that already has its header.
func EncodeRecords(buf []byte, records []*persistence.LogRecord) ([]byte, error) {
	buf, _, err := AppendRecords(buf, records)
	return buf, err
}

// AppendRecords is EncodeRecords that also returns each record's Span
// relative to the start of buf.
func AppendRecords(buf []byte, records []*persistence.LogRecord) ([]byte, []Span, error) {
	opts := proto.MarshalOptions{}
	spans := make([]Span, 0, len(records))
	for i, r := range records {
		m, err := opts.MarshalAppend(nil, &etcdserverpb.WatchResponse{Events: r.Events})
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

// Offsets returns the span of every record in a file without decoding any
// of them: it reads only the length prefixes. With the spans, a record can be
// read from storage by itself (see DecodeMessage). A truncated final record
// is an error; see Scan for recovering a file with a torn tail.
func Offsets(data []byte) ([]Span, error) {
	spans, complete, err := Scan(data)
	if err != nil {
		return nil, err
	}
	if complete != len(data) {
		return nil, fmt.Errorf("decoding record %d: truncated", len(spans))
	}
	return spans, nil
}

// Scan returns the spans of the complete records in a file, and the number
// of bytes they occupy including the header. If complete < len(data), the
// file ends in a torn record (a write that was cut short); the file's valid
// content is data[:complete].
func Scan(data []byte) (spans []Span, complete int, err error) {
	if !bytes.HasPrefix(data, magic) {
		return nil, 0, fmt.Errorf("not a cloudetcd log file (bad header)")
	}
	pos := len(magic)
	for pos < len(data) {
		n, k := protowire.ConsumeVarint(data[pos:])
		if k < 0 {
			if perr := protowire.ParseError(k); errors.Is(perr, io.ErrUnexpectedEOF) {
				// Not enough bytes for the length prefix: a torn tail.
				return spans, pos, nil
			} else {
				return nil, 0, fmt.Errorf("decoding record %d: %w", len(spans), perr)
			}
		}
		if uint64(len(data)-pos-k) < n {
			return spans, pos, nil
		}
		spans = append(spans, Span{Offset: pos + k, Length: int(n)})
		pos += k + int(n)
	}
	return spans, pos, nil
}

// DecodeMessage decodes one record from its message bytes (the Span returned
// by Offsets).
func DecodeMessage(m []byte) (*persistence.LogRecord, error) {
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
