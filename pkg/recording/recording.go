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

// Package recording captures the etcd gRPC traffic a cloudetcd server receives
// as a JSONL file, one Entry per line. Recordings are the raw material for the
// workload model in pkg/workload: they are analyzed offline to learn what the
// traffic from a kube-apiserver looks like (key patterns, request shapes,
// cadences, value sizes) so that it can be regenerated at a different scale
// without a kube-apiserver in the loop.
//
// Messages are encoded with protojson (proto field names), so a recording can
// be decoded back into the exact etcdserverpb messages.
package recording

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Kind identifies what an Entry describes.
type Kind string

const (
	// KindUnary is a complete unary RPC: request, response (or error) and latency.
	KindUnary Kind = "unary"
	// KindStreamOpen marks the start of a streaming RPC (Watch, LeaseKeepAlive).
	KindStreamOpen Kind = "stream-open"
	// KindStreamRecv is a message received from the client on a stream.
	KindStreamRecv Kind = "stream-recv"
	// KindStreamSend is a message sent to the client on a stream.
	KindStreamSend Kind = "stream-send"
	// KindStreamClose marks the end of a streaming RPC; Latency is its lifetime.
	KindStreamClose Kind = "stream-close"
)

// Entry is one line of a recording.
type Entry struct {
	// Time is when the entry was recorded (for unary RPCs, when the request arrived).
	Time time.Time `json:"time"`
	Kind Kind      `json:"kind"`
	// Method is the full gRPC method, e.g. "/etcdserverpb.KV/Txn".
	Method string `json:"method"`
	// Stream identifies the stream for stream-* entries; unique within a recording.
	Stream int64 `json:"stream,omitempty"`
	// Request is the protojson-encoded request (unary, stream-recv).
	Request json.RawMessage `json:"request,omitempty"`
	// Response is the protojson-encoded response (unary, stream-send).
	Response json.RawMessage `json:"response,omitempty"`
	// Latency is the server-side handling time (unary) or stream lifetime (stream-close).
	Latency time.Duration `json:"latency,omitempty"`
	Error   string        `json:"error,omitempty"`
}

var marshalOptions = protojson.MarshalOptions{UseProtoNames: true}

// Recorder writes entries to a JSONL stream. It is safe for concurrent use.
type Recorder struct {
	mu  sync.Mutex
	w   *bufio.Writer
	c   io.Closer
	err error

	nextStream atomic.Int64
	entries    atomic.Int64
}

// NewFileRecorder creates a Recorder that writes to path, truncating it.
func NewFileRecorder(path string) (*Recorder, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating recording %q: %w", path, err)
	}
	return NewRecorder(f), nil
}

// NewRecorder creates a Recorder writing to w. If w is an io.Closer, Close
// closes it.
func NewRecorder(w io.Writer) *Recorder {
	r := &Recorder{w: bufio.NewWriterSize(w, 1<<20)}
	if c, ok := w.(io.Closer); ok {
		r.c = c
	}
	return r
}

// Entries returns the number of entries written so far.
func (r *Recorder) Entries() int64 { return r.entries.Load() }

// Record appends an entry.
func (r *Recorder) Record(e *Entry) {
	b, err := json.Marshal(e)
	if err != nil {
		r.setErr(err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	if _, err := r.w.Write(b); err != nil {
		r.err = err
		return
	}
	if err := r.w.WriteByte('\n'); err != nil {
		r.err = err
		return
	}
	r.entries.Add(1)
}

func (r *Recorder) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

// Flush writes buffered entries to the underlying writer.
func (r *Recorder) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	return r.w.Flush()
}

// Close flushes and closes the recording, returning the first error encountered.
func (r *Recorder) Close() error {
	err := r.Flush()
	if r.c != nil {
		if cerr := r.c.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

func encode(m any) json.RawMessage {
	pm, ok := m.(proto.Message)
	if !ok {
		b, _ := json.Marshal(fmt.Sprintf("%T", m))
		return b
	}
	b, err := marshalOptions.Marshal(pm)
	if err != nil {
		b, _ = json.Marshal(fmt.Sprintf("marshal error: %v", err))
	}
	return b
}

// UnaryInterceptor returns a gRPC unary server interceptor that records each call.
func (r *Recorder) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		e := &Entry{
			Time:    start,
			Kind:    KindUnary,
			Method:  info.FullMethod,
			Request: encode(req),
			Latency: time.Since(start),
		}
		if err != nil {
			e.Error = err.Error()
		} else {
			e.Response = encode(resp)
		}
		r.Record(e)
		return resp, err
	}
}

// StreamInterceptor returns a gRPC stream server interceptor that records the
// open/close of each stream and every message exchanged on it.
func (r *Recorder) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		id := r.nextStream.Add(1)
		start := time.Now()
		r.Record(&Entry{Time: start, Kind: KindStreamOpen, Method: info.FullMethod, Stream: id})
		err := handler(srv, &recordingStream{ServerStream: ss, r: r, id: id, method: info.FullMethod})
		e := &Entry{Time: time.Now(), Kind: KindStreamClose, Method: info.FullMethod, Stream: id, Latency: time.Since(start)}
		if err != nil {
			e.Error = err.Error()
		}
		r.Record(e)
		return err
	}
}

type recordingStream struct {
	grpc.ServerStream
	r      *Recorder
	id     int64
	method string
}

func (s *recordingStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.r.Record(&Entry{Time: time.Now(), Kind: KindStreamRecv, Method: s.method, Stream: s.id, Request: encode(m)})
	}
	return err
}

func (s *recordingStream) SendMsg(m any) error {
	s.r.Record(&Entry{Time: time.Now(), Kind: KindStreamSend, Method: s.method, Stream: s.id, Response: encode(m)})
	return s.ServerStream.SendMsg(m)
}

// maxLineSize bounds a single recording line; values are k8s objects, which are
// at most a few MB, and List responses can carry many of them.
const maxLineSize = 256 << 20

// Read decodes entries from r, calling fn for each. fn returning an error stops
// the read and returns that error.
func Read(r io.Reader, fn func(*Entry) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), maxLineSize)
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		e := &Entry{}
		if err := json.Unmarshal(b, e); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return sc.Err()
}

// ReadFile is Read over the file at path.
func ReadFile(path string, fn func(*Entry) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return Read(f, fn)
}

// Unmarshal decodes a protojson message from a recording into m.
func Unmarshal(raw json.RawMessage, m proto.Message) error {
	return protojson.Unmarshal(raw, m)
}
