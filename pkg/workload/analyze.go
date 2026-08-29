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
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"justinsb.com/cloudetcd/pkg/recording"
)

// Analysis summarizes a recording of kube-apiserver traffic by request shape
// and key pattern, so that it can be compared with what the model generates
// (and the model corrected where it differs).
type Analysis struct {
	Start, End time.Time
	Entries    int64

	// Shapes is keyed by a description such as "txn update /registry/leases/kube-node-lease/*".
	Shapes map[string]*ShapeStats

	// Watches is keyed by the watched key pattern.
	Watches map[string]*WatchShape

	// Streams counts stream opens by method.
	Streams map[string]int64
	// KeepAlives is the number of LeaseKeepAlive requests received.
	KeepAlives int64
	// ProgressRequests is the number of watch progress requests received.
	ProgressRequests int64

	// Nodes is the number of distinct keys under /registry/minions.
	Nodes int
}

// ShapeStats accumulates one request shape.
type ShapeStats struct {
	Count        int64
	Errors       int64
	Latency      Histogram
	Keys         map[string]struct{}
	ValueBytes   int64
	ValueSamples int64
	MinValue     int
	MaxValue     int
	// Values keeps a few representative values for blob extraction.
	values [][]byte
}

// WatchShape accumulates one watched pattern.
type WatchShape struct {
	Created   int64
	Events    int64
	Responses int64
	PrevKV    int64
	Progress  int64 // progress_notify requested
}

const maxValueSamples = 64

// Duration is the span of the recording.
func (a *Analysis) Duration() time.Duration { return a.End.Sub(a.Start) }

func (a *Analysis) shape(name string) *ShapeStats {
	s := a.Shapes[name]
	if s == nil {
		s = &ShapeStats{Keys: map[string]struct{}{}}
		a.Shapes[name] = s
	}
	return s
}

func (s *ShapeStats) addValue(v []byte) {
	n := len(v)
	s.ValueBytes += int64(n)
	s.ValueSamples++
	if s.MinValue == 0 || n < s.MinValue {
		s.MinValue = n
	}
	if n > s.MaxValue {
		s.MaxValue = n
	}
	if len(s.values) < maxValueSamples {
		s.values = append(s.values, v)
	} else {
		// Reservoir-ish: keep the set spread over the recording.
		s.values[int(s.ValueSamples)%maxValueSamples] = v
	}
}

// medianValue returns the sample value closest to the median size.
func (s *ShapeStats) medianValue() []byte {
	if len(s.values) == 0 {
		return nil
	}
	vals := append([][]byte(nil), s.values...)
	sort.Slice(vals, func(i, j int) bool { return len(vals[i]) < len(vals[j]) })
	return vals[len(vals)/2]
}

// keyPattern generalizes an apiserver key: /registry/<resource>/<ns>/<name>
// becomes /registry/<resource>/*/* except that well-known system namespaces
// are kept, so that node leases (kube-node-lease) stay distinct from other
// leases. Keys outside /registry are returned as is.
func keyPattern(key string) string {
	const prefix = "/registry/"
	if !strings.HasPrefix(key, prefix) {
		return key
	}
	parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
	if len(parts) == 0 {
		return key
	}
	out := []string{parts[0]}
	// Group-qualified resources ("apiextensions.k8s.io/customresourcedefinitions").
	rest := parts[1:]
	if len(rest) > 0 && strings.Contains(parts[0], ".") {
		out = append(out, rest[0])
		rest = rest[1:]
	}
	for i, p := range rest {
		switch {
		case i == 0 && len(rest) > 1 && isSystemNamespace(p):
			out = append(out, p)
		default:
			out = append(out, "*")
		}
	}
	return prefix + strings.Join(out, "/")
}

func isSystemNamespace(ns string) bool {
	switch ns {
	case "kube-system", "kube-node-lease", "kube-public", "default":
		return true
	}
	return false
}

// Analyze reads a recording and summarizes it.
func Analyze(path string) (*Analysis, error) {
	a := &Analysis{
		Shapes:  map[string]*ShapeStats{},
		Watches: map[string]*WatchShape{},
		Streams: map[string]int64{},
	}
	nodes := map[string]struct{}{}
	// Watches are created by a request on a stream and identified by the
	// watch_id in the server's "created" response; pending holds the patterns
	// of create requests awaiting that response, per stream.
	pending := map[int64][]string{}
	watchByID := map[[2]int64]string{}

	err := recording.ReadFile(path, func(e *recording.Entry) error {
		a.Entries++
		if a.Start.IsZero() || e.Time.Before(a.Start) {
			a.Start = e.Time
		}
		if e.Time.After(a.End) {
			a.End = e.Time
		}
		switch e.Kind {
		case recording.KindUnary:
			name, key, values, err := unaryShape(e)
			if err != nil {
				return err
			}
			s := a.shape(name)
			s.Count++
			s.Latency.Observe(e.Latency)
			if e.Error != "" {
				s.Errors++
			}
			if key != "" {
				s.Keys[key] = struct{}{}
				if strings.HasPrefix(key, "/registry/minions/") {
					nodes[key] = struct{}{}
				}
			}
			for _, v := range values {
				s.addValue(v)
			}
		case recording.KindStreamOpen:
			a.Streams[e.Method]++
		case recording.KindStreamRecv:
			switch e.Method {
			case "/etcdserverpb.Watch/Watch":
				req := &etcdserverpb.WatchRequest{}
				if err := recording.Unmarshal(e.Request, req); err != nil {
					return err
				}
				switch r := req.RequestUnion.(type) {
				case *etcdserverpb.WatchRequest_CreateRequest:
					pattern := keyPattern(string(r.CreateRequest.Key))
					w := a.Watches[pattern]
					if w == nil {
						w = &WatchShape{}
						a.Watches[pattern] = w
					}
					w.Created++
					if r.CreateRequest.PrevKv {
						w.PrevKV++
					}
					if r.CreateRequest.ProgressNotify {
						w.Progress++
					}
					pending[e.Stream] = append(pending[e.Stream], pattern)
				case *etcdserverpb.WatchRequest_ProgressRequest:
					a.ProgressRequests++
				}
			case "/etcdserverpb.Lease/LeaseKeepAlive":
				a.KeepAlives++
			}
		case recording.KindStreamSend:
			if e.Method != "/etcdserverpb.Watch/Watch" {
				return nil
			}
			resp := &etcdserverpb.WatchResponse{}
			if err := recording.Unmarshal(e.Response, resp); err != nil {
				return err
			}
			if resp.Created {
				if q := pending[e.Stream]; len(q) > 0 {
					watchByID[[2]int64{e.Stream, resp.WatchId}] = q[0]
					pending[e.Stream] = q[1:]
				}
				return nil
			}
			if pattern, ok := watchByID[[2]int64{e.Stream, resp.WatchId}]; ok {
				w := a.Watches[pattern]
				w.Responses++
				w.Events += int64(len(resp.Events))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	a.Nodes = len(nodes)
	return a, nil
}

// unaryShape classifies a unary entry, returning the shape name, the key it
// touched (if a single one) and any values written.
func unaryShape(e *recording.Entry) (name, key string, values [][]byte, err error) {
	switch e.Method {
	case "/etcdserverpb.KV/Range":
		req := &etcdserverpb.RangeRequest{}
		if err := recording.Unmarshal(e.Request, req); err != nil {
			return "", "", nil, err
		}
		return rangeShape(req), string(req.Key), nil, nil
	case "/etcdserverpb.KV/Put":
		req := &etcdserverpb.PutRequest{}
		if err := recording.Unmarshal(e.Request, req); err != nil {
			return "", "", nil, err
		}
		name := "put " + keyPattern(string(req.Key))
		if req.Lease != 0 {
			name += " lease"
		}
		return name, string(req.Key), [][]byte{req.Value}, nil
	case "/etcdserverpb.KV/DeleteRange":
		req := &etcdserverpb.DeleteRangeRequest{}
		if err := recording.Unmarshal(e.Request, req); err != nil {
			return "", "", nil, err
		}
		return "delete " + keyPattern(string(req.Key)), string(req.Key), nil, nil
	case "/etcdserverpb.KV/Txn":
		req := &etcdserverpb.TxnRequest{}
		if err := recording.Unmarshal(e.Request, req); err != nil {
			return "", "", nil, err
		}
		return txnShape(req)
	case "/etcdserverpb.Lease/LeaseGrant":
		req := &etcdserverpb.LeaseGrantRequest{}
		if err := recording.Unmarshal(e.Request, req); err != nil {
			return "", "", nil, err
		}
		return fmt.Sprintf("lease grant ttl=%ds", req.TTL), "", nil, nil
	default:
		return strings.TrimPrefix(e.Method, "/etcdserverpb."), "", nil, nil
	}
}

func rangeShape(req *etcdserverpb.RangeRequest) string {
	pattern := keyPattern(string(req.Key))
	var flags []string
	if len(req.RangeEnd) == 0 {
		flags = append(flags, "get")
	} else if req.CountOnly {
		flags = append(flags, "count")
	} else {
		flags = append(flags, "list")
		if req.Limit > 0 {
			flags = append(flags, fmt.Sprintf("limit=%d", req.Limit))
		}
	}
	if req.Revision > 0 {
		flags = append(flags, "rev")
	}
	if req.KeysOnly {
		flags = append(flags, "keys-only")
	}
	if req.Serializable {
		flags = append(flags, "serializable")
	}
	return "range " + strings.Join(flags, ",") + " " + pattern
}

func txnShape(req *etcdserverpb.TxnRequest) (name, key string, values [][]byte, err error) {
	var cmp []string
	for _, c := range req.Compare {
		if key == "" {
			key = string(c.Key)
		}
		target := strings.ToLower(c.Target.String())
		switch t := c.TargetUnion.(type) {
		case *etcdserverpb.Compare_ModRevision:
			if t.ModRevision == 0 {
				target += "=0"
			}
		case *etcdserverpb.Compare_Version:
			if t.Version == 0 {
				target += "=0"
			}
		}
		cmp = append(cmp, target)
	}
	var ops []string
	for _, op := range req.Success {
		switch o := op.Request.(type) {
		case *etcdserverpb.RequestOp_RequestPut:
			s := "put"
			if o.RequestPut.Lease != 0 {
				s += "+lease"
			}
			ops = append(ops, s)
			values = append(values, o.RequestPut.Value)
			if key == "" {
				key = string(o.RequestPut.Key)
			}
		case *etcdserverpb.RequestOp_RequestDeleteRange:
			ops = append(ops, "delete")
			if key == "" {
				key = string(o.RequestDeleteRange.Key)
			}
		case *etcdserverpb.RequestOp_RequestRange:
			ops = append(ops, "range")
			if key == "" {
				key = string(o.RequestRange.Key)
			}
		case *etcdserverpb.RequestOp_RequestTxn:
			ops = append(ops, "txn")
		}
	}
	var failure []string
	for _, op := range req.Failure {
		switch op.Request.(type) {
		case *etcdserverpb.RequestOp_RequestRange:
			failure = append(failure, "range")
		case *etcdserverpb.RequestOp_RequestPut:
			failure = append(failure, "put")
		case *etcdserverpb.RequestOp_RequestDeleteRange:
			failure = append(failure, "delete")
		}
	}
	// Name the apiserver's canonical shapes.
	switch {
	case len(cmp) == 1 && cmp[0] == "mod=0" && len(ops) == 1 && strings.HasPrefix(ops[0], "put"):
		name = "txn create"
	case len(cmp) == 1 && cmp[0] == "mod" && len(ops) == 1 && strings.HasPrefix(ops[0], "put"):
		name = "txn update"
	case len(cmp) == 1 && cmp[0] == "mod" && len(ops) == 1 && ops[0] == "delete":
		name = "txn delete"
	default:
		name = fmt.Sprintf("txn if(%s) then(%s) else(%s)", strings.Join(cmp, ","), strings.Join(ops, ","), strings.Join(failure, ","))
	}
	for _, op := range ops {
		if strings.HasSuffix(op, "+lease") {
			name += " lease"
			break
		}
	}
	return name + " " + keyPattern(key), key, values, nil
}

// Text renders the analysis.
func (a *Analysis) Text() string {
	var sb strings.Builder
	d := a.Duration()
	secs := d.Seconds()
	if secs <= 0 {
		secs = 1
	}
	fmt.Fprintf(&sb, "recording: %d entries over %s (%s .. %s), %d nodes seen\n",
		a.Entries, d.Round(time.Second), a.Start.Format(time.RFC3339), a.End.Format(time.RFC3339), a.Nodes)

	tw := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "shape\tcount\terrors\trate/s\tper-node/min\tkeys\tvalue avg\tvalue min\tvalue max\tp50\tp99\tmax")
	names := make([]string, 0, len(a.Shapes))
	for n := range a.Shapes {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool {
		if a.Shapes[names[i]].Count != a.Shapes[names[j]].Count {
			return a.Shapes[names[i]].Count > a.Shapes[names[j]].Count
		}
		return names[i] < names[j]
	})
	for _, n := range names {
		s := a.Shapes[n]
		avg := ""
		if s.ValueSamples > 0 {
			avg = fmt.Sprintf("%d", s.ValueBytes/s.ValueSamples)
		}
		perNode := ""
		if a.Nodes > 0 {
			perNode = fmt.Sprintf("%.2f", float64(s.Count)/secs*60/float64(a.Nodes))
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.3f\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			n, s.Count, s.Errors, float64(s.Count)/secs, perNode, len(s.Keys), avg, orDash(s.MinValue), orDash(s.MaxValue),
			fmtDur(s.Latency.Percentile(0.5)), fmtDur(s.Latency.Percentile(0.99)), fmtDur(s.Latency.Max()))
	}
	tw.Flush()

	if len(a.Watches) > 0 {
		tw = tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "watch\tcreated\tprev_kv\tprogress_notify\tresponses\tevents\tevents/s")
		names = names[:0]
		for n := range a.Watches {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			w := a.Watches[n]
			fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%.3f\n", n, w.Created, w.PrevKV, w.Progress, w.Responses, w.Events, float64(w.Events)/secs)
		}
		tw.Flush()
	}
	fmt.Fprintf(&sb, "streams: %v; lease keepalives: %d; watch progress requests: %d\n", a.Streams, a.KeepAlives, a.ProgressRequests)
	return sb.String()
}

func orDash(n int) string {
	if n == 0 {
		return "-"
	}
	return fmt.Sprintf("%d", n)
}

// blobPatterns maps key patterns in a recording to Blobs fields.
var blobPatterns = map[string]func(*Blobs) *[]byte{
	"/registry/minions/*":                  func(b *Blobs) *[]byte { return &b.Node },
	"/registry/leases/kube-node-lease/*":   func(b *Blobs) *[]byte { return &b.NodeLease },
	"/registry/pods/*/*":                   func(b *Blobs) *[]byte { return &b.Pod },
	"/registry/events/*/*":                 func(b *Blobs) *[]byte { return &b.Event },
	"/registry/masterleases/*":             func(b *Blobs) *[]byte { return &b.MasterLease },
	"/registry/leases/kube-node-lease/*/*": func(b *Blobs) *[]byte { return &b.NodeLease },
}

// ExtractBlobs picks a median-sized value written for each resource type in
// the analysis. Resource types absent from the recording keep their
// synthetic blob.
func (a *Analysis) ExtractBlobs() *Blobs {
	b := SyntheticBlobs()
	for pattern, field := range blobPatterns {
		// Prefer the steady-state value (what updates write, e.g. a Node with
		// its status filled in) over the value the object was created with.
		var best *ShapeStats
		bestRank := -1
		for name, s := range a.Shapes {
			if !strings.HasSuffix(name, " "+pattern) || s.ValueSamples == 0 {
				continue
			}
			rank := 0
			if strings.HasPrefix(name, "txn update") {
				rank = 1
			}
			if rank > bestRank || (rank == bestRank && s.ValueSamples > best.ValueSamples) {
				best, bestRank = s, rank
			}
		}
		if best != nil {
			if v := best.medianValue(); len(v) > 0 {
				*field(b) = v
			}
		}
	}
	return b
}
