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
)

// Report summarizes one phase of a run.
type Report struct {
	Phase    string        `json:"phase"`
	Duration time.Duration `json:"duration"`

	Nodes int `json:"nodes"`
	Pods  int `json:"pods"`
	Keys  int `json:"keys"`

	// Ops is keyed by op label.
	Ops map[string]*OpReport `json:"ops"`
	// TotalOps and TotalRate cover every op.
	TotalOps  int64   `json:"totalOps"`
	TotalRate float64 `json:"totalRate"`
	Errors    int64   `json:"errors"`

	// SchedulingLag is how late ops started relative to the model's schedule.
	SchedulingLag *LatencyReport `json:"schedulingLag,omitempty"`

	// Watches is keyed by resource prefix.
	Watches map[string]*WatchReport `json:"watches,omitempty"`

	// ErrorSamples lists distinct errors seen and how often.
	ErrorSamples map[string]int64 `json:"errorSamples,omitempty"`
}

// OpReport summarizes one op label.
type OpReport struct {
	Count     int64 `json:"count"`
	Errors    int64 `json:"errors"`
	Conflicts int64 `json:"conflicts"`
	// Rate is the achieved rate per second; ExpectedRate is what the model
	// asked for (0 when the phase is not rate-driven).
	Rate         float64       `json:"rate"`
	ExpectedRate float64       `json:"expectedRate,omitempty"`
	Latency      LatencyReport `json:"latency"`
}

// WatchReport summarizes one watched prefix.
type WatchReport struct {
	Events           int64         `json:"events"`
	EventRate        float64       `json:"eventRate"`
	ProgressNotifies int64         `json:"progressNotifies"`
	Restarts         int64         `json:"restarts"`
	Lag              LatencyReport `json:"lag"`
}

// LatencyReport holds percentiles of a Histogram.
type LatencyReport struct {
	Count int64         `json:"count"`
	Mean  time.Duration `json:"mean"`
	P50   time.Duration `json:"p50"`
	P90   time.Duration `json:"p90"`
	P99   time.Duration `json:"p99"`
	P999  time.Duration `json:"p999"`
	Max   time.Duration `json:"max"`
}

func latencyReport(h *Histogram) LatencyReport {
	return LatencyReport{
		Count: h.Count(),
		Mean:  h.Mean(),
		P50:   h.Percentile(0.5),
		P90:   h.Percentile(0.9),
		P99:   h.Percentile(0.99),
		P999:  h.Percentile(0.999),
		Max:   h.Max(),
	}
}

func (r *Runner) report(phase string, stats *Stats, expected map[string]float64) *Report {
	d := time.Since(stats.start)
	secs := d.Seconds()
	rep := &Report{
		Phase:    phase,
		Duration: d,
		Nodes:    r.cfg.Nodes,
		Pods:     r.cfg.Pods(),
		Keys:     r.cfg.Keys(),
		Ops:      map[string]*OpReport{},
		Watches:  map[string]*WatchReport{},
	}
	stats.mu.RLock()
	defer stats.mu.RUnlock()
	for label, st := range stats.ops {
		count := st.Count.Load()
		errs := st.Errors.Load()
		rep.Ops[label] = &OpReport{
			Count:        count,
			Errors:       errs,
			Conflicts:    st.Conflicts.Load(),
			Rate:         float64(count) / secs,
			ExpectedRate: expected[label],
			Latency:      latencyReport(&st.Latency),
		}
		rep.TotalOps += count
		rep.Errors += errs
	}
	for label, rate := range expected {
		if _, ok := rep.Ops[label]; !ok && rate > 0 {
			rep.Ops[label] = &OpReport{ExpectedRate: rate}
		}
	}
	rep.TotalRate = float64(rep.TotalOps) / secs
	if stats.SchedulingLag.Count() > 0 {
		lr := latencyReport(&stats.SchedulingLag)
		rep.SchedulingLag = &lr
	}
	for label, ws := range stats.watches {
		rep.Watches[label] = &WatchReport{
			Events:           ws.Events.Load(),
			EventRate:        float64(ws.Events.Load()) / secs,
			ProgressNotifies: ws.ProgressNotifies.Load(),
			Restarts:         ws.Restarts.Load(),
			Lag:              latencyReport(&ws.Lag),
		}
	}
	if len(stats.errs) > 0 {
		rep.ErrorSamples = map[string]int64{}
		for msg, n := range stats.errs {
			rep.ErrorSamples[msg] = n
		}
	}
	return rep
}

// Text renders the report as a table.
func (r *Report) Text() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "== %s: %d nodes, %d pods (%d keys), %s, %d ops (%.0f/s), %d errors\n",
		r.Phase, r.Nodes, r.Pods, r.Keys, r.Duration.Round(time.Millisecond), r.TotalOps, r.TotalRate, r.Errors)
	tw := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "op\tcount\terrors\tconflicts\trate/s\texpected/s\tp50\tp90\tp99\tp99.9\tmax")
	labels := make([]string, 0, len(r.Ops))
	for l := range r.Ops {
		labels = append(labels, l)
	}
	sort.Strings(labels)
	for _, l := range labels {
		op := r.Ops[l]
		expected := ""
		if op.ExpectedRate > 0 {
			expected = fmt.Sprintf("%.2f", op.ExpectedRate)
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%.2f\t%s\t%s\t%s\t%s\t%s\t%s\n",
			l, op.Count, op.Errors, op.Conflicts, op.Rate, expected,
			fmtDur(op.Latency.P50), fmtDur(op.Latency.P90), fmtDur(op.Latency.P99), fmtDur(op.Latency.P999), fmtDur(op.Latency.Max))
	}
	tw.Flush()
	if r.SchedulingLag != nil {
		l := r.SchedulingLag
		fmt.Fprintf(&sb, "scheduling lag: p50 %s  p90 %s  p99 %s  max %s (ops started this long after they were due)\n",
			fmtDur(l.P50), fmtDur(l.P90), fmtDur(l.P99), fmtDur(l.Max))
	}
	if len(r.Watches) > 0 {
		tw = tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "watch\tevents\tevents/s\tprogress\trestarts\tlag p50\tlag p90\tlag p99\tlag max")
		names := make([]string, 0, len(r.Watches))
		for n := range r.Watches {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			w := r.Watches[n]
			fmt.Fprintf(tw, "%s\t%d\t%.2f\t%d\t%d\t%s\t%s\t%s\t%s\n", n, w.Events, w.EventRate, w.ProgressNotifies, w.Restarts,
				fmtDur(w.Lag.P50), fmtDur(w.Lag.P90), fmtDur(w.Lag.P99), fmtDur(w.Lag.Max))
		}
		tw.Flush()
	}
	if len(r.ErrorSamples) > 0 {
		msgs := make([]string, 0, len(r.ErrorSamples))
		for m := range r.ErrorSamples {
			msgs = append(msgs, m)
		}
		sort.Strings(msgs)
		fmt.Fprintln(&sb, "errors:")
		for _, m := range msgs {
			fmt.Fprintf(&sb, "  %6d  %s\n", r.ErrorSamples[m], m)
		}
	}
	return sb.String()
}

func fmtDur(d time.Duration) string {
	switch {
	case d == 0:
		return "-"
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}
