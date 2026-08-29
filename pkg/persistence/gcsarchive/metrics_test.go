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

package gcsarchive

import (
	"testing"
)

func TestMetricsClientOptions(t *testing.T) {
	grid := []struct {
		name     string
		endpoint string
	}{
		{name: "unset disables client metrics", endpoint: ""},
		{name: "tcp collector endpoint", endpoint: "http://collector.observability.svc:4317"},
		{name: "unix domain socket endpoint", endpoint: "unix:///run/collector/otlp.sock"},
	}
	for _, testCase := range grid {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", testCase.endpoint)
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
			opts, err := metricsClientOptions(t.Context())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(opts) != 1 {
				t.Fatalf("expected exactly one client option, got %d", len(opts))
			}
		})
	}
}
