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

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// kwok is only used by TestCaptureKwok, so its download helpers live in a
// test file (the unused lint runs on non-test code).

// defaultKwokVersion is the kwok release used for traffic captures. Override
// with KWOK_VERSION.
const defaultKwokVersion = "v0.8.0"

// kwokStages are the Stage configs (relative to kustomize/stage in the kwok
// repo) that make the kwok controller behave like kubelets: nodes register
// and go Ready, renew their leases and report status, and pods go Running,
// complete and delete. The standalone binary ships without any.
var kwokStages = []string{
	"node/fast/node-initialize.yaml",
	"node/heartbeat-with-lease/node-heartbeat-with-lease.yaml",
	"pod/fast/pod-ready.yaml",
	"pod/fast/pod-complete.yaml",
	"pod/fast/pod-delete.yaml",
}

// kwokInstall is a kwok controller binary with its stage configs.
type kwokInstall struct {
	binPath string
	stages  []string
}

// ensureKwok returns a kwok controller for the current platform, downloading
// and caching it (and its stage configs) under .build/ if necessary.
func ensureKwok(ctx context.Context, t *testing.T) *kwokInstall {
	t.Helper()
	version := os.Getenv("KWOK_VERSION")
	if version == "" {
		version = defaultKwokVersion
	}
	cacheDir := filepath.Join(repoRoot(t), ".build", "e2e-bin", "kwok-"+version)
	if err := os.MkdirAll(filepath.Join(cacheDir, "stages"), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", cacheDir, err)
	}

	install := &kwokInstall{binPath: filepath.Join(cacheDir, "kwok")}
	if fi, err := os.Stat(install.binPath); err != nil || !fi.Mode().IsRegular() {
		url := fmt.Sprintf("https://github.com/kubernetes-sigs/kwok/releases/download/%s/kwok-%s-%s", version, runtime.GOOS, runtime.GOARCH)
		t.Logf("downloading kwok %s from %s", version, url)
		downloadTo(ctx, t, url, install.binPath, 0o755)
	}
	for _, stage := range kwokStages {
		path := filepath.Join(cacheDir, "stages", filepath.Base(stage))
		if fi, err := os.Stat(path); err != nil || !fi.Mode().IsRegular() {
			url := fmt.Sprintf("https://raw.githubusercontent.com/kubernetes-sigs/kwok/%s/kustomize/stage/%s", version, stage)
			downloadTo(ctx, t, url, path, 0o644)
		}
		install.stages = append(install.stages, path)
	}
	return install
}

// downloadTo downloads url to dest atomically with the given mode.
func downloadTo(ctx context.Context, t *testing.T, url, dest string, mode os.FileMode) {
	t.Helper()
	tmp := dest + ".tmp"
	if err := download(ctx, url, tmp); err != nil {
		_ = os.Remove(tmp)
		t.Fatalf("download %s: %v", url, err)
	}
	if err := os.Chmod(tmp, mode); err != nil {
		t.Fatalf("chmod %s: %v", tmp, err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		t.Fatalf("rename %s into place: %v", dest, err)
	}
}
