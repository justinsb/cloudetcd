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
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// defaultKubeVersion is the kube-apiserver release downloaded by default.
// Override with the KUBE_APISERVER_VERSION environment variable.
const defaultKubeVersion = "v1.36.2"

func kubeVersion() string {
	if v := os.Getenv("KUBE_APISERVER_VERSION"); v != "" {
		return v
	}
	return defaultKubeVersion
}

// skipIfUnsupported skips the test on platforms where kube-apiserver is not
// published. dl.k8s.io ships kube-apiserver only for linux/{amd64,arm64};
// macOS users normally obtain it via envtest/kubebuilder instead.
func skipIfUnsupported(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skipf("kube-apiserver is published only for linux on dl.k8s.io (current platform: %s/%s); run this e2e in CI or a Linux environment", runtime.GOOS, runtime.GOARCH)
	}
}

// repoRoot walks up from the working directory to find the git repository root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo root (.git) walking up from %s", dir)
		}
		dir = parent
	}
}

// ensureKubeAPIServer downloads (and caches under .build/) the kube-apiserver
// binary for the current platform, returning the path to the executable.
func ensureKubeAPIServer(ctx context.Context, t *testing.T) string {
	t.Helper()
	version := kubeVersion()
	cacheDir := filepath.Join(repoRoot(t), ".build", "e2e-bin", version)
	binPath := filepath.Join(cacheDir, "kube-apiserver")

	if fi, err := os.Stat(binPath); err == nil && fi.Mode().IsRegular() {
		return binPath
	}

	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", cacheDir, err)
	}

	url := fmt.Sprintf("https://dl.k8s.io/release/%s/bin/%s/%s/kube-apiserver", version, runtime.GOOS, runtime.GOARCH)
	t.Logf("downloading kube-apiserver %s (%s/%s) from %s", version, runtime.GOOS, runtime.GOARCH, url)

	tmp := binPath + ".tmp"
	if err := download(ctx, url, tmp); err != nil {
		_ = os.Remove(tmp)
		t.Fatalf("download kube-apiserver: %v", err)
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		t.Fatalf("chmod kube-apiserver: %v", err)
	}
	if err := os.Rename(tmp, binPath); err != nil {
		t.Fatalf("rename kube-apiserver into place: %v", err)
	}
	return binPath
}

func download(ctx context.Context, url, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: unexpected status %s", url, resp.Status)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
