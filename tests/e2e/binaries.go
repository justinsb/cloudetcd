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
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// defaultKubeVersion is the kube-apiserver release used by default. Override
// with the KUBE_APISERVER_VERSION environment variable. On Linux this is the
// exact dl.k8s.io release; on macOS its minor version selects the envtest build.
const defaultKubeVersion = "v1.36.2"

func kubeVersion() string {
	if v := os.Getenv("KUBE_APISERVER_VERSION"); v != "" {
		return v
	}
	return defaultKubeVersion
}

// skipIfUnsupported skips the test on platforms where we cannot obtain a
// kube-apiserver binary. It is available for linux/{amd64,arm64} from dl.k8s.io
// and for darwin/{amd64,arm64} from the envtest (kubebuilder-tools) distribution.
func skipIfUnsupported(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "linux", "darwin":
		return
	default:
		t.Skipf("kube-apiserver e2e supports only linux (dl.k8s.io) and darwin (envtest); current platform: %s/%s", runtime.GOOS, runtime.GOARCH)
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

// ensureKubeAPIServer returns the path to a kube-apiserver binary for the
// current platform, downloading and caching it under .build/ if necessary.
// On Linux it fetches the official release from dl.k8s.io; on macOS, where
// dl.k8s.io has no server builds, it uses setup-envtest (kubebuilder-tools).
func ensureKubeAPIServer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "darwin" {
		return ensureKubeAPIServerFromEnvtest(ctx, t)
	}
	return ensureKubeAPIServerFromRelease(ctx, t)
}

// ensureKubeAPIServerFromRelease downloads the official kube-apiserver release
// binary from dl.k8s.io (Linux).
func ensureKubeAPIServerFromRelease(ctx context.Context, t *testing.T) string {
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

// ensureKubeAPIServerFromEnvtest uses setup-envtest to download the
// kubebuilder-tools bundle (which includes a darwin kube-apiserver) and returns
// the path to its kube-apiserver. envtest publishes one build per minor version,
// so we request the minor of kubeVersion() (e.g. "1.36.x") and let it pick the
// latest available patch, which may differ from the dl.k8s.io patch.
func ensureKubeAPIServerFromEnvtest(ctx context.Context, t *testing.T) string {
	t.Helper()
	binDir := filepath.Join(repoRoot(t), ".build", "envtest")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", binDir, err)
	}

	selector := envtestVersionSelector(kubeVersion())
	module := "sigs.k8s.io/controller-runtime/tools/setup-envtest@" + setupEnvtestRef()
	t.Logf("fetching kube-apiserver via setup-envtest (version %s, %s/%s) into %s", selector, runtime.GOOS, runtime.GOARCH, binDir)

	// `setup-envtest use ... -p path` downloads/caches the bundle (a no-op when
	// already present) and prints the directory containing the binaries.
	cmd := exec.CommandContext(ctx, "go", "run", module,
		"use", selector,
		"--bin-dir", binDir,
		"--os", runtime.GOOS,
		"--arch", runtime.GOARCH,
		"-p", "path",
	)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("setup-envtest failed: %v\n%s", err, stderr.String())
	}

	dir := strings.TrimSpace(string(out))
	binPath := filepath.Join(dir, "kube-apiserver")
	if _, err := os.Stat(binPath); err != nil {
		t.Fatalf("kube-apiserver not found at %s after setup-envtest: %v", binPath, err)
	}
	return binPath
}

func setupEnvtestRef() string {
	if v := os.Getenv("SETUP_ENVTEST_VERSION"); v != "" {
		return v
	}
	return "latest"
}

// envtestVersionSelector converts a kube version like "v1.36.2" into the envtest
// selector "1.36.x".
func envtestVersionSelector(v string) string {
	v = strings.TrimPrefix(v, "v")
	if parts := strings.Split(v, "."); len(parts) >= 2 {
		return parts[0] + "." + parts[1] + ".x"
	}
	return v
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
