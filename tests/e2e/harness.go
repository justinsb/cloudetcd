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

// Package e2e contains end-to-end tests that run a real kube-apiserver against
// cloudetcd. They are gated on the RUN_E2E environment variable (set
// automatically by `ap e2e`) and only run on Linux, where kube-apiserver is
// available for download.
package e2e

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"justinsb.com/cloudetcd/pkg/api"
	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/logfactory"
	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
	"justinsb.com/cloudetcd/pkg/storage/memorystorage"
)

// Harness runs a cloudetcd instance and a kube-apiserver pointed at it, and
// exposes a Kubernetes client connected to that apiserver.
type Harness struct {
	t *testing.T

	// Client is a Kubernetes client authenticated as a cluster admin.
	Client *kubernetes.Clientset
	// RESTConfig is the rest.Config backing Client, for tests that need their
	// own typed/dynamic clients.
	RESTConfig *rest.Config
	// CloudetcdAddr is the host:port cloudetcd listens on.
	CloudetcdAddr string
}

// NewHarness starts cloudetcd and a kube-apiserver and returns a ready Harness.
// All processes are torn down via t.Cleanup. It skips the test on unsupported
// platforms.
func NewHarness(t *testing.T) *Harness {
	t.Helper()
	skipIfUnsupported(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	workDir := t.TempDir()

	cloudetcdAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	startCloudetcd(ctx, t, cloudetcdAddr)

	p := generatePKI(t, workDir)

	apiserverPort := freePort(t)
	binPath := ensureKubeAPIServer(ctx, t)
	startKubeAPIServer(ctx, t, binPath, workDir, p, cloudetcdAddr, apiserverPort)

	host := fmt.Sprintf("https://127.0.0.1:%d", apiserverPort)
	restConfig := &rest.Config{
		Host: host,
		TLSClientConfig: rest.TLSClientConfig{
			CAData:   p.caCertPEM,
			CertData: p.adminCertPEM,
			KeyData:  p.adminKeyPEM,
		},
	}

	waitForAPIServerReady(ctx, t, host, p)

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatalf("build kubernetes client: %v", err)
	}

	return &Harness{
		t:             t,
		Client:        client,
		RESTConfig:    restConfig,
		CloudetcdAddr: cloudetcdAddr,
	}
}

// freePort asks the OS for an unused TCP port. There is a small TOCTOU window
// between closing the listener and the port being reused, which is acceptable
// for tests.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startCloudetcd starts an in-process cloudetcd server on addr and blocks until
// it is serving. The backend defaults to an in-memory log; set E2E_CLOUDETCD_LOG
// (e.g. "filesystem:///tmp/cloudetcd-e2e") to use a different backend.
func startCloudetcd(ctx context.Context, t *testing.T, addr string) {
	t.Helper()

	var lg persistence.Log
	if uri := os.Getenv("E2E_CLOUDETCD_LOG"); uri != "" {
		var err error
		if lg, err = logfactory.NewLog(ctx, uri); err != nil {
			t.Fatalf("create log %q: %v", uri, err)
		}
	} else {
		lg = memorylog.New()
	}

	store, err := memorystorage.NewMemoryStorage(lg)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	server := api.NewServer(store)
	errCh := make(chan error, 1)
	go func() { errCh <- server.Start(ctx, addr) }()
	t.Cleanup(func() { _ = server.GracefulStop() })

	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("cloudetcd exited before becoming ready: %v", err)
		default:
		}

		cli, err := clientv3.New(clientv3.Config{Endpoints: []string{addr}, DialTimeout: 2 * time.Second})
		if err == nil {
			cctx, ccancel := context.WithTimeout(ctx, 2*time.Second)
			_, gErr := cli.Get(cctx, "e2e-readiness-probe")
			ccancel()
			_ = cli.Close()
			if gErr == nil {
				return
			}
			lastErr = gErr
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("cloudetcd did not become ready at %s: %v", addr, lastErr)
}

// startKubeAPIServer launches kube-apiserver as a subprocess pointed at
// cloudetcd. It mirrors the flags documented in docs/start-kube.md.
func startKubeAPIServer(ctx context.Context, t *testing.T, binPath, workDir string, p *pki, cloudetcdAddr string, port int) {
	t.Helper()

	certDir := filepath.Join(workDir, "apiserver")
	if err := os.MkdirAll(certDir, 0o755); err != nil {
		t.Fatalf("mkdir apiserver cert dir: %v", err)
	}

	args := []string{
		"--etcd-servers=http://" + cloudetcdAddr,
		fmt.Sprintf("--secure-port=%d", port),
		"--bind-address=127.0.0.1",
		"--advertise-address=127.0.0.1",
		"--cert-dir=" + certDir,
		"--tls-cert-file=" + p.servingCertFile,
		"--tls-private-key-file=" + p.servingKeyFile,
		"--client-ca-file=" + p.caCertFile,
		"--service-account-key-file=" + p.saKeyFile,
		"--service-account-signing-key-file=" + p.saKeyFile,
		"--service-account-issuer=https://kubernetes.default.svc.cluster.local",
		"--authorization-mode=AlwaysAllow",
		"--service-cluster-ip-range=10.0.0.0/24",
		"--allow-privileged=true",
		"--disable-admission-plugins=ServiceAccount",
	}

	cmd := exec.CommandContext(ctx, binPath, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start kube-apiserver: %v", err)
	}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
		// Safe to read out only after Wait(): the os/exec copy goroutines have
		// finished by then.
		if t.Failed() {
			t.Logf("kube-apiserver args: %v", args)
			t.Logf("kube-apiserver output (tail):\n%s", tail(out.String(), 8000))
		}
	})
}

// waitForAPIServerReady polls /readyz until the apiserver reports ready. A 200
// on /readyz means all post-start hooks (including the bootstrap controllers
// that write the default namespaces and RBAC policy into cloudetcd) succeeded.
func waitForAPIServerReady(ctx context.Context, t *testing.T, host string, p *pki) {
	t.Helper()

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(p.caCertPEM) {
		t.Fatalf("failed to load CA cert into pool")
	}
	clientCert, err := tls.X509KeyPair(p.adminCertPEM, p.adminKeyPEM)
	if err != nil {
		t.Fatalf("load admin keypair: %v", err)
	}
	httpc := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      caPool,
				Certificates: []tls.Certificate{clientCert},
			},
		},
	}

	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, host+"/readyz", nil)
		resp, err := httpc.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return
		}
		lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, tail(string(body), 500))
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("kube-apiserver did not become ready at %s/readyz: %v", host, lastErr)
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "...(truncated)...\n" + s[len(s)-n:]
}
