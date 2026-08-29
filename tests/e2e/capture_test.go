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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"justinsb.com/cloudetcd/pkg/api"
	"justinsb.com/cloudetcd/pkg/recording"
	"justinsb.com/cloudetcd/pkg/workload"
)

// TestCaptureKwok records the etcd traffic a kube-apiserver generates for a
// cluster of CAPTURE_NODES kwok-simulated nodes running CAPTURE_PODS_PER_NODE
// pods each, over CAPTURE_DURATION of steady state, plus the pods' deletion.
// The recording (CAPTURE_OUT, default .build/capture/kwok-<n>n-<p>p.jsonl)
// is the raw material for the traffic model in pkg/workload:
//
//	RUN_CAPTURE=1 CAPTURE_NODES=20 CAPTURE_DURATION=2m go test ./tests/e2e -run TestCaptureKwok -v
//	go run ./cmd/cloud-etcd-bench analyze .build/capture/kwok-20n-5p.jsonl
//	go run ./cmd/cloud-etcd-bench extract-blobs .build/capture/kwok-20n-5p.jsonl pkg/workload/blobs
//
// kwok fakes the kubelets (node registration, lease renewal, status reports,
// pod status) without running containers, so this scales to thousands of
// nodes on one machine. What it does not reproduce is a real kubelet's object
// sizes (a real Node carries its image list) and the controller-manager's
// traffic, since no controller-manager runs here.
func TestCaptureKwok(t *testing.T) {
	if os.Getenv("RUN_CAPTURE") == "" {
		t.Skip("Skipping capture; set RUN_CAPTURE=1 to run")
	}
	nodes := envInt(t, "CAPTURE_NODES", 10)
	podsPerNode := envInt(t, "CAPTURE_PODS_PER_NODE", 5)
	duration := envDuration(t, "CAPTURE_DURATION", time.Minute)
	out := os.Getenv("CAPTURE_OUT")
	if out == "" {
		out = filepath.Join(repoRoot(t), ".build", "capture", fmt.Sprintf("kwok-%dn-%dp.jsonl", nodes, podsPerNode))
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		t.Fatal(err)
	}

	recorder, err := recording.NewFileRecorder(out)
	if err != nil {
		t.Fatal(err)
	}
	// Registered before the harness so that it runs after the servers stop.
	t.Cleanup(func() {
		if err := recorder.Close(); err != nil {
			t.Errorf("closing recording: %v", err)
		}
		t.Logf("recording written to %s (%d entries)", out, recorder.Entries())
	})

	ctx := t.Context()
	kwok := ensureKwok(ctx, t)
	h := NewHarnessWithOptions(t, HarnessOptions{ServerOptions: []api.Option{api.WithRecorder(recorder)}})
	startKwok(ctx, t, kwok, h.Kubeconfig)

	// Register nodes. kwok --manage-all-nodes adopts every node and plays its
	// kubelet: it marks it Ready, renews its lease and reports status.
	t.Logf("creating %d nodes", nodes)
	for i := 0; i < nodes; i++ {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:        fmt.Sprintf("kwok-node-%d", i),
				Annotations: map[string]string{"kwok.x-k8s.io/node": "fake"},
				Labels:      map[string]string{"type": "kwok"},
			},
			Spec: corev1.NodeSpec{
				Taints: []corev1.Taint{{Key: "kwok.x-k8s.io/node", Value: "fake", Effect: corev1.TaintEffectNoSchedule}},
			},
			Status: corev1.NodeStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("32"),
					corev1.ResourceMemory: resource.MustParse("256Gi"),
					corev1.ResourcePods:   resource.MustParse("110"),
				},
			},
		}
		if _, err := h.Client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
	}
	waitFor(t, 2*time.Minute, "all nodes Ready", func() (bool, error) {
		list, err := h.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		ready := 0
		for _, n := range list.Items {
			for _, c := range n.Status.Conditions {
				if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
					ready++
				}
			}
		}
		return ready == nodes, nil
	})

	// Schedule pods directly onto the nodes (there is no scheduler); kwok
	// reports them Running.
	const ns = "capture"
	if _, err := h.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	t.Logf("creating %d pods", nodes*podsPerNode)
	for i := 0; i < nodes; i++ {
		for j := 0; j < podsPerNode; j++ {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d-%d", i, j), Namespace: ns, Labels: map[string]string{"app": "capture"}},
				Spec: corev1.PodSpec{
					NodeName:    fmt.Sprintf("kwok-node-%d", i),
					Containers:  []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
					Tolerations: []corev1.Toleration{{Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpExists}},
				},
			}
			if _, err := h.Client.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				t.Fatalf("create pod %s: %v", pod.Name, err)
			}
		}
	}
	waitFor(t, 5*time.Minute, "all pods Running", func() (bool, error) {
		list, err := h.Client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		running := 0
		for _, p := range list.Items {
			if p.Status.Phase == corev1.PodRunning {
				running++
			}
		}
		return running == nodes*podsPerNode, nil
	})

	t.Logf("steady state for %s", duration)
	select {
	case <-time.After(duration):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// Tear the pods down so the recording includes graceful deletion.
	t.Logf("deleting pods")
	if err := h.Client.CoreV1().Pods(ns).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}); err != nil {
		t.Fatalf("delete pods: %v", err)
	}
	waitFor(t, 5*time.Minute, "all pods gone", func() (bool, error) {
		list, err := h.Client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		return len(list.Items) == 0, nil
	})

	if err := recorder.Flush(); err != nil {
		t.Fatal(err)
	}
	analysis, err := workload.Analyze(out)
	if err != nil {
		t.Fatalf("analyzing recording: %v", err)
	}
	t.Logf("analysis of %s:\n%s", out, analysis.Text())
}

// startKwok runs the kwok controller against the apiserver in kubeconfig,
// managing every node in the cluster.
func startKwok(ctx context.Context, t *testing.T, kwok *kwokInstall, kubeconfig string) {
	t.Helper()
	args := []string{
		"--kubeconfig=" + kubeconfig,
		"--manage-all-nodes=true",
		"--node-lease-duration-seconds=40", // kubelet default: renewed every 10s
	}
	for _, stage := range kwok.stages {
		args = append(args, "--config="+stage)
	}
	cmd := exec.CommandContext(ctx, kwok.binPath, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start kwok: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("kwok args: %v", args)
			t.Logf("kwok output (tail):\n%s", tail(out.String(), 8000))
		}
	})
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Second)
	}
}

func envInt(t *testing.T, name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return n
}

func envDuration(t *testing.T, name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return d
}
