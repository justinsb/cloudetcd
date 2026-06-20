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
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// TestKubeAPIServerSmoke boots a real kube-apiserver backed by cloudetcd and
// exercises create/get/list/watch/delete through it. Reaching this point at all
// means cloudetcd successfully served the apiserver's bootstrap writes; the
// explicit operations below confirm Txn/Range/Watch/Delete round-trip correctly.
func TestKubeAPIServerSmoke(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping E2E test; set RUN_E2E=1 to run")
	}

	h := NewHarness(t)
	ctx := t.Context()
	cms := h.Client.CoreV1().ConfigMaps

	const ns = "cloudetcd-e2e"

	// Namespace: create then read back.
	if _, err := h.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if got, err := h.Client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{}); err != nil {
		t.Fatalf("get namespace: %v", err)
	} else if got.Name != ns {
		t.Fatalf("namespace name = %q, want %q", got.Name, ns)
	}

	// ConfigMap: create, then read back and verify the payload survived the
	// round-trip through cloudetcd.
	if _, err := cms(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm1", Namespace: ns},
		Data:       map[string]string{"hello": "world"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create configmap cm1: %v", err)
	}
	got, err := cms(ns).Get(ctx, "cm1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get configmap cm1: %v", err)
	}
	if got.Data["hello"] != "world" {
		t.Fatalf("cm1 data = %v, want hello=world", got.Data)
	}

	// List: cm1 should be present.
	list, err := cms(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list configmaps: %v", err)
	}
	if !containsConfigMap(list.Items, "cm1") {
		t.Fatalf("cm1 not found in list of %d configmaps", len(list.Items))
	}

	// Watch: start from the list's resourceVersion, create cm2, and expect to
	// observe an ADDED event. This exercises cloudetcd's watch path.
	w, err := cms(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
	if err != nil {
		t.Fatalf("watch configmaps: %v", err)
	}
	defer w.Stop()

	if _, err := cms(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm2", Namespace: ns},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create configmap cm2: %v", err)
	}
	if !awaitConfigMapEvent(t, w, watch.Added, "cm2", 30*time.Second) {
		t.Fatalf("did not observe ADDED watch event for cm2")
	}

	// Delete: cm1 should then be gone.
	if err := cms(ns).Delete(ctx, "cm1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete configmap cm1: %v", err)
	}
	if _, err := cms(ns).Get(ctx, "cm1", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected NotFound getting deleted cm1, got: %v", err)
	}
}

func containsConfigMap(items []corev1.ConfigMap, name string) bool {
	for i := range items {
		if items[i].Name == name {
			return true
		}
	}
	return false
}

func awaitConfigMapEvent(t *testing.T, w watch.Interface, typ watch.EventType, name string, timeout time.Duration) bool {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				return false
			}
			if cm, isCM := ev.Object.(*corev1.ConfigMap); ev.Type == typ && isCM && cm.Name == name {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}
