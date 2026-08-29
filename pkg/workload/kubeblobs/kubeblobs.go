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

// Package kubeblobs builds the values the workload writes from real
// Kubernetes API objects, encoded exactly as kube-apiserver stores them in
// etcd: the protobuf serializer with its "k8s\x00" framing, at the storage
// version. The objects are modelled on what a kubelet reports for a node in
// a managed cluster (a Node with its image list, a Running Pod from a
// Deployment, a node Lease, a kubelet Event) so that sizes and byte content
// are representative without needing a capture.
//
// This is the only part of the benchmark that imports Kubernetes API types;
// pkg/workload itself treats values as opaque bytes.
package kubeblobs

import (
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"

	"justinsb.com/cloudetcd/pkg/workload"
)

// Options shapes the generated objects.
type Options struct {
	// Images is the number of container images the Node reports in its
	// status. Real kubelets report up to 50 (--node-status-max-images); this
	// is what makes a Node object large.
	Images int
	// Containers is the number of containers in the Pod.
	Containers int
}

// DefaultOptions returns options resembling a busy node in a managed cluster.
func DefaultOptions() Options {
	return Options{Images: 30, Containers: 2}
}

// Generate builds and encodes a Blobs set with DefaultOptions.
func Generate() (*workload.Blobs, error) {
	return GenerateWithOptions(DefaultOptions())
}

// GenerateWithOptions builds and encodes a Blobs set.
func GenerateWithOptions(opts Options) (*workload.Blobs, error) {
	b := &workload.Blobs{}
	var err error
	if b.Node, err = Encode(Node("node-000000", opts), corev1.SchemeGroupVersion); err != nil {
		return nil, fmt.Errorf("encoding Node: %w", err)
	}
	if b.NodeLease, err = Encode(Lease("node-000000"), coordinationv1.SchemeGroupVersion); err != nil {
		return nil, fmt.Errorf("encoding Lease: %w", err)
	}
	if b.Pod, err = Encode(Pod("ns-000", "pod-000000-000-0", "node-000000", opts), corev1.SchemeGroupVersion); err != nil {
		return nil, fmt.Errorf("encoding Pod: %w", err)
	}
	if b.Event, err = Encode(Event("ns-000", "pod-000000-000-0", "node-000000"), corev1.SchemeGroupVersion); err != nil {
		return nil, fmt.Errorf("encoding Event: %w", err)
	}
	if b.MasterLease, err = Encode(MasterLease("10.0.0.1"), corev1.SchemeGroupVersion); err != nil {
		return nil, fmt.Errorf("encoding masterlease Endpoints: %w", err)
	}
	return b, nil
}

// Encode serializes obj the way kube-apiserver writes it to etcd.
func Encode(obj runtime.Object, gv schema.GroupVersion) ([]byte, error) {
	s := protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)
	return runtime.Encode(scheme.Codecs.EncoderForVersion(s, gv), obj)
}

// Decode is the inverse of Encode, for tests.
func Decode(data []byte) (runtime.Object, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(data, nil, nil)
	return obj, err
}

// managed builds a managedFields entry. The apiserver persists these for
// every object, and they are a large share of a stored object's size: a
// node Lease is ~500 B stored, of which the Lease's own fields are ~200 B.
func managed(manager string, op metav1.ManagedFieldsOperationType, apiVersion, fields string, sub string) metav1.ManagedFieldsEntry {
	e := metav1.ManagedFieldsEntry{
		Manager:    manager,
		Operation:  op,
		APIVersion: apiVersion,
		Time:       &now,
		FieldsType: "FieldsV1",
		FieldsV1:   &metav1.FieldsV1{Raw: []byte(fields)},
	}
	if sub != "" {
		e.Subresource = sub
	}
	return e
}

var (
	baseTime = time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	created  = metav1.NewTime(baseTime.Add(-30 * 24 * time.Hour))
	now      = metav1.NewTime(baseTime)
)

func uid(s string) types.UID {
	return types.UID(fmt.Sprintf("%08x-0000-4000-8000-%012x", len(s), hash(s)))
}

func hash(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h & 0xffffffffffff
}

// Node is a kubelet-reported Node as found in a managed cluster.
func Node(name string, opts Options) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			UID:               uid(name),
			ResourceVersion:   "1",
			CreationTimestamp: created,
			Labels: map[string]string{
				"beta.kubernetes.io/arch":                   "amd64",
				"beta.kubernetes.io/instance-type":          "n2-standard-8",
				"beta.kubernetes.io/os":                     "linux",
				"cloud.google.com/gke-nodepool":             "default-pool",
				"cloud.google.com/gke-os-distribution":      "cos",
				"cloud.google.com/machine-family":           "n2",
				"failure-domain.beta.kubernetes.io/region":  "us-central1",
				"failure-domain.beta.kubernetes.io/zone":    "us-central1-a",
				"kubernetes.io/arch":                        "amd64",
				"kubernetes.io/hostname":                    name,
				"kubernetes.io/os":                          "linux",
				"node.kubernetes.io/instance-type":          "n2-standard-8",
				"topology.kubernetes.io/region":             "us-central1",
				"topology.kubernetes.io/zone":               "us-central1-a",
				"topology.gke.io/zone":                      "us-central1-a",
				"cloud.google.com/gke-boot-disk":            "pd-balanced",
				"cloud.google.com/gke-container-runtime":    "containerd",
				"cloud.google.com/gke-logging-variant":      "DEFAULT",
				"cloud.google.com/gke-provisioning":         "standard",
				"cloud.google.com/gke-stack-type":           "IPV4",
				"iam.gke.io/gke-metadata-server-enabled":    "true",
				"node.kubernetes.io/masq-agent-ds-ready":    "true",
				"projectcalico.org/ds-ready":                "true",
				"cloud.google.com/private-node":             "false",
				"cloud.google.com/gke-cpu-scaling-level":    "8",
				"cloud.google.com/gke-max-pods-per-node":    "110",
				"cloud.google.com/gke-netd-ready":           "true",
				"cloud.google.com/gke-spot":                 "false",
				"kubernetes.io/role":                        "node",
				"node-role.kubernetes.io/node":              "",
				"topology.kubernetes.io/subzone":            "us-central1-a-1",
				"cloud.google.com/gke-node-pool-group-name": "default",
			},
			Annotations: map[string]string{
				"container.googleapis.com/instance_id":                   "1234567890123456789",
				"csi.volume.kubernetes.io/nodeid":                        `{"pd.csi.storage.gke.io":"projects/example/zones/us-central1-a/instances/` + name + `"}`,
				"node.alpha.kubernetes.io/ttl":                           "0",
				"node.gke.io/last-applied-node-labels":                   "cloud.google.com/gke-boot-disk=pd-balanced,cloud.google.com/gke-container-runtime=containerd,cloud.google.com/gke-nodepool=default-pool,cloud.google.com/machine-family=n2",
				"node.gke.io/last-applied-node-taints":                   "",
				"volumes.kubernetes.io/controller-managed-attach-detach": "true",
			},
			ManagedFields: []metav1.ManagedFieldsEntry{
				managed("kubelet", metav1.ManagedFieldsOperationUpdate, "v1",
					`{"f:metadata":{"f:annotations":{".":{},"f:csi.volume.kubernetes.io/nodeid":{},"f:volumes.kubernetes.io/controller-managed-attach-detach":{}},"f:labels":{".":{},"f:beta.kubernetes.io/arch":{},"f:beta.kubernetes.io/instance-type":{},"f:beta.kubernetes.io/os":{},"f:cloud.google.com/gke-boot-disk":{},"f:cloud.google.com/gke-container-runtime":{},"f:cloud.google.com/gke-nodepool":{},"f:cloud.google.com/gke-os-distribution":{},"f:cloud.google.com/machine-family":{},"f:failure-domain.beta.kubernetes.io/region":{},"f:failure-domain.beta.kubernetes.io/zone":{},"f:kubernetes.io/arch":{},"f:kubernetes.io/hostname":{},"f:kubernetes.io/os":{},"f:node.kubernetes.io/instance-type":{},"f:topology.kubernetes.io/region":{},"f:topology.kubernetes.io/zone":{}}},"f:spec":{"f:providerID":{}}}`, ""),
				managed("kube-controller-manager", metav1.ManagedFieldsOperationUpdate, "v1",
					`{"f:metadata":{"f:annotations":{"f:node.alpha.kubernetes.io/ttl":{}}},"f:spec":{"f:podCIDR":{},"f:podCIDRs":{".":{},"v:\"10.4.0.0/24\"":{}}}}`, ""),
				managed("node-problem-detector", metav1.ManagedFieldsOperationUpdate, "v1",
					`{"f:status":{"f:conditions":{"k:{\"type\":\"CorruptDockerOverlay2\"}":{".":{},"f:lastHeartbeatTime":{},"f:lastTransitionTime":{},"f:message":{},"f:reason":{},"f:status":{},"f:type":{}},"k:{\"type\":\"FrequentContainerdRestart\"}":{".":{},"f:lastHeartbeatTime":{},"f:lastTransitionTime":{},"f:message":{},"f:reason":{},"f:status":{},"f:type":{}},"k:{\"type\":\"FrequentDockerRestart\"}":{".":{},"f:lastHeartbeatTime":{},"f:lastTransitionTime":{},"f:message":{},"f:reason":{},"f:status":{},"f:type":{}},"k:{\"type\":\"FrequentKubeletRestart\"}":{".":{},"f:lastHeartbeatTime":{},"f:lastTransitionTime":{},"f:message":{},"f:reason":{},"f:status":{},"f:type":{}},"k:{\"type\":\"FrequentUnregisterNetDevice\"}":{".":{},"f:lastHeartbeatTime":{},"f:lastTransitionTime":{},"f:message":{},"f:reason":{},"f:status":{},"f:type":{}},"k:{\"type\":\"KernelDeadlock\"}":{".":{},"f:lastHeartbeatTime":{},"f:lastTransitionTime":{},"f:message":{},"f:reason":{},"f:status":{},"f:type":{}},"k:{\"type\":\"ReadonlyFilesystem\"}":{".":{},"f:lastHeartbeatTime":{},"f:lastTransitionTime":{},"f:message":{},"f:reason":{},"f:status":{},"f:type":{}}}}}`, "status"),
				managed("kubelet", metav1.ManagedFieldsOperationUpdate, "v1",
					`{"f:status":{"f:addresses":{".":{},"k:{\"type\":\"ExternalIP\"}":{".":{},"f:address":{},"f:type":{}},"k:{\"type\":\"Hostname\"}":{".":{},"f:address":{},"f:type":{}},"k:{\"type\":\"InternalDNS\"}":{".":{},"f:address":{},"f:type":{}},"k:{\"type\":\"InternalIP\"}":{".":{},"f:address":{},"f:type":{}}},"f:allocatable":{".":{},"f:cpu":{},"f:ephemeral-storage":{},"f:hugepages-1Gi":{},"f:hugepages-2Mi":{},"f:memory":{},"f:pods":{}},"f:capacity":{".":{},"f:cpu":{},"f:ephemeral-storage":{},"f:hugepages-1Gi":{},"f:hugepages-2Mi":{},"f:memory":{},"f:pods":{}},"f:conditions":{"k:{\"type\":\"DiskPressure\"}":{"f:lastHeartbeatTime":{}},"k:{\"type\":\"MemoryPressure\"}":{"f:lastHeartbeatTime":{}},"k:{\"type\":\"PIDPressure\"}":{"f:lastHeartbeatTime":{}},"k:{\"type\":\"Ready\"}":{"f:lastHeartbeatTime":{},"f:lastTransitionTime":{},"f:message":{},"f:reason":{},"f:status":{}}},"f:daemonEndpoints":{"f:kubeletEndpoint":{"f:Port":{}}},"f:images":{},"f:nodeInfo":{"f:architecture":{},"f:bootID":{},"f:containerRuntimeVersion":{},"f:kernelVersion":{},"f:kubeProxyVersion":{},"f:kubeletVersion":{},"f:machineID":{},"f:operatingSystem":{},"f:osImage":{},"f:systemUUID":{}}}}`, "status"),
			},
		},
		Spec: corev1.NodeSpec{
			PodCIDR:    "10.4.0.0/24",
			PodCIDRs:   []string{"10.4.0.0/24"},
			ProviderID: "gce://example/us-central1-a/" + name,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("8"),
				corev1.ResourceEphemeralStorage: resource.MustParse("98831908Ki"),
				"hugepages-1Gi":                 resource.MustParse("0"),
				"hugepages-2Mi":                 resource.MustParse("0"),
				corev1.ResourceMemory:           resource.MustParse("32874268Ki"),
				corev1.ResourcePods:             resource.MustParse("110"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("7910m"),
				corev1.ResourceEphemeralStorage: resource.MustParse("47060071478"),
				"hugepages-1Gi":                 resource.MustParse("0"),
				"hugepages-2Mi":                 resource.MustParse("0"),
				corev1.ResourceMemory:           resource.MustParse("29296412Ki"),
				corev1.ResourcePods:             resource.MustParse("110"),
			},
			Phase: corev1.NodeRunning,
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.128.0.10"},
				{Type: corev1.NodeExternalIP, Address: "34.10.20.30"},
				{Type: corev1.NodeInternalDNS, Address: name + ".c.example.internal"},
				{Type: corev1.NodeHostName, Address: name},
			},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: 10250}},
			NodeInfo: corev1.NodeSystemInfo{
				MachineID:               "1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f",
				SystemUUID:              "1c2d3e4f-5a6b-7c8d-9e0f-1a2b3c4d5e6f",
				BootID:                  "aa11bb22-cc33-dd44-ee55-ff6677889900",
				KernelVersion:           "6.6.72+",
				OSImage:                 "Container-Optimized OS from Google",
				ContainerRuntimeVersion: "containerd://1.7.24",
				KubeletVersion:          "v1.36.2-gke.100",
				KubeProxyVersion:        "v1.36.2-gke.100",
				OperatingSystem:         "linux",
				Architecture:            "amd64",
			},
		},
	}
	for _, c := range []struct {
		t      corev1.NodeConditionType
		status corev1.ConditionStatus
		reason string
		msg    string
	}{
		{"FrequentKubeletRestart", corev1.ConditionFalse, "NoFrequentKubeletRestart", "kubelet is functioning properly"},
		{"FrequentDockerRestart", corev1.ConditionFalse, "NoFrequentDockerRestart", "docker is functioning properly"},
		{"FrequentContainerdRestart", corev1.ConditionFalse, "NoFrequentContainerdRestart", "containerd is functioning properly"},
		{"KernelDeadlock", corev1.ConditionFalse, "KernelHasNoDeadlock", "kernel has no deadlock"},
		{"ReadonlyFilesystem", corev1.ConditionFalse, "FilesystemIsNotReadOnly", "Filesystem is not read-only"},
		{"CorruptDockerOverlay2", corev1.ConditionFalse, "NoCorruptDockerOverlay2", "docker overlay2 is functioning properly"},
		{"FrequentUnregisterNetDevice", corev1.ConditionFalse, "NoFrequentUnregisterNetDevice", "node is functioning properly"},
		{corev1.NodeNetworkUnavailable, corev1.ConditionFalse, "RouteCreated", "NodeController create implicit route"},
		{corev1.NodeMemoryPressure, corev1.ConditionFalse, "KubeletHasSufficientMemory", "kubelet has sufficient memory available"},
		{corev1.NodeDiskPressure, corev1.ConditionFalse, "KubeletHasNoDiskPressure", "kubelet has no disk pressure"},
		{corev1.NodePIDPressure, corev1.ConditionFalse, "KubeletHasSufficientPID", "kubelet has sufficient PID available"},
		{corev1.NodeReady, corev1.ConditionTrue, "KubeletReady", "kubelet is posting ready status"},
	} {
		n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
			Type:               c.t,
			Status:             c.status,
			LastHeartbeatTime:  now,
			LastTransitionTime: created,
			Reason:             c.reason,
			Message:            c.msg,
		})
	}
	for i := 0; i < opts.Images; i++ {
		img := fmt.Sprintf("registry.example.com/team-%d/service-%02d", i%5, i)
		n.Status.Images = append(n.Status.Images, corev1.ContainerImage{
			Names: []string{
				fmt.Sprintf("%s@sha256:%064x", img, hash(img)*uint64(i+1)),
				fmt.Sprintf("%s:v1.%d.%d", img, i%7, i%13),
			},
			SizeBytes: 50_000_000 + int64(i)*7_000_000,
		})
	}
	return n
}

// Lease is a node heartbeat Lease in kube-node-lease.
func Lease(nodeName string) *coordinationv1.Lease {
	dur := int32(40)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:              nodeName,
			Namespace:         corev1.NamespaceNodeLease,
			UID:               uid("lease/" + nodeName),
			ResourceVersion:   "1",
			CreationTimestamp: created,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       nodeName,
				UID:        uid(nodeName),
			}},
			ManagedFields: []metav1.ManagedFieldsEntry{
				managed("kubelet", metav1.ManagedFieldsOperationUpdate, "coordination.k8s.io/v1",
					`{"f:metadata":{"f:ownerReferences":{".":{},"k:{\"uid\":\"`+string(uid(nodeName))+`\"}":{}}},"f:spec":{"f:holderIdentity":{},"f:leaseDurationSeconds":{},"f:renewTime":{}}}`, ""),
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &nodeName,
			LeaseDurationSeconds: &dur,
			RenewTime:            &metav1.MicroTime{Time: baseTime},
		},
	}
}

// Pod is a Running pod owned by a ReplicaSet, as a kubelet reports it.
func Pod(namespace, name, nodeName string, opts Options) *corev1.Pod {
	controller := true
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			UID:               uid(namespace + "/" + name),
			ResourceVersion:   "1",
			CreationTimestamp: created,
			GenerateName:      "web-7d4b9c6f8-",
			Labels: map[string]string{
				"app":                          "web",
				"app.kubernetes.io/instance":   "web",
				"app.kubernetes.io/name":       "web",
				"app.kubernetes.io/version":    "1.4.2",
				"pod-template-hash":            "7d4b9c6f8",
				"team":                         "platform",
				"security.istio.io/tlsMode":    "istio",
				"service.istio.io/canonical-n": "web",
			},
			Annotations: map[string]string{
				"kubectl.kubernetes.io/restartedAt":              "2026-08-01T10:00:00Z",
				"prometheus.io/port":                             "9102",
				"prometheus.io/scrape":                           "true",
				"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "ReplicaSet",
				Name:               "web-7d4b9c6f8",
				UID:                uid("rs/web-7d4b9c6f8"),
				Controller:         &controller,
				BlockOwnerDeletion: &controller,
			}},
			ManagedFields: []metav1.ManagedFieldsEntry{
				managed("kube-controller-manager", metav1.ManagedFieldsOperationUpdate, "v1",
					`{"f:metadata":{"f:annotations":{".":{},"f:cluster-autoscaler.kubernetes.io/safe-to-evict":{},"f:kubectl.kubernetes.io/restartedAt":{},"f:prometheus.io/port":{},"f:prometheus.io/scrape":{}},"f:generateName":{},"f:labels":{".":{},"f:app":{},"f:app.kubernetes.io/instance":{},"f:app.kubernetes.io/name":{},"f:app.kubernetes.io/version":{},"f:pod-template-hash":{},"f:team":{}},"f:ownerReferences":{".":{},"k:{\"uid\":\"`+string(uid("rs/web-7d4b9c6f8"))+`\"}":{}}},"f:spec":{"f:containers":{"k:{\"name\":\"web-0\"}":{".":{},"f:args":{},"f:env":{".":{},"k:{\"name\":\"ENVIRONMENT\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"NODE_NAME\"}":{".":{},"f:name":{},"f:valueFrom":{".":{},"f:fieldRef":{}}},"k:{\"name\":\"OTEL_EXPORTER_OTLP_ENDPOINT\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"POD_NAME\"}":{".":{},"f:name":{},"f:valueFrom":{".":{},"f:fieldRef":{}}},"k:{\"name\":\"POD_NAMESPACE\"}":{".":{},"f:name":{},"f:valueFrom":{".":{},"f:fieldRef":{}}}},"f:image":{},"f:imagePullPolicy":{},"f:livenessProbe":{".":{},"f:failureThreshold":{},"f:httpGet":{".":{},"f:path":{},"f:port":{},"f:scheme":{}},"f:initialDelaySeconds":{},"f:periodSeconds":{},"f:successThreshold":{},"f:timeoutSeconds":{}},"f:name":{},"f:ports":{".":{},"k:{\"containerPort\":8080,\"protocol\":\"TCP\"}":{".":{},"f:containerPort":{},"f:name":{},"f:protocol":{}},"k:{\"containerPort\":9102,\"protocol\":\"TCP\"}":{".":{},"f:containerPort":{},"f:name":{},"f:protocol":{}}},"f:readinessProbe":{".":{},"f:failureThreshold":{},"f:httpGet":{".":{},"f:path":{},"f:port":{},"f:scheme":{}},"f:periodSeconds":{},"f:successThreshold":{},"f:timeoutSeconds":{}},"f:resources":{".":{},"f:limits":{".":{},"f:cpu":{},"f:memory":{}},"f:requests":{".":{},"f:cpu":{},"f:memory":{}}},"f:terminationMessagePath":{},"f:terminationMessagePolicy":{},"f:volumeMounts":{".":{},"k:{\"mountPath\":\"/etc/web\"}":{".":{},"f:mountPath":{},"f:name":{},"f:readOnly":{}}}}},"f:dnsPolicy":{},"f:enableServiceLinks":{},"f:restartPolicy":{},"f:schedulerName":{},"f:securityContext":{},"f:serviceAccount":{},"f:serviceAccountName":{},"f:terminationGracePeriodSeconds":{},"f:volumes":{".":{},"k:{\"name\":\"config\"}":{".":{},"f:configMap":{".":{},"f:defaultMode":{},"f:name":{}},"f:name":{}}}}}`, ""),
				managed("kubelet", metav1.ManagedFieldsOperationUpdate, "v1",
					`{"f:status":{"f:conditions":{"k:{\"type\":\"ContainersReady\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}},"k:{\"type\":\"Initialized\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}},"k:{\"type\":\"PodReadyToStartContainers\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}},"k:{\"type\":\"Ready\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}}},"f:containerStatuses":{},"f:hostIP":{},"f:hostIPs":{},"f:phase":{},"f:podIP":{},"f:podIPs":{".":{},"k:{\"ip\":\"10.4.0.23\"}":{".":{},"f:ip":{}}},"f:startTime":{}}}`, "status"),
			},
		},
		Spec: corev1.PodSpec{
			NodeName:                      nodeName,
			ServiceAccountName:            "web",
			DeprecatedServiceAccount:      "web",
			RestartPolicy:                 corev1.RestartPolicyAlways,
			DNSPolicy:                     corev1.DNSClusterFirst,
			SchedulerName:                 "default-scheduler",
			TerminationGracePeriodSeconds: ptr[int64](30),
			Priority:                      ptr[int32](0),
			EnableServiceLinks:            ptr(true),
			PreemptionPolicy:              ptr(corev1.PreemptLowerPriority),
			SecurityContext:               &corev1.PodSecurityContext{},
			Tolerations: []corev1.Toleration{
				{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr[int64](300)},
				{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr[int64](300)},
			},
			Volumes: []corev1.Volume{
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "web-config"}, DefaultMode: ptr[int32](420)}}},
				{Name: "kube-api-access-x7k2p", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
					DefaultMode: ptr[int32](420),
					Sources: []corev1.VolumeProjection{
						{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{ExpirationSeconds: ptr[int64](3607), Path: "token"}},
						{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
							Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
						{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace",
							FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
					},
				}}},
			},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			HostIP:    "10.128.0.10",
			HostIPs:   []corev1.HostIP{{IP: "10.128.0.10"}},
			PodIP:     "10.4.0.23",
			PodIPs:    []corev1.PodIP{{IP: "10.4.0.23"}},
			StartTime: &created,
			QOSClass:  corev1.PodQOSBurstable,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReadyToStartContainers, Status: corev1.ConditionTrue, LastTransitionTime: created},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: created},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: created},
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: created},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: created},
			},
		},
	}
	for i := 0; i < opts.Containers; i++ {
		cname := fmt.Sprintf("web-%d", i)
		image := fmt.Sprintf("registry.example.com/platform/web-%d:v1.4.2", i)
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
			Name:            cname,
			Image:           image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Args:            []string{"--port=8080", "--config=/etc/web/config.yaml", "--log-level=info"},
			Ports: []corev1.ContainerPort{
				{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
				{Name: "metrics", ContainerPort: 9102, Protocol: corev1.ProtocolTCP},
			},
			Env: []corev1.EnvVar{
				{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"}}},
				{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}},
				{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "spec.nodeName"}}},
				{Name: "ENVIRONMENT", Value: "production"},
				{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "http://otel-collector.monitoring.svc:4317"},
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("512Mi")},
			},
			LivenessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstrFromString("http"), Scheme: corev1.URISchemeHTTP}},
				InitialDelaySeconds: 10, TimeoutSeconds: 1, PeriodSeconds: 10, SuccessThreshold: 1, FailureThreshold: 3},
			ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstrFromString("http"), Scheme: corev1.URISchemeHTTP}},
				TimeoutSeconds: 1, PeriodSeconds: 5, SuccessThreshold: 1, FailureThreshold: 3},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "config", MountPath: "/etc/web", ReadOnly: true},
				{Name: "kube-api-access-x7k2p", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true},
			},
			TerminationMessagePath:   "/dev/termination-log",
			TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		})
		p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, corev1.ContainerStatus{
			Name:         cname,
			Image:        image,
			ImageID:      fmt.Sprintf("registry.example.com/platform/web-%d@sha256:%064x", i, hash(image)),
			ContainerID:  fmt.Sprintf("containerd://%064x", hash(cname)),
			Ready:        true,
			Started:      ptr(true),
			RestartCount: 0,
			State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: created}},
		})
	}
	return p
}

// Event is a kubelet-emitted Event about a pod.
func Event(namespace, podName, nodeName string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:              podName + ".186d3a4f2c1e5b70",
			Namespace:         namespace,
			UID:               uid("event/" + podName),
			ResourceVersion:   "1",
			CreationTimestamp: now,
			ManagedFields: []metav1.ManagedFieldsEntry{
				managed("kubelet", metav1.ManagedFieldsOperationUpdate, "v1",
					`{"f:count":{},"f:firstTimestamp":{},"f:involvedObject":{},"f:lastTimestamp":{},"f:message":{},"f:reason":{},"f:reportingComponent":{},"f:reportingInstance":{},"f:source":{"f:component":{},"f:host":{}},"f:type":{}}`, ""),
			},
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:            "Pod",
			Namespace:       namespace,
			Name:            podName,
			UID:             uid(namespace + "/" + podName),
			APIVersion:      "v1",
			ResourceVersion: "123456789",
			FieldPath:       "spec.containers{web-0}",
		},
		Reason:              "Pulled",
		Message:             `Container image "registry.example.com/platform/web-0:v1.4.2" already present on machine`,
		Source:              corev1.EventSource{Component: "kubelet", Host: nodeName},
		FirstTimestamp:      now,
		LastTimestamp:       now,
		Count:               1,
		Type:                corev1.EventTypeNormal,
		ReportingController: "kubelet",
		ReportingInstance:   nodeName,
	}
}

// MasterLease is the Endpoints object kube-apiserver stores under
// /registry/masterleases/<ip> to advertise itself.
func MasterLease(ip string) *corev1.Endpoints {
	return &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: ip, ResourceVersion: "1"},
		Subsets:    []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: ip}}}},
	}
}

func ptr[T any](v T) *T { return &v }
