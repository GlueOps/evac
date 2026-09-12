package preflight

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
	"github.com/GlueOps/evac/internal/scope"
)

func ptr[T any](v T) *T { return &v }

type nodeOpt func(*corev1.Node)

func node(name string, cpu, mem string, opts ...nodeOpt) corev1.Node {
	n := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{corev1.LabelHostname: name},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(mem),
			},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	for _, o := range opts {
		o(&n)
	}
	return n
}

func controlPlane() nodeOpt {
	return func(n *corev1.Node) { n.Labels["node-role.kubernetes.io/control-plane"] = "true" }
}
func cordoned() nodeOpt { return func(n *corev1.Node) { n.Spec.Unschedulable = true } }
func notReady() nodeOpt {
	return func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionFalse }
}
func labelled(k, v string) nodeOpt {
	return func(n *corev1.Node) { n.Labels[k] = v }
}
func noScheduleTaint() nodeOpt {
	return func(n *corev1.Node) {
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule})
	}
}

type podOpt func(*corev1.Pod)

func pod(ns, name, nodeName, cpu, mem string, opts ...podOpt) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{}},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name: "app",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(cpu),
					corev1.ResourceMemory: resource.MustParse(mem),
				}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	// Owned by a StatefulSet so it is not classified unmanaged, which keeps
	// these fixtures focused on scheduling rather than classification.
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "StatefulSet", Name: "app", Controller: ptr(true)}}
	for _, o := range opts {
		o(&p)
	}
	return p
}

func withPodLabels(l map[string]string) podOpt {
	return func(p *corev1.Pod) { p.Labels = l }
}

func withNodeSelector(k, v string) podOpt {
	return func(p *corev1.Pod) { p.Spec.NodeSelector = map[string]string{k: v} }
}

func withAntiAffinity(match map[string]string) podOpt {
	return func(p *corev1.Pod) {
		p.Spec.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{MatchLabels: match},
					TopologyKey:   corev1.LabelHostname,
				}},
			},
		}
	}
}

func run(t *testing.T, nodes []corev1.Node, pods []corev1.Pod, selected ...string) *Results {
	t.Helper()
	snap := kube.NewSnapshotForTest(kube.SnapshotFixture{
		TakenAt: time.Now(), Nodes: nodes, Pods: pods,
	})
	all := inventory.Build(snap)
	byName := inventory.ByName(all)

	var sel []inventory.Node
	for _, name := range selected {
		sel = append(sel, *byName[name])
	}
	sc, err := scope.Build(snap, sel, "test")
	if err != nil {
		t.Fatal(err)
	}
	return Run(sc)
}

func find(r *Results, check string) *Finding {
	for i := range r.Findings {
		if r.Findings[i].Check == check {
			return &r.Findings[i]
		}
	}
	return nil
}

// --- capacity --------------------------------------------------------------

func TestCapacityShortfallBlocksTheDrain(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{node("n1", "4", "8Gi"), node("n2", "1", "1Gi")},
		[]corev1.Pod{pod("app", "big", "n1", "3", "6Gi")},
		"n1")

	f := find(res, "capacity")
	if f == nil || f.Severity != Fatal {
		t.Fatalf("capacity finding = %+v, want a fatal shortfall", f)
	}
	if !res.Failed() {
		t.Error("Failed() = false; a capacity shortfall must block the drain")
	}
}

func TestCapacitySubtractsWhatAlreadyRunsOnRemainingNodes(t *testing.T) {
	t.Parallel()
	// n2 looks big enough on paper, but is already nearly full.
	res := run(t,
		[]corev1.Node{node("n1", "4", "8Gi"), node("n2", "4", "8Gi")},
		[]corev1.Pod{
			pod("app", "moving", "n1", "3", "6Gi"),
			pod("app", "resident", "n2", "3500m", "7Gi"),
		},
		"n1")

	f := find(res, "capacity")
	if f == nil || f.Severity != Fatal {
		t.Errorf("capacity = %+v, want fatal — comparing against raw allocatable would ignore the resident pod", f)
	}
}

// Observed on k3s: the server node is untainted and schedulable, and actively
// receives workloads. Omitting it under-reports capacity on every such cluster.
func TestSchedulableControlPlaneNodesCountTowardRemainingCapacity(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("agent-0", "1", "1Gi"),
			node("server-0", "8", "16Gi", controlPlane()), // untainted, schedulable
		},
		[]corev1.Pod{pod("app", "moving", "agent-0", "2", "4Gi")},
		"agent-0")

	f := find(res, "capacity")
	if f == nil || f.Severity == Fatal {
		t.Errorf("capacity = %+v, want ok — a schedulable control plane node is a valid scheduling target", f)
	}
}

// A cordoned or tainted node is not a landing place and must not be counted.
func TestCordonedAndTaintedNodesAreNotCountedAsCapacity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opt  nodeOpt
	}{
		{"cordoned", cordoned()},
		{"NoSchedule taint", noScheduleTaint()},
		{"NotReady", notReady()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := run(t,
				[]corev1.Node{node("n1", "4", "8Gi"), node("n2", "8", "16Gi", tc.opt)},
				[]corev1.Pod{pod("app", "moving", "n1", "3", "6Gi")},
				"n1")

			f := find(res, "capacity")
			if f == nil || f.Severity != Fatal {
				t.Errorf("capacity = %+v, want fatal — a %s node cannot receive pods", f, tc.name)
			}
		})
	}
}

func TestDrainingEverySchedulableNodeIsFatal(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{node("n1", "4", "8Gi"), node("n2", "4", "8Gi")},
		[]corev1.Pod{pod("app", "web", "n1", "100m", "128Mi")},
		"n1", "n2")

	f := find(res, "capacity")
	if f == nil || f.Severity != Fatal {
		t.Errorf("capacity = %+v, want fatal when nothing remains schedulable", f)
	}
}

// --- affinity --------------------------------------------------------------

func TestNodeSelectorWithNoRemainingMatchIsFatal(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("gpu-1", "8", "16Gi", labelled("pool", "gpu")),
			node("cpu-1", "8", "16Gi", labelled("pool", "cpu")),
		},
		[]corev1.Pod{pod("ml", "trainer", "gpu-1", "1", "1Gi", withNodeSelector("pool", "gpu"))},
		"gpu-1")

	f := find(res, "node-affinity")
	if f == nil || f.Severity != Fatal {
		t.Fatalf("node-affinity = %+v, want fatal — the only gpu node is in the selection", f)
	}
	if !strings.Contains(strings.Join(f.Detail, " "), "trainer") {
		t.Errorf("detail does not name the stuck pod: %v", f.Detail)
	}
}

// The failure the spec expects to matter most here: a 3-replica StatefulSet
// with one-replica-per-node anti-affinity cannot reschedule when the other two
// occupy the remaining nodes. Capacity is fine; the scheduler still refuses.
func TestAntiAffinityTrapIsFatalEvenWhenCapacityIsFine(t *testing.T) {
	t.Parallel()
	app := map[string]string{"app": "loki"}
	res := run(t,
		[]corev1.Node{node("n1", "8", "16Gi"), node("n2", "8", "16Gi"), node("n3", "8", "16Gi")},
		[]corev1.Pod{
			pod("obs", "loki-0", "n1", "100m", "128Mi", withPodLabels(app), withAntiAffinity(app)),
			pod("obs", "loki-1", "n2", "100m", "128Mi", withPodLabels(app), withAntiAffinity(app)),
			pod("obs", "loki-2", "n3", "100m", "128Mi", withPodLabels(app), withAntiAffinity(app)),
		},
		"n1")

	if f := find(res, "capacity"); f != nil && f.Severity == Fatal {
		t.Fatal("capacity reported a shortfall; this fixture has plenty of room")
	}
	f := find(res, "pod-anti-affinity")
	if f == nil || f.Severity != Fatal {
		t.Fatalf("pod-anti-affinity = %+v, want fatal — n2 and n3 already host a matching replica", f)
	}
}

func TestAntiAffinityIsSatisfiedWhenAFreeDomainRemains(t *testing.T) {
	t.Parallel()
	app := map[string]string{"app": "loki"}
	res := run(t,
		[]corev1.Node{node("n1", "8", "16Gi"), node("n2", "8", "16Gi"), node("n3", "8", "16Gi")},
		[]corev1.Pod{
			pod("obs", "loki-0", "n1", "100m", "128Mi", withPodLabels(app), withAntiAffinity(app)),
			pod("obs", "loki-1", "n2", "100m", "128Mi", withPodLabels(app), withAntiAffinity(app)),
			// n3 is free.
		},
		"n1")

	if f := find(res, "pod-anti-affinity"); f != nil {
		t.Errorf("pod-anti-affinity = %+v, want no finding — n3 is a free topology domain", f)
	}
}

// --- NotReady --------------------------------------------------------------

// A broken node is a legitimate reason to drain, so this warns rather than
// blocks.
func TestNotReadySelectedNodeWarnsButDoesNotBlock(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{node("n1", "4", "8Gi", notReady()), node("n2", "8", "16Gi")},
		[]corev1.Pod{pod("app", "web", "n1", "100m", "128Mi")},
		"n1")

	f := find(res, "not-ready-nodes")
	if f == nil {
		t.Fatal("no not-ready finding")
	}
	if f.Severity != Warning {
		t.Errorf("severity = %q, want %q — draining a broken node is a normal thing to do", f.Severity, Warning)
	}
	if res.Failed() {
		t.Error("Failed() = true; a NotReady node must not block the drain")
	}
}
