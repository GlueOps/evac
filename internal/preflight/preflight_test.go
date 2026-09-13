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
	if !res.Failed() {
		t.Error("Failed() = false; a capacity shortfall must block the drain")
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

// The failure that matters most here: a 3-replica StatefulSet with
// one-replica-per-node anti-affinity cannot reschedule when the other two
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

// --- control plane as the only landing place --------------------------------

// The k3s shape, and the reason this check exists: the server node carries the
// control-plane label but no taint, so cordoning every agent leaves it as the
// only place anything can be scheduled. Capacity arithmetic says that is fine —
// it usually has room — which is exactly what makes it worth blocking.
func TestDrainingEveryWorkerOntoAnUntaintedControlPlaneIsFatal(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("agent-0", "4", "8Gi"),
			node("agent-1", "4", "8Gi"),
			node("server-0", "8", "16Gi", controlPlane()),
		},
		[]corev1.Pod{pod("app", "web", "agent-0", "1", "1Gi")},
		"agent-0", "agent-1")

	f := find(res, "control-plane-only")
	if f == nil {
		t.Fatal("no control-plane-only finding; draining every worker onto the control plane must be caught")
	}
	if f.Severity != Fatal {
		t.Errorf("Severity = %q, want %q", f.Severity, Fatal)
	}
	if !res.Failed() {
		t.Error("Failed() = false; this must block the drain")
	}
	if !strings.Contains(f.Summary, "server-0") {
		t.Errorf("Summary = %q, want it to name the node that would receive everything", f.Summary)
	}
	// The capacity check must still pass. The two are independent: capacity is
	// about room, this is about where.
	if c := find(res, "capacity"); c == nil || c.Severity == Fatal {
		t.Errorf("capacity = %+v, want ok — the control plane does have room, that is not the objection", c)
	}
}

// One worker left out is the whole difference. Nothing is forced onto the
// control plane, so there is nothing to report.
func TestLeavingOneWorkerSchedulableIsNotFatal(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("agent-0", "4", "8Gi"),
			node("agent-1", "4", "8Gi"),
			node("server-0", "8", "16Gi", controlPlane()),
		},
		[]corev1.Pod{pod("app", "web", "agent-0", "1", "1Gi")},
		"agent-0")

	if f := find(res, "control-plane-only"); f != nil {
		t.Errorf("control-plane-only = %+v, want none; agent-1 is still a landing place", f)
	}
	if res.Failed() {
		t.Error("Failed() = true; draining one of two workers must be allowed")
	}
}

// The kubeadm shape. A NoSchedule taint keeps the control plane out of the
// remaining set entirely, so this check cannot fire there — the drain is
// refused by capacity instead, which is the more accurate complaint.
func TestTaintedControlPlaneIsReportedAsNoCapacityNotAsControlPlaneOnly(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("agent-0", "4", "8Gi"),
			node("server-0", "8", "16Gi", controlPlane(), noScheduleTaint()),
		},
		[]corev1.Pod{pod("app", "web", "agent-0", "1", "1Gi")},
		"agent-0")

	if f := find(res, "control-plane-only"); f != nil {
		t.Errorf("control-plane-only = %+v, want none; a tainted control plane is not a landing place at all", f)
	}
	c := find(res, "capacity")
	if c == nil || c.Severity != Fatal {
		t.Fatalf("capacity = %+v, want fatal — nothing schedulable remains", c)
	}
	if !strings.Contains(c.Summary, "no schedulable nodes remain") {
		t.Errorf("Summary = %q, want the no-schedulable-nodes wording", c.Summary)
	}
}

// Every remaining control-plane node is named, in a stable order, so the
// operator can see exactly what would have received the workload.
func TestEveryRemainingControlPlaneNodeIsNamedInOrder(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("agent-0", "4", "8Gi"),
			node("cp-b", "8", "16Gi", controlPlane()),
			node("cp-a", "8", "16Gi", controlPlane()),
		},
		[]corev1.Pod{pod("app", "web", "agent-0", "1", "1Gi")},
		"agent-0")

	f := find(res, "control-plane-only")
	if f == nil {
		t.Fatal("no control-plane-only finding")
	}
	if !strings.Contains(f.Summary, "cp-a, cp-b") {
		t.Errorf("Summary = %q, want both nodes named in sorted order", f.Summary)
	}
}

// An etcd-labelled node is control plane too. The signals are defined in one
// place, and this pins that this check uses that definition rather than a
// second copy of it that could drift.
func TestControlPlaneDetectionUsesTheSharedSignals(t *testing.T) {
	t.Parallel()
	for _, label := range []string{
		"node-role.kubernetes.io/control-plane",
		"node-role.kubernetes.io/master",
		"node-role.kubernetes.io/etcd",
	} {
		t.Run(label, func(t *testing.T) {
			res := run(t,
				[]corev1.Node{
					node("agent-0", "4", "8Gi"),
					node("server-0", "8", "16Gi", labelled(label, "true")),
				},
				[]corev1.Pod{pod("app", "web", "agent-0", "1", "1Gi")},
				"agent-0")

			if f := find(res, "control-plane-only"); f == nil {
				t.Errorf("%s did not mark the node control plane", label)
			}
		})
	}
}

// The capacity line is the sentence an operator reads and believes. "onto 1
// remaining node(s)" provoked exactly the question it did not answer: which
// node? Naming it, and marking a control plane as such, is the point.
func TestCapacityLineNamesWhereTheWorkloadIsGoing(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("agent-0", "4", "8Gi"),
			node("agent-1", "4", "8Gi"),
			node("server-0", "8", "16Gi", controlPlane()),
		},
		[]corev1.Pod{pod("app", "web", "agent-0", "1", "1Gi")},
		"agent-0")

	f := find(res, "capacity")
	if f == nil {
		t.Fatal("no capacity finding")
	}
	for _, want := range []string{"agent-1", "server-0 (control plane)"} {
		if !strings.Contains(f.Summary, want) {
			t.Errorf("Summary = %q, want it to contain %q", f.Summary, want)
		}
	}
}

// A large cluster must not turn a one-line reassurance into a paragraph.
func TestCapacityLineTruncatesALongNodeList(t *testing.T) {
	t.Parallel()
	nodes := []corev1.Node{node("agent-0", "4", "8Gi")}
	for _, n := range []string{"w1", "w2", "w3", "w4", "w5"} {
		nodes = append(nodes, node(n, "4", "8Gi"))
	}
	res := run(t, nodes,
		[]corev1.Pod{pod("app", "web", "agent-0", "1", "1Gi")},
		"agent-0")

	f := find(res, "capacity")
	if f == nil {
		t.Fatal("no capacity finding")
	}
	if !strings.Contains(f.Summary, "and 2 more") {
		t.Errorf("Summary = %q, want the list truncated with a count of the rest", f.Summary)
	}
	if strings.Contains(f.Summary, "w4") || strings.Contains(f.Summary, "w5") {
		t.Errorf("Summary = %q, want only the first three named", f.Summary)
	}
}

// checkWidth is a magic number one character from breaking. Every check name
// must leave at least one space of gutter, or the summary runs into the name.
func TestSummaryColumnFitsEveryCheckName(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"capacity", "not-ready-nodes", "node-affinity",
		"pod-anti-affinity", "control-plane-only",
	} {
		if len(name) >= checkWidth {
			t.Errorf("check %q is %d chars, checkWidth is %d — widen it", name, len(name), checkWidth)
		}
	}
}

// A surviving worker with no headroom is the same incident: the scheduler
// fills it and puts the rest on the control plane. Checking node identity
// alone meant one tiny or fully booked agent switched this guard off.
func TestAFullSurvivingWorkerDoesNotSilenceTheCheck(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("agent-0", "4", "8Gi"),
			node("agent-1", "4", "8Gi"),
			node("agent-2", "1", "1Gi"),
			node("server-0", "16", "32Gi", controlPlane()),
		},
		[]corev1.Pod{
			pod("app", "full", "agent-2", "1", "1Gi"), // agent-2 has nothing left
			pod("app", "a", "agent-0", "3", "6Gi"),
			pod("app", "b", "agent-1", "3", "6Gi"),
		},
		"agent-0", "agent-1")

	f := find(res, "control-plane-only")
	if f == nil {
		t.Fatal("not flagged: 6 cpu must move and agent-2 has zero free, so it lands on server-0")
	}
	if f.Severity != Fatal {
		t.Errorf("Severity = %q, want %q", f.Severity, Fatal)
	}
	if !strings.Contains(f.Summary, "server-0") {
		t.Errorf("Summary = %q, want it to name the control plane node", f.Summary)
	}
}

// The mirror image: workers that can absorb the load mean there is nothing to
// report, even though a schedulable control plane node is also available.
func TestWorkersWithHeadroomMeanNoFinding(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("agent-0", "4", "8Gi"),
			node("agent-1", "16", "32Gi"),
			node("server-0", "16", "32Gi", controlPlane()),
		},
		[]corev1.Pod{pod("app", "a", "agent-0", "3", "6Gi")},
		"agent-0")

	if f := find(res, "control-plane-only"); f != nil {
		t.Errorf("control-plane-only = %+v, want none; agent-1 can take the whole workload", f)
	}
}

// A drain that relocates nothing must not be refused. Where zero pods would
// land is not a question worth blocking on.
func TestDrainThatRelocatesNothingIsNotRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		pods []corev1.Pod
	}{
		{"empty node", nil},
		{"only excluded pods", []corev1.Pod{daemonSetPod("kube-system", "ds", "agent-0", "1", "1Gi")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := run(t,
				[]corev1.Node{
					node("agent-0", "4", "8Gi"),
					node("server-0", "16", "32Gi", controlPlane()),
				},
				tc.pods, "agent-0")

			if f := find(res, "control-plane-only"); f != nil {
				t.Errorf("control-plane-only = %+v, want none; nothing is being evicted", f)
			}
			if res.Failed() {
				t.Error("Failed() = true; a drain that moves nothing must be allowed")
			}
		})
	}
}

// evac never uncordons and a re-run is the documented recovery, so a worker
// left cordoned by an earlier run is routine. Telling the operator "the
// selection covers every worker" is false there, and telling them to drop
// nodes from a one-node selection is unactionable. Name the real blocker.
func TestACordonedWorkerIsNamedWithTheReasonItIsUnavailable(t *testing.T) {
	t.Parallel()
	res := run(t,
		[]corev1.Node{
			node("agent-0", "4", "8Gi", cordoned()), // left over from a previous run
			node("agent-1", "4", "8Gi"),
			node("server-0", "16", "32Gi", controlPlane()),
		},
		[]corev1.Pod{pod("app", "a", "agent-1", "3", "6Gi")},
		"agent-1")

	f := find(res, "control-plane-only")
	if f == nil {
		t.Fatal("not flagged; agent-0 is cordoned so only server-0 can take the load")
	}
	joined := strings.Join(f.Detail, "\n")
	if !strings.Contains(joined, "agent-0") || !strings.Contains(joined, "already cordoned") {
		t.Errorf("Detail = %q, want agent-0 named as cordoned", joined)
	}
	if strings.Contains(joined, "covers every worker") {
		t.Errorf("Detail = %q, still claims the selection covers every worker", joined)
	}
}

func daemonSetPod(ns, name, nodeName, cpu, mem string) corev1.Pod {
	p := pod(ns, name, nodeName, cpu, mem)
	p.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "DaemonSet", Name: "ds", Controller: ptr(true),
	}}
	return p
}
