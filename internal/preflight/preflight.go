// Package preflight implements §8's checks: the things that turn a maintenance
// window into everything-Pending if nobody looks first.
package preflight

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/GlueOps/evac/internal/classify"
	"github.com/GlueOps/evac/internal/scope"
)

// Severity distinguishes what blocks a drain from what merely deserves saying.
type Severity string

const (
	// Fatal blocks the drain. --yes does not override these; only
	// --ignore-preflight does, and it logs loudly what it overrode.
	Fatal Severity = "fatal"
	// Warning is reported and proceeds.
	Warning Severity = "warning"
)

// Finding is one preflight result.
type Finding struct {
	Severity Severity
	Check    string
	Summary  string
	// Detail holds the per-item explanation lines.
	Detail []string
}

// Results collects the findings.
type Results struct {
	Findings []Finding
}

// Failed reports whether anything blocks the drain.
func (r *Results) Failed() bool {
	for _, f := range r.Findings {
		if f.Severity == Fatal {
			return true
		}
	}
	return false
}

// Add appends a finding.
func (r *Results) Add(f Finding) { r.Findings = append(r.Findings, f) }

// Run executes every §8 check against a computed scope.
func Run(s *scope.Scope) *Results {
	res := &Results{}
	remaining := remainingNodes(s)

	checkNotReady(res, s)
	checkCapacity(res, s, remaining)
	checkNodeAffinity(res, s, remaining)
	checkPodAntiAffinity(res, s, remaining)
	return res
}

// remainingNodes is the set that will still be schedulable once phase 1 has
// cordoned everything selected.
//
// This deliberately includes schedulable control-plane nodes. They are excluded
// from *draining* by §3.1 but remain valid scheduling targets — on k3s they are
// untainted and routinely receive workloads — so leaving them out would
// under-report capacity and raise false shortfalls on every k3s cluster.
func remainingNodes(s *scope.Scope) []*corev1.Node {
	selected := make(map[string]bool, len(s.Nodes))
	for i := range s.Nodes {
		selected[s.Nodes[i].Name] = true
	}

	var out []*corev1.Node
	for i := range s.Snapshot.Nodes {
		n := &s.Snapshot.Nodes[i]
		if selected[n.Name] || n.Spec.Unschedulable {
			continue
		}
		if !nodeReady(n) {
			continue // a NotReady node is not a landing place
		}
		if hasBlockingTaint(n) {
			continue
		}
		out = append(out, n)
	}
	return out
}

func nodeReady(n *corev1.Node) bool {
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == corev1.NodeReady {
			return n.Status.Conditions[i].Status == corev1.ConditionTrue
		}
	}
	return false
}

// hasBlockingTaint reports a NoSchedule taint that ordinary workloads will not
// tolerate. Counting a tainted node as available capacity would be the same
// mistake as counting a cordoned one.
func hasBlockingTaint(n *corev1.Node) bool {
	for i := range n.Spec.Taints {
		t := &n.Spec.Taints[i]
		if t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute {
			return true
		}
	}
	return false
}

// --- capacity --------------------------------------------------------------

func checkCapacity(res *Results, s *scope.Scope, remaining []*corev1.Node) {
	if len(remaining) == 0 {
		res.Add(Finding{
			Severity: Fatal,
			Check:    "capacity",
			Summary:  "no schedulable nodes remain after cordoning the selection",
			Detail:   []string{"every schedulable node in the cluster is in the selection, so evicted pods have nowhere to go"},
		})
		return
	}

	// What has to move.
	needCPU, needMem := resource.Quantity{}, resource.Quantity{}
	for _, r := range s.Pods {
		if r.Class == classify.Excluded {
			continue
		}
		cpu, mem := podRequests(r.Pod)
		needCPU.Add(cpu)
		needMem.Add(mem)
	}

	// What is free on the nodes that remain: allocatable minus what already
	// runs there. Comparing against raw allocatable would ignore the existing
	// workload and report capacity that does not exist.
	remainingNames := make(map[string]bool, len(remaining))
	freeCPU, freeMem := resource.Quantity{}, resource.Quantity{}
	for _, n := range remaining {
		remainingNames[n.Name] = true
		freeCPU.Add(*n.Status.Allocatable.Cpu())
		freeMem.Add(*n.Status.Allocatable.Memory())
	}
	for i := range s.Snapshot.Pods {
		p := &s.Snapshot.Pods[i]
		if !remainingNames[p.Spec.NodeName] || isTerminal(p) {
			continue
		}
		cpu, mem := podRequests(p)
		freeCPU.Sub(cpu)
		freeMem.Sub(mem)
	}

	var short []string
	if needCPU.Cmp(freeCPU) > 0 {
		short = append(short, fmt.Sprintf("cpu: need %s, %s available across %d node(s)",
			needCPU.String(), freeCPU.String(), len(remaining)))
	}
	if needMem.Cmp(freeMem) > 0 {
		short = append(short, fmt.Sprintf("memory: need %s, %s available across %d node(s)",
			needMem.String(), freeMem.String(), len(remaining)))
	}

	if len(short) > 0 {
		res.Add(Finding{
			Severity: Fatal,
			Check:    "capacity",
			Summary:  "insufficient capacity on the nodes that will remain schedulable",
			Detail:   short,
		})
		return
	}
	res.Add(Finding{
		Severity: Warning,
		Check:    "capacity",
		Summary: fmt.Sprintf("ok — moving %s cpu / %s memory onto %d remaining node(s)",
			needCPU.String(), needMem.String(), len(remaining)),
	})
}

// podRequests sums container requests, taking the larger of init and regular
// containers the way the scheduler does.
func podRequests(p *corev1.Pod) (cpu, mem resource.Quantity) {
	for i := range p.Spec.Containers {
		r := p.Spec.Containers[i].Resources.Requests
		cpu.Add(*r.Cpu())
		mem.Add(*r.Memory())
	}
	// Init containers run sequentially, so the peak is the largest single one,
	// not their sum.
	var initCPU, initMem resource.Quantity
	for i := range p.Spec.InitContainers {
		r := p.Spec.InitContainers[i].Resources.Requests
		if r.Cpu().Cmp(initCPU) > 0 {
			initCPU = *r.Cpu()
		}
		if r.Memory().Cmp(initMem) > 0 {
			initMem = *r.Memory()
		}
	}
	if initCPU.Cmp(cpu) > 0 {
		cpu = initCPU
	}
	if initMem.Cmp(mem) > 0 {
		mem = initMem
	}
	return cpu, mem
}

func isTerminal(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

// --- node affinity ---------------------------------------------------------

// checkNodeAffinity catches the case where the selection includes every node a
// workload is allowed to run on. Capacity may be fine and the scheduler still
// has nowhere to put the pod — which looks like a mystery at 2am.
func checkNodeAffinity(res *Results, s *scope.Scope, remaining []*corev1.Node) {
	var stuck []string
	for _, r := range s.Pods {
		if r.Class == classify.Excluded {
			continue
		}
		if !hasNodeConstraint(r.Pod) {
			continue
		}
		if anyNodeFits(r.Pod, remaining) {
			continue
		}
		stuck = append(stuck, fmt.Sprintf("%s/%s has no remaining node matching its nodeSelector/nodeAffinity",
			r.Pod.Namespace, r.Pod.Name))
	}
	if len(stuck) > 0 {
		res.Add(Finding{
			Severity: Fatal,
			Check:    "node-affinity",
			Summary:  fmt.Sprintf("%d pod(s) have no eligible node once the selection is cordoned", len(stuck)),
			Detail:   dedupe(stuck),
		})
	}
}

func hasNodeConstraint(p *corev1.Pod) bool {
	if len(p.Spec.NodeSelector) > 0 {
		return true
	}
	return p.Spec.Affinity != nil &&
		p.Spec.Affinity.NodeAffinity != nil &&
		p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil
}

func anyNodeFits(p *corev1.Pod, nodes []*corev1.Node) bool {
	for _, n := range nodes {
		if nodeMatches(p, n) {
			return true
		}
	}
	return false
}

func nodeMatches(p *corev1.Pod, n *corev1.Node) bool {
	// nodeSelector is a plain subset match.
	for k, v := range p.Spec.NodeSelector {
		if n.Labels[k] != v {
			return false
		}
	}
	if p.Spec.Affinity == nil || p.Spec.Affinity.NodeAffinity == nil {
		return true
	}
	req := p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if req == nil || len(req.NodeSelectorTerms) == 0 {
		return true
	}
	// Terms are ORed; expressions within a term are ANDed.
	for _, term := range req.NodeSelectorTerms {
		if termMatches(term, n) {
			return true
		}
	}
	return false
}

func termMatches(term corev1.NodeSelectorTerm, n *corev1.Node) bool {
	for _, expr := range term.MatchExpressions {
		if !exprMatches(expr, n.Labels) {
			return false
		}
	}
	for _, expr := range term.MatchFields {
		// The only field selector the scheduler supports is metadata.name.
		if expr.Key == "metadata.name" && !valueMatches(expr, n.Name) {
			return false
		}
	}
	return true
}

func exprMatches(expr corev1.NodeSelectorRequirement, l map[string]string) bool {
	val, present := l[expr.Key]
	switch expr.Operator {
	case corev1.NodeSelectorOpExists:
		return present
	case corev1.NodeSelectorOpDoesNotExist:
		return !present
	case corev1.NodeSelectorOpIn:
		return present && contains(expr.Values, val)
	case corev1.NodeSelectorOpNotIn:
		return !present || !contains(expr.Values, val)
	default:
		// Gt/Lt on numeric labels: rare, and guessing wrong here would produce
		// a false blocker. Treat as satisfiable and let the scheduler decide.
		return true
	}
}

func valueMatches(expr corev1.NodeSelectorRequirement, v string) bool {
	switch expr.Operator {
	case corev1.NodeSelectorOpIn:
		return contains(expr.Values, v)
	case corev1.NodeSelectorOpNotIn:
		return !contains(expr.Values, v)
	default:
		return true
	}
}

// --- pod anti-affinity -----------------------------------------------------

// checkPodAntiAffinity is the more likely blocker for these workloads: a
// 3-replica StatefulSet with one-replica-per-node anti-affinity — typical for
// loki, vault and prometheus — cannot reschedule an evicted replica when the
// other two occupy the remaining nodes. Capacity is fine, the scheduler still
// refuses, and the drain stalls with no obvious cause.
func checkPodAntiAffinity(res *Results, s *scope.Scope, remaining []*corev1.Node) {
	selected := make(map[string]bool, len(s.Nodes))
	for i := range s.Nodes {
		selected[s.Nodes[i].Name] = true
	}

	// Pods that will still be running after the drain, indexed by node. These
	// are what occupy topology domains.
	staying := map[string][]*corev1.Pod{}
	for i := range s.Snapshot.Pods {
		p := &s.Snapshot.Pods[i]
		if p.Spec.NodeName == "" || selected[p.Spec.NodeName] || isTerminal(p) {
			continue
		}
		staying[p.Spec.NodeName] = append(staying[p.Spec.NodeName], p)
	}

	nodesByName := map[string]*corev1.Node{}
	for _, n := range remaining {
		nodesByName[n.Name] = n
	}

	var stuck []string
	for _, r := range s.Pods {
		if r.Class == classify.Excluded {
			continue
		}
		terms := requiredAntiAffinity(r.Pod)
		if len(terms) == 0 {
			continue
		}
		if anyNodeSatisfiesAntiAffinity(r.Pod, terms, remaining, staying) {
			continue
		}
		stuck = append(stuck, fmt.Sprintf(
			"%s/%s cannot be placed: every remaining node already hosts a pod matching its required podAntiAffinity",
			r.Pod.Namespace, r.Pod.Name))
	}

	if len(stuck) > 0 {
		res.Add(Finding{
			Severity: Fatal,
			Check:    "pod-anti-affinity",
			Summary:  fmt.Sprintf("%d pod(s) cannot be rescheduled without violating required podAntiAffinity", len(stuck)),
			Detail:   dedupe(stuck),
		})
	}
}

func requiredAntiAffinity(p *corev1.Pod) []corev1.PodAffinityTerm {
	if p.Spec.Affinity == nil || p.Spec.Affinity.PodAntiAffinity == nil {
		return nil
	}
	return p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func anyNodeSatisfiesAntiAffinity(
	pod *corev1.Pod,
	terms []corev1.PodAffinityTerm,
	remaining []*corev1.Node,
	staying map[string][]*corev1.Pod,
) bool {
	for _, candidate := range remaining {
		if !nodeMatches(pod, candidate) {
			continue // excluded by node affinity anyway
		}
		if satisfiesAllTerms(pod, terms, candidate, remaining, staying) {
			return true
		}
	}
	return false
}

func satisfiesAllTerms(
	pod *corev1.Pod,
	terms []corev1.PodAffinityTerm,
	candidate *corev1.Node,
	remaining []*corev1.Node,
	staying map[string][]*corev1.Pod,
) bool {
	for _, term := range terms {
		sel, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
		if err != nil {
			continue // unparseable: do not invent a blocker
		}
		domain, ok := candidate.Labels[term.TopologyKey]
		if !ok {
			// The candidate carries no value for the topology key, so it cannot
			// be in any domain — the scheduler treats this as unschedulable for
			// a required term.
			return false
		}
		// Any pod matching the selector, still running, in the same domain,
		// blocks placement here.
		for _, other := range remaining {
			if other.Labels[term.TopologyKey] != domain {
				continue
			}
			for _, p := range staying[other.Name] {
				if !namespaceInScope(term, pod, p) {
					continue
				}
				if sel.Matches(labels.Set(p.Labels)) {
					return false
				}
			}
		}
	}
	return true
}

// namespaceInScope implements the term's namespace scoping. Default is the
// pod's own namespace.
func namespaceInScope(term corev1.PodAffinityTerm, pod, other *corev1.Pod) bool {
	if len(term.Namespaces) == 0 {
		return other.Namespace == pod.Namespace
	}
	return contains(term.Namespaces, other.Namespace)
}

// --- NotReady --------------------------------------------------------------

func checkNotReady(res *Results, s *scope.Scope) {
	nr := s.NotReadyNodes()
	if len(nr) == 0 {
		return
	}
	detail := make([]string, 0, len(nr))
	for _, n := range nr {
		detail = append(detail, fmt.Sprintf("%s is NotReady — its kubelet is not confirming termination, so evictions there will not complete on their own", n.Name))
	}
	res.Add(Finding{
		Severity: Warning,
		Check:    "not-ready-nodes",
		Summary:  fmt.Sprintf("%d selected node(s) are NotReady", len(nr)),
		Detail:   detail,
	})
}

// --- helpers ---------------------------------------------------------------

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	// Long lists are noise; the first few plus a count is what gets read.
	const max = 10
	if len(out) > max {
		extra := len(out) - max
		out = append(out[:max], fmt.Sprintf("... and %d more", extra))
	}
	return out
}

// Summary renders a one-line result for each check.
func (r *Results) Summary() string {
	var b strings.Builder
	for _, f := range r.Findings {
		marker := "ok  "
		if f.Severity == Fatal {
			marker = "FAIL"
		} else if strings.HasPrefix(f.Summary, "ok") {
			marker = "ok  "
		} else {
			marker = "warn"
		}
		fmt.Fprintf(&b, "  %s  %-18s %s\n", marker, f.Check, f.Summary)
		for _, d := range f.Detail {
			fmt.Fprintf(&b, "          %s\n", d)
		}
	}
	return b.String()
}
