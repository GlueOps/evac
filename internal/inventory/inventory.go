// Package inventory builds the per-node view §3 renders and every other
// command selects from.
package inventory

import (
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/GlueOps/evac/internal/kube"
)

// Control-plane signals, per §3.1. Any one of them is enough.
const (
	labelControlPlane = "node-role.kubernetes.io/control-plane"
	labelMaster       = "node-role.kubernetes.io/master"
	labelEtcd         = "node-role.kubernetes.io/etcd"
	taintControlPlane = "node-role.kubernetes.io/control-plane"
)

// Node is the aggregated view of one node.
type Node struct {
	Name   string
	Labels map[string]string

	// Age is time since the Node object was created.
	Age time.Duration
	// Uptime is time since the Ready condition last changed. This is a proxy
	// for boot time and is approximate: it actually reflects the last Ready
	// flap, so a node that briefly went NotReady reports a short uptime without
	// having rebooted. §3 requires the column be labelled honestly for this
	// reason — the gap between Age and Uptime is the signal, not Uptime alone.
	Uptime time.Duration

	KubeletVersion string
	KernelVersion  string
	OSImage        string

	// Pods counts non-DaemonSet pods on this node. DaemonSet pods are excluded
	// because they are never drained and counting them would overstate what a
	// drain actually moves.
	Pods int
	// PVCs counts distinct PVCs referenced by those pods.
	PVCs int

	Ready       bool
	Schedulable bool
	// ControlPlane nodes are never drainable by any path (§3.1).
	ControlPlane bool
	// ControlPlaneSignal names which §3.1 signal matched, for the annotation
	// shown in the table.
	ControlPlaneSignal string

	node *corev1.Node
}

// Raw returns the underlying Node object.
func (n *Node) Raw() *corev1.Node { return n.node }

// Drainable reports whether this node may be drained at all. Control plane and
// etcd nodes never are: draining them risks etcd quorum loss, which stops the
// cluster accepting writes, breaks this tool's own API calls mid-run, and
// requires a manual restore.
func (n *Node) Drainable() bool { return !n.ControlPlane }

// Build aggregates a snapshot into per-node rows.
//
// Everything here is computed in memory from the snapshot's list results. There
// is deliberately no per-node query: §3 requires a fixed number of list calls
// regardless of cluster size.
func Build(snap *kube.Snapshot) []Node {
	// Index PVC references by pod so the counts below are a single pass.
	podsByNode := make(map[string][]*corev1.Pod, len(snap.Nodes))
	for i := range snap.Pods {
		p := &snap.Pods[i]
		if p.Spec.NodeName == "" {
			continue // unscheduled; belongs to no node
		}
		podsByNode[p.Spec.NodeName] = append(podsByNode[p.Spec.NodeName], p)
	}

	out := make([]Node, 0, len(snap.Nodes))
	for i := range snap.Nodes {
		n := &snap.Nodes[i]
		row := Node{
			Name:           n.Name,
			Labels:         n.Labels,
			Age:            since(n.CreationTimestamp.Time, snap.TakenAt),
			KubeletVersion: n.Status.NodeInfo.KubeletVersion,
			KernelVersion:  n.Status.NodeInfo.KernelVersion,
			OSImage:        n.Status.NodeInfo.OSImage,
			Schedulable:    !n.Spec.Unschedulable,
			node:           n,
		}
		row.Ready, row.Uptime = readiness(n, snap.TakenAt)
		row.ControlPlane, row.ControlPlaneSignal = controlPlane(n)
		row.Pods, row.PVCs = counts(podsByNode[n.Name])
		out = append(out, row)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// counts returns the non-DaemonSet pod count and the number of distinct PVCs
// those pods reference.
func counts(pods []*corev1.Pod) (podCount, pvcCount int) {
	seen := make(map[string]struct{})
	for _, p := range pods {
		if ownedByDaemonSet(p) {
			continue
		}
		podCount++
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				seen[p.Namespace+"/"+v.PersistentVolumeClaim.ClaimName] = struct{}{}
			}
		}
	}
	return podCount, len(seen)
}

func ownedByDaemonSet(p *corev1.Pod) bool {
	for _, ref := range p.OwnerReferences {
		if ref.Controller != nil && *ref.Controller && ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

// readiness reports whether the node is Ready, and how long it has held that
// state.
func readiness(n *corev1.Node, now time.Time) (ready bool, uptime time.Duration) {
	for i := range n.Status.Conditions {
		c := &n.Status.Conditions[i]
		if c.Type != corev1.NodeReady {
			continue
		}
		return c.Status == corev1.ConditionTrue, since(c.LastTransitionTime.Time, now)
	}
	// No Ready condition at all — a node that has never reported.
	return false, 0
}

// controlPlane implements §3.1 detection.
//
// On k3s only the label fires: the server node carries
// node-role.kubernetes.io/control-plane but no taint at all, and is fully
// schedulable. The taint check is therefore a belt-and-braces path for kubeadm
// rather than the primary signal.
func controlPlane(n *corev1.Node) (bool, string) {
	for _, l := range []string{labelControlPlane, labelMaster, labelEtcd} {
		if _, ok := n.Labels[l]; ok {
			return true, "label " + l
		}
	}
	for i := range n.Spec.Taints {
		t := &n.Spec.Taints[i]
		if t.Key == taintControlPlane && t.Effect == corev1.TaintEffectNoSchedule {
			return true, "taint " + taintControlPlane + ":NoSchedule"
		}
	}
	return false, ""
}

func since(t, now time.Time) time.Duration {
	if t.IsZero() {
		return 0
	}
	if d := now.Sub(t); d > 0 {
		return d
	}
	return 0
}

// Drainable returns only the nodes that may be drained.
func Drainable(nodes []Node) []Node {
	out := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if n.Drainable() {
			out = append(out, n)
		}
	}
	return out
}

// ControlPlaneNames lists the excluded nodes, for the note §4 shows above the
// picker and the message §3.1 requires when a selection is filtered.
func ControlPlaneNames(nodes []Node) []string {
	var out []string
	for _, n := range nodes {
		if n.ControlPlane {
			out = append(out, n.Name)
		}
	}
	return out
}

// SingleNodeCluster reports the §3.1 edge case: an install where the only node
// is both control plane and worker, so nothing is drainable. Worth its own
// message rather than reporting an empty selection.
func SingleNodeCluster(nodes []Node) bool {
	return len(nodes) == 1 && nodes[0].ControlPlane
}

// ByName indexes rows for selection lookups.
func ByName(nodes []Node) map[string]*Node {
	m := make(map[string]*Node, len(nodes))
	for i := range nodes {
		m[nodes[i].Name] = &nodes[i]
	}
	return m
}

// CuratedLabels implements §3's default label display: show the labels an
// operator chose to set, hide the well-known topology noise every node carries.
func CuratedLabels(l map[string]string) string {
	var keys []string
	for k := range l {
		if isWellKnownLabel(k) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+l[k])
	}
	return strings.Join(parts, ",")
}

// isWellKnownLabel reports the upstream labels present on essentially every
// node, which add width without adding information.
func isWellKnownLabel(k string) bool {
	switch {
	case strings.HasPrefix(k, "kubernetes.io/"),
		strings.HasPrefix(k, "topology.kubernetes.io/"),
		strings.HasPrefix(k, "node.kubernetes.io/"),
		strings.HasPrefix(k, "beta.kubernetes.io/"),
		strings.HasPrefix(k, "topology.k8s.io/"),
		strings.HasPrefix(k, "node-role.kubernetes.io/"):
		return true
	}
	return false
}
