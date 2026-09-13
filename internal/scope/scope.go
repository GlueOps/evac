// Package scope computes what a drain will actually touch: which pods are in
// play, which PVCs will be destroyed, and what the guards say about each.
package scope

import (
	"sort"

	corev1 "k8s.io/api/core/v1"

	"github.com/GlueOps/evac/internal/classify"
	"github.com/GlueOps/evac/internal/guard"
	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
)

// Scope is the full picture the plan renders and the drain executes.
type Scope struct {
	Nodes []inventory.Node
	// Pods are every pod on the selected nodes, classified.
	Pods []classify.Result
	// PVCs are the claims belonging to in-scope pods, each with a guard verdict.
	PVCs []PVCTarget
	// Provider is the cluster-wide guard result. When Disabled, no PVC is
	// deleted anywhere in this run.
	Provider guard.ProviderVerdict

	Snapshot *kube.Snapshot
}

// PVCTarget is one claim considered for deletion, tied to the pods that put it
// in scope.
type PVCTarget struct {
	PVC *corev1.PersistentVolumeClaim
	// Pod is the first in-scope pod found referencing the claim, used for
	// display. Deletion is driven by Pods below.
	Pod *corev1.Pod
	// Pods is every in-scope pod referencing this claim.
	//
	// A ReadWriteOnce local volume may legitimately be mounted by more than one
	// pod on the same node, and attributing the claim to just one of them meant
	// the delete was issued while another pod still held the pvc-protection
	// finalizer — a guaranteed five-minute stall, exit 5, and a claim left
	// marked for deletion underneath a pod that is still running.
	Pods []*corev1.Pod
	// Decision is the per-PVC volume-source verdict.
	Decision guard.Decision
	// AlreadyDeleting is true when the PVC already carries a deletionTimestamp.
	// §7 treats that as self-evidently unfinished work from a previous run: the
	// delete is skipped and the flow proceeds straight to eviction.
	AlreadyDeleting bool
}

// LastConsumer reports whether pod is the final referencing pod still to be
// moved, given the set of pods already handled.
//
// Only then is it meaningful to wait for the claim to disappear: while another
// selected pod still references it, pvc-protection holding it is the correct
// state rather than a stall. "Last" cannot be decided from the list alone —
// consumers may be split across phase 2 and phase 3, and under --parallel
// across nodes for a ReadWriteMany volume — so it is answered from what has
// actually been moved.
//
// moved is keyed by namespace/name.
func (t PVCTarget) LastConsumer(pod *corev1.Pod, moved map[string]bool) bool {
	for _, p := range t.Pods {
		if p.Namespace == pod.Namespace && p.Name == pod.Name {
			continue
		}
		if !moved[p.Namespace+"/"+p.Name] {
			return false
		}
	}
	return true
}

// Deletable reports whether this PVC will actually be deleted, accounting for
// both guards.
func (t PVCTarget) Deletable(providerDisabled bool) bool {
	return !providerDisabled && t.Decision.Allowed
}

// Build computes the scope for a set of selected nodes.
func Build(snap *kube.Snapshot, nodes []inventory.Node, contextName string) (*Scope, error) {
	cl, err := classify.FromSnapshot(snap)
	if err != nil {
		return nil, err
	}

	selected := make(map[string]bool, len(nodes))
	for i := range nodes {
		selected[nodes[i].Name] = true
	}

	var onNodes []*corev1.Pod
	for i := range snap.Pods {
		p := &snap.Pods[i]
		if !selected[p.Spec.NodeName] {
			continue
		}
		// Pods that have already reached a terminal phase are not part of the
		// drain. They hold no resources, nothing needs evicting, and — the
		// reason this matters — phase 4 would otherwise wait on a Job pod that
		// finished days ago. Every k3s cluster carries a Succeeded
		// helm-install-* Job pod, so without this the Job wait runs to its
		// deadline on the first node of every drain.
		if isTerminal(p) {
			continue
		}
		onNodes = append(onNodes, p)
	}
	sort.Slice(onNodes, func(i, j int) bool {
		if onNodes[i].Namespace != onNodes[j].Namespace {
			return onNodes[i].Namespace < onNodes[j].Namespace
		}
		return onNodes[i].Name < onNodes[j].Name
	})

	s := &Scope{
		Nodes:    nodes,
		Pods:     cl.ClassifyAll(onNodes),
		Snapshot: snap,
		Provider: guard.DetectProvider(snap.Nodes, snap.CSIDrivers, snap.StorageClasses, contextName),
	}
	s.PVCs = discoverPVCs(snap, s.Pods)
	return s, nil
}

// isTerminal reports a pod that has finished and will not run again.
func isTerminal(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

// discoverPVCs finds the claims to destroy.
//
// Discovery is scoped to the *target pod set*, never swept by node. Sweeping by
// node would pick up claims belonging to pods that are never evicted — an
// excluded Job pod, a DaemonSet pod — and those pods would keep running with a
// PVC stuck Terminating underneath them.
func discoverPVCs(snap *kube.Snapshot, pods []classify.Result) []PVCTarget {
	pvcsByKey := make(map[string]*corev1.PersistentVolumeClaim, len(snap.PVCs))
	for i := range snap.PVCs {
		p := &snap.PVCs[i]
		pvcsByKey[p.Namespace+"/"+p.Name] = p
	}
	pvIndex := guard.IndexPVs(snap.PVs)
	scIndex := guard.IndexStorageClasses(snap.StorageClasses)

	var out []PVCTarget
	seen := make(map[string]int)

	for _, res := range pods {
		// Excluded pods are not evicted, so their claims are not in scope.
		if res.Class == classify.Excluded {
			continue
		}
		for _, vol := range res.Pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			key := res.Pod.Namespace + "/" + vol.PersistentVolumeClaim.ClaimName
			if idx, ok := seen[key]; ok {
				// Already in scope via another pod: record this one too rather
				// than dropping it, so the verification step knows when the
				// last consumer has gone.
				out[idx].Pods = append(out[idx].Pods, res.Pod)
				continue
			}
			pvc, ok := pvcsByKey[key]
			if !ok {
				continue // referenced but absent; nothing to delete
			}
			seen[key] = len(out)
			out = append(out, PVCTarget{
				PVC:             pvc,
				Pod:             res.Pod,
				Pods:            []*corev1.Pod{res.Pod},
				Decision:        guard.Evaluate(pvc, pvIndex, scIndex),
				AlreadyDeleting: pvc.DeletionTimestamp != nil,
			})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].PVC.Namespace != out[j].PVC.Namespace {
			return out[i].PVC.Namespace < out[j].PVC.Namespace
		}
		return out[i].PVC.Name < out[j].PVC.Name
	})
	return out
}

// --- derived views ---------------------------------------------------------

// ByClass returns the pods in one bucket.
func (s *Scope) ByClass(c classify.Class) []classify.Result {
	var out []classify.Result
	for _, r := range s.Pods {
		if r.Class == c {
			out = append(out, r)
		}
	}
	return out
}

// Unmanaged returns pods with no controller — permanent losses, which §8
// requires be listed separately and labelled as not coming back.
func (s *Scope) Unmanaged() []classify.Result {
	var out []classify.Result
	for _, r := range s.Pods {
		if r.Unmanaged() {
			out = append(out, r)
		}
	}
	return out
}

// DowntimeBearing returns the fragile pods that actually go down: single
// replica held by a PDB that allows zero disruptions. The rest of the fragile
// bucket would have evicted cleanly.
func (s *Scope) DowntimeBearing() []classify.Result {
	var out []classify.Result
	for _, r := range s.Pods {
		if r.BearsDowntime() {
			out = append(out, r)
		}
	}
	return out
}

// DeletablePVCs returns only the claims that will actually be destroyed.
func (s *Scope) DeletablePVCs() []PVCTarget {
	var out []PVCTarget
	for _, t := range s.PVCs {
		if t.Deletable(s.Provider.Disabled) {
			out = append(out, t)
		}
	}
	return out
}

// NamespaceSummary is the §8 blast radius: the inverse query, which is what a
// reviewer actually wants to see.
type NamespaceSummary struct {
	Namespace string
	Pods      int
	PVCs      int
	// NoPDB counts in-scope pods that no PodDisruptionBudget selects.
	NoPDB int
}

// ByNamespace aggregates the blast radius, sorted by namespace.
func (s *Scope) ByNamespace() []NamespaceSummary {
	agg := map[string]*NamespaceSummary{}
	get := func(ns string) *NamespaceSummary {
		if _, ok := agg[ns]; !ok {
			agg[ns] = &NamespaceSummary{Namespace: ns}
		}
		return agg[ns]
	}

	for _, r := range s.Pods {
		if r.Class == classify.Excluded {
			continue
		}
		e := get(r.Pod.Namespace)
		e.Pods++
		if len(r.MatchingPDBs) == 0 {
			e.NoPDB++
		}
	}
	for _, t := range s.DeletablePVCs() {
		get(t.PVC.Namespace).PVCs++
	}

	out := make([]NamespaceSummary, 0, len(agg))
	for _, v := range agg {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Namespace < out[j].Namespace })
	return out
}

// NotReadyNodes returns selected nodes whose kubelet is not reporting.
//
// Evicting a pod there sets a deletion timestamp, but nothing terminates the
// container because the kubelet that would do it is not answering — so the
// eviction never completes on its own. §8 flags these prominently; it is a
// warning rather than an error, since a broken node is a legitimate reason to
// drain.
func (s *Scope) NotReadyNodes() []inventory.Node {
	var out []inventory.Node
	for _, n := range s.Nodes {
		if !n.Ready {
			out = append(out, n)
		}
	}
	return out
}

// JobPods returns the excluded Job pods phase 4 waits on.
func (s *Scope) JobPods() []classify.Result {
	var out []classify.Result
	for _, r := range s.Pods {
		if r.Class == classify.Excluded && r.Exclusion == classify.ExcludedJob {
			out = append(out, r)
		}
	}
	return out
}
