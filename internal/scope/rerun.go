package scope

import (
	"sort"

	corev1 "k8s.io/api/core/v1"

	"github.com/GlueOps/evac/internal/guard"
)

// Leftover is work discovered from the PV side rather than from pods.
//
// §7's recovery model is that every operation is convergent and a re-run
// re-derives reality. The pod-derived path covers the normal case, and a PVC
// carrying a deletionTimestamp is self-evidently unfinished work. The gap opens
// only if a run died between classification and the PVC delete — at which point
// there may be no pod left to discover the claim through.
//
// This second path closes it: list PVs, match nodeAffinity against the selected
// nodes, and follow claimRef back. It is a complete alternative rather than a
// partial one, because the only deletable volume source is pv.spec.local and
// the API requires local PVs to carry nodeAffinity — so every PVC this tool can
// delete is reachable this way.
type Leftover struct {
	PV *corev1.PersistentVolume
	// PVC is the claim the PV points at, or nil when the claim is already gone.
	PVC *corev1.PersistentVolumeClaim
	// Node is the node the volume is pinned to.
	Node string
	// Reason describes what makes this leftover work.
	Reason string
}

// Leftovers runs the PV-side discovery and returns anything the pod-derived
// scope did not already account for.
func (s *Scope) Leftovers() []Leftover {
	selected := make(map[string]bool, len(s.Nodes))
	for i := range s.Nodes {
		selected[s.Nodes[i].Name] = true
	}

	// Claims the pod-derived path already found, so the two can be diffed.
	known := make(map[string]bool, len(s.PVCs))
	for _, t := range s.PVCs {
		known[t.PVC.Namespace+"/"+t.PVC.Name] = true
	}

	pvcsByKey := make(map[string]*corev1.PersistentVolumeClaim, len(s.Snapshot.PVCs))
	for i := range s.Snapshot.PVCs {
		p := &s.Snapshot.PVCs[i]
		pvcsByKey[p.Namespace+"/"+p.Name] = p
	}

	var out []Leftover
	for i := range s.Snapshot.PVs {
		pv := &s.Snapshot.PVs[i]

		// Only local volumes are ever this tool's business.
		if guard.VolumeSource(pv) != "Local" {
			continue
		}
		node, ok := pinnedNode(pv)
		if !ok || !selected[node] {
			continue
		}

		switch {
		case pv.Status.Phase == corev1.VolumeReleased:
			// The claim is gone but the PV remains. With a Delete reclaim
			// policy this clears itself; with Retain it accumulates, and
			// someone pays for a disk nobody watches.
			out = append(out, Leftover{
				PV: pv, Node: node,
				Reason: "PV is Released — its claim is gone but the volume remains",
			})

		case pv.Spec.ClaimRef == nil:
			out = append(out, Leftover{
				PV: pv, Node: node,
				Reason: "PV has no claimRef — nothing references this volume",
			})

		default:
			key := pv.Spec.ClaimRef.Namespace + "/" + pv.Spec.ClaimRef.Name
			pvc, exists := pvcsByKey[key]
			if !exists {
				out = append(out, Leftover{
					PV: pv, Node: node,
					Reason: "PV points at a claim that no longer exists",
				})
				continue
			}
			if known[key] {
				continue // already in scope via its pod
			}
			if pvc.DeletionTimestamp != nil {
				// The signature of a run that died mid-flight: marked for
				// deletion, but no pod left to discover it through.
				out = append(out, Leftover{
					PV: pv, PVC: pvc, Node: node,
					Reason: "PVC is marked for deletion but no pod references it — unfinished work from an earlier run",
				})
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PV.Name < out[j].PV.Name })
	return out
}

// pinnedNode reads the hostname a local PV is bound to.
//
// The API requires local PVs to carry nodeAffinity, which is what makes the
// PV-side discovery path complete rather than best-effort.
func pinnedNode(pv *corev1.PersistentVolume) (string, bool) {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return "", false
	}
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Key != corev1.LabelHostname || expr.Operator != corev1.NodeSelectorOpIn {
				continue
			}
			if len(expr.Values) > 0 {
				return expr.Values[0], true
			}
		}
	}
	return "", false
}
