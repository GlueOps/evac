package classify

import (
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// --- fixture helpers -------------------------------------------------------

func ptr[T any](v T) *T { return &v }

func pod(ns, name string, opts ...func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{}},
		Spec:       corev1.PodSpec{RestartPolicy: corev1.RestartPolicyAlways},
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func withLabels(l map[string]string) func(*corev1.Pod) {
	return func(p *corev1.Pod) { p.Labels = l }
}

func ownedBy(kind, name string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		p.OwnerReferences = append(p.OwnerReferences, metav1.OwnerReference{
			Kind: kind, Name: name, Controller: ptr(true),
		})
	}
}

// nonControllerOwner attaches an ownerReference that is NOT the controller.
// Pods can carry these; they must not be read as "something will recreate me".
func nonControllerOwner(kind, name string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		p.OwnerReferences = append(p.OwnerReferences, metav1.OwnerReference{
			Kind: kind, Name: name, Controller: ptr(false),
		})
	}
}

func rs(ns, name string, replicas int32, deployment string) appsv1.ReplicaSet {
	r := appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.ReplicaSetSpec{Replicas: ptr(replicas)},
	}
	if deployment != "" {
		r.OwnerReferences = []metav1.OwnerReference{{
			Kind: "Deployment", Name: deployment, Controller: ptr(true),
		}}
	}
	return r
}

func deploy(ns, name string, replicas int32) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr(replicas)},
	}
}

func sts(ns, name string, replicas int32) appsv1.StatefulSet {
	return appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr(replicas)},
	}
}

func pdb(ns, name string, sel *metav1.LabelSelector) policyv1.PodDisruptionBudget {
	return policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector:     sel,
			MinAvailable: ptr(intstr.FromInt32(1)),
		},
	}
}

func matchLabels(l map[string]string) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: l}
}

// --- replica resolution ----------------------------------------------------

// The mistake §5 names explicitly. Mid-rollout the old ReplicaSet is scaled to
// 1 while the Deployment still wants 3. Reading the ReplicaSet would call this
// pod fragile and delete it outright, at the exact moment a drain is riskiest.
// The fixtures must disagree or the test cannot catch the bug.
func TestReplicaCountComesFromDeploymentNotReplicaSetMidRollout(t *testing.T) {
	c := New(Inputs{
		ReplicaSets: []appsv1.ReplicaSet{rs("app", "web-old", 1, "web")},
		Deployments: []appsv1.Deployment{deploy("app", "web", 3)},
		PDBs:        []policyv1.PodDisruptionBudget{pdb("app", "web", matchLabels(map[string]string{"app": "web"}))},
	})

	got := c.Classify(pod("app", "web-old-abc", withLabels(map[string]string{"app": "web"}), ownedBy("ReplicaSet", "web-old")))

	if got.Class != Normal {
		t.Errorf("class = %q, want %q (Deployment wants 3 replicas; the ReplicaSet's 1 is a rollout artifact)", got.Class, Normal)
	}
	if got.ControllerKind != "Deployment" || got.ControllerName != "web" {
		t.Errorf("controller = %s/%s, want Deployment/web", got.ControllerKind, got.ControllerName)
	}
	if got.Replicas == nil || *got.Replicas != 3 {
		t.Errorf("replicas = %v, want 3 (read from the Deployment, not the ReplicaSet)", got.Replicas)
	}
}

// spec.replicas is desired state; status.readyReplicas is current health. A
// 3-replica workload with two pods temporarily unhealthy is not fragile.
func TestUnhealthyReplicasDoNotMakeAWorkloadFragile(t *testing.T) {
	d := deploy("app", "web", 3)
	d.Status.ReadyReplicas = 1 // two replicas currently down
	c := New(Inputs{
		ReplicaSets: []appsv1.ReplicaSet{rs("app", "web-1", 3, "web")},
		Deployments: []appsv1.Deployment{d},
		PDBs:        []policyv1.PodDisruptionBudget{pdb("app", "web", matchLabels(map[string]string{"app": "web"}))},
	})

	got := c.Classify(pod("app", "web-1-abc", withLabels(map[string]string{"app": "web"}), ownedBy("ReplicaSet", "web-1")))

	if got.Class != Normal {
		t.Errorf("class = %q, want %q (spec.replicas=3 governs, not status.readyReplicas=1)", got.Class, Normal)
	}
}

func TestSingleReplicaStatefulSetIsFragile(t *testing.T) {
	c := New(Inputs{
		StatefulSets: []appsv1.StatefulSet{sts("db", "vault", 1)},
		PDBs:         []policyv1.PodDisruptionBudget{pdb("db", "vault", matchLabels(map[string]string{"app": "vault"}))},
	})

	got := c.Classify(pod("db", "vault-0", withLabels(map[string]string{"app": "vault"}), ownedBy("StatefulSet", "vault")))

	if got.Class != Fragile {
		t.Fatalf("class = %q, want %q", got.Class, Fragile)
	}
	if !got.hasFragileReason(FragileSingleReplica) {
		t.Errorf("reasons = %v, want to include %q", got.FragileReasons, FragileSingleReplica)
	}
	// This is the population that actually takes downtime: one replica held by
	// a PDB that allows zero disruptions. §5 requires it be surfaced separately.
	if !got.BearsDowntime() {
		t.Error("BearsDowntime() = false, want true (single replica with a matching PDB)")
	}
}

// A bare ReplicaSet with no Deployment above it is a legitimate top controller.
func TestBareReplicaSetIsItsOwnTopController(t *testing.T) {
	c := New(Inputs{
		ReplicaSets: []appsv1.ReplicaSet{rs("app", "orphan", 2, "")},
		PDBs:        []policyv1.PodDisruptionBudget{pdb("app", "all", &metav1.LabelSelector{})},
	})

	got := c.Classify(pod("app", "orphan-xyz", ownedBy("ReplicaSet", "orphan")))

	if got.ControllerKind != "ReplicaSet" {
		t.Errorf("controller kind = %q, want ReplicaSet", got.ControllerKind)
	}
	if got.Replicas == nil || *got.Replicas != 2 {
		t.Errorf("replicas = %v, want 2", got.Replicas)
	}
}

// An unrecognised controller kind (a custom operator's CRD) leaves the replica
// count unknown. The conservative direction is eviction, not direct deletion.
func TestUnknownControllerKindDoesNotTriggerSingleReplica(t *testing.T) {
	c := New(Inputs{
		PDBs: []policyv1.PodDisruptionBudget{pdb("app", "all", &metav1.LabelSelector{})},
	})

	got := c.Classify(pod("app", "kafka-0", ownedBy("KafkaCluster", "kafka")))

	if got.Replicas != nil {
		t.Errorf("replicas = %v, want nil (kind is unresolvable)", got.Replicas)
	}
	if got.Class != Normal {
		t.Errorf("class = %q, want %q — unknown replica count must not imply fragile, since fragile means direct delete", got.Class, Normal)
	}
	if got.Unmanaged() {
		t.Error("Unmanaged() = true, want false — it has a controller, just not one we can resolve")
	}
}

// --- PDB matching ----------------------------------------------------------

// The trap §5 names: an empty selector reads like "matches nothing" and in fact
// matches every pod in the namespace.
func TestEmptyPDBSelectorMatchesEveryPodInItsNamespace(t *testing.T) {
	c := New(Inputs{
		StatefulSets: []appsv1.StatefulSet{sts("app", "web", 3)},
		PDBs:         []policyv1.PodDisruptionBudget{pdb("app", "catch-all", &metav1.LabelSelector{})},
	})

	got := c.Classify(pod("app", "web-0", withLabels(map[string]string{"totally": "unrelated"}), ownedBy("StatefulSet", "web")))

	if len(got.MatchingPDBs) != 1 || got.MatchingPDBs[0] != "catch-all" {
		t.Errorf("matching PDBs = %v, want [catch-all]", got.MatchingPDBs)
	}
	if got.Class != Normal {
		t.Errorf("class = %q, want %q", got.Class, Normal)
	}
}

// A nil selector is the opposite of an empty one and must select nothing.
func TestNilPDBSelectorMatchesNothing(t *testing.T) {
	c := New(Inputs{
		StatefulSets: []appsv1.StatefulSet{sts("app", "web", 3)},
		PDBs:         []policyv1.PodDisruptionBudget{pdb("app", "broken", nil)},
	})

	got := c.Classify(pod("app", "web-0", withLabels(map[string]string{"app": "web"}), ownedBy("StatefulSet", "web")))

	if len(got.MatchingPDBs) != 0 {
		t.Errorf("matching PDBs = %v, want none", got.MatchingPDBs)
	}
	if !got.hasFragileReason(FragileNoPDB) {
		t.Errorf("reasons = %v, want to include %q", got.FragileReasons, FragileNoPDB)
	}
}

func TestPDBInAnotherNamespaceNeverApplies(t *testing.T) {
	c := New(Inputs{
		StatefulSets: []appsv1.StatefulSet{sts("app", "web", 3)},
		PDBs: []policyv1.PodDisruptionBudget{
			pdb("other", "web", matchLabels(map[string]string{"app": "web"})),
		},
	})

	got := c.Classify(pod("app", "web-0", withLabels(map[string]string{"app": "web"}), ownedBy("StatefulSet", "web")))

	if len(got.MatchingPDBs) != 0 {
		t.Errorf("matching PDBs = %v, want none — the PDB is in namespace 'other'", got.MatchingPDBs)
	}
	if got.Class != Fragile {
		t.Errorf("class = %q, want %q (no PDB applies)", got.Class, Fragile)
	}
}

// --- exclusions ------------------------------------------------------------

// A DaemonSet pod has one replica per node and typically no PDB, so it would
// look maximally fragile. Exclusions must be evaluated first.
func TestDaemonSetPodIsExcludedNotFragile(t *testing.T) {
	c := New(Inputs{})
	got := c.Classify(pod("kube-system", "cilium-abc", ownedBy("DaemonSet", "cilium")))

	if got.Class != Excluded {
		t.Fatalf("class = %q, want %q — a DaemonSet pod has no PDB and one replica per node, so order of checks matters", got.Class, Excluded)
	}
	if got.Exclusion != ExcludedDaemonSet {
		t.Errorf("exclusion = %q, want %q", got.Exclusion, ExcludedDaemonSet)
	}
}

func TestJobPodIsExcluded(t *testing.T) {
	c := New(Inputs{})
	got := c.Classify(pod("analytics", "rollup-x7f", ownedBy("Job", "rollup")))

	if got.Class != Excluded || got.Exclusion != ExcludedJob {
		t.Errorf("class/exclusion = %q/%q, want %q/%q", got.Class, got.Exclusion, Excluded, ExcludedJob)
	}
}

func TestMirrorPodIsExcluded(t *testing.T) {
	c := New(Inputs{})
	p := pod("kube-system", "kube-apiserver-node-a")
	p.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "abc123"}

	got := c.Classify(p)

	if got.Class != Excluded || got.Exclusion != ExcludedMirror {
		t.Errorf("class/exclusion = %q/%q, want %q/%q", got.Class, got.Exclusion, Excluded, ExcludedMirror)
	}
}

// The local-path provisioner's cleanup pod has no controller and sets
// spec.nodeName directly, so on a re-run it would otherwise be reported as a
// permanent pod loss and deleted.
func TestLocalPathHelperPodIsExcludedNotTreatedAsUnmanaged(t *testing.T) {
	c := New(Inputs{})
	p := pod("kube-system", "helper-pod-delete-pvc-c8381767-c95f")
	p.Spec.RestartPolicy = corev1.RestartPolicyNever

	got := c.Classify(p)

	if got.Class != Excluded {
		t.Fatalf("class = %q, want %q", got.Class, Excluded)
	}
	if got.Exclusion != ExcludedStorageHelper {
		t.Errorf("exclusion = %q, want %q", got.Exclusion, ExcludedStorageHelper)
	}
	if got.Unmanaged() {
		t.Error("Unmanaged() = true — a re-run would report this as a permanent pod loss and delete it")
	}
}

// The helper-pod exclusion must not swallow a real user pod that happens to
// share the name prefix. Each conjunct of isStorageHelper is relaxed on its
// own: flipping all of them at once, as this test used to, means any single
// check could be dropped unnoticed.
func TestUserPodNamedHelperPodIsNotExcluded(t *testing.T) {
	c := New(Inputs{})

	t.Run("controller-owned but RestartPolicy=Never", func(t *testing.T) {
		p := pod("default", "helper-pod-mine", ownedBy("StatefulSet", "helper"))
		p.Spec.RestartPolicy = corev1.RestartPolicyNever

		if got := c.Classify(p); got.Class == Excluded {
			t.Error("a controller-owned pod was excluded as a storage helper; the provisioner's pod has no controller")
		}
	})

	t.Run("unowned but RestartPolicy=Always", func(t *testing.T) {
		p := pod("default", "helper-pod-mine")
		p.Spec.RestartPolicy = corev1.RestartPolicyAlways

		if got := c.Classify(p); got.Class == Excluded {
			t.Error("an always-restarting pod was excluded as a storage helper; the provisioner's pod runs once")
		}
	})

	t.Run("name merely contains the prefix", func(t *testing.T) {
		p := pod("default", "my-helper-pod-x")
		p.Spec.RestartPolicy = corev1.RestartPolicyNever

		if got := c.Classify(p); got.Class == Excluded {
			t.Error("a pod whose name only contains the prefix was excluded")
		}
	})
}

// --- unmanaged pods --------------------------------------------------------

func TestPodWithNoOwnerIsUnmanagedAndFragile(t *testing.T) {
	c := New(Inputs{})
	got := c.Classify(pod("debug", "shell-jdoe"))

	if got.Class != Fragile {
		t.Fatalf("class = %q, want %q", got.Class, Fragile)
	}
	if !got.Unmanaged() {
		t.Errorf("Unmanaged() = false, reasons = %v", got.FragileReasons)
	}
	if got.ControllerKind != "" {
		t.Errorf("controller kind = %q, want empty", got.ControllerKind)
	}
}

// A non-controller ownerReference does not mean something will recreate the
// pod. Only controller:true counts.
func TestNonControllerOwnerReferenceStillCountsAsUnmanaged(t *testing.T) {
	c := New(Inputs{})
	got := c.Classify(pod("debug", "orphan", nonControllerOwner("ReplicaSet", "gone")))

	if !got.Unmanaged() {
		t.Errorf("Unmanaged() = false — the ownerReference is not marked controller:true, so nothing recreates this pod")
	}
}

// A pod can be fragile for several independent reasons at once, and all of
// them should be reported so the operator sees the full picture.
func TestMultipleFragileReasonsAreAllRecorded(t *testing.T) {
	c := New(Inputs{StatefulSets: []appsv1.StatefulSet{sts("db", "solo", 1)}})

	got := c.Classify(pod("db", "solo-0", ownedBy("StatefulSet", "solo")))

	if !got.hasFragileReason(FragileSingleReplica) || !got.hasFragileReason(FragileNoPDB) {
		t.Errorf("reasons = %v, want both %q and %q", got.FragileReasons, FragileSingleReplica, FragileNoPDB)
	}
	// No PDB means eviction would have worked; this is not the downtime cohort.
	if got.BearsDowntime() {
		t.Error("BearsDowntime() = true, want false — with no PDB the eviction API would not refuse it")
	}
}

// --- PDB selector forms ----------------------------------------------------

// Every other PDB test here uses MatchLabels. Without this, replacing
// LabelSelectorAsSelector with a MatchLabels-only comparison passes the whole
// suite — while every matchExpressions-selected pod silently becomes
// FragileNoPDB and is direct-deleted in phase 3 instead of evicted.
func TestPDBWithMatchExpressionsIsHonoured(t *testing.T) {
	t.Parallel()
	c := New(Inputs{
		StatefulSets: []appsv1.StatefulSet{sts("app", "web", 3)},
		PDBs: []policyv1.PodDisruptionBudget{pdb("app", "web", &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "app",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"web", "api"},
			}},
		})},
	})

	got := c.Classify(pod("app", "web-0", withLabels(map[string]string{"app": "web"}), ownedBy("StatefulSet", "web")))

	if len(got.MatchingPDBs) != 1 || got.MatchingPDBs[0] != "web" {
		t.Fatalf("matching PDBs = %v, want [web] — a matchExpressions selector must be evaluated, not ignored", got.MatchingPDBs)
	}
	if got.Class != Normal {
		t.Errorf("class = %q, want %q; treating this pod as fragile would delete it outright", got.Class, Normal)
	}
}

// A pod can be selected by several budgets. Reporting only the first would
// under-state what is holding an eviction when one of them stalls.
func TestAllMatchingPDBsAreReported(t *testing.T) {
	t.Parallel()
	labels := map[string]string{"app": "web", "tier": "front"}
	c := New(Inputs{
		StatefulSets: []appsv1.StatefulSet{sts("app", "web", 3)},
		PDBs: []policyv1.PodDisruptionBudget{
			pdb("app", "by-app", matchLabels(map[string]string{"app": "web"})),
			pdb("app", "by-tier", matchLabels(map[string]string{"tier": "front"})),
			pdb("app", "catch-all", &metav1.LabelSelector{}),
		},
	})

	got := c.Classify(pod("app", "web-0", withLabels(labels), ownedBy("StatefulSet", "web")))

	if len(got.MatchingPDBs) != 3 {
		t.Errorf("matching PDBs = %v, want all three", got.MatchingPDBs)
	}
}

// An unparseable selector must not be treated as a match: doing so would
// reclassify the pod as Normal and send it into an eviction that stalls.
func TestUnparseablePDBSelectorDoesNotMatch(t *testing.T) {
	t.Parallel()
	c := New(Inputs{
		StatefulSets: []appsv1.StatefulSet{sts("app", "web", 3)},
		PDBs: []policyv1.PodDisruptionBudget{pdb("app", "broken", &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key: "app", Operator: "Bogus", Values: []string{"web"},
			}},
		})},
	})

	got := c.Classify(pod("app", "web-0", withLabels(map[string]string{"app": "web"}), ownedBy("StatefulSet", "web")))

	if len(got.MatchingPDBs) != 0 {
		t.Errorf("matching PDBs = %v, want none from a malformed selector", got.MatchingPDBs)
	}
	if !got.hasFragileReason(FragileNoPDB) {
		t.Errorf("reasons = %v, want %q", got.FragileReasons, FragileNoPDB)
	}
}

// --- broken ownerReference chains ------------------------------------------

// The ReplicaSet names a Deployment that is not in the snapshot — RBAC gap, or
// deleted mid-drain. The replica count is then unknowable, and this line is one
// edit away from §5's named catastrophe: falling back to rs.Spec.Replicas here
// would make a mid-rollout ReplicaSet at 1 look fragile and delete it outright.
func TestMissingDeploymentLeavesReplicaCountUnknown(t *testing.T) {
	t.Parallel()
	c := New(Inputs{
		ReplicaSets: []appsv1.ReplicaSet{rs("app", "web-old", 1, "web")},
		// Deployment "web" deliberately absent.
		PDBs: []policyv1.PodDisruptionBudget{pdb("app", "web", &metav1.LabelSelector{})},
	})

	got := c.Classify(pod("app", "web-old-abc", ownedBy("ReplicaSet", "web-old")))

	if got.Replicas != nil {
		t.Errorf("replicas = %v, want nil — the ReplicaSet's own count must not be substituted", *got.Replicas)
	}
	if got.ControllerKind != "Deployment" {
		t.Errorf("controller kind = %q, want Deployment (the name is known even when the object is not)", got.ControllerKind)
	}
	if got.hasFragileReason(FragileSingleReplica) {
		t.Error("an unknown replica count was treated as a single replica; that deletes the pod instead of evicting it")
	}
	if got.Unmanaged() {
		t.Error("Unmanaged() = true — the pod has a controller, it just cannot be resolved")
	}
}

// The ReplicaSet named by the pod is itself absent from the snapshot.
func TestMissingReplicaSetLeavesReplicaCountUnknown(t *testing.T) {
	t.Parallel()
	c := New(Inputs{PDBs: []policyv1.PodDisruptionBudget{pdb("app", "all", &metav1.LabelSelector{})}})

	got := c.Classify(pod("app", "web-abc", ownedBy("ReplicaSet", "gone")))

	if got.Replicas != nil {
		t.Errorf("replicas = %v, want nil", *got.Replicas)
	}
	if got.ControllerKind != "ReplicaSet" {
		t.Errorf("controller kind = %q, want ReplicaSet", got.ControllerKind)
	}
	if got.Unmanaged() {
		t.Error("Unmanaged() = true — a controller reference exists even if the object does not")
	}
}

// A StatefulSet or ReplicationController that is named but absent behaves the
// same way: the name survives, the count does not.
func TestMissingStatefulSetLeavesReplicaCountUnknown(t *testing.T) {
	t.Parallel()
	c := New(Inputs{PDBs: []policyv1.PodDisruptionBudget{pdb("db", "all", &metav1.LabelSelector{})}})

	got := c.Classify(pod("db", "vault-0", ownedBy("StatefulSet", "vault")))

	if got.Replicas != nil {
		t.Errorf("replicas = %v, want nil", *got.Replicas)
	}
	if got.Class != Normal {
		t.Errorf("class = %q, want %q — unknown must not imply fragile", got.Class, Normal)
	}
}

// --- replica count boundaries ----------------------------------------------

// The single-replica test is `== 1`. A `<= 1` mutation would additionally
// capture scaled-to-zero workloads, and a `< 2` one the same; both change which
// pods get deleted rather than evicted.
func TestSingleReplicaTestIsExactlyOne(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		replicas int32
		fragile  bool
	}{
		{0, false}, // scaled to zero: nothing to protect, nothing to delete
		{1, true},
		{2, false},
		{3, false},
	} {
		t.Run(fmt.Sprintf("replicas=%d", tc.replicas), func(t *testing.T) {
			c := New(Inputs{
				StatefulSets: []appsv1.StatefulSet{sts("app", "web", tc.replicas)},
				PDBs:         []policyv1.PodDisruptionBudget{pdb("app", "web", &metav1.LabelSelector{})},
			})
			got := c.Classify(pod("app", "web-0", ownedBy("StatefulSet", "web")))

			if got.hasFragileReason(FragileSingleReplica) != tc.fragile {
				t.Errorf("single-replica reason = %v, want %v for replicas=%d",
					!tc.fragile, tc.fragile, tc.replicas)
			}
		})
	}
}
