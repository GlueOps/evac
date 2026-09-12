//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/drain"
	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/output"
	"github.com/GlueOps/evac/internal/scope"
)

func ptr[T any](v T) *T { return &v }

// TestPhase2OrderingMovesLocalVolumeToAnotherNode is the check §5 called for
// before this tool could be trusted: delete a live StatefulSet PVC in the
// specified order and confirm the replacement comes up with a fresh volume on a
// different node rather than wedging Pending forever.
//
// The failure mode it guards against is subtle. Under the intuitive order —
// evict, then delete the PVC — the controller can recreate the pod before the
// claim is gone; the new pod binds to a healthy PVC whose PV is pinned to the
// node just cordoned, goes Pending indefinitely, and now holds the finalizer on
// the claim being deleted. Nothing about that is visible from unit tests: the
// fake clientset does not run controllers and does not honour finalizers.
func TestPhase2OrderingMovesLocalVolumeToAnotherNode(t *testing.T) {
	workerNodes(t, 2) // skip unless there is somewhere for the pod to move to
	ctx := context.Background()
	ns := createNamespace(t)

	sts := statefulSet(ns, "data", 1)
	if _, err := client.Clientset.AppsV1().StatefulSets(ns).Create(ctx, sts, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := waitForPod(t, ns, "data-0", 3*time.Minute)
	originalNode := pod.Spec.NodeName
	originalPV := boundPV(t, ns, "vol-data-0")

	t.Logf("data-0 is on %s with PV %s", originalNode, originalPV)

	// Registered BEFORE the drain: a t.Fatal on any path below would otherwise
	// leave the node cordoned forever, and workerNodes then filters it out so
	// later tests SKIP rather than fail — one real failure silently degrades
	// the rest of the suite.
	t.Cleanup(func() { uncordon(t, originalNode) })

	// Drain the node it landed on.
	sc := buildScope(t, originalNode)
	if len(sc.DeletablePVCs()) != 1 {
		t.Fatalf("scope found %d deletable PVC(s), want 1 — the local volume should be in scope",
			len(sc.DeletablePVCs()))
	}

	rec, err := output.New(output.Options{Stdout: os.Stdout, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()

	opts := drain.Defaults()
	opts.EvictionTimeout = 3 * time.Minute
	opts.JobDeadline = 2 * time.Minute
	opts.PVCTimeout = 2 * time.Minute

	res, err := drain.New(client.Clientset, rec, sc, opts, "evac drain").Run(ctx)
	if err != nil {
		t.Fatalf("drain returned an error: %v", err)
	}
	if res.Failed() {
		t.Fatalf("drain failed with exit code %d", res.Code())
	}

	// The replacement must be running, on a different node, on a new volume.
	replacement := waitForPod(t, ns, "data-0", 3*time.Minute)
	if replacement.Spec.NodeName == originalNode {
		t.Errorf("replacement is back on %s; the drain did not move it", originalNode)
	}
	newPV := boundPV(t, ns, "vol-data-0")
	if newPV == originalPV {
		t.Errorf("PV is still %s; the claim was not actually destroyed and recreated", originalPV)
	}
	t.Logf("data-0 moved to %s with a fresh PV %s", replacement.Spec.NodeName, newPV)

	// The old PV must be gone: local-path uses a Delete reclaim policy, so
	// leaving one behind would accumulate orphans nobody watches.
	if _, err := client.Clientset.CoreV1().PersistentVolumes().Get(ctx, originalPV, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("old PV %s still exists (err=%v)", originalPV, err)
	}

	// Cordon is one-way: the tool must never uncordon.
	node, err := client.Clientset.CoreV1().Nodes().Get(ctx, originalNode, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !node.Spec.Unschedulable {
		t.Errorf("%s is schedulable again; cordon is one-way and the operator uncordons", originalNode)
	}
	t.Cleanup(func() { uncordon(t, originalNode) })
}

// TestRerunIsConvergent checks §7: running a second time against an
// already-drained node must succeed and change nothing, rather than erroring on
// objects that are already gone.
func TestRerunIsConvergent(t *testing.T) {
	nodes := workerNodes(t, 2)
	ctx := context.Background()
	target := nodes[0].Name

	sc := buildScope(t, target)
	rec, err := output.New(output.Options{Stdout: os.Stdout, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()

	opts := drain.Defaults()
	opts.EvictionTimeout = 3 * time.Minute
	opts.JobDeadline = time.Minute

	t.Cleanup(func() { uncordon(t, target) })

	eng := drain.New(client.Clientset, rec, sc, opts, "evac drain")
	if _, err := eng.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Re-derive scope from live state, as a real re-run does.
	second := buildScope(t, target)
	res, err := drain.New(client.Clientset, rec, second, opts, "evac drain").Run(ctx)
	if err != nil {
		t.Fatalf("second run errored; every operation is meant to be convergent: %v", err)
	}
	if res.Failed() {
		t.Errorf("second run reported exit code %d; a re-run over finished work must succeed", res.Code())
	}
}

// --- helpers ---------------------------------------------------------------

func buildScope(t *testing.T, nodeName string) *scope.Scope {
	t.Helper()
	snap, err := client.FullSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	all := inventory.Build(snap)
	byName := inventory.ByName(all)
	n, ok := byName[nodeName]
	if !ok {
		t.Fatalf("node %s not found", nodeName)
	}
	sc, err := scope.Build(snap, []inventory.Node{*n}, client.Context)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func createNamespace(t *testing.T) string {
	t.Helper()
	// Include the test name so a collision with a still-Terminating namespace
	// from another test is impossible rather than merely unlikely.
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r >= 'A' && r <= 'Z':
			return r + 32
		}
		return '-'
	}, t.Name())
	if len(safe) > 40 {
		safe = safe[:40]
	}
	name := fmt.Sprintf("evac-it-%s-%d", strings.Trim(safe, "-"), time.Now().UnixNano()%100000)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if _, err := client.Clientset.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Wait for the namespace to actually go away. Deletion is asynchronous and
	// these tests share a cluster, so returning early leaves the previous
	// test's pods running on the node the next one is about to drain — which
	// silently turns an independent test into a dependent one.
	t.Cleanup(func() {
		ctx := context.Background()
		if err := client.Clientset.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			return
		}
		deadline := time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			if _, err := client.Clientset.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
				return
			}
			time.Sleep(2 * time.Second)
		}
		t.Logf("warning: namespace %s was still terminating after 2m", name)
	})
	return name
}

func statefulSet(ns, name string, replicas int32) *appsv1.StatefulSet {
	labels := map[string]string{"app": name}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    ptr(replicas),
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr(int64(3)),
					Containers: []corev1.Container{{
						Name:         "app",
						Image:        "busybox:1.36",
						Command:      []string{"sh", "-c", "sleep 86400"},
						VolumeMounts: []corev1.VolumeMount{{Name: "vol", MountPath: "/data"}},
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("20m"),
							corev1.ResourceMemory: resource.MustParse("16Mi"),
						}},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "vol"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: ptr("local-path"),
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("50Mi"),
					}},
				},
			}},
		},
	}
}

func waitForPod(t *testing.T, ns, name string, timeout time.Duration) *corev1.Pod {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		p, err := client.Clientset.CoreV1().Pods(ns).Get(context.Background(), name, metav1.GetOptions{})
		if err == nil && p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil {
			return p
		}
		if time.Now().After(deadline) {
			t.Fatalf("pod %s/%s did not reach Running within %s (last err: %v)", ns, name, timeout, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func boundPV(t *testing.T, ns, pvcName string) string {
	t.Helper()
	pvc, err := client.Clientset.CoreV1().PersistentVolumeClaims(ns).
		Get(context.Background(), pvcName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Spec.VolumeName == "" {
		t.Fatalf("pvc %s/%s is not bound", ns, pvcName)
	}
	return pvc.Spec.VolumeName
}

// uncordon restores a node after a test. The tool itself never does this —
// cordon is one-way by design — but a test must not leave the cluster degraded
// for the next one.
func uncordon(t *testing.T, name string) {
	t.Helper()
	patch := []byte(`{"spec":{"unschedulable":null}}`)
	_, err := client.Clientset.CoreV1().Nodes().Patch(
		context.Background(), name, "application/strategic-merge-patch+json", patch, metav1.PatchOptions{})
	if err != nil {
		t.Logf("warning: could not uncordon %s: %v", name, err)
	}
}
