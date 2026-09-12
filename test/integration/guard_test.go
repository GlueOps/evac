//go:build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/drain"
	"github.com/GlueOps/evac/internal/guard"
	"github.com/GlueOps/evac/internal/output"
)

// nfsStorageClass is created by test/integration/setup-nfs.sh. Tests that need
// a genuinely non-local volume skip without it rather than failing, so the
// normal integration run does not require the extra setup.
const nfsStorageClass = "nfs-csi"

func requireNFS(t *testing.T) {
	t.Helper()
	_, err := client.Clientset.StorageV1().StorageClasses().
		Get(context.Background(), nfsStorageClass, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		t.Skipf("StorageClass %q is absent; run test/integration/setup-nfs.sh %s first",
			nfsStorageClass, client.Context)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// TestNFSBackedVolumeSurvivesADrain is the test that unit tests cannot be.
//
// The volume-source allowlist is exercised in unit tests by reflecting over
// every PersistentVolumeSource field, but those fixtures are structs someone
// wrote. This drives the same guard against a PV that a real CSI driver
// produced, mounted by a real pod, on a real node — which is the only way to
// know the field paths are right and that a drain actually leaves the claim
// alone while still moving the workload.
//
// NFS is the case the spec calls out as must-persist: losing one would mean
// deleting data on a NAS that has nothing to do with the node being drained.
func TestNFSBackedVolumeSurvivesADrain(t *testing.T) {
	requireNFS(t)
	workerNodes(t, 2)
	ctx := context.Background()
	ns := createNamespace(t)

	sts := statefulSet(ns, "nfsapp", 1)
	sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName = ptr(nfsStorageClass)
	sts.Spec.VolumeClaimTemplates[0].Spec.AccessModes = []corev1.PersistentVolumeAccessMode{
		corev1.ReadWriteMany,
	}
	if _, err := client.Clientset.AppsV1().StatefulSets(ns).Create(ctx, sts, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	pod := waitForPod(t, ns, "nfsapp-0", 4*time.Minute)
	originalNode := pod.Spec.NodeName
	pvName := boundPV(t, ns, "vol-nfsapp-0")

	// Sanity-check the fixture itself: if this is not CSI-backed, the test
	// proves nothing.
	pv, err := client.Clientset.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if src := guard.VolumeSource(pv); src != "CSI" {
		t.Fatalf("PV %s has volume source %q, want CSI — the fixture is not what this test needs", pvName, src)
	}
	t.Logf("nfsapp-0 on %s, PV %s backed by CSI driver %s", originalNode, pvName, pv.Spec.CSI.Driver)

	// The guard must exclude it from the deletion set before anything runs.
	sc := buildScope(t, originalNode)
	for _, target := range sc.DeletablePVCs() {
		if target.PVC.Namespace == ns {
			t.Fatalf("PVC %s/%s is in the deletion set; a CSI volume must never be deletable",
				target.PVC.Namespace, target.PVC.Name)
		}
	}

	rec, err := output.New(output.Options{Stdout: os.Stdout, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()

	opts := drain.Defaults()
	opts.EvictionTimeout = 3 * time.Minute
	opts.JobDeadline = 2 * time.Minute

	t.Cleanup(func() { uncordon(t, originalNode) })

	res, err := drain.New(client.Clientset, rec, sc, opts, "evac drain").Run(ctx)
	if err != nil {
		t.Fatalf("drain errored: %v", err)
	}
	if res.Failed() {
		t.Fatalf("drain failed with exit code %d", res.Code())
	}

	// The claim must be untouched: same volume, no deletion timestamp.
	pvc, err := client.Clientset.CoreV1().PersistentVolumeClaims(ns).
		Get(ctx, "vol-nfsapp-0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the NFS-backed PVC is gone: %v", err)
	}
	if pvc.DeletionTimestamp != nil {
		t.Error("the NFS-backed PVC was marked for deletion")
	}
	if pvc.Spec.VolumeName != pvName {
		t.Errorf("PV changed from %s to %s; the volume was replaced", pvName, pvc.Spec.VolumeName)
	}
	if _, err := client.Clientset.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{}); err != nil {
		t.Errorf("the backing PV is gone: %v", err)
	}

	// ...and the workload must still have moved. Refusing to delete a claim
	// must not mean refusing to drain.
	moved := waitForPod(t, ns, "nfsapp-0", 4*time.Minute)
	if moved.Spec.NodeName == originalNode {
		t.Errorf("pod is still on %s; the drain kept the claim but did not move the workload", originalNode)
	}
	t.Logf("claim preserved on PV %s; pod moved to %s", pvName, moved.Spec.NodeName)
}

func createStorageClass(t *testing.T, name, provisioner string) {
	t.Helper()
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: provisioner,
	}
	if _, err := client.Clientset.StorageV1().StorageClasses().
		Create(context.Background(), sc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Clientset.StorageV1().StorageClasses().
			Delete(context.Background(), name, metav1.DeleteOptions{})
		waitGone(t, func() error {
			_, err := client.Clientset.StorageV1().StorageClasses().
				Get(context.Background(), name, metav1.GetOptions{})
			return err
		})
	})
}

// waitGuardClear blocks until the provider guard reports enabled, so each
// signal is measured against a clean slate rather than the previous one's
// leftovers.
func waitGuardClear(t *testing.T, node string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		sc := buildScope(t, node)
		if !sc.Provider.Disabled {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the provider guard never released: %s", sc.Provider.Signal)
		}
		time.Sleep(time.Second)
	}
}

// waitGone polls a getter until it reports NotFound. Object deletion is
// asynchronous, and these signals are read from cluster-wide state.
func waitGone(t *testing.T, get func() error) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if apierrors.IsNotFound(get()) {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("a cluster-scoped test object was still present after 60s; later tests in this package would see it")
			return
		}
		time.Sleep(time.Second)
	}
}

// TestProviderGuardSuppressesRealClaims replaces four near-identical subtests.
//
// All four §5 provider signals are already driven by
// guard.TestProviderDetection as a pure function. What a real cluster uniquely
// establishes is the wiring: that FullSnapshot actually collects the
// StorageClasses DetectProvider reads, and — the part the old version asserted
// vacuously — that tripping the guard suppresses claims that WOULD otherwise
// have been deleted. Asserting "zero deletable PVCs" is meaningless unless
// there was one to begin with, and on this shared cluster there usually is not.
//
// One signal is enough to prove the wiring; the cheapest is a StorageClass,
// which needs no node object and no providerID immutability workaround.
func TestProviderGuardSuppressesRealClaims(t *testing.T) {
	nodes := workerNodes(t, 2)
	ctx := context.Background()
	ns := createNamespace(t)

	// A real local-path claim, so the suppression below has something to act on.
	sts := statefulSet(ns, "data", 1)
	if _, err := client.Clientset.AppsV1().StatefulSets(ns).Create(ctx, sts, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := waitForPod(t, ns, "data-0", 3*time.Minute)
	target := pod.Spec.NodeName
	_ = nodes

	before := buildScope(t, target)
	if before.Provider.Disabled {
		t.Fatalf("the guard was already tripped before any signal: %s", before.Provider.Signal)
	}
	var deletable int
	for _, p := range before.DeletablePVCs() {
		if p.PVC.Namespace == ns {
			deletable++
		}
	}
	if deletable != 1 {
		t.Fatalf("%d deletable PVC(s) in %s before the signal, want 1 — without one the suppression assertion proves nothing", deletable, ns)
	}

	// Registered BEFORE the StorageClass so that LIFO cleanup order runs this
	// check AFTER the deletion it is verifying. Registering it last would run
	// it first, while the signal is still present.
	t.Cleanup(func() {
		if sc := buildScope(t, target); sc.Provider.Disabled {
			t.Errorf("the guard is still tripped after cleanup: %s", sc.Provider.Signal)
		}
	})

	createStorageClass(t, "evac-test-gp3", "ebs.csi.aws.com")

	after := buildScope(t, target)
	if !after.Provider.Disabled {
		t.Fatal("PVC deletion is still enabled after an EBS StorageClass appeared")
	}
	if want := "StorageClass evac-test-gp3 uses provisioner ebs.csi.aws.com"; after.Provider.Signal != want {
		t.Errorf("signal = %q, want %q", after.Provider.Signal, want)
	}
	if n := len(after.DeletablePVCs()); n != 0 {
		t.Errorf("%d PVC(s) are still deletable with the provider guard tripped", n)
	}

}

// The live cluster's own context must not look like EKS, or every test in this
// package would be running with the guard latched on and proving nothing. The
// ARN-parsing itself is a pure function, covered by guard.TestProviderDetection.
func TestLiveContextIsNotMistakenForEKS(t *testing.T) {
	if live := guard.DetectProvider(nil, nil, nil, client.Context); live.Disabled {
		t.Errorf("context %q trips the EKS check: %s", client.Context, live.Signal)
	}
}
