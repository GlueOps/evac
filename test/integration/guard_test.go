//go:build integration

package integration

import (
	"context"
	"os"
	"strings"
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

	res, err := drain.New(client.Clientset, rec, sc, opts, "evac drain").Run(ctx)
	if err != nil {
		t.Fatalf("drain errored: %v", err)
	}
	if res.Failed() {
		t.Fatalf("drain failed with exit code %d", res.Code())
	}
	t.Cleanup(func() { uncordon(t, originalNode) })

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

// TestAWSSignalsDisablePVCDeletion drives each of §5's four provider signals
// against a live cluster.
//
// Each is applied, checked, and removed in turn, so a failure cannot leave the
// guard latched on for later tests. Serial by design: these mutate
// cluster-scoped objects that DetectProvider reads.
func TestAWSSignalsDisablePVCDeletion(t *testing.T) {
	workerNodes(t, 1)
	ctx := context.Background()

	if sc := buildScope(t, workerNodes(t, 1)[0].Name); sc.Provider.Disabled {
		t.Fatalf("the guard is already tripped before any signal was applied: %s", sc.Provider.Signal)
	}

	tests := []struct {
		name       string
		wantSignal string
		apply      func(t *testing.T)
	}{
		{
			name:       "node providerID with an aws:// prefix",
			wantSignal: "providerID",
			apply: func(t *testing.T) {
				// providerID is immutable once the kubelet has set it, so this
				// introduces a node object rather than patching a real one.
				n := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "ip-10-0-1-42.ec2.internal"},
					Spec:       corev1.NodeSpec{ProviderID: "aws:///us-east-1a/i-0abcdef1234567890"},
				}
				if _, err := client.Clientset.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = client.Clientset.CoreV1().Nodes().Delete(ctx, n.Name, metav1.DeleteOptions{})
				})
			},
		},
		{
			name:       "the EBS CSI driver object exists",
			wantSignal: "CSIDriver ebs.csi.aws.com",
			apply: func(t *testing.T) {
				d := &storagev1.CSIDriver{
					ObjectMeta: metav1.ObjectMeta{Name: "ebs.csi.aws.com"},
					Spec:       storagev1.CSIDriverSpec{AttachRequired: ptr(true), PodInfoOnMount: ptr(false)},
				}
				if _, err := client.Clientset.StorageV1().CSIDrivers().Create(ctx, d, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = client.Clientset.StorageV1().CSIDrivers().Delete(ctx, d.Name, metav1.DeleteOptions{})
				})
			},
		},
		{
			name:       "a StorageClass using the EBS CSI provisioner",
			wantSignal: "ebs.csi.aws.com",
			apply:      func(t *testing.T) { createStorageClass(t, "evac-test-gp3", "ebs.csi.aws.com") },
		},
		{
			name:       "a StorageClass using the in-tree aws-ebs provisioner",
			wantSignal: "kubernetes.io/aws-ebs",
			apply:      func(t *testing.T) { createStorageClass(t, "evac-test-gp2", "kubernetes.io/aws-ebs") },
		},
	}

	node := workerNodes(t, 1)[0].Name
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.apply(t)

			sc := buildScope(t, node)
			if !sc.Provider.Disabled {
				t.Fatalf("PVC deletion is still enabled after applying %q", tc.name)
			}
			if !strings.Contains(sc.Provider.Signal, tc.wantSignal) {
				t.Errorf("signal = %q, want it to mention %q", sc.Provider.Signal, tc.wantSignal)
			}
			// Nothing may be deletable while the guard is latched, regardless
			// of what each individual volume source says.
			if n := len(sc.DeletablePVCs()); n != 0 {
				t.Errorf("%d PVC(s) are still deletable with the provider guard tripped", n)
			}
		})
	}

	// The cleanups above run as this function returns; confirm the guard
	// actually releases, or every later test would silently run with deletion
	// disabled and prove nothing.
	t.Cleanup(func() {
		if sc := buildScope(t, node); sc.Provider.Disabled {
			t.Errorf("the guard is still tripped after cleanup: %s", sc.Provider.Signal)
		}
	})
}

// TestEKSARNContextNameDisablesPVCDeletion covers the fourth signal, which is
// read from the kubecontext name rather than from any cluster object. It
// catches an EKS cluster whose nodes have not reported a providerID yet.
func TestEKSARNContextNameDisablesPVCDeletion(t *testing.T) {
	got := guard.DetectProvider(nil, nil, nil, "arn:aws:eks:us-east-1:123456789012:cluster/prod")
	if !got.Disabled {
		t.Fatal("an EKS ARN context name did not disable PVC deletion")
	}
	if !strings.Contains(got.Signal, "EKS ARN") {
		t.Errorf("signal = %q, want it to name the ARN", got.Signal)
	}

	// The real cluster's own context must not look like EKS, or every other
	// test in this package would be running with the guard latched on.
	if live := guard.DetectProvider(nil, nil, nil, client.Context); live.Disabled {
		t.Errorf("context %q trips the EKS check: %s", client.Context, live.Signal)
	}
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
	})
}
