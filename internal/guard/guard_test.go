package guard

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- the exhaustiveness check ----------------------------------------------

// verdicts records a deliberate decision for every volume source Kubernetes
// defines. Exactly one is deletable.
//
// This map is the allowlist's audit trail: the test below fails when client-go
// introduces a volume source that is not listed here, forcing a conscious
// decision instead of letting a new backend inherit a default.
var verdicts = map[string]bool{
	"Local": true, // node-local volume — the only deletable source (§5)

	// hostPath is deliberately excluded even though it looks local: it is just
	// a path on the node, so one pointing into an NFS mount would look local
	// while writing to a NAS. Excluding it removes that gap entirely rather
	// than papering over it with a path-prefix denylist.
	"HostPath": false,

	// Everything below is network or cloud storage that must persist.
	"GCEPersistentDisk":    false,
	"AWSElasticBlockStore": false,
	"NFS":                  false,
	"ISCSI":                false,
	"Glusterfs":            false,
	"RBD":                  false,
	"FlexVolume":           false,
	"Cinder":               false,
	"CephFS":               false,
	"Flocker":              false,
	"FC":                   false,
	"AzureFile":            false,
	"AzureDisk":            false,
	"VsphereVolume":        false,
	"Quobyte":              false,
	"PhotonPersistentDisk": false,
	"PortworxVolume":       false,
	"ScaleIO":              false,
	"StorageOS":            false,
	"CSI":                  false,
}

func TestEveryVolumeSourceHasAnExplicitVerdict(t *testing.T) {
	t.Parallel()
	src := reflect.TypeOf(corev1.PersistentVolumeSource{})

	for i := range src.NumField() {
		name := src.Field(i).Name
		if _, ok := verdicts[name]; !ok {
			t.Errorf(`volume source %q has no verdict.

A new PersistentVolumeSource has appeared in client-go. It is refused by default
because the allowlist admits only %q — this test exists so that is a decision
someone made rather than an accident. Add %q to the verdicts map with a comment
explaining why it is or is not node-local.`, name, sourceLocal, name)
		}
	}

	// The reverse direction: a stale entry means the map is describing a field
	// that no longer exists, which makes the audit trail misleading.
	fields := make(map[string]bool, src.NumField())
	for i := range src.NumField() {
		fields[src.Field(i).Name] = true
	}
	for name := range verdicts {
		if !fields[name] {
			t.Errorf("verdicts lists %q, which is no longer a PersistentVolumeSource field", name)
		}
	}
}

// Exactly one source may be deleted. If this count ever changes, it was a
// deliberate widening of the blast radius and should be reviewed as such.
func TestExactlyOneVolumeSourceIsDeletable(t *testing.T) {
	t.Parallel()
	var allowed []string
	for name, ok := range verdicts {
		if ok {
			allowed = append(allowed, name)
		}
	}
	if len(allowed) != 1 || allowed[0] != sourceLocal {
		t.Errorf("deletable sources = %v, want exactly [%s]", allowed, sourceLocal)
	}
}

// Every source marked false in the map must actually be refused by Evaluate.
// This closes the gap between the audit trail and the running code.
func TestRefusedSourcesAreActuallyRefused(t *testing.T) {
	t.Parallel()
	src := reflect.TypeOf(corev1.PersistentVolumeSource{})

	for i := range src.NumField() {
		field := src.Field(i)
		if verdicts[field.Name] {
			continue
		}
		t.Run(field.Name, func(t *testing.T) {
			pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-1"}}
			// Populate just this one source field with a zero value.
			reflect.ValueOf(&pv.Spec.PersistentVolumeSource).Elem().
				Field(i).Set(reflect.New(field.Type.Elem()))

			got := Evaluate(boundPVC("pv-1"), map[string]*corev1.PersistentVolume{"pv-1": pv}, nil)

			if got.Allowed {
				t.Errorf("Evaluate allowed deletion of a %s-backed PV", field.Name)
			}
			if got.Source != field.Name {
				t.Errorf("Source = %q, want %q", got.Source, field.Name)
			}
		})
	}
}

// --- per-PVC evaluation ----------------------------------------------------

func boundPVC(volumeName string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data-0"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: volumeName},
	}
}

func TestLocalVolumeIsAllowed(t *testing.T) {
	t.Parallel()
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{Path: "/var/lib/rancher/k3s/storage/pvc-abc"},
			},
		},
	}
	got := Evaluate(boundPVC("pv-1"), map[string]*corev1.PersistentVolume{"pv-1": pv}, nil)

	if !got.Allowed {
		t.Errorf("Allowed = false (%s), want true", got.Reason)
	}
	if got.Source != sourceLocal {
		t.Errorf("Source = %q, want %q", got.Source, sourceLocal)
	}
}

// hostPath looks local and is not. This is the case most likely to be argued
// back in later, so it gets its own named test.
func TestHostPathIsRefusedEvenThoughItLooksLocal(t *testing.T) {
	t.Parallel()
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				// Pointing into an NFS mount: Kubernetes cannot tell.
				HostPath: &corev1.HostPathVolumeSource{Path: "/mnt/nfs-share/data"},
			},
		},
	}
	got := Evaluate(boundPVC("pv-1"), map[string]*corev1.PersistentVolume{"pv-1": pv}, nil)

	if got.Allowed {
		t.Error("Allowed = true — a hostPath may point into a network mount; Kubernetes has no idea what is behind it")
	}
}

func TestCSIRefusalNamesTheDriver(t *testing.T) {
	t.Parallel()
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: "nfs.csi.k8s.io"},
			},
		},
	}
	got := Evaluate(boundPVC("pv-1"), map[string]*corev1.PersistentVolume{"pv-1": pv}, nil)

	if got.Allowed {
		t.Fatal("Allowed = true, want false")
	}
	if want := "nfs.csi.k8s.io"; !contains(got.Reason, want) {
		t.Errorf("Reason = %q, want it to name the driver %q", got.Reason, want)
	}
}

func TestUnboundPVCIsRefusedAndReportsItsProvisioner(t *testing.T) {
	t.Parallel()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "app",
			Name:        "data-0",
			Annotations: map[string]string{"volume.kubernetes.io/storage-provisioner": "rancher.io/local-path"},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
	got := Evaluate(pvc, nil, nil)

	if got.Allowed {
		t.Error("Allowed = true — an unbound PVC has no PV to identify, and §5 requires positive identification")
	}
	if !contains(got.Reason, "rancher.io/local-path") {
		t.Errorf("Reason = %q, want it to report the provisioner for context", got.Reason)
	}
}

// Absence of evidence is not evidence of locality.
func TestMissingPVIsRefused(t *testing.T) {
	t.Parallel()
	got := Evaluate(boundPVC("pv-gone"), map[string]*corev1.PersistentVolume{}, nil)

	if got.Allowed {
		t.Error("Allowed = true — the bound PV was not found, so the backing volume is unknown")
	}
}

func TestPVWithNoVolumeSourceIsRefused(t *testing.T) {
	t.Parallel()
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-1"}}
	got := Evaluate(boundPVC("pv-1"), map[string]*corev1.PersistentVolume{"pv-1": pv}, nil)

	if got.Allowed {
		t.Error("Allowed = true for a PV declaring no volume source at all")
	}
}

// --- provider detection ----------------------------------------------------

func TestProviderDetection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		nodes       []corev1.Node
		csiDrivers  []storagev1.CSIDriver
		classes     []storagev1.StorageClass
		contextName string
		wantOff     bool
	}{
		{
			name:    "aws providerID on a node disables deletion",
			nodes:   []corev1.Node{nodeWithProviderID("ip-10-0-0-1", "aws:///us-east-1a/i-abc")},
			wantOff: true,
		},
		{
			name:       "ebs csi driver object disables deletion",
			csiDrivers: []storagev1.CSIDriver{{ObjectMeta: metav1.ObjectMeta{Name: "ebs.csi.aws.com"}}},
			wantOff:    true,
		},
		{
			name:    "in-tree aws-ebs storageclass disables deletion",
			classes: []storagev1.StorageClass{{ObjectMeta: metav1.ObjectMeta{Name: "gp2"}, Provisioner: "kubernetes.io/aws-ebs"}},
			wantOff: true,
		},
		{
			name:        "eks arn context name disables deletion",
			contextName: "arn:aws:eks:us-east-1:123456789012:cluster/prod",
			wantOff:     true,
		},
		{
			name:        "k3s cluster with local-path is left enabled",
			nodes:       []corev1.Node{nodeWithProviderID("k3d-agent-0", "k3s://k3d-agent-0")},
			classes:     []storagev1.StorageClass{{ObjectMeta: metav1.ObjectMeta{Name: "local-path"}, Provisioner: "rancher.io/local-path"}},
			contextName: "k3d-evac-dev",
			wantOff:     false,
		},
		{
			name:       "an unrelated CSI driver does not trip the provider check",
			csiDrivers: []storagev1.CSIDriver{{ObjectMeta: metav1.ObjectMeta{Name: "nfs.csi.k8s.io"}}},
			// The per-PVC check refuses these; the provider kill switch is
			// specifically about AWS.
			wantOff: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DetectProvider(tc.nodes, tc.csiDrivers, tc.classes, tc.contextName)
			if got.Disabled != tc.wantOff {
				t.Errorf("Disabled = %v, want %v (signal %q)", got.Disabled, tc.wantOff, got.Signal)
			}
			if got.Disabled && got.Signal == "" {
				t.Error("guard tripped without naming the signal that tripped it")
			}
		})
	}
}

func nodeWithProviderID(name, id string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.NodeSpec{ProviderID: id},
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
