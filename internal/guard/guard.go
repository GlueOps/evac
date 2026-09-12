// Package guard implements §5's two independent checks on PVC deletion: a
// cluster-wide provider kill switch, and a per-PVC backing-volume allowlist.
//
// Neither has an override flag, by design. A guard with an escape hatch is a
// guard someone types past at 2am.
package guard

import (
	"fmt"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

// Provisioner names that identify AWS EBS, in-tree and CSI.
const (
	provisionerEBSCSI   = "ebs.csi.aws.com"
	provisionerEBSInTre = "kubernetes.io/aws-ebs"
	eksContextPrefix    = "arn:aws:eks:"
	awsProviderIDPrefix = "aws://"
)

// ProviderVerdict is the cluster-wide check. When Disabled is true, no PVC is
// deleted anywhere in the run.
type ProviderVerdict struct {
	Disabled bool
	// Signal names the specific evidence that tripped the guard, so the
	// operator can see why rather than being told "no".
	Signal string
}

// DetectProvider implements §5's provider detection. Any one signal is enough.
//
// This is narrower than the per-PVC check below and exists to fail fast and
// loudly on the environment where PVC deletion is most dangerous: EBS volumes
// have a detach lifecycle, and deleting the PVC out from under one can strand a
// VolumeAttachment and a real disk that no Kubernetes object tracks anymore.
func DetectProvider(
	nodes []corev1.Node,
	csiDrivers []storagev1.CSIDriver,
	classes []storagev1.StorageClass,
	contextName string,
) ProviderVerdict {
	// providerID is definitive: it is set by the cloud provider on every node.
	for i := range nodes {
		if strings.HasPrefix(nodes[i].Spec.ProviderID, awsProviderIDPrefix) {
			return ProviderVerdict{
				Disabled: true,
				Signal:   fmt.Sprintf("node %s providerID prefix %s", nodes[i].Name, awsProviderIDPrefix),
			}
		}
	}
	for i := range csiDrivers {
		if csiDrivers[i].Name == provisionerEBSCSI {
			return ProviderVerdict{Disabled: true, Signal: "CSIDriver " + provisionerEBSCSI + " present"}
		}
	}
	for i := range classes {
		switch classes[i].Provisioner {
		case provisionerEBSCSI, provisionerEBSInTre:
			return ProviderVerdict{
				Disabled: true,
				Signal:   fmt.Sprintf("StorageClass %s uses provisioner %s", classes[i].Name, classes[i].Provisioner),
			}
		}
	}
	// An EKS context name is an ARN. This catches a cluster whose nodes have
	// not reported a providerID yet.
	if strings.HasPrefix(contextName, eksContextPrefix) {
		return ProviderVerdict{Disabled: true, Signal: "kubecontext name is an EKS ARN"}
	}
	return ProviderVerdict{}
}

// Decision is the per-PVC verdict.
type Decision struct {
	// Allowed is true only on positive identification of a local volume.
	Allowed bool
	// Source is the backing volume source: the PersistentVolumeSource field
	// name ("Local", "NFS", "CSI", ...), or "" when it could not be determined.
	Source string
	// Reason explains a refusal in operator-facing terms.
	Reason string
}

// sourceLocal is the one allowed entry. Written as an allowlist deliberately:
// a blocklist would need updating for every storage backend that ever appears,
// whereas this covers NFS, EBS, iSCSI, CephFS, RBD, Azure, GCE PD, vSphere,
// Portworx, Longhorn and every future CSI driver with no maintenance.
const sourceLocal = "Local"

// Evaluate decides whether one PVC's backing volume may be deleted.
//
// Authority order follows §5: the PV's volume source is the volume definition
// itself, so it cannot be stale or absent the way an annotation can. The
// annotation and StorageClass are used only to describe what a refused volume
// actually is, never to authorise a deletion.
func Evaluate(
	pvc *corev1.PersistentVolumeClaim,
	pvsByName map[string]*corev1.PersistentVolume,
	classesByName map[string]*storagev1.StorageClass,
) Decision {
	if pvc.Spec.VolumeName == "" {
		// Unbound: there is no PV to inspect, so the backing volume cannot be
		// identified. Refuse. Nothing is lost by doing so — an unbound PVC has
		// no provisioned data behind it — and §5 requires positive
		// identification rather than an inference from absence.
		return Decision{
			Source: "",
			Reason: fmt.Sprintf("PVC is unbound (%s); backing volume cannot be identified%s",
				pvc.Status.Phase, describeProvisioner(pvc, classesByName)),
		}
	}

	pv, ok := pvsByName[pvc.Spec.VolumeName]
	if !ok {
		// Bound to a PV that is not in the snapshot. Could be a race, could be
		// an RBAC gap. Either way it is absence of evidence.
		return Decision{
			Reason: fmt.Sprintf("bound PV %q was not found; cannot confirm the backing volume", pvc.Spec.VolumeName),
		}
	}

	src := VolumeSource(pv)
	switch src {
	case sourceLocal:
		return Decision{Allowed: true, Source: src}
	case "":
		return Decision{
			Reason: fmt.Sprintf("PV %q declares no volume source at all", pv.Name),
		}
	default:
		return Decision{
			Source: src,
			Reason: fmt.Sprintf("PV %q is backed by %s, not a node-local volume", pv.Name, describeSource(src, pv)),
		}
	}
}

// VolumeSource reports which volume source a PV declares, by name.
//
// Implemented by reflection over PersistentVolumeSource rather than a switch,
// and that is the point: every field of that struct is a pointer to a source
// type, so a volume source introduced in a future client-go bump is reported
// under its own name and therefore refused by Evaluate, with no code change and
// no silent fall-through. Fail-closed becomes structural rather than a promise.
func VolumeSource(pv *corev1.PersistentVolume) string {
	v := reflect.ValueOf(pv.Spec.PersistentVolumeSource)
	t := v.Type()
	for i := range t.NumField() {
		if f := v.Field(i); f.Kind() == reflect.Pointer && !f.IsNil() {
			return t.Field(i).Name
		}
	}
	return ""
}

// describeSource adds the CSI driver name, which is the detail that tells an
// operator whether they are looking at NFS, EBS or something else entirely.
func describeSource(src string, pv *corev1.PersistentVolume) string {
	if src == "CSI" && pv.Spec.CSI != nil {
		return fmt.Sprintf("CSI driver %s", pv.Spec.CSI.Driver)
	}
	return src
}

// describeProvisioner reads the convenience signals §5 allows for reporting:
// the provisioner annotation set by the provisioning controller, falling back
// to the StorageClass. Neither can authorise a deletion.
func describeProvisioner(pvc *corev1.PersistentVolumeClaim, classesByName map[string]*storagev1.StorageClass) string {
	for _, k := range []string{
		"volume.kubernetes.io/storage-provisioner",
		"volume.beta.kubernetes.io/storage-provisioner",
	} {
		if p, ok := pvc.Annotations[k]; ok && p != "" {
			return fmt.Sprintf(" (provisioner %s)", p)
		}
	}
	if pvc.Spec.StorageClassName != nil {
		if sc, ok := classesByName[*pvc.Spec.StorageClassName]; ok {
			return fmt.Sprintf(" (StorageClass %s, provisioner %s)", sc.Name, sc.Provisioner)
		}
	}
	return ""
}

// IndexPVs builds the lookup Evaluate needs.
func IndexPVs(pvs []corev1.PersistentVolume) map[string]*corev1.PersistentVolume {
	m := make(map[string]*corev1.PersistentVolume, len(pvs))
	for i := range pvs {
		m[pvs[i].Name] = &pvs[i]
	}
	return m
}

// IndexStorageClasses builds the lookup Evaluate needs.
func IndexStorageClasses(classes []storagev1.StorageClass) map[string]*storagev1.StorageClass {
	m := make(map[string]*storagev1.StorageClass, len(classes))
	for i := range classes {
		m[classes[i].Name] = &classes[i]
	}
	return m
}
