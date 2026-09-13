package kube

import (
	"context"
	"fmt"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// listPageSize bounds each list response. client-go does not paginate List()
// on its own, so without this a cluster with thousands of pods returns one
// enormous response. The number of distinct List operations stays fixed
// either way; only the page count grows with the cluster.
const listPageSize = 500

// Snapshot is the cluster state every command reads from. It is taken once and
// never refreshed: every command that renders it prints when it was taken, and
// scope is re-derived from live state on each run rather than cached across
// runs.
//
// Inventory covers the node table. Full adds what the PVC guards and preflight
// need. Nothing in the tool may query per node or per pod.
type Snapshot struct {
	TakenAt time.Time
	Context string

	Nodes []corev1.Node
	Pods  []corev1.Pod
	PVCs  []corev1.PersistentVolumeClaim

	// Populated by Full only.
	PVs            []corev1.PersistentVolume
	StorageClasses []storagev1.StorageClass
	CSIDrivers     []storagev1.CSIDriver
	PDBs           []policyv1.PodDisruptionBudget

	// Workload owners, needed to resolve replica counts for the fragile test.
	// These are listed rather than fetched per pod: walking ownerReferences with
	// a Get per hop would be a per-pod query, which this tool never issues.
	// DaemonSets and Jobs are deliberately absent — identified by ownerReference
	// kind alone, needing no lookup.
	ReplicaSets            []appsv1.ReplicaSet
	Deployments            []appsv1.Deployment
	StatefulSets           []appsv1.StatefulSet
	ReplicationControllers []corev1.ReplicationController

	full bool
}

// Full reports whether the guard and preflight inputs were collected.
func (s *Snapshot) Full() bool { return s.full }

// Inventory takes the three-call snapshot the node table needs.
func (c *Client) Inventory(ctx context.Context) (*Snapshot, error) {
	s := &Snapshot{TakenAt: time.Now().UTC(), Context: c.Context}
	err := runParallel(
		func() (err error) { s.Nodes, err = c.listNodes(ctx); return },
		func() (err error) { s.Pods, err = c.listPods(ctx); return },
		func() (err error) { s.PVCs, err = c.listPVCs(ctx); return },
	)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// FullSnapshot adds the inputs the volume-source guard, provider detection and
// fragile classification need. Still a fixed number of list calls.
func (c *Client) FullSnapshot(ctx context.Context) (*Snapshot, error) {
	s := &Snapshot{TakenAt: time.Now().UTC(), Context: c.Context, full: true}
	err := runParallel(
		func() (err error) { s.Nodes, err = c.listNodes(ctx); return },
		func() (err error) { s.Pods, err = c.listPods(ctx); return },
		func() (err error) { s.PVCs, err = c.listPVCs(ctx); return },
		func() (err error) { s.PVs, err = c.listPVs(ctx); return },
		func() (err error) { s.StorageClasses, err = c.listStorageClasses(ctx); return },
		func() (err error) { s.CSIDrivers, err = c.listCSIDrivers(ctx); return },
		func() (err error) { s.PDBs, err = c.listPDBs(ctx); return },
		func() (err error) { s.ReplicaSets, err = c.listReplicaSets(ctx); return },
		func() (err error) { s.Deployments, err = c.listDeployments(ctx); return },
		func() (err error) { s.StatefulSets, err = c.listStatefulSets(ctx); return },
		func() (err error) { s.ReplicationControllers, err = c.listRCs(ctx); return },
	)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// runParallel runs the list calls concurrently and returns the first error.
// They are independent reads, so there is no reason to pay for them serially on
// a large cluster.
func runParallel(fns ...func() error) error {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		firstEr error
	)
	for _, fn := range fns {
		wg.Add(1)
		go func(f func() error) {
			defer wg.Done()
			if err := f(); err != nil {
				mu.Lock()
				if firstEr == nil {
					firstEr = err
				}
				mu.Unlock()
			}
		}(fn)
	}
	wg.Wait()
	return firstEr
}

// paginate walks a paginated list to completion.
//
// A continue token can expire mid-walk if the caller is slow and etcd compacts
// the revision out from under it; the API server reports that as 410 Gone. The
// correct response is to start the listing over rather than return a partial
// set, because every caller here treats the result as a complete picture and a
// silently short list would under-report blast radius.
func paginate[T any](page func(metav1.ListOptions) ([]T, string, error)) ([]T, error) {
	const maxRestarts = 2
	for restart := 0; ; restart++ {
		var (
			out  []T
			opts = metav1.ListOptions{Limit: listPageSize}
		)
		for {
			items, cont, err := page(opts)
			if err != nil {
				if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
					if restart < maxRestarts {
						break // restart the whole listing
					}
					return nil, fmt.Errorf("list expired repeatedly while paginating: %w", err)
				}
				return nil, err
			}
			out = append(out, items...)
			if cont == "" {
				return out, nil
			}
			opts.Continue = cont
		}
	}
}

func (c *Client) listNodes(ctx context.Context) ([]corev1.Node, error) {
	return paginate(func(o metav1.ListOptions) ([]corev1.Node, string, error) {
		l, err := c.Clientset.CoreV1().Nodes().List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing nodes: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

// listPods lists across all namespaces. A drain operates on whatever is on the
// selected nodes regardless of namespace, so there is no namespace filter
// anywhere in this tool.
func (c *Client) listPods(ctx context.Context) ([]corev1.Pod, error) {
	return paginate(func(o metav1.ListOptions) ([]corev1.Pod, string, error) {
		l, err := c.Clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing pods: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func (c *Client) listPVCs(ctx context.Context) ([]corev1.PersistentVolumeClaim, error) {
	return paginate(func(o metav1.ListOptions) ([]corev1.PersistentVolumeClaim, string, error) {
		l, err := c.Clientset.CoreV1().PersistentVolumeClaims(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing persistentvolumeclaims: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func (c *Client) listPVs(ctx context.Context) ([]corev1.PersistentVolume, error) {
	return paginate(func(o metav1.ListOptions) ([]corev1.PersistentVolume, string, error) {
		l, err := c.Clientset.CoreV1().PersistentVolumes().List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing persistentvolumes: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func (c *Client) listStorageClasses(ctx context.Context) ([]storagev1.StorageClass, error) {
	return paginate(func(o metav1.ListOptions) ([]storagev1.StorageClass, string, error) {
		l, err := c.Clientset.StorageV1().StorageClasses().List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing storageclasses: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func (c *Client) listCSIDrivers(ctx context.Context) ([]storagev1.CSIDriver, error) {
	return paginate(func(o metav1.ListOptions) ([]storagev1.CSIDriver, string, error) {
		l, err := c.Clientset.StorageV1().CSIDrivers().List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing csidrivers: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func (c *Client) listPDBs(ctx context.Context) ([]policyv1.PodDisruptionBudget, error) {
	return paginate(func(o metav1.ListOptions) ([]policyv1.PodDisruptionBudget, string, error) {
		l, err := c.Clientset.PolicyV1().PodDisruptionBudgets(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing poddisruptionbudgets: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func (c *Client) listReplicaSets(ctx context.Context) ([]appsv1.ReplicaSet, error) {
	return paginate(func(o metav1.ListOptions) ([]appsv1.ReplicaSet, string, error) {
		l, err := c.Clientset.AppsV1().ReplicaSets(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing replicasets: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func (c *Client) listDeployments(ctx context.Context) ([]appsv1.Deployment, error) {
	return paginate(func(o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
		l, err := c.Clientset.AppsV1().Deployments(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing deployments: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func (c *Client) listStatefulSets(ctx context.Context) ([]appsv1.StatefulSet, error) {
	return paginate(func(o metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
		l, err := c.Clientset.AppsV1().StatefulSets(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing statefulsets: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

func (c *Client) listRCs(ctx context.Context) ([]corev1.ReplicationController, error) {
	return paginate(func(o metav1.ListOptions) ([]corev1.ReplicationController, string, error) {
		l, err := c.Clientset.CoreV1().ReplicationControllers(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return nil, "", fmt.Errorf("listing replicationcontrollers: %w", err)
		}
		return l.Items, l.Continue, nil
	})
}

// SnapshotFixture is the input to NewSnapshotForTest.
type SnapshotFixture struct {
	TakenAt                time.Time
	Context                string
	Nodes                  []corev1.Node
	Pods                   []corev1.Pod
	PVCs                   []corev1.PersistentVolumeClaim
	PVs                    []corev1.PersistentVolume
	StorageClasses         []storagev1.StorageClass
	CSIDrivers             []storagev1.CSIDriver
	PDBs                   []policyv1.PodDisruptionBudget
	ReplicaSets            []appsv1.ReplicaSet
	Deployments            []appsv1.Deployment
	StatefulSets           []appsv1.StatefulSet
	ReplicationControllers []corev1.ReplicationController
}

// NewSnapshotForTest builds a full snapshot from fixtures.
//
// Exported because the packages that consume a snapshot live outside this one
// and the full marker is unexported — an inventory-only snapshot must stay
// distinguishable from a complete one, since classification silently produces
// wrong replica counts without the owner lists.
func NewSnapshotForTest(f SnapshotFixture) *Snapshot {
	return &Snapshot{
		TakenAt: f.TakenAt, Context: f.Context,
		Nodes: f.Nodes, Pods: f.Pods, PVCs: f.PVCs, PVs: f.PVs,
		StorageClasses: f.StorageClasses, CSIDrivers: f.CSIDrivers, PDBs: f.PDBs,
		ReplicaSets: f.ReplicaSets, Deployments: f.Deployments,
		StatefulSets: f.StatefulSets, ReplicationControllers: f.ReplicationControllers,
		full: true,
	}
}
