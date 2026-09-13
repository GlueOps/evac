//go:build integration

package integration

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/classify"
	"github.com/GlueOps/evac/internal/drain"
	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/output"
	"github.com/GlueOps/evac/internal/scope"
)

// syncBuffer collects recorder output. The recorder funnels everything through
// one writer goroutine, but the test reads from another, so the buffer still
// needs its own lock.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestParallelDrainsMultipleNodes covers the concurrent path, which has no
// other coverage at any level.
//
// Three properties matter and none of them are visible from a serial run:
// cordon still covers every selected node up front rather than per worker,
// every log line carries its node name so interleaved output stays readable,
// and the run reports an aggregate rather than whichever worker finished last.
func TestParallelDrainsMultipleNodes(t *testing.T) {
	workerNodes(t, 3) // need somewhere for the evicted pods to land
	ctx := context.Background()

	// Put real work on the cluster first. Draining empty nodes exercises the
	// worker pool but proves nothing about concurrent eviction, and earlier
	// tests in this package tend to leave the nodes already drained.
	ns := createNamespace(t)
	spreadWorkload(t, ns, "spread", 6)

	targets := busiestNodes(t, ns, 2)
	if len(targets) < 2 {
		t.Skipf("workload landed on %d node(s); need 2 with pods to drain concurrently", len(targets))
	}
	for _, n := range targets {
		t.Cleanup(func() { uncordon(t, n) })
	}
	t.Logf("draining %v concurrently", targets)

	sc := buildScopeFor(t, targets)
	if len(sc.ByClass(classify.Normal))+len(sc.ByClass(classify.Fragile)) == 0 {
		t.Fatal("no evictable pods in scope; the concurrent path would not be exercised")
	}

	out := &syncBuffer{}
	rec, err := output.New(output.Options{Stdout: out, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()

	opts := drain.Defaults()
	opts.Parallel = 2
	opts.EvictionTimeout = 4 * time.Minute
	opts.JobDeadline = time.Minute
	opts.PollInterval = time.Second

	res, err := drain.New(client.Clientset, rec, sc, opts, "evac drain").Run(ctx)
	rec.ClearProgress()
	_ = rec.Close()

	if err != nil {
		t.Fatalf("parallel drain errored: %v\n%s", err, out.String())
	}
	if res.Code() != exitcode.OK {
		t.Fatalf("exit code = %d, want 0\n%s", res.Code(), out.String())
	}

	// Both nodes must appear in the aggregate, not just the last one finished.
	if len(res.Nodes) != len(targets) {
		t.Errorf("reported %d node outcome(s), want %d", len(res.Nodes), len(targets))
	}
	reported := map[string]bool{}
	for _, n := range res.Nodes {
		reported[n.Node] = true
	}
	for _, want := range targets {
		if !reported[want] {
			t.Errorf("%s is missing from the aggregate result", want)
		}
	}

	// Both must actually be cordoned. Phase 1 covers the whole selected set up
	// front regardless of parallelism: cordoning per worker would let pods
	// evicted from one node land on another that is still schedulable.
	for _, n := range targets {
		if !cordoned(t, n) {
			t.Errorf("%s was not cordoned", n)
		}
	}

	// Interleaved output is unreadable without the node name on every line.
	body := out.String()
	for _, n := range targets {
		if !strings.Contains(body, "node="+n) {
			t.Errorf("no log line carries node=%s; interleaved output would be unreadable:\n%s", n, body)
		}
	}

	// Nothing may remain on either node beyond DaemonSets and finished pods.
	for _, n := range targets {
		blockers, jobs := remainingOn(t, n)
		if len(blockers) > 0 || len(jobs) > 0 {
			t.Errorf("%s still hosts %d evictable and %d Job pod(s) after a successful drain", n, len(blockers), len(jobs))
		}
	}
}

// buildScopeFor builds a scope covering several nodes at once.
func buildScopeFor(t *testing.T, names []string) *scope.Scope {
	t.Helper()
	snap, err := client.FullSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	byName := inventory.ByName(inventory.Build(snap))

	var sel []inventory.Node
	for _, n := range names {
		node, ok := byName[n]
		if !ok {
			t.Fatalf("node %s not found", n)
		}
		sel = append(sel, *node)
	}
	sc, err := scope.Build(snap, sel, client.Context)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

// remainingOn reports what is still on a node, applying the same exclusions
// phase 4's terminal check uses.
func remainingOn(t *testing.T, node string) (blockers, jobs []string) {
	t.Helper()
	snap, err := client.FullSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := range snap.Pods {
		p := &snap.Pods[i]
		if p.Spec.NodeName != node {
			continue
		}
		if p.Status.Phase == "Succeeded" || p.Status.Phase == "Failed" {
			continue
		}
		kind := ""
		for _, ref := range p.OwnerReferences {
			if ref.Controller != nil && *ref.Controller {
				kind = ref.Kind
			}
		}
		switch {
		case kind == "DaemonSet":
		case strings.HasPrefix(p.Name, "helper-pod-"):
		case kind == "Job":
			jobs = append(jobs, p.Name)
		default:
			blockers = append(blockers, p.Name)
		}
	}
	return blockers, jobs
}

// spreadWorkload creates a Deployment with enough replicas to land on several
// nodes, with no affinity so the drain can actually move them.
func spreadWorkload(t *testing.T, ns, name string, replicas int32) {
	t.Helper()
	labels := map[string]string{"app": name}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr(int64(1)),
					// Spread across nodes so more than one target has work,
					// without the hard constraint that would stop rescheduling.
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
						MaxSkew:           1,
						TopologyKey:       corev1.LabelHostname,
						WhenUnsatisfiable: corev1.ScheduleAnyway,
						LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
					}},
					Containers: []corev1.Container{{
						Name:    "app",
						Image:   "busybox:1.36",
						Command: []string{"sh", "-c", "sleep 86400"},
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10m"),
							corev1.ResourceMemory: resource.MustParse("16Mi"),
						}},
					}},
				},
			},
		},
	}
	if _, err := client.Clientset.AppsV1().Deployments(ns).Create(context.Background(), dep, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Minute)
	for {
		d, err := client.Clientset.AppsV1().Deployments(ns).Get(context.Background(), name, metav1.GetOptions{})
		if err == nil && d.Status.ReadyReplicas == replicas {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("deployment %s/%s never reached %d ready", ns, name, replicas)
		}
		time.Sleep(2 * time.Second)
	}
}

// busiestNodes returns up to n drainable nodes actually hosting pods from ns,
// most-loaded first.
func busiestNodes(t *testing.T, ns string, n int) []string {
	t.Helper()
	pods, err := client.Clientset.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	drainable := map[string]bool{}
	for _, node := range workerNodes(t, 1) {
		drainable[node.Name] = true
	}

	counts := map[string]int{}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName != "" && drainable[p.Spec.NodeName] {
			counts[p.Spec.NodeName]++
		}
	}
	type kv struct {
		node string
		n    int
	}
	var ranked []kv
	for node, c := range counts {
		ranked = append(ranked, kv{node, c})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].n != ranked[j].n {
			return ranked[i].n > ranked[j].n
		}
		return ranked[i].node < ranked[j].node
	})

	var out []string
	for _, r := range ranked {
		if len(out) == n {
			break
		}
		out = append(out, r.node)
	}
	return out
}
