//go:build integration

package integration

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/GlueOps/evac/internal/drain"
	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/output"
)

// TestPDBStallReportsTheBlockingBudget covers §5's most likely real failure.
//
// A PodDisruptionBudget that can never be satisfied makes the eviction API
// return 429 forever, not slowly. The tool is supposed to time out and then
// name the budget with its current numbers — disruptionsAllowed, currentHealthy
// and desiredHealthy — because without those the operator sees "stuck" and has
// to go find it by hand, which is the runbook this tool exists to replace.
//
// The fixture is a 2-replica Deployment with minAvailable: 2, so
// disruptionsAllowed is 0 while both replicas are healthy. The pods are not
// pinned to a node, so preflight passes and the stall is genuinely an eviction
// problem rather than a scheduling one.
func TestPDBStallReportsTheBlockingBudget(t *testing.T) {
	workerNodes(t, 2)
	ctx := context.Background()
	ns := createNamespace(t)

	labels := map[string]string{"app": "sticky"}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "sticky"},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(int32(2)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr(int64(1)),
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
	if _, err := client.Clientset.AppsV1().Deployments(ns).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "sticky"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			// Equal to the replica count, so zero disruptions are ever allowed.
			MinAvailable: ptr(intstr.FromInt32(2)),
			Selector:     &metav1.LabelSelector{MatchLabels: labels},
		},
	}
	if _, err := client.Clientset.PolicyV1().PodDisruptionBudgets(ns).Create(ctx, pdb, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	target := waitForLabelledPodNode(t, ns, "app=sticky", 3*time.Minute)
	t.Cleanup(func() { uncordon(t, target) })

	var buf bytes.Buffer
	rec, err := output.New(output.Options{Stdout: &buf, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()

	opts := drain.Defaults()
	// Short, so the test takes seconds rather than the spec's ten minutes. The
	// behaviour under test is what happens at expiry, not the duration.
	opts.EvictionTimeout = 25 * time.Second
	opts.JobDeadline = 30 * time.Second
	opts.PollInterval = time.Second

	sc := buildScope(t, target)
	res, _ := drain.New(client.Clientset, rec, sc, opts, "evac drain").Run(ctx)
	rec.ClearProgress()
	_ = rec.Close()

	if res.Code() != exitcode.EvictionTimeout {
		t.Fatalf("exit code = %d, want %d (eviction timeout)\n%s", res.Code(), exitcode.EvictionTimeout, buf.String())
	}

	out := buf.String()
	// The numbers are the whole point of the diagnostic.
	for _, want := range []string{
		"eviction timeout",
		"blocked by PDB " + ns + "/sticky",
		"disruptionsAllowed",
		"currentHealthy",
		"desiredHealthy",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the diagnostic is missing %q; without it the operator has to hunt by hand\n--- output ---\n%s", want, out)
		}
	}
	if !strings.Contains(out, "kubectl describe pdb") {
		t.Errorf("the diagnostic does not suggest a command to run:\n%s", out)
	}
}

// TestJobDeadlineExceededReportsWhatIsStillRunning covers §5 phase 4.
//
// Job pods are never evicted; they finish on their own. When they do not finish
// within the deadline the tool must exit non-zero rather than silently reporting
// success on an incomplete drain, and must name what is still running with each
// pod's own age — that is what tells the operator whether something is nearly
// done or wedged.
func TestJobDeadlineExceededReportsWhatIsStillRunning(t *testing.T) {
	nodes := workerNodes(t, 2)
	ctx := context.Background()
	ns := createNamespace(t)
	target := nodes[0].Name

	// Pinned to the target node: Job pods are excluded from the drain, so
	// preflight does not evaluate their scheduling constraints.
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "slow"},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr(int32(0)),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					NodeSelector:  map[string]string{corev1.LabelHostname: target},
					Containers: []corev1.Container{{
						Name:    "work",
						Image:   "busybox:1.36",
						Command: []string{"sh", "-c", "sleep 600"},
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10m"),
							corev1.ResourceMemory: resource.MustParse("16Mi"),
						}},
					}},
				},
			},
		},
	}
	if _, err := client.Clientset.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForLabelledPodNode(t, ns, "job-name=slow", 3*time.Minute)
	t.Cleanup(func() { uncordon(t, target) })

	var buf bytes.Buffer
	rec, err := output.New(output.Options{Stdout: &buf, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()

	opts := drain.Defaults()
	opts.EvictionTimeout = time.Minute
	opts.JobDeadline = 20 * time.Second
	opts.PollInterval = time.Second

	sc := buildScope(t, target)
	if len(sc.JobPods()) == 0 {
		t.Fatal("the Job pod is not in scope; phase 4 would have nothing to wait on")
	}

	res, _ := drain.New(client.Clientset, rec, sc, opts, "evac drain -f nodes.txt").Run(ctx)
	rec.ClearProgress()
	_ = rec.Close()

	if res.Code() != exitcode.JobTimeout {
		t.Fatalf("exit code = %d, want %d (job wait deadline)\n%s", res.Code(), exitcode.JobTimeout, buf.String())
	}

	out := buf.String()
	for _, want := range []string{
		"job wait deadline exceeded",
		"Job pod(s) still running",
		"Namespaces affected: " + ns,
		"Nodes remain cordoned",
		// §5 requires the re-run command verbatim, node file path included.
		"evac drain -f nodes.txt",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the deadline message is missing %q\n--- output ---\n%s", want, out)
		}
	}

	// Cordon is one-way: a failed drain must leave the node cordoned so the
	// remaining Jobs keep draining and a re-run picks up where this left off.
	if !cordoned(t, target) {
		t.Errorf("%s was left schedulable after a failed drain", target)
	}
}

// waitForLabelledPodNode waits for a pod matching the selector to be Running
// and returns the node it landed on.
func waitForLabelledPodNode(t *testing.T, ns, selector string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		list, err := client.Clientset.CoreV1().Pods(ns).List(context.Background(),
			metav1.ListOptions{LabelSelector: selector})
		if err == nil {
			for i := range list.Items {
				p := &list.Items[i]
				if p.Status.Phase == corev1.PodRunning && p.Spec.NodeName != "" && p.DeletionTimestamp == nil {
					return p.Spec.NodeName
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no Running pod matching %q in %s within %s", selector, ns, timeout)
		}
		time.Sleep(2 * time.Second)
	}
}
