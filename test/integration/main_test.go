//go:build integration

// Package integration exercises the drain against a real cluster.
//
// These tests delete PVCs. They are gated three ways, deliberately:
//
//  1. The `integration` build tag, so `go test ./...` cannot even compile them.
//  2. EVAC_TEST_CLUSTER must name a kubecontext, and the active context must
//     match it.
//  3. The target cluster must carry a sentinel namespace with an opt-in label.
//
// The third is the one that matters. The first two are properties of the
// invoker's shell, which a typo defeats; the sentinel is a property of the
// *target*, so a mistyped context name cannot reach a cluster that was never
// marked as disposable.
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/kube"
)

const (
	// sentinelNamespace must exist on the target cluster, carrying
	// sentinelLabel, before any of these tests will run.
	sentinelNamespace = "evac-integration-sentinel"
	sentinelLabel     = "evac.glueops.dev/destructive-tests"
	sentinelValue     = "allowed"

	envCluster = "EVAC_TEST_CLUSTER"
	// envRequire turns the "no cluster, skip quietly" path into a hard failure.
	// The workflow sets it; nothing else does. Without it these tests exit 0
	// for anyone who has no cluster, which is the behaviour a developer wants —
	// but in CI that same path means `go test` prints ok and the entire suite
	// silently does nothing.
	//
	// Deliberately not keyed on the ambient CI variable: ci.yml also sets that
	// while running `go vet -tags integration`, so the requirement would be
	// inferred from the environment rather than declared by the workflow that
	// actually means it.
	envRequire = "EVAC_REQUIRE_INTEGRATION"
)

var client *kube.Client

func TestMain(m *testing.M) {
	wanted := os.Getenv(envCluster)
	if wanted == "" && os.Getenv(envRequire) != "" {
		fatal("%s is set but %s is not — refusing to report success for a suite that did not run",
			envRequire, envCluster)
	}
	if wanted == "" {
		fmt.Fprintf(os.Stderr, `
integration tests skipped: %s is not set.

These tests DELETE PVCs. To run them, point %s at a disposable cluster and
label it as such:

  k3d cluster create evac-it
  kubectl --context k3d-evac-it create namespace %s
  kubectl --context k3d-evac-it label namespace %s %s=%s
  EVAC_TEST_CLUSTER=k3d-evac-it go test -tags integration ./test/integration/...

`, envCluster, envCluster, sentinelNamespace, sentinelNamespace, sentinelLabel, sentinelValue)
		os.Exit(0)
	}

	cl, err := kube.New(kube.Options{Context: wanted})
	if err != nil {
		fatal("connecting to %s: %v", wanted, err)
	}

	// Guard 2: the resolved context must be the one that was asked for. A
	// --context that silently fell back to the active one would be exactly the
	// accident this is here to prevent.
	if cl.Context != wanted {
		fatal("resolved context is %q but %s asked for %q", cl.Context, envCluster, wanted)
	}

	// Guard 3: the target itself must be marked disposable.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns, err := cl.Clientset.CoreV1().Namespaces().Get(ctx, sentinelNamespace, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		fatal(`cluster %q has no %q namespace.

This cluster is not marked as safe to destroy data on. These tests delete PVCs.
If this really is a disposable cluster:

  kubectl --context %s create namespace %s
  kubectl --context %s label namespace %s %s=%s`,
			wanted, sentinelNamespace, wanted, sentinelNamespace, wanted, sentinelNamespace, sentinelLabel, sentinelValue)
	}
	if err != nil {
		fatal("reading sentinel namespace: %v", err)
	}
	if ns.Labels[sentinelLabel] != sentinelValue {
		fatal("namespace %s exists but is not labelled %s=%s; refusing to destroy data on %q",
			sentinelNamespace, sentinelLabel, sentinelValue, wanted)
	}

	client = cl
	os.Exit(m.Run())
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "\nREFUSING TO RUN: "+format+"\n\n", args...)
	os.Exit(1)
}

// workerNodes returns the drainable nodes, failing the test if there are too
// few for the scenario.
func workerNodes(t *testing.T, atLeast int) []corev1.Node {
	t.Helper()
	list, err := client.Clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var out []corev1.Node
	for _, n := range list.Items {
		if _, cp := n.Labels["node-role.kubernetes.io/control-plane"]; cp {
			continue
		}
		if n.Spec.Unschedulable {
			continue
		}
		out = append(out, n)
	}
	if len(out) < atLeast {
		t.Skipf("need %d schedulable worker nodes, found %d", atLeast, len(out))
	}
	return out
}
