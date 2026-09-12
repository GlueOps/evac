// Package kube handles cluster access: kubeconfig resolution, client
// construction, and the fixed set of list calls the rest of the tool reads from.
package kube

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Client bundles a clientset with the identity of the cluster it talks to.
// The context name is carried because several outputs (the node file header,
// the log file name, the confirmation prompt) have to name the target cluster,
// and re-resolving it each time invites the two from drifting apart.
type Client struct {
	Clientset kubernetes.Interface
	// Context is the resolved kubecontext name, or "" when the kubeconfig
	// specifies no current context (in-cluster config, for example).
	Context string
	// Host is the API server URL, shown when the context name is empty so the
	// operator still has something to recognise the cluster by.
	Host string
}

// Options are the connection flags. Both mirror kubectl's meaning exactly:
// operators already know what they do, and §1 requires the standard loading
// rules so KUBECONFIG, --kubeconfig and --context all behave as expected.
type Options struct {
	// Kubeconfig is an explicit path, overriding KUBECONFIG. Empty means
	// "use the default loading rules".
	Kubeconfig string
	// Context overrides the current context. Empty means "use the active one".
	Context string
}

// New resolves the kubeconfig and builds a client.
//
// §10 deliberately avoids k8s.io/cli-runtime's genericclioptions here: it
// registers around twenty flags in one shot, including -n, which §1 rejects for
// a node-scoped tool. Declaring the two flags by hand and feeding them to the
// deferred loader costs very little and keeps the flag surface honest.
func New(opts Options) (*Client, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if opts.Kubeconfig != "" {
		rules.ExplicitPath = opts.Kubeconfig
	}

	overrides := &clientcmd.ConfigOverrides{}
	if opts.Context != "" {
		overrides.CurrentContext = opts.Context
	}

	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)

	restCfg, err := cc.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	tuneRateLimits(restCfg)

	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("building client: %w", err)
	}

	return &Client{
		Clientset: cs,
		Context:   resolveContextName(cc, opts.Context),
		Host:      restCfg.Host,
	}, nil
}

// resolveContextName reports which context is actually in effect. An explicit
// --context wins; otherwise it is read back from the merged config.
func resolveContextName(cc clientcmd.ClientConfig, explicit string) string {
	if explicit != "" {
		return explicit
	}
	raw, err := cc.RawConfig()
	if err != nil {
		return ""
	}
	return raw.CurrentContext
}

// tuneRateLimits raises client-side throttling well above the defaults.
//
// client-go defaults to 5 QPS with a burst of 10, which exists to stop
// controllers hammering the API server in a hot loop. This is a short-lived
// interactive CLI issuing a bounded number of list calls, and the default makes
// a large cluster's inventory visibly slow for no benefit. The API server's own
// priority and fairness handles real protection.
func tuneRateLimits(cfg *rest.Config) {
	cfg.QPS = 50
	cfg.Burst = 100
	if cfg.UserAgent == "" {
		cfg.UserAgent = "evac"
	}
}

// Target describes the cluster for display. Used wherever output has to name
// what it is about to act on.
func (c *Client) Target() string {
	if c.Context != "" {
		return c.Context
	}
	if c.Host != "" {
		return c.Host
	}
	return "unknown cluster"
}
