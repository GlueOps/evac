package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/GlueOps/evac/internal/drain"
	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/lock"
	"github.com/GlueOps/evac/internal/output"
	"github.com/GlueOps/evac/internal/preflight"
	"github.com/GlueOps/evac/internal/render"
	"github.com/GlueOps/evac/internal/scope"
)

func newDrainCmd(g *globals) *cobra.Command {
	var (
		sf              selectionFlags
		yes             bool
		ignorePreflight bool
		brief, full     bool
		parallel        parallelValue
		evictionTimeout time.Duration
		jobDeadline     time.Duration
		pvcTimeout      time.Duration
		jsonOut         bool
		logFile         string
		noLogFile       bool
	)

	cmd := &cobra.Command{
		Use:   "drain",
		Short: "Cordon, evict, and destroy local PVCs on the selected nodes",
		Long: `Drain the selected worker nodes.

This destroys data. Every PVC belonging to an evicted pod is deleted, provided
it is backed by a node-local volume; anything else is refused, and deletion is
disabled entirely on AWS/EKS and CSI-backed clusters.

drain runs the plan itself and prompts before acting, so what you approve is
derived from the same live state the drain is about to act on.

Nodes are left cordoned afterwards. Uncordon them with kubectl once the
maintenance work is done.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if brief && full {
				return usageErr("--brief and --full are mutually exclusive")
			}

			// §6: the lock covers drain only. nodes and plan are read-only and
			// must never take it — an operator should always be able to look at
			// a cluster while a drain runs.
			cl, snap, sel, err := sf.resolve(cmd, g, true)
			if err != nil {
				return err
			}

			lk, err := lock.Acquire(cl.Target())
			if err != nil {
				return exitcode.Wrap(exitcode.LockHeld, err)
			}
			defer lk.Release()

			rec, err := output.New(output.Options{
				Stdout:    cmd.OutOrStdout(),
				JSON:      jsonOut,
				LogFile:   logFile,
				NoLogFile: noLogFile,
				Context:   cl.Context,
			})
			if err != nil {
				return exitcode.Wrap(exitcode.Error, err)
			}
			defer rec.Close()

			if p := rec.LogPath(); p != "" {
				rec.Rawf("Log file: %s\n\n", p)
			}

			sc, err := scope.Build(snap, sel.Nodes, cl.Context)
			if err != nil {
				return exitcode.Wrap(exitcode.Error, err)
			}
			pf := preflight.Run(sc)

			var planOut strings.Builder
			if err := render.Plan(&planOut, sc, pf, render.PlanOptions{
				Context: cl.Target(), Brief: brief, Full: full,
			}); err != nil {
				return exitcode.Wrap(exitcode.Error, err)
			}
			rec.Raw(planOut.String())

			// Preflight failures block the drain. --yes does not override them:
			// headless mode should be able to run a drain someone already
			// reasoned about, but must never walk into everything-Pending
			// silently.
			if pf.Failed() {
				if !ignorePreflight {
					return exitcode.Wrap(exitcode.Preflight, fmt.Errorf(
						"preflight failed — refusing to drain.\n"+
							"       Fix the input, or pass --ignore-preflight if you know better"))
				}
				rec.Event(output.Event{
					Level: output.LevelWarn,
					Msg:   "--ignore-preflight: proceeding past failed preflight checks",
				})
				for _, f := range pf.Findings {
					if f.Severity == preflight.Fatal {
						rec.Warnf("", "", "overriding preflight failure: %s — %s", f.Check, f.Summary)
					}
				}
			}

			if err := confirm(cmd, rec, sc, yes); err != nil {
				return err
			}

			opts := drain.Defaults()
			opts.EvictionTimeout = evictionTimeout
			opts.JobDeadline = jobDeadline
			opts.PVCTimeout = pvcTimeout
			opts.Parallel = parallel.workers(len(sel.Nodes))

			if opts.Parallel > 1 {
				// §5: fragile pods bypass eviction by design, so concurrency
				// means those all go down at once. Worth saying out loud when
				// the count is non-trivial.
				if n := len(sc.DowntimeBearing()); n > 0 {
					rec.Warnf("", "", "--parallel %d with %d single-replica workload(s): those will go down simultaneously", opts.Parallel, n)
				}
			}

			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			rerun := rerunCommand(sf, sel.Origin)
			eng := drain.New(cl.Clientset, rec, sc, opts, rerun)

			res, runErr := eng.Run(ctx)
			rec.ClearProgress()
			reportOutcome(rec, res)

			if p := rec.LogPath(); p != "" {
				rec.Rawf("\nLog file: %s\n", p)
			}
			if runErr != nil {
				return runErr
			}
			if res.Failed() {
				return exitcode.Wrap(res.Code(), firstErr(res))
			}
			rec.Infof("", "", "drain complete — nodes remain cordoned; uncordon with kubectl when maintenance is done")
			return nil
		},
	}

	sf.register(cmd)
	f := cmd.Flags()
	f.BoolVar(&yes, "yes", false, "skip the confirmation prompt (does not bypass preflight)")
	f.BoolVar(&ignorePreflight, "ignore-preflight", false, "proceed past failed preflight checks, logging what was overridden")
	f.BoolVar(&brief, "brief", false, "collapse the PVC list to per-namespace counts")
	f.BoolVar(&full, "full", false, "show the full PVC list even when long")
	f.Var(&parallel, "parallel", "drain N nodes concurrently, or 'all'")
	f.DurationVar(&evictionTimeout, "eviction-timeout", 10*time.Minute, "per-pod eviction timeout")
	f.DurationVar(&jobDeadline, "job-deadline", 30*time.Minute, "per-node deadline for waiting on Job pods")
	f.DurationVar(&pvcTimeout, "pvc-timeout", 5*time.Minute, "how long a PVC may stay Terminating")
	f.BoolVar(&jsonOut, "json", false, "emit events as newline-delimited JSON on stdout")
	f.StringVar(&logFile, "log-file", "", "override the audit log path")
	f.BoolVar(&noLogFile, "no-log-file", false, "do not write an audit log file")
	return cmd
}

// confirm renders the §8 confirmation block and reads the answer.
func confirm(cmd *cobra.Command, rec *output.Recorder, sc *scope.Scope, yes bool) error {
	summary := render.ConfirmationSummary(sc)
	rec.Raw("\n" + summary)

	if yes {
		rec.Raw("\n--yes given, proceeding without confirmation.\n\n")
		return nil
	}

	// Default to no. If stdin is not a TTY, fail rather than proceeding or
	// hanging on a prompt nobody can answer.
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return exitcode.Wrap(exitcode.Usage, fmt.Errorf(
			"stdin is not a terminal and --yes was not given; refusing to drain unattended"))
	}

	fmt.Fprint(cmd.OutOrStdout(), "\nProceed? [y/N] ")
	answer, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return exitcode.Wrap(exitcode.Aborted, fmt.Errorf("reading confirmation: %w", err))
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		rec.Raw("\n")
		return nil
	default:
		return exitcode.Wrap(exitcode.Aborted, fmt.Errorf("aborted at the confirmation prompt"))
	}
}

// reportOutcome prints the per-node table §9's aggregation rule calls for, so a
// reader can see which node produced the reported exit code.
func reportOutcome(rec *output.Recorder, res drain.Result) {
	if len(res.Nodes) <= 1 {
		return
	}
	var b strings.Builder
	b.WriteString("\nPer-node outcome:\n")
	for _, n := range res.Nodes {
		status := "ok"
		if n.Code != exitcode.OK {
			status = fmt.Sprintf("exit %d", n.Code)
		}
		fmt.Fprintf(&b, "  %-28s %s\n", n.Node, status)
	}
	rec.Raw(b.String())
}

func firstErr(res drain.Result) error {
	for _, n := range res.Nodes {
		if n.Err != nil {
			return n.Err
		}
	}
	return fmt.Errorf("drain failed")
}

// rerunCommand reconstructs the invocation to suggest in failure diagnostics,
// which §5 requires be given verbatim including the node file path.
func rerunCommand(sf selectionFlags, origin string) string {
	switch {
	case len(sf.nodes) > 0:
		return "evac drain --nodes " + strings.Join(sf.nodes, ",")
	case sf.selector != "":
		return "evac drain --selector " + sf.selector
	case sf.file != "":
		return "evac drain -f " + sf.file
	case origin != "":
		return "evac drain -f " + origin
	default:
		return "evac drain"
	}
}
