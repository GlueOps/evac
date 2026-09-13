package cli

import (
	"bufio"
	"context"
	"errors"
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
		outputFormat    string
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

			// The lock covers drain only. nodes and plan are read-only and
			// must never take it — an operator should always be able to look at
			// a cluster while a drain runs.
			cl, snap, sel, err := sf.resolve(cmd, g, true)
			if err != nil {
				return err
			}

			lk, err := lock.Acquire(cl.Target())
			if err != nil {
				// Exit 8 means "another drain holds the lock" — worth a wrapper
				// retrying shortly. Acquire also fails for reasons that will never
				// clear on their own — a read-only XDG_RUNTIME_DIR, a lock file
				// owned by someone else — and reporting those as 8 sends the
				// wrapper into a wait for a drain that does not exist.
				var held *lock.HeldError
				if errors.As(err, &held) {
					return exitcode.Wrap(exitcode.LockHeld, err)
				}
				return exitcode.Wrap(exitcode.Error, err)
			}
			// A release failure is not actionable — the kernel drops the
			// flock when this process exits regardless.
			defer func() { _ = lk.Release() }()

			switch outputFormat {
			case "text", "json":
			default:
				return usageErr("--output %q: want text or json", outputFormat)
			}

			rec, err := output.New(output.Options{
				Stdout:    cmd.OutOrStdout(),
				JSON:      outputFormat == "json",
				LogFile:   logFile,
				NoLogFile: noLogFile,
				Context:   cl.Context,
			})
			if err != nil {
				return exitcode.Wrap(exitcode.Error, err)
			}
			// Closing the recorder drains the queue and flushes the audit
			// file. It must happen before the process exits or the final and
			// most important lines are lost, so a failure here is reported on
			// stderr — the recorder itself is gone by then.
			defer func() {
				if err := rec.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "WARN  closing the audit log: %v\n", err)
				}
			}()

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
				// Fragile pods bypass eviction by design, so concurrency
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
				return exitcode.Wrap(res.Code(), reportedErr(res))
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
	f.StringVar(&outputFormat, "output", "text",
		"output format: text, or json for newline-delimited events on stdout")
	f.StringVar(&logFile, "log-file", "", "override the audit log path")
	f.BoolVar(&noLogFile, "no-log-file", false, "do not write an audit log file")
	return cmd
}

// confirm renders the confirmation block and reads the answer.
func confirm(cmd *cobra.Command, rec *output.Recorder, sc *scope.Scope, yes bool) error {
	summary := render.ConfirmationSummary(sc)
	rec.Raw("\n" + summary)

	if yes {
		rec.Raw("\n--yes given, proceeding without confirmation.\n\n")
		return nil
	}

	// Default to no, and read the answer from the controlling terminal rather
	// than from stdin.
	//
	// stdin is not necessarily free: `-f -` consumes it for the node list, which
	// is a documented pipeline —
	//
	//	kubectl get nodes -l pool=a -o name | evac drain -f -
	//
	// Reading the confirmation from stdin there would see EOF, and the only way
	// out would be --yes, which turns the documented path into one that skips
	// confirmation on a destructive command. Those two are kept apart
	// deliberately, so the prompt goes to the terminal instead.
	// Everything above must be on screen before the question is asked: the
	// prompt bypasses the recorder, so without this barrier it can overtake the
	// summary still sitting in the queue.
	rec.Flush()

	prompt, err := openTerminal()
	if err != nil {
		// No controlling terminal at all: genuinely unattended. Fail rather
		// than proceeding or hanging on a question nobody can answer.
		return exitcode.Wrap(exitcode.Usage, fmt.Errorf(
			"no terminal available to confirm on and --yes was not given; refusing to drain unattended"))
	}
	// Close failure is not actionable: either this is a duplicate handle on
	// stdin, or a /dev/tty the process is about to stop using.
	defer func() { _ = prompt.Close() }()

	fmt.Fprint(prompt, "\nProceed? [y/N] ")
	answer, err := bufio.NewReader(prompt).ReadString('\n')
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

// openTerminal returns the controlling terminal for reading the confirmation.
//
// Prefers stdin when it is a terminal, so a plain `evac drain` behaves exactly
// as before. Falls back to /dev/tty, which is what makes the piped node-list
// form work: stdin is the node list, but the operator is still sitting there.
func openTerminal() (*os.File, error) {
	if term.IsTerminal(int(os.Stdin.Fd())) {
		// A separate *os.File over the same descriptor, so the caller's
		// deferred Close cannot close the real stdin out from under anything.
		return os.NewFile(os.Stdin.Fd(), "/dev/stdin"), nil
	}
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if !term.IsTerminal(int(tty.Fd())) {
		_ = tty.Close()
		return nil, fmt.Errorf("/dev/tty is not a terminal")
	}
	return tty, nil
}

// reportOutcome prints the per-node table the aggregated exit code calls for,
// so a reader can see which node produced the reported code.
func reportOutcome(rec *output.Recorder, res drain.Result) {
	// The old guard was `len(res.Nodes) <= 1`, which meant a run that failed on
	// its FIRST node printed nothing at all — while phase 1 had already
	// cordoned every selected node. Always report when anything was skipped.
	if len(res.Nodes) <= 1 && len(res.Skipped()) == 0 {
		return
	}

	// Emitted as events as well as prose. Raw text is suppressed under
	// --output=json, so a table alone would mean the one thing a wrapper needs
	// in order to be precise — which node produced which code — reaching
	// humans and not machines.
	for _, n := range res.Nodes {
		if n.Skipped {
			rec.Event(output.Event{
				Level: output.LevelWarn,
				Node:  n.Node,
				Msg:   "node outcome",
				Attrs: map[string]string{"state": "cordoned, not drained"},
			})
			continue
		}
		level := output.LevelInfo
		if n.Code != exitcode.OK {
			level = output.LevelError
		}
		rec.Event(output.Event{
			Level: level,
			Node:  n.Node,
			Msg:   "node outcome",
			Attrs: map[string]string{"exit_code": fmt.Sprint(int(n.Code))},
		})
	}

	var b strings.Builder
	b.WriteString("\nPer-node outcome:\n")
	for _, n := range res.Nodes {
		var status string
		switch {
		case n.Skipped:
			status = "cordoned, NOT drained"
		case n.Code != exitcode.OK:
			status = fmt.Sprintf("exit %d", n.Code)
		default:
			status = "drained"
		}
		fmt.Fprintf(&b, "  %-28s %s\n", n.Node, status)
	}

	// Cordon is one-way, so say plainly what is still out of service.
	if skipped := res.Skipped(); len(skipped) > 0 {
		fmt.Fprintf(&b, "\n  %d node(s) were cordoned by phase 1 but never drained, because an\n"+
			"  earlier node failed. They remain unschedulable until you uncordon them\n"+
			"  or a re-run completes:\n", len(skipped))
		for _, n := range skipped {
			fmt.Fprintf(&b, "    kubectl uncordon %s\n", n)
		}
	}
	rec.Raw(b.String())
}

// reportedErr returns the error belonging to the node whose exit code is being
// reported.
//
// Taking the first failure in slice order instead meant that under --parallel,
// stderr could describe node A's job-wait timeout while the process exited with
// node B's code — a human reading the message and a wrapper reading the code
// would reach different conclusions about the same run.
func reportedErr(res drain.Result) error {
	code := res.Code()
	for _, n := range res.Nodes {
		if !n.Skipped && n.Err != nil && n.Code == code {
			return n.Err
		}
	}
	for _, n := range res.Nodes {
		if n.Err != nil {
			return n.Err
		}
	}
	return fmt.Errorf("drain failed")
}

// rerunCommand reconstructs the invocation to suggest in failure diagnostics,
// which must be given verbatim including the node file path.
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
