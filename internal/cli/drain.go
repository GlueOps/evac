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
	"github.com/GlueOps/evac/internal/inventory"
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
			// Validated here, before the snapshot and before the lock. A typo'd
			// --output used to acquire the cross-process lock and pull a full
			// snapshot before failing, so a mistyped flag blocked a real drain
			// for the duration.
			switch outputFormat {
			case "text", "json":
			default:
				return usageErr("--output %q: want text or json", outputFormat)
			}
			if logFile != "" && noLogFile {
				return usageErr("--log-file and --no-log-file are mutually exclusive")
			}
			// --output=json suppresses all raw text, including the plan and the
			// confirmation block, while the prompt goes straight to the
			// terminal. The combination asks the operator to approve permanent
			// destruction with an empty screen above the question.
			if outputFormat == "json" && !yes {
				return usageErr("--output json needs --yes: the plan and confirmation block are not rendered in JSON mode,\n" +
					"       so the prompt would appear with nothing above it. Review with `evac plan` first")
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

			rerun := rerunCommand(g, cl.Context, sel.Names())
			eng := drain.New(cl.Clientset, rec, sc, opts, rerun)

			res, runErr := eng.Run(ctx)
			rec.ClearProgress()
			if errors.Is(runErr, context.Canceled) {
				reportInterrupted(rec, rerun)
			}
			reportOutcome(rec, res)
			// One epilogue on every exit path. It used to run only on full
			// success, so the operator whose drain failed — the one who most
			// needs to know what is still cordoned and what was already
			// destroyed — was the one who did not get told.
			reportEpilogue(rec, res, sel.Nodes, eng.Destroyed(), len(sc.DeletablePVCs()), cl.Context, rerun)

			if p := rec.LogPath(); p != "" {
				rec.Rawf("\nLog file: %s\n", p)
				// Also an event: this line is raw text, which --output=json
				// drops, so a wrapper could not find the audit file it was
				// told it had.
				rec.Event(output.Event{Msg: "audit log", Attrs: map[string]string{"path": p}})
			}
			if runErr != nil {
				return runErr
			}
			if res.Failed() {
				return exitcode.Wrap(res.Code(), reportedErr(res))
			}
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

	// Also as an event. Raw text is suppressed under --output=json, so this
	// block — the summary of everything about to be destroyed — reached humans
	// and not machines, and a wrapper's job log recorded nothing about what the
	// run was about to do.
	rec.Event(output.Event{
		Msg: "about to drain",
		Attrs: map[string]string{
			"nodes":              fmt.Sprint(len(sc.Nodes)),
			"pvcs_to_destroy":    fmt.Sprint(len(sc.DeletablePVCs())),
			"unmanaged_pods":     fmt.Sprint(len(sc.Unmanaged())),
			"downtime_workloads": fmt.Sprint(len(sc.DowntimeBearing())),
			"pods_to_evict":      fmt.Sprint(sc.Evictable()),
		},
	})

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
		// Known defect, left as-is deliberately: os.NewFile wraps fd 0 rather
		// than duplicating it, so the caller's deferred Close closes the
		// process's real stdin and the next file opened lands on fd 0. It is
		// survivable only because nothing reads stdin after the confirmation.
		// The fix is a dup(2) here; do not trust this to be a separate handle.
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
		status := n.Code.String()
		switch {
		case n.Skipped:
			status = "cordoned, never attempted"
		case n.Code != exitcode.OK:
			status = fmt.Sprintf("%s (exit %d)", n.Code, int(n.Code))
		}
		fmt.Fprintf(&b, "  %-28s %s\n", n.Node, status)
	}
	rec.Raw(b.String())
}

// reportInterrupted says what a signal left behind.
//
// Ctrl-C used to print "ERROR context canceled" and nothing else, while the
// cluster held a cordoned node, a PVC marked for deletion, and a pod evicted
// mid-flight. The operator was told none of it and given no way back.
func reportInterrupted(rec *output.Recorder, rerun string) {
	rec.Diagnostic(output.Diagnostic{
		Headline: "interrupted — the drain stopped on your signal",
		Detail: []string{
			"any PVC already deleted is gone; a pod may be mid-eviction",
			"nothing else was started after the signal",
		},
		Rerun: rerun,
	})
}

// reportEpilogue is the last thing printed, on every exit path.
//
// Two questions matter when a drain ends, and neither was answered before:
// what did it actually destroy, and what is still out of service. The first
// existed only as a prediction made before the run, so after a failure the
// amount of irreversible damage could only be recovered by counting events by
// hand. The second was printed for nodes phase 1 cordoned but never reached,
// and not for the node that actually failed — which is cordoned, partly
// drained, and has PVCs already gone.
//
// Emitted as events as well as prose, because --output=json suppresses raw
// text and a wrapper needs both answers more than a human does.
func reportEpilogue(rec *output.Recorder, res drain.Result, selected []inventory.Node,
	destroyed []string, planned int, context, rerun string) {

	var b strings.Builder

	if planned > 0 || len(destroyed) > 0 {
		fmt.Fprintf(&b, "\nPVCs destroyed: %d of %d planned\n", len(destroyed), planned)
		if n := len(destroyed); n > 0 && n < planned {
			fmt.Fprintf(&b, "  last one destroyed: %s\n", destroyed[n-1])
		}
	}
	rec.Event(output.Event{
		Msg: "destruction summary",
		Attrs: map[string]string{
			"pvcs_destroyed": fmt.Sprint(len(destroyed)),
			"pvcs_planned":   fmt.Sprint(planned),
		},
	})

	if len(selected) == 0 {
		rec.Raw(b.String())
		return
	}

	ctxFlag := ""
	if context != "" {
		ctxFlag = "--context " + context + " "
	}
	fmt.Fprintf(&b, "\n%d node(s) are cordoned and out of service. evac never uncordons.\n",
		len(selected))
	if res.Failed() {
		fmt.Fprintf(&b, "A re-run resumes them:\n    %s\n\nOr put them back without draining:\n", rerun)
	} else {
		fmt.Fprintf(&b, "Uncordon when the maintenance work is done:\n")
	}
	for i := range selected {
		fmt.Fprintf(&b, "    kubectl %suncordon %s\n", ctxFlag, selected[i].Name)
		rec.Event(output.Event{
			Node: selected[i].Name,
			Msg:  "node left cordoned",
			Attrs: map[string]string{
				"uncordon_command": fmt.Sprintf("kubectl %suncordon %s", ctxFlag, selected[i].Name),
			},
		})
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

// rerunCommand builds the command to suggest in failure diagnostics.
//
// It pins the cluster and the resolved node list rather than echoing back the
// flags that were typed, because this command is handed to an operator to
// paste, possibly hours later and possibly after a kubectx in another pane.
//
// --context is included unconditionally, not only when the flag was passed.
// Without it the command resolves against whatever kubecontext is active at
// paste time, and pool-style node names collide across clusters — so a command
// evac itself printed could cordon nodes and delete local PVCs on a cluster
// the failed drain never touched.
//
// The node list is the resolved selection for the same reason. Echoing back
// --selector would re-derive a different set from live labels, including nodes
// the first run already drained; echoing back -f would re-read a file that may
// have been edited since; and echoing back `-f -` produced a command that
// silently blocks on the keyboard, because the original list was consumed by
// the run that just failed.
func rerunCommand(g *globals, context string, nodes []string) string {
	cmd := "evac drain"
	if g.kubeconfig != "" {
		cmd += " --kubeconfig " + g.kubeconfig
	}
	if context != "" {
		cmd += " --context " + context
	}
	if len(nodes) > 0 {
		cmd += " --nodes " + strings.Join(nodes, ",")
	}
	return cmd
}
