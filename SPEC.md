# evac — Build Spec

**`evac`** drains Kubernetes worker nodes and destroys their local PVCs.

A CLI for node maintenance, replacing a ~10-command manual runbook. Operates on one cluster
at a time (the active kubecontext). Written for clusters where local volume data is
disposable — PVC destruction is the intended behavior, not a side effect.

Published as `glueops/evac`. Binary name: `evac`.

> **This tool destroys data by design.** It deletes the PersistentVolumeClaims of every pod
> it evicts. It is built for clusters using node-local storage (k3s / kubeadm with
> local-path or local-static provisioning) where that data is regenerable. It refuses to
> delete anything not backed by `pv.spec.local`, and refuses entirely on CSI-backed or
> AWS/EKS clusters (§5). Read §5 before running it anywhere.

---

## 1. Scope and constraints

- **Single binary, single version.** Not a plugin architecture, not multiple
  independently versioned tools.
- **Standalone CLI**, not a kubectl plugin. Rejected because kubectl's global flags
  (notably `-n`) don't map onto a node-scoped, multi-namespace operation, and
  inheriting them means inheriting the expectation that they work.
- **Active kubecontext governs the target cluster.** Load kubeconfig using the standard
  loading rules so `KUBECONFIG`, `--kubeconfig`, and `--context` all behave as expected.
- **One cluster per invocation.** No fleet loop, no parallel clusters, no aggregated
  reporting, no per-cluster config files.
- **Operator decides targets.** No defaults, no automatic targeting, no "all nodes"
  fallback. If no selection is given, print the inventory and exit.
- **No resumability.** No checkpoint state, no resume command. Recovery is re-running
  the tool (see §7).
- **Worker nodes only.** Control plane and etcd nodes are excluded everywhere — from
  inventory selection, from the interactive picker, and from the drain itself. See §3.1.
- **Cordon is one-way.** The tool never uncordons; the operator does that with kubectl
  after the actual maintenance work is done.
- **Public release.** Published as `glueops/evac`, so the tool runs against clusters its
  authors have never seen. Every guard fails closed (§3.1, §5), and the README must state
  the operating assumptions up front — worker nodes only, node-local storage, no CSI — so
  people self-select out before installing rather than after.

---

## 2. Commands

```
evac nodes                          # inventory table; -i to select and write node file
evac plan  [-f nodes.txt]           # blast radius + preflight, no mutations
evac drain [-f nodes.txt]           # plan + confirm + execute
```

`plan` and `drain` read the default node file (§4) when `-f` is omitted, so the common
path is `evac nodes -i` followed by `evac drain` with no arguments.

Both also accept `-f -` to read the node list from stdin, so
`kubectl get nodes -l ... -o name | evac drain -f -` works without the picker.

**`drain` runs the plan itself.** It renders the full §8 output, then prompts for
confirmation, then executes. The operator never has to run `plan` separately — and because
the plan is computed in the same invocation, what they approve is derived from the same
live state the drain is about to act on, not from a plan generated twenty minutes earlier.

`evac plan` remains as a standalone read-only command, for reviewing ahead of the window
or attaching to a change ticket. It is optional, not a required step.

**`--yes` skips the confirmation prompt** for headless use. It does **not** bypass
preflight failures — see §8.

Node selection may also be given directly as `--nodes a,b,c` or
`--selector k=v`; the file is the ergonomic path, not the only one.

---

## 3. Inventory (`evac nodes`)

Read-only. Ship this first — it's useful on day one even while draining is still manual,
and it's the data the picker renders.

**Data collection: a handful of list calls, never per-node queries.** For the inventory
table: nodes, all pods, all PVCs. Build per-node aggregates in memory.

**These must be *paginated* list calls.** client-go does not paginate `List()` for you, so
a plain call against a large cluster returns one enormous response. Use
`metav1.ListOptions{Limit: 500}` and follow the `Continue` token. The number of list calls
stays fixed per resource type; only the page count varies with cluster size.

`plan` and `drain` need more — PVs and StorageClasses for the volume-source guard (§5),
CSIDrivers for provider detection, PDBs for fragile classification. Still list calls, still
a fixed small number regardless of cluster size. The rule is never to query per node or per
pod.

**Columns:**

| Column | Source |
|---|---|
| Node name | `metadata.name` |
| Labels | see label display policy below |
| Age | `metadata.creationTimestamp` |
| Uptime | `status.conditions[Ready].lastTransitionTime` — **approximate** |
| Kubelet | `status.nodeInfo.kubeletVersion` |
| Kernel | `status.nodeInfo.kernelVersion` |
| OS image | `status.nodeInfo.osImage` |
| Pods | count of non-DaemonSet pods via `spec.nodeName` |
| PVCs | count from pods' `spec.volumes[].persistentVolumeClaim` |
| Ready / Schedulable | conditions + `spec.unschedulable` |

**On uptime:** node object age and host boot time diverge, and the gap is the signal the
operator wants — a node 142d old with 2h uptime already rebooted. The `Ready` condition
transition time is a proxy for boot but actually reflects the last Ready flap. Label the
column honestly. Real boot time needs node-problem-detector or a metrics source; treat
that as optional future work.

**Label display policy.** Nodes carry many labels; dumping all of them makes every row
wrap. Three modes:

- Default: curated subset — non-standard labels (e.g. `glueops.dev/*`), hiding the
  well-known `kubernetes.io/*` and `topology.kubernetes.io/*` noise.
- `--label-columns k1,k2` promotes specific labels to their own columns. Mirrors
  `kubectl get nodes -L`, so the muscle memory exists.
- `--show-labels` dumps everything, for investigation.

Support `--sort-by` (uptime and kubelet version are the useful ones).

### 3.1 Control plane exclusion

**This tool drains worker nodes only.** Control plane and etcd nodes are never drainable,
by any path.

Draining control plane nodes risks etcd quorum loss — on k3s, server nodes run embedded
etcd; on kubeadm, control plane nodes do too. Losing quorum stops the cluster accepting
writes, breaks the tool's own API calls mid-run, and requires a manual etcd restore.
Excluding them removes the entire failure class rather than guarding against it.

**Detection.** A node is control plane if it has any of:

- label `node-role.kubernetes.io/control-plane`
- label `node-role.kubernetes.io/master`
- label `node-role.kubernetes.io/etcd` (k3s server nodes)
- taint `node-role.kubernetes.io/control-plane:NoSchedule`

**The taint check will not fire on k3s.** Verified on k3s v1.35: the server node carries
`node-role.kubernetes.io/control-plane=true` but **no taint at all**, and is fully
schedulable — during a drain test it received a rescheduled StatefulSet pod. Label
detection therefore carries this check alone on the target clusters; the taint check is a
belt-and-braces path for kubeadm that is untested by construction here. A single-server
k3s install also carries no `node-role.kubernetes.io/etcd` label (that appears only on
embedded-etcd HA servers), so do not rely on it either.

**Consequence for §8:** control-plane nodes are excluded from *draining* but remain valid
*scheduling targets*. The capacity preflight must therefore count schedulable
control-plane nodes in the post-cordon denominator, or it will under-report available
capacity and raise false shortfalls on every k3s cluster.

**Enforcement at every layer**, not just one:

- `nodes` inventory shows them, marked `control-plane` and greyed/annotated as excluded.
  Show rather than hide — an operator who can't find a node will go looking, and silence is
  worse than an explanation.
- Interactive selection (§4) renders them unselectable.
- `--selector` and `--nodes` filter them out, reporting what was dropped and why.
- **`drain` refuses outright** if the node file names one. This is the layer that matters:
  the node file is hand-editable, so the check cannot live only in selection.

There is no override flag, for the same reason as the PVC guard (§5) — an override is
something someone types past at 2am.

**Edge case: single-node clusters.** A single-node k3s install has one node that is both
control plane and worker, so the tool will find nothing drainable there and should say so
plainly rather than reporting an empty selection. Not expected in the target environment
(10+ nodes per cluster) but worth a clear message across 20 clusters.

---

## 4. Interactive selection (`evac nodes -i`)

Not a separate command. `evac nodes` renders the inventory table from §3; `-i` makes that
table selectable and writes the chosen nodes to the node file on exit.

**Strictly a selection aid.** It reads, it never mutates, and it never drains. The drain
runs as a separate invocation. This keeps the destructive path free of TUI code and keeps
the execution transcript in normal scrollback.

- Same table, same columns, same three-call snapshot as §3.
- **Snapshot at open.** No background refresh. Show the snapshot timestamp so someone who
  has had it open for twenty minutes knows what they are looking at. Staleness is safe
  because the drain re-derives scope from live state anyway.
- Fuzzy filter, space to multi-select.
- On exit, writes the node file and prints the resolved path.

**Implementation note.** Build on `huh`'s `MultiSelect`, with option strings column-aligned
via `text/tabwriter` — in a monospace terminal an aligned suffix reads as a table, so the
inventory columns survive. This borrows the filter, multi-select and selection-set
machinery rather than rebuilding it.

Two deliberate reductions follow from that choice, because `huh` supports neither:

- **No in-picker sort.** `--sort-by` applies to `evac nodes`; the picker renders in that
  order.
- **Control-plane nodes are named in a note above the picker, not rendered as
  unselectable rows** — `huh` has no disabled-option support. This still satisfies §3.1's
  reasoning (*show rather than hide; silence is worse than an explanation*) without
  per-row state:

```
3 control-plane nodes excluded: server-0, server-1, server-2
```

Rejected: `bubbles/table`, which cannot express this at all — `Row` is a bare `[]string`
with no metadata and `Styles` is only `{Header, Cell, Selected}`, so there is no per-row
styling, no disabled rows, no multi-select and no filtering. Building the full §4
experience on `viewport` directly is 600–900 lines, which is disproportionate for the
least safety-critical component in the tool. If that is ever built, pin Charm v2
(`charm.land/bubbletea/v2`) — v2 shipped 2026-02-23 with breaking changes.

**Terminal width.** The inventory needs ~140 columns to render all fields. Both the picker
and the plain `evac nodes` renderer must define a column drop order for narrower
terminals rather than wrapping every row.

**Node file format** — plain lines with comments:

```
# generated 2026-09-12T14:02:11Z
# context: glueops-prod-eu-1
node-a-01
node-a-04
node-b-02
```

Greppable, hand-editable, diffable, trivially hand-writable when someone skips the picker.

The context line is informational only — it records which cluster the list was generated
against, which is useful when someone finds the file later. **No check is performed against
it.** `drain` operates on the active kubecontext regardless.

**Default path is per-context**, e.g. `./evac-nodes-<context>.txt`. A fixed filename would
mean selecting for cluster B silently overwrites the list for cluster A. The context name
must be sanitized before use — EKS contexts are ARNs
(`arn:aws:eks:us-east-1:123456789012:cluster/prod`) containing colons and slashes.

`plan` and `drain` resolve the same default when `-f` is omitted, so the common path is
`evac nodes -i` then `evac drain` with no arguments.

**Rejected:** clipboard integration via OSC 52. Works, but needs tmux `set-clipboard on`
plus outer terminal support, gives no confirmation it landed, and the file gives a
reviewable artifact instead.

Everything selectable interactively must also be expressible as flags (`--nodes`,
`--selector`). Interactive mode writes a node file; it is not a second interface.

---

## 5. Drain rules

These are **hardcoded, not configurable.** They encode what order works; they are not
decisions to re-make at 2am.

### Phase 1 — cordon all selected nodes (runs first)

Cordon every selected node **before classification**, not after. Cordoning is a safe
mutation and it freezes the picture: with all targets unschedulable, the pod set on those
nodes stops changing, so the classification below is accurate when it is used rather than
stale by the time eviction starts.

Cordon all of them before any eviction. If you cordon-and-drain node by node, evicted pods
land on other selected-but-not-yet-cordoned nodes and get moved twice.

**This happens regardless of parallelism** (see Parallelism below) — cordoning is always
fleet-wide across the selected set, and only the eviction phases are batched.

### Phase 1b — classify

Classification must happen before any eviction, because two classes get pulled out of the
normal flow. Also capture the pod→PVC mapping **now**: it is the input to phase 2's
PVC-first ordering.

Three buckets:

1. **Excluded** — Job/CronJob-owned pods, DaemonSet pods, mirror pods.
2. **Fragile** — pods whose owner has `replicas == 1`, or that have no PodDisruptionBudget,
   or that have no controller owner at all (see "unmanaged pods" below).
3. **Normal** — everything else.

**Classification mechanics** — both fragile tests need spelling out, or they get improvised:

*"Has no PDB"* is not a lookup. List PDBs in the pod's namespace and evaluate each one's
`spec.selector` against the pod's labels; the pod has a PDB if any selector matches. A PDB
in another namespace never applies. An empty selector (`{}`) matches every pod in the
namespace — easy to misread as matching nothing.

*"replicas == 1"* requires walking `ownerReferences` to the top controller, which is one hop
for some kinds and two for others:

| Pod owner | Path to replica count |
|---|---|
| StatefulSet | direct — `spec.replicas` |
| DaemonSet | n/a — excluded |
| Job | n/a — excluded |
| ReplicaSet | **two hops**: ReplicaSet → its Deployment owner → `spec.replicas` |
| ReplicationController | direct |
| none | unmanaged (below) |

Reading `spec.replicas` off the ReplicaSet instead of the Deployment is the easy mistake —
it's usually the same number, but diverges mid-rollout when two ReplicaSets are live, which
is exactly when a drain is riskiest. Use `spec.replicas` (desired), not
`status.readyReplicas`, so a temporarily-unhealthy 3-replica workload isn't misclassified as
fragile.

### Always-on drain behaviors

These are hardcoded, not flags. They correspond to what the current manual runbook already
passes to `kubectl drain`, and to rules already stated elsewhere in this spec.

- **Ignore DaemonSets** (`--ignore-daemonsets`). Not really a choice: the DaemonSet
  controller tolerates the unschedulable taint, so evicted DaemonSet pods return
  immediately. Every cluster has at least a CNI DaemonSet, so drain without this always
  fails.
- **Delete emptyDir data** (`--delete-emptydir-data`). Consistent with unconditional PVC
  destruction (§5, PVC destruction) — emptyDir is strictly more ephemeral than a PVC and
  dies with the pod regardless. Refusing to evict over emptyDir would contradict that rule.
- **Mirror pods** (static pods from `/etc/kubernetes/manifests`) are skipped
  unconditionally by the drain library. No flag involved; the kubelet owns them.

### Unmanaged pods

A pod with no `ownerReferences` entry marked `controller: true` has no controller —
nothing will recreate it. `kubectl drain` refuses these unless `--force` is passed.

Sources in practice: `kubectl run --restart=Never`, leftover `kubectl debug` pods,
hand-applied Pod manifests, pods orphaned by `kubectl delete --cascade=orphan` or by
deleting a ReplicaSet directly, and some Helm hooks.

**Do not expose a `--force` flag.** Instead:

- Detect unmanaged pods during classification and **always list them separately** in
  `plan` and in the run output, labeled as permanent:

```
Unmanaged pods (no controller — will NOT be recreated):
  NAMESPACE   POD              NODE        AGE
  debug       shell-jdoe       node-a-01   3d
```

- Handle them with the fragile cohort (direct delete, phase 3). Given this environment
  accepts data loss, deleting them is correct — the value is in the operator *seeing* it,
  not in blocking it.

Current clusters are believed to be fully controller-managed, so this class is usually
empty. It appears after one `kubectl run` during an incident, which is exactly when
nobody wants a drain to stop cold or to silently destroy someone's debug session.

### Parallelism

**Default: one node at a time.** Phases 2–4 run to completion on a node before the next
node starts. Serial is the safe default: the first node reveals whether the capacity math
was right, and a mistake stops after one node instead of all of them.

```
--parallel N     drain N nodes concurrently
--parallel all   drain every selected node concurrently
```

Notes on the concurrent path:

- Cordon still covers all selected nodes up front. Only eviction is batched.
- Concurrent evictions still go through the eviction API, so PDBs are respected across
  workers — the API server arbitrates, not the tool. Fragile pods bypass eviction by
  design (§ phase 3), so `--parallel all` on a cluster with many single-replica workloads
  means those all go down at once. Call this out in `plan` output when the fragile count
  is non-trivial and parallelism is above 1.
- Log lines must carry the node name so interleaved output stays readable.
- One worker failing shouldn't abandon in-flight work on other nodes; let the others
  finish their current node, then report the aggregate.

### Phase 2 — normal pods

**Order: delete the PVC first, then evict the pod.**

```
1. Delete PVC          → sets deletionTimestamp; pvc-protection finalizer holds it
2. Evict pod           → eviction API
3. Wait: pod object fully GONE (not merely Terminating)
4. Verify PVC actually deleted (finalizer cleared)
5. Wait: replacement pod reaches Running
```

**Why this order.** Deleting a PVC does not remove it — it marks it, and the
`kubernetes.io/pvc-protection` finalizer holds it while any pod still references it. Actual
removal happens when the last referencing pod object disappears.

With the intuitive order (evict, then delete PVC) the StatefulSet controller can recreate
the pod *before* the PVC is deleted. The new pod binds to a healthy PVC whose PV has
`nodeAffinity` to the node just cordoned, goes Pending indefinitely, and now holds the
finalizer on the PVC being deleted. Deadlock, and phase 3's direct delete makes it worse by
removing the pod object faster.

Marking the PVC first means the deletionTimestamp is already set when the replacement pod
appears, so it cannot cleanly bind; the controller waits rather than wedging. Old pod gone →
finalizer clears → PVC removed → controller creates a fresh PVC from the volumeClaimTemplate
→ scheduler places it on an uncordoned node → local-path provisions there.

**Step 3 must wait for the pod object to be gone, not `Terminating`.** The finalizer is not
released while any pod object references the PVC, including a terminating one.

**Accepted consequence.** If a run dies between steps 1 and 2, a PVC is left marked for
deletion while its pod still runs — that data dies the moment the pod restarts for any
reason. Acceptable given the data-loss posture, and re-running finishes the job. Note this
state did not exist under the old ordering.

**Benefit for §7.** A PVC carrying a deletionTimestamp is self-evidently unfinished work, so
a re-run can discover it directly rather than inferring from PVs.

**Validated on a dev cluster (k3s v1.35, local-path).** Deleting a live StatefulSet PVC in
this order and then evicting the pod behaved exactly as reasoned: the PVC took a
`deletionTimestamp` and was held `Bound` by `pvc-protection`; on eviction the old pod and
PVC disappeared together; the controller created a fresh PVC from the volumeClaimTemplate;
and the replacement pod was scheduled onto a different, uncordoned node and reached
Running — about 15 seconds end to end, with no wedge. The old PV was reclaimed and the new
PV's `nodeAffinity` pointed at the new node. This should become an integration test (§10)
rather than remaining a manual check.

The deterministic alternative below is therefore **not needed**, and is retained only as a
fallback if a future cluster behaves differently. The deterministic alternative, if it
does wedge, is to scale the StatefulSet to 0, delete PVCs, and scale back up — no controller
racing at all. `persistentVolumeClaimRetentionPolicy: whenScaled: Delete` makes that even
simpler by having Kubernetes delete the PVCs itself (§13).

**Per-pod eviction timeout: 10 minutes**, configurable via `--eviction-timeout`. Applies
from first eviction attempt to the pod object actually being gone. On expiry: fail, exit
non-zero, and print enough for the operator to debug that specific pod by hand.

**PDB stalls are the common cause.** The eviction API returns 429 when evicting would
violate a PodDisruptionBudget. Usually transient — the workload's other replicas come back
and the retry succeeds. It becomes permanent when the budget can never be satisfied:

- `minAvailable: 1` on a single-replica workload → zero disruptions, forever. Already
  handled: these are classified fragile and bypass eviction (phase 3).
- A replica already unhealthy elsewhere, holding the budget at its floor.
- A misconfigured PDB whose selector matches nothing but still blocks.

Retry with backoff for the timeout duration, then report the blocking PDB by name with its
current numbers — `disruptionsAllowed`, `currentHealthy`, `desiredHealthy`. Without those,
the operator sees "stuck" and has to go find it by hand, which is the runbook this tool
exists to replace.

```
ERROR  eviction timeout after 10m0s

  glueops/loki-write-1 on node-a-01
    blocked by PDB glueops/loki-write
    disruptionsAllowed: 0   currentHealthy: 2   desiredHealthy: 2

  Debug:
    kubectl describe pdb -n glueops loki-write
    kubectl get pods -n glueops -l app=loki-write -o wide

  Re-run once resolved:  evac drain
```

**WaitForFirstConsumer is not a failure.** Local volumes require
`volumeBindingMode: WaitForFirstConsumer`, so a freshly created PVC stays `Pending` until
the pod using it is scheduled. That is the designed behavior, not a stall.

**Wait on the pod reaching Running, never on the PVC reaching Bound.** The PVC binds as a
consequence of scheduling; waiting on it inverts the dependency and will report false
failures on every run.

### NotReady nodes

A NotReady node's kubelet has stopped reporting. Evicting a pod there sets a deletion
timestamp, but nothing terminates the container, because the kubelet that would do it isn't
answering. The pod sits `Terminating` indefinitely and the phase 2 wait never completes.

This matters because "the node is broken" is a normal reason to drain.

**The tool must not force-delete these pods automatically.** Force-delete removes the pod
object without the kubelet confirming the container is dead. If the node is genuinely down,
that's fine. If it's network-partitioned but still running — NotReady means *not
reporting*, not *not running* — the container is alive and the controller immediately
starts a second copy. Two `vault-0` instances both in the raft cluster is the split-brain
case the `--force --grace-period=0` warnings exist for.

The tool cannot distinguish "dead" from "unreachable," so it must not choose.

**Behavior:**

- Preflight flags any NotReady node in the selection, prominently.
- Eviction proceeds normally on those nodes — harmless, it just won't complete.
- The phase 2 eviction timeout bounds the wait.
- On expiry, fail with both real options named, so the operator can check whether the box
  is actually alive and then act deliberately:

```
ERROR  eviction timeout after 10m0s — node-a-03 is NotReady

  3 pods stuck Terminating; kubelet is not confirming termination.

  Confirm whether the node is actually down, then either:
    # node is dead — force-remove the pod objects
    kubectl delete pod -n glueops loki-write-1 --force --grace-period=0

    # node is not coming back — delete the Node object and let pod GC clean up
    kubectl delete node node-a-03

  Do NOT force-delete if the node may still be running: the container keeps
  running and the controller will start a second copy.

  Re-run once resolved:  evac drain
```

Deleting the Node object is usually the cleaner path when the node isn't returning — pod GC
removes the bound pods safely and controllers can recreate without split-brain risk.

### Phase 3 — fragile pods

**Direct delete, not eviction.** A single-replica workload with `minAvailable: 1` allows
zero disruptions, so the eviction API returns 429 forever — not slowly, never. These pods
migrate regardless, accepting downtime. This is what `kubectl drain --disable-eviction`
does.

Note the bucket holds two populations: "no PDB" would evict fine, "replicas=1 with a
restrictive PDB" is the one that needs the delete path. Same handling, but the second
group is where downtime lands — surface that list separately in `plan` output.

PVC handling uses the same order as phase 2: **delete the PVC first, then delete the pod.**
This matters more here, not less — direct delete removes the pod object immediately, so the
controller recreates faster than under eviction and the race window is tighter.

### Phase 4 — wait on Jobs

Job and CronJob pods are never evicted; they finish on their own. Cordoning means no new
Job pods land on the node, so they drain naturally — but "naturally" could be minutes or
hours.

Terminal condition: **no non-DaemonSet, non-Job pods remain on the selected nodes.**
Then wait for Job pods with a configurable deadline.

**Make the terminal check a poll, not a single evaluation.** Deleting a local-path PVC
spawns a short-lived helper pod on the node to remove the directory — the provisioner sets
`spec.nodeName` directly, so cordon doesn't block it. These are unowned pods that appear on
the node mid-drain and exit within seconds. They must not be counted as drain blockers, and
they are not "unmanaged pods" in the §5 sense (classification runs before any PVC deletion,
so they're never in the classified set). A polling terminal check absorbs them naturally. A
one-shot check fails spuriously.

**The classifier must also exclude them explicitly**, because the "classification runs
first" argument only holds on a clean first pass. A captured helper pod looks like this:

```
name:       helper-pod-delete-pvc-c8381767-...
namespace:  kube-system          nodeName: <the node being drained>
ownerRefs:  NONE                 restartPolicy: Never
```

No controller owner and `spec.nodeName` set directly — which makes it **indistinguishable
from an unmanaged pod** under §5's rules. On a re-run (§7) classification happens fresh
against live state, so a helper pod alive at that moment would be reported to the operator
as a permanent pod loss and direct-deleted in phase 3. Recognize and skip it. If a helper pod is still present at the deadline, cleanup
genuinely failed — provisioner down, or node NotReady — and the existing stuck-PVC error
path applies.

**Deadline scope: per node**, since phases 2–4 run per node under the default serial
model. Worst case total wait is therefore N × deadline, which is worth noting in the
`plan` output when the deadline is long and the node count is high. A separate overall
budget can be added later if that turns out to matter.

**On deadline expiry: fail, exit non-zero, and tell the operator to re-run.** Nodes stay
cordoned (cordon is one-way — see §1), so the remaining Jobs keep draining naturally in
the meantime and a later re-run picks up wherever things stand. Non-zero exit means the
tool is honest with a pipeline rather than silently reporting success on an incomplete
drain.

The expiry message must include:

- **Which Job pods are still running**, with namespace, pod name, node, and each pod's
  own age — the per-pod age is what tells the operator whether something is nearly done
  or wedged.
- **Namespaces affected**, so they know who to ask.
- **Total elapsed time** for the drain so far, and the deadline that was exceeded.
- **The re-run command**, verbatim, including the node file path.

```
14:47:03  ERROR  job wait deadline exceeded after 30m0s (drain elapsed 41m12s)

  3 Job pods still running on selected nodes:
    NAMESPACE    POD                        NODE        AGE
    analytics    nightly-rollup-29014-x7f   node-a-01   38m
    analytics    nightly-rollup-29014-k2p   node-a-04   38m
    billing      invoice-sync-29014-9bt     node-a-01   12m

  Namespaces affected: analytics, billing
  All other pods drained successfully. Nodes remain cordoned.

  Re-run when these finish:
    evac drain -f evac-nodes.txt
```

Exit code `3` — see the table in §9.

Optionally offer to suspend the CronJobs up front to shorten the tail. It prevents new
invocations starting during the window, at the cost of mutating user objects — so it's
opt-in, not default.

### PVC destruction

**Unconditional within a permitted cluster**, all namespaces, for any pod in scope. Data
loss is the intended outcome. No label gate, no `--force` flag.

Because there's no per-PVC gate, put the safety budget into **targeting** instead: correct
cluster, correct node set, accurate preview. The question the tool must answer confidently
is "am I looking at the right objects," not "should I really delete this."

**Scope PVC discovery to the target pod set, not to the node.** If discovery sweeps by
node, you'll delete PVCs belonging to pods you never evicted — those pods keep running
with a PVC stuck in `Terminating`.

### PVC deletion guard — CSI-backed clusters

PVC deletion is **permanently disabled on AWS/EKS and on any cluster using a CSI driver.**
The target clusters (k3s, kubeadm, etc.) use node-local storage with no CSI driver, where
deleting a PVC is a clean local operation. On a CSI-backed cluster it is not: the volume
has a detach lifecycle, and deleting the PVC out from under it can strand a
`VolumeAttachment` and a real disk that no Kubernetes object tracks anymore.

**There is no override flag for this.** Not `--force`, not `--i-know-what-im-doing`. A
guard with an escape hatch is a guard someone types past at 2am. If the tool detects a
CSI-backed cluster, PVC deletion is off for that invocation, full stop.

**Drain itself still works on these clusters.** Only the PVC deletion step is suppressed.
Cordon, evict, and the Job wait all behave normally.

Two independent checks, both enforced:

**1. Provider detection.** Any of the following means AWS, and PVC deletion is off:

- Any node's `spec.providerID` has the `aws://` prefix — definitive, present on every node
  in a cloud-provider cluster
- A `CSIDriver` object named `ebs.csi.aws.com` exists
- Any StorageClass uses provisioner `ebs.csi.aws.com` or in-tree `kubernetes.io/aws-ebs`
- The kubecontext name is an EKS ARN (`arn:aws:eks:...`)

**2. Per-PVC volume source check.** Before deleting any individual PVC, determine what
actually backs it. Three sources, in order of authority:

- **PV volume source (authoritative).** Follow `pvc.spec.volumeName` to the PV and inspect
  what's set. This is the volume definition itself, not metadata about it, so it can't be
  stale or missing the way an annotation can.
- **PVC annotation (convenience).** `volume.kubernetes.io/storage-provisioner` — or
  `volume.beta.kubernetes.io/storage-provisioner` on older clusters — is set by the
  provisioning controller and holds the provisioner name directly, so it avoids a PV
  lookup. Only present on *dynamically* provisioned PVCs; absence means unknown, not local.
- **StorageClass provisioner (fallback).** Only needed when the PVC is unbound and has no
  PV to inspect.

**The allowlist is exactly one PV volume source type:**

| Source | Action |
|---|---|
| `pv.spec.local` | delete |
| *everything else* | **refuse** |

Written as an allowlist deliberately. A blocklist ("exclude EBS") would need updating for
every storage backend that ever appears; an allowlist covers NFS, EBS, iSCSI, CephFS, RBD,
Azure Disk/File, GCE PD, vSphere, Portworx, Longhorn, and every future CSI driver with no
maintenance.

**`hostPath` is deliberately excluded.** A hostPath PV is just a path on the node —
Kubernetes has no idea what's behind it, so one pointing into an NFS mount
(`/mnt/nfs-share/data`) would look local while writing to the NAS. Excluding it removes
that gap entirely rather than papering over it with a path-prefix denylist.

Confirmed safe to exclude: on the k3s clusters, `rancher.io/local-path` produces PVs with
`spec.local` populated (paths under `/var/lib/rancher/k3s/storage`) and `spec.hostPath`
empty. Nothing is lost by dropping hostPath.

**NFS must persist** and has three shapes, all refused by the above:

- `pv.spec.nfs` — direct NFS PV
- `pv.spec.csi` with driver `nfs.csi.k8s.io` — CSI NFS driver
- `nfs-subdir-external-provisioner` — produces one or the other depending on version

**Known provisioners in use** (verify per cluster type with
`kubectl get sc -o custom-columns=NAME:.metadata.name,PROVISIONER:.provisioner`):

- k3s: StorageClass `local-path`, provisioner `rancher.io/local-path`, PVs use
  `spec.local` — **confirmed**
- kubeadm: `rancher.io/local-path` if running Rancher's local-path-provisioner, or
  `kubernetes.io/no-provisioner` (StorageClass usually `local-storage`) if running
  `sig-storage-local-static-provisioner`. Both produce `spec.local` PVs. **Confirm which.**

This second check is the more durable one. The real risk is not AWS as such, it's any
volume that isn't node-local — so checking the volume source blocks a newly introduced CSI
driver or NFS mount on any cluster by default, rather than requiring someone to remember to
add a detection rule.

**Fail closed.** If provider detection is inconclusive, or a PVC's backing volume cannot be
identified, or the source is anything other than `pv.spec.local`: refuse. Deletion happens
only on positive identification of a local volume source, never on absence of evidence.

When the guard trips, say so clearly in both `plan` and run output, and report what the
drain will do instead:

```
NOTE  PVC deletion disabled: CSI-backed cluster detected
      signal: node providerID prefix aws://
      6 PVCs in scope will NOT be deleted; pods will rebind on reschedule
```

### Stuck PVC deletions

A PVC held by `kubernetes.io/pvc-protection` clears on its own once the last consuming pod
object is gone — which is why the phase 2 ordering marks it for deletion first and waits for
the pod object to disappear. There is no force flag for PVCs,
and the tool must not patch finalizers off. Stripping `pvc-protection` while a
`VolumeAttachment` still exists is exactly how a volume ends up attached to a node with no
Kubernetes object tracking it.

If a PVC is still `Terminating` after a timeout, something genuinely didn't go away — a
lingering pod, a stuck `VolumeAttachment`, a controller problem. Report it and exit
non-zero rather than papering over it. Same shape as the Job deadline failure: name what's
stuck, and suggest re-running once resolved.

```
ERROR  2 PVCs stuck in Terminating after 5m0s
       platform/data-argocd-0   finalizers: kubernetes.io/pvc-protection
       platform/data-argocd-1   finalizers: kubernetes.io/pvc-protection

       Check for lingering pods and VolumeAttachments on the selected nodes.
       Re-run once resolved:  evac drain
```

---

## 6. Mutual exclusion

Only one drain may run at a time. Two concurrent runs would each derive scope from live
state while the other mutates it, producing overlapping cordons and double evictions.

**Local advisory file lock (`flock`).** One lock file, taken for the duration of a `drain`.

Path: `$XDG_RUNTIME_DIR/evac.lock`, falling back to `$TMPDIR`, then `/tmp/evac-$UID.lock`.
The uid in the fallback name matters — `/tmp` is world-writable and a fixed name lets
another user block or interfere with the lock.

**On macOS the fallback is the normal path, not the exception:** `$XDG_RUNTIME_DIR` is
essentially never set there. Prefer `$TMPDIR` (per-user, under `/var/folders/...`). If the
path does land in `/tmp`, open with `O_NOFOLLOW` and verify ownership and mode after
opening — a predictable filename in a world-writable directory is a symlink-swap target,
and the uid in the name does not address that.

**Open-flag ordering matters.** Open `O_RDWR|O_CREAT|O_CLOEXEC` — **never `O_TRUNC`** —
then `flock`, then `ftruncate(0)` and write the holder metadata. Truncating at open time
wipes a live holder's pid/context/start-time on every *failed* attempt, so the second
operator's error message would destroy the very data it was about to print.

`O_CLOEXEC` is required for the stale-lock claim below to hold: flock attaches to the open
file description, not the process, so a forked child inheriting the descriptor keeps the
lock alive after the parent dies.

**Reading the holder metadata races with writing it.** The process that loses the lock
reads the file while holding nothing, so it may see an empty or half-written record. Do
*not* fix this by writing to a temp file and renaming — rename swaps the inode, and the
lock lives on the open file description of the original inode, so a third process opening
the path would get a fresh inode and acquire the lock successfully. Instead, retry the
read a few times at short intervals and otherwise degrade to `another drain is already
running (holder details unavailable)`. Correctness must never depend on the metadata; it
is diagnostic text only.

Scope is global, not per-context: one drain at a time, period, matching the "single
cluster at a time" constraint in §1.

**No stale-lock handling is needed.** The kernel releases a `flock` when the holding
process dies, by any means including SIGKILL. A crashed run can be retried immediately,
with no cleanup and no expiry window. This is a genuine advantage over an in-cluster
Lease, which would need a duration, a renewal loop, and a force-unlock escape hatch.

Acquire non-blocking (`LOCK_EX | LOCK_NB`) and fail fast rather than queueing — an
operator who runs it twice wants to be told, not left waiting.

Write the holder's pid, kubecontext, and start time into the file so the error is
actionable:

```
ERROR  another drain is already running (pid 48122, started 8m ago)
       context: glueops-prod-eu-1
```

**Known limitation, accepted:** this only prevents concurrent runs on the same machine. Two
operators on two laptops can still drain the same cluster simultaneously. An in-cluster
`coordination.k8s.io/v1` Lease would cover that case if it ever becomes a real problem.

Scope: the lock covers `drain` only. `nodes` and `plan` are read-only and must
never take it — an operator should always be able to look at a cluster while a drain runs.

---

## 7. Idempotent re-run

**The recovery model.** No checkpoint, no resume. Every operation is convergent:

- Cordon on a cordoned node → no-op
- Evict a pod that's gone → no-op
- Delete a deleted PVC → no-op
- A PVC already carrying a `deletionTimestamp` → skip the delete, proceed to eviction
- Pods already off the selected nodes → not in scope

A second run re-derives reality and finishes whatever remains. First run's failures become
second run's work.

**This requires that scope be derived fresh from live cluster state every run.** Never
from a cache, never from a file beyond the node list itself.

**The one gap, now much smaller.** Under the phase 2 ordering (§5), the PVC is marked for
deletion *before* the pod is evicted, so a PVC carrying a `deletionTimestamp` is
self-evidently unfinished work and is directly discoverable on re-run. The gap only opens if
a run dies between classification and the PVC delete. Cover it with a second discovery path
anyway:

- **PV-side lookup:** list PVs, match `spec.nodeAffinity` against the selected nodes,
  follow `spec.claimRef` back to the PVC. Works with no pods in the picture. **Confirmed
  viable:** the only deletable volume source is `pv.spec.local` (§5), and the API requires
  local PVs to carry `nodeAffinity` — so every PVC this tool can delete is discoverable
  this way. This is a complete alternative path, not a partial one.
- At minimum, report orphaned/`Released` PVs on the selected nodes so the operator sees
  what's left over.

Running both discovery paths and diffing them is cheap and catches orphans.

---

## 8. Plan / preflight (`evac plan`)

Read-only. Output is the artifact a second person reviews before the run, and what goes
in the change ticket.

**Ordering matters.** PVC destruction is the only irreversible part of this operation —
everything else reschedules. So the PVC detail leads the output, above pod counts, capacity
math, and everything else. An operator scanning quickly should hit the permanent losses
first, not last.

### Output order

**1. PVCs to be destroyed — per pod, with what backs them**

```
PVCs to be DESTROYED (3 nodes, glueops-prod-eu-1):

  NAMESPACE   POD              PVC                        SOURCE
  glueops     vault-0          data-vault-0               local
  glueops     loki-write-0     loki_data-loki-write-0     local
  glueops     kps-prometheus-0 prometheus-db-...-0        local

  3 PVCs
```

Only PVCs that will actually be deleted appear here. PVCs the volume-source guard keeps
are not listed — they're not a loss and don't need review.

Pods with no PVCs are omitted too; they appear in the namespace summary below.

Size handling: show in full by default, since twenty rows is readable. `--brief` collapses
to per-namespace counts. Two hundred rows is not readable, so above a threshold
(say 50) collapse automatically and note that `--full` expands it.

**2. PVC guard status** — shown *only* when PVC deletion is suppressed cluster-wide (§5,
PVC deletion guard), naming which signal tripped it. On a normal cluster where deletion is
enabled, nothing is printed here. Per-PVC keeps are never reported.

**3. Permanent pod losses** — unmanaged pods (no controller), labeled as not coming back.

**4. Everything else:**

- Target cluster context and node list
- Pods by classification (normal / fragile / excluded-Job / DaemonSet)
- The fragile list, with the downtime-bearing subset called out
- Job pods that will be waited on
- **Blast radius by namespace** — the inverse query, which is what a reviewer wants:

```
Draining 3 nodes affects:
  platform      8 pods, 3 PVCs
  monitoring    4 pods, 1 PVC
  team-alpha    2 pods, 0 PVCs   ← 1 pod has no PDB
```

**5. Preflight results** (below).

### Preflight checks

- **Capacity.** Sum pod requests on selected nodes against allocatable on the remaining
  schedulable set. Compute against nodes that will actually be schedulable after phase 1,
  not against all nodes. **This includes schedulable control-plane nodes** — they are
  excluded from draining (§3.1) but remain valid scheduling targets, and on k3s they are
  untainted and routinely receive workloads. Omitting them under-reports capacity. Getting this wrong turns a maintenance window into
  everything-Pending, discovered halfway through.
- **Affinity trap.** If the selected set includes every node matching some workload's
  `nodeSelector` or node affinity, those pods have nowhere to go regardless of raw capacity.
  Cheap to detect, and it's the failure that looks like a mystery at 2am.
- **Pod anti-affinity trap.** Check `podAntiAffinity` with
  `requiredDuringSchedulingIgnoredDuringExecution`, not just node affinity. This is the more
  likely blocker for the workloads in these clusters: a 3-replica StatefulSet with
  one-replica-per-node anti-affinity (typical for loki, vault, prometheus) cannot reschedule
  an evicted replica when the other two occupy the remaining nodes. Capacity is fine, the
  scheduler still refuses, and the drain stalls with no obvious cause. For each evictable
  pod with required anti-affinity, confirm at least one schedulable node exists that
  satisfies the topology constraint once phase 1 cordons are applied.
- **NotReady nodes** in the selection, flagged prominently — evictions there will not
  complete on their own (§5, NotReady nodes). Warning, not a hard error, since a broken
  node is a legitimate reason to drain.

**Preflight failures block the drain.** Capacity shortfall and the affinity trap are hard
errors, not warnings — they exit non-zero before anything is cordoned. `--yes` does not
override them; it only skips the confirmation prompt. A separate `--ignore-preflight`
exists for the case where the operator knows better (workloads that will be deleted
anyway, requests that are wildly over-provisioned), and it must log loudly what it
overrode.

Keeping these separate is the point: headless mode should be able to run a drain someone
already reasoned about, but should never be able to walk into everything-Pending silently.

### Confirmation in `evac drain`

After rendering the above, prompt. Repeat the destructive totals immediately above the
prompt — the PVC table may have scrolled off, and this line is what the operator's eye
lands on:

```
About to drain 3 nodes in glueops-prod-eu-1:
  3 PVCs DESTROYED
  2 unmanaged pods deleted permanently
  14 pods evicted

Proceed? [y/N]
```

Irreversible items first here too, and pod eviction — the recoverable part — last.

Default to no. If stdin is not a TTY and `--yes` was not passed, fail rather than
proceeding or hanging on a prompt nobody can answer.

---

## 9. Output and audit

- **Streaming, timestamped, line-oriented.** No full-screen UI during execution. The
  transcript is the audit trail for an operation that destroys data — it must be
  `tee`-able, greppable, and survive process exit.

```
14:02:11  phase=2 ns=platform  evicting pod/argocd-repo-server-7d4  node=node-a-01
14:02:14  phase=2 ns=platform  pod terminated  (3.1s)
14:02:14  phase=2 ns=platform  deleting pvc/data-argocd-0  sc=local-path
```

- **Always write a log file**, in addition to stdout. An operation that destroys data should
  not depend on the operator having remembered to `tee`. Default to a timestamped path
  (`./evac-<context>-<timestamp>.log`) and print the location at both start and end.
  `--log-file` overrides, `--no-log-file` opts out.
- `--output=json` emitting the same events, for shipping elsewhere.
- Target echo and confirmation before acting are specified in §8. This catches "I switched
  contexts three terminals ago."
- Progress indication during waits is fine (carriage returns, not a TUI).

### Exit codes

Distinct codes matter because "not finished yet" and "something is broken" call for
different responses from a wrapper — the first is worth retrying on a timer, the second
needs a human.

| Code | Meaning | Retry sensible? |
|---|---|---|
| `0` | Drain completed | — |
| `1` | Error — API failure, unexpected condition | No |
| `2` | Usage error — bad flags, unreadable node file | No |
| `3` | Timed out waiting on Job pods (§5 phase 4) | Yes, on a timer |
| `4` | Eviction timeout (§5 phase 2) — often a PDB stall | Only after investigating |
| `5` | PVC stuck `Terminating` past its timeout (§5) | Only after investigating |
| `6` | Preflight failed — capacity, affinity, anti-affinity | No, fix the input |
| `7` | Refused — control plane node in selection (§3.1) | No |
| `8` | Lock held by another run (§6) | Yes, briefly |
| `9` | Aborted — operator answered `n` at the confirmation prompt | — |
| `130` | Interrupted (SIGINT) mid-run | Only after investigating |

Codes 3 through 5 all leave nodes cordoned and are safe to resolve and re-run (§7).

**Aggregation under `--parallel`.** Different nodes can fail for different reasons in one
run, and the spec must say which code wins. **Severity precedence, highest first:
`1 > 5 > 4 > 3`.** The reasoning is that `3` is the only code a wrapper should retry on a
timer; if it could mask a node that needs a human, a wrapper would retry forever against an
unresolved PDB stall. Print a per-node outcome table at the end, and include per-node codes
in the JSON output so wrappers can be precise.

**Check-order precedence.** Where two conditions apply at once, `7` (control plane in
selection) is evaluated before `6` (preflight), since the refusal is categorical.

Two cases the table previously left unassigned: stdin is not a TTY and `--yes` was not
passed (§8) is a **usage** error, code `2`; and the §3.1 single-node cluster with nothing
drainable is also `2`, not `0` — reporting success for a drain that did nothing is exactly
the dishonesty the non-zero-exit rule exists to prevent.

---

## 10. Implementation notes

- **Go**, with `client-go`. Build with Go **1.26+** — `client-go` and `golang.org/x/sys`
  both declare a `go 1.26.0` floor.
- **Do not import `k8s.io/kubectl/pkg/drain`.** Call the `policy/v1` Eviction subresource
  directly (`Pods(ns).EvictV1(...)`).

  An earlier revision of this spec mandated the library on the grounds that it provides
  "the eviction helper, PDB handling, DaemonSet and mirror-pod filtering." Two of those
  reasons do not survive contact:

  - **It performs no PDB evaluation.** PodDisruptionBudgets are enforced server-side by
    the eviction subresource; the library only reacts to the resulting 429. Importing it
    buys nothing here.
  - **Its control flow is node-batch, not per-pod.** `DeleteOrEvictPods` spawns a
    goroutine per pod, owns an unbounded 429 retry loop, and then waits across the whole
    set. §5 phase 2 requires strict per-pod serialization with a PVC delete interleaved
    *before* eviction and a finalizer check *after* the pod object is gone. That cannot be
    expressed through it.
  - **Its filters violate the query rule below.** `GetPodsForDeletion` issues a per-node
    LIST with a `spec.nodeName` field selector, and `daemonSetFilter` does a DaemonSet
    `Get()` per pod.

  What is actually lost is ~250 lines, most of which §5 phase 1b mandates writing anyway —
  and the classification here is *richer* than the library's, which does neither the
  two-hop ownerRef walk nor PDB selector matching. **Worth copying deliberately:** its
  `waitForDelete` "gone" test is NotFound **or UID changed**, which is exactly the §5
  phase 2 step-3 semantic and is easy to get wrong.

- **Client construction.** Typed clientset only. No informers (LIST+WATCH plus cache
  construction is pure overhead for a process that exits in minutes) and no
  controller-runtime (drags in manager/scheme machinery and a cache-backed client).
- **Paginate every list.** client-go does *not* paginate a plain `List()`. §3's "fixed
  small number of list calls" means a fixed number of *paginated* list calls — set
  `metav1.ListOptions{Limit: 500}` and follow the `Continue` token for pods, PVCs, and PVs.
  The call count stays fixed per resource type; only page count varies with cluster size.
- **Use a `Watch`, not polling Gets, for the phase 2 step-3 wait.** The delete event is
  needed precisely, and it is one connection instead of N polls.
- **Pin `client-go` to the oldest server minor targeted** (v0.35.x for k3s 1.35, which
  also covers kubeadm on 1.34–1.36 under the ±1 skew policy). Note kubectl v0.37 tracks
  Kubernetes 1.37 — two minors ahead of k3s 1.35 and outside the supported window.
- Cobra for commands. **Do not use `k8s.io/cli-runtime`'s `genericclioptions`** — its
  `AddFlags` registers ~20 flags in one shot, including `-n`, with no selective
  registration, which is precisely what §1 rejects. Declare `--kubeconfig` and `--context`
  by hand (~15 lines) and feed them to the loader below.
- Load kubeconfig via `clientcmd.NewNonInteractiveDeferredLoadingClientConfig` with
  default loading rules (`ClientConfigLoadingRules.ExplicitPath`,
  `ConfigOverrides.CurrentContext`).
- **Tables: stdlib `text/tabwriter`.** Not `cli-runtime/pkg/printers` — it leaves the
  dependency graph entirely once `k8s.io/kubectl` is dropped, and re-adding it pulls
  `kustomize/api`, `kustomize/kyaml`, `moby/term`, `treeprint`, and a third-party
  tabwriter fork. `labels.FormatLabels` (in `apimachinery`, a dependency regardless) makes
  `--show-labels` one line; `--label-columns` is ~40 lines.
- **Logging: no logging library.** §9's three outputs — the human line stream, the audit
  file, and `--output=json` — are three renderings of one event model, and §5's error
  blocks are not log lines at all. Define `Event` and `Diagnostic` types and fan them out
  to three sinks. slog/zerolog/zap would each want to own the schema.
- **Redirect klog.** client-go logs via klog straight to stderr; left alone it interleaves
  with §9's format and never reaches the log file.
- **Locking: `golang.org/x/sys/unix.Flock` directly**, not `gofrs/flock` — the latter does
  not expose the file descriptor, which §6 needs to write holder metadata into the locked
  file. See §6 for the open-flag ordering.
- **Never call `os.Exit` from a worker.** It skips defers and discards buffered log lines.
  Workers return errors; `main` exits only after the recorder is closed. For a tool whose
  transcript is the audit trail, this is the likeliest way to lose the most important
  final lines.
- **RBAC:** consider a dedicated service account with only the verbs this needs — `delete`
  on PVCs, `patch` on nodes for cordon, `create` on pod eviction, and `list` on pods,
  PVCs, PVs, nodes, PDBs, StorageClasses, and CSIDrivers (§5's guards need all four of
  those last). Enforcing scope at the API server is stronger than enforcing it in the
  binary.
- Bash is the wrong tool here. Error handling and state tracking are exactly what it's
  worst at, and this is code where a swallowed error deletes data.

---

## 11. Naming and distribution

Binary: `evac`. Repo: `glueops/evac`.

- **Name verified free (2026-09-12):** Homebrew core returns 404 for `evac`; the Go module
  proxy has no published versions for `github.com/GlueOps/evac`; the krew index holds 407
  plugins and none is `evac` (nearest is `slowdrain`); and no GitHub repository named
  `evac` occupies this space — the hits are crowd-evacuation simulations and an EVA-CLIP
  preprocessor.
- **Module path is `github.com/GlueOps/evac`**, matching the repository's capitalization.
  This is safe: the module proxy `!`-encodes capitals (`!glue!ops`) so case-insensitive
  filesystems cannot collide, and capitalized module paths are routine. The one real
  hazard is documentation — a lowercase import fails hard (`module declares its path as
  github.com/GlueOps/evac but was required as github.com/glueops/evac`), so every README
  and install line must use exact case: `go install github.com/GlueOps/evac@latest`.
- `kubectl-evac` would make `kubectl evac ...` work if the plugin form is ever wanted; the
  krew index is clear. The standalone invocation stays primary either way (§1).
- Repo description should lead with the destructive behavior, not the convenience:
  *"Drains Kubernetes worker nodes and destroys their local PVCs. For clusters where local
  volume data is disposable."*
- Avoid the abbreviation `gops` anywhere — it's an existing Go process-diagnostic tool.

---

## 12. Deliberately out of scope

- Resumability / checkpointing — replaced by idempotent re-run
- Uncordon — operator does it with kubectl after maintenance
- Multi-cluster / fleet operation
- Namespace-ordered phases — dropped
- Old/new node generation concept — dropped; selection is purely operator choice
- `migrate-namespaces` mode — existed only to serve namespace ordering
- Clipboard integration — replaced by the node list file
- TUI for execution
- Configurable phase ordering

---

## 13. Open questions

1. **The "one or two other things"** from the original runbook that were never pinned
   down. If they're additional destructive steps in the same sequence, they belong in §5
   now. If they're separate operations, they're separate subcommands and can wait.
2. **PVC recreation path.** Deleting a PVC is half the transaction; something must
   recreate it.
   - StatefulSet `volumeClaimTemplates` → controller recreates automatically. Fine.
   - Standalone PVC manifest + Deployment → nothing recreates it. Pod sits Pending on
     `persistentvolumeclaim "x" not found` indefinitely.
   - Likely GitOps (Argo/Flux) re-syncs it — but verify sync policy, and watch the race
     where auto-sync recreates the PVC while the old pod is still terminating and it
     rebinds to the old node's PV.
   - Determines whether the tool needs to trigger a sync or just wait.
3. **Reclaim policy check.** ~~If the backing PVs are `Retain`...~~ **Answered for k3s:**
   the `local-path` StorageClass ships `reclaimPolicy: Delete`, and a drain test left zero
   PVs and zero PVCs behind — no `Released` orphans to clean up. Still worth confirming on
   kubeadm clusters, where the provisioner may differ (§5, known provisioners).
4. **StatefulSet `persistentVolumeClaimRetentionPolicy`.** ~~May handle PVC cleanup
   natively...~~ **Answered: it does not help.** Tested directly — with
   `whenScaled: Delete` set, evicting a pod left the PVC untouched, bound to the identical
   PV. The policy fires on scale-down only, and a drain does not scale the StatefulSet.
   Scaling to 0 *did* delete every PVC automatically, so the policy only assists the
   scale-to-0 fallback that §5 phase 2 no longer needs. **Phase 2's explicit PVC deletion
   stays exactly as specified.**

5. **PV volume source, confirmed.** `rancher.io/local-path` on k3s v1.35 produces PVs with
   `spec.local` populated (paths under `/var/lib/rancher/k3s/storage`), `spec.hostPath`
   empty, and `nodeAffinity` on `kubernetes.io/hostname` always present — so §5's allowlist
   premise and §7's PV-side discovery path both hold. Both provisioner annotations are set
   (`volume.kubernetes.io/storage-provisioner` and the `beta` form), plus
   `volume.kubernetes.io/selected-node` as a bonus discovery signal.
   of the workload. If it covers your PVCs, a large chunk of this spec disappears.