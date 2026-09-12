# evac

**Drains Kubernetes worker nodes and destroys their local PVCs. For clusters where local volume data is disposable.**

`evac` replaces a ~10-command manual runbook for node maintenance. It cordons the nodes you
select, evicts their pods in a specific order, deletes the PersistentVolumeClaims those pods
were using, and waits for the workloads to come back somewhere else.

---

## Read this before installing

**This tool destroys data by design.** Deleting PVCs is the intended behaviour, not a side
effect. It is built for clusters using node-local storage — k3s or kubeadm with `local-path`
or `sig-storage-local-static-provisioner` — where that data is regenerable.

It is the wrong tool for your cluster if any of these are true:

- Your PersistentVolumes are backed by anything other than `pv.spec.local` — NFS, EBS, Ceph,
  Longhorn, or any CSI driver. `evac` refuses to delete these, so it will drain but leave
  every claim in place.
- You are on AWS or EKS. PVC deletion is disabled entirely there, with no override.
- You need to drain control plane or etcd nodes. `evac` refuses, at every layer, with no
  override flag.
- The data on those volumes matters. There is no dry-run-only mode that leaves PVCs alone,
  because deleting them is the point.

There is deliberately no `--force`, no `--i-know-what-im-doing`, and no way to switch the
volume-source guard off. A guard with an escape hatch is a guard someone types past at 2am.

## Install

```sh
go install github.com/GlueOps/evac@latest
```

The capitalisation matters — Go module paths are case-sensitive, and a lowercase import fails
with `module declares its path as: github.com/GlueOps/evac`.

Or download a binary from [releases](https://github.com/GlueOps/evac/releases).

## Use

```sh
evac nodes                 # inventory table
evac nodes -i              # pick nodes interactively, write a node file
evac plan                  # blast radius + preflight, no mutations
evac drain                 # plan, confirm, execute
```

`plan` and `drain` read a per-context node file when `-f` is omitted, so the common path is
`evac nodes -i` followed by a bare `evac drain`.

Selection can also be given directly, and `-f -` reads stdin:

```sh
evac drain --nodes node-a-01,node-a-04
evac drain --selector pool=a
kubectl get nodes -l pool=a -o name | evac drain -f -
```

`evac` operates on the **active kubecontext**. `KUBECONFIG`, `--kubeconfig` and `--context`
all behave as they do with kubectl.

### What a drain actually does

1. **Cordon** every selected node, before any eviction — so pods evicted from one node cannot
   land on another node you are about to drain.
2. **Classify** each pod as normal, fragile, or excluded.
3. **Normal pods** — delete the PVC, then evict through the eviction API (PDBs respected),
   wait for the pod object to disappear, then wait for the workload to be whole again.
4. **Fragile pods** — same PVC ordering, but deleted directly. A single-replica workload with
   `minAvailable: 1` allows zero disruptions, so the eviction API would refuse it forever.
5. **Job pods** are never evicted; they finish on their own and `evac` waits for them.

Nodes are left **cordoned**. `evac` never uncordons — you do that with `kubectl` once the
maintenance work is done.

### Why PVCs are deleted before pods are evicted

Deleting a PVC does not remove it; it marks it, and the `pvc-protection` finalizer holds it
while any pod still references it. With the intuitive order — evict, then delete the PVC —
the controller can recreate the pod *before* the PVC is gone. The new pod binds to a healthy
claim whose PV is pinned to the node you just cordoned, goes `Pending` indefinitely, and now
holds the finalizer on the claim being deleted.

Marking the PVC first means the deletion timestamp is already set when the replacement
appears, so it cannot cleanly bind and the controller waits instead of wedging.

## Safety properties

| | |
|---|---|
| **Worker nodes only** | Control plane and etcd nodes are excluded from the inventory picker, from `--nodes` and `--selector`, and a node file naming one is refused outright. No override. |
| **Local volumes only** | Only `pv.spec.local` is deletable. Everything else — including `hostPath`, which may point into an NFS mount — is refused. Written as an allowlist, so a future CSI driver is refused with no code change. |
| **Never on AWS/EKS** | Detected from node `providerID`, the EBS CSI driver, StorageClass provisioners, or an EKS ARN context name. Drain still works; PVC deletion is off. |
| **One drain at a time** | A `flock` on `$XDG_RUNTIME_DIR/evac.lock`. `nodes` and `plan` never take it, so you can always look at a cluster mid-drain. |
| **Idempotent** | No checkpoints. Every operation is convergent and scope is re-derived from live state, so a failed run is recovered by running it again. |
| **Always audited** | Every run writes a timestamped log file in addition to stdout. `--output=json` emits the same events. |

Preflight blocks the drain on capacity shortfalls and on affinity traps — the case where the
nodes you selected are the only ones a workload is allowed to run on. `--yes` skips the
confirmation prompt but **does not** bypass preflight; `--ignore-preflight` does, and logs
loudly what it overrode.

## Exit codes

| Code | Meaning | Retry sensible? |
|---|---|---|
| `0` | Drain completed | — |
| `1` | Error — API failure, unexpected condition | No |
| `2` | Usage error — bad flags, unreadable node file | No |
| `3` | Timed out waiting on Job pods | Yes, on a timer |
| `4` | Eviction timeout — often a PDB stall | Only after investigating |
| `5` | PVC stuck `Terminating` | Only after investigating |
| `6` | Preflight failed | No, fix the input |
| `7` | Refused — control plane node in selection | No |
| `8` | Lock held by another run | Yes, briefly |
| `9` | Aborted at the confirmation prompt | — |
| `130` | Interrupted | — |

Codes 3–5 leave nodes cordoned and are safe to resolve and re-run. Under `--parallel`, the
most severe code wins, and `3` never masks a failure that needs a human.

## RBAC

`evac` needs `list` on nodes, pods, PVCs, PVs, PodDisruptionBudgets, StorageClasses,
CSIDrivers, and the workload controllers; `patch` on nodes; `create` on pod eviction; and
`delete` on PVCs and pods. Enforcing that at the API server is stronger than enforcing it in
the binary.

## Development

There is no need for a local Go toolchain — every target runs in a container:

```sh
make docker-build      # build the binary
make docker-test       # unit tests
make docker-vet        # go vet
```

CI runs the same targets natively. Integration tests are gated three ways and are **not** run
by `go test ./...`:

```sh
k3d cluster create evac-it
kubectl --context k3d-evac-it create namespace evac-integration-sentinel
kubectl --context k3d-evac-it label namespace evac-integration-sentinel \
  evac.glueops.dev/destructive-tests=allowed

EVAC_TEST_CLUSTER=k3d-evac-it go test -tags integration ./test/integration/...
```

Some tests need a volume that is genuinely *not* node-local, to prove the guard
refuses it. `test/integration/setup-nfs.sh k3d-evac-it` installs `csi-driver-nfs`
over an NFS server container; without it those tests skip rather than fail.

The sentinel namespace is the guard that matters: it is a property of the target cluster
rather than of your shell, so a mistyped context name cannot reach a cluster nobody marked as
disposable.

[`SPEC.md`](SPEC.md) is the source of truth for behaviour and carries the reasoning behind
each decision.

## License

See [LICENSE](LICENSE).
