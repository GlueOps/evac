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

### Linux, one command

<!-- x-release-please-start-version -->
```sh
VERSION=0.1.0
curl -fsSL "https://github.com/GlueOps/evac/releases/download/v${VERSION}/evac_${VERSION}_linux_$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/').tar.gz" \
  | tar -xzf - evac && sudo install -m 0755 evac /usr/local/bin/evac && rm evac
```
<!-- x-release-please-end -->

`evac` is then on your `PATH` at `/usr/local/bin/evac` — check with `evac version`. The
`uname` expression picks amd64 or arm64 for you. Use `wget -qO-` in place of `curl -fsSL` if
that is what you have, and drop everything after `tar` to leave the binary in the current
directory instead of installing it system-wide.

The version above is rewritten by release-please on every release, so it always names the
current one. To pin a different release, set `VERSION` yourself.

### With Go

```sh
go install github.com/GlueOps/evac/cmd/evac@latest
```

The `/cmd/evac` suffix is required: the module root holds no `main` package, so installing
`github.com/GlueOps/evac` itself fails. The capitalisation matters too — Go module paths are
case-sensitive, and a lowercase path fails with
`module declares its path as: github.com/GlueOps/evac`.

### By hand

Every release carries linux and darwin builds for amd64 and arm64, plus `checksums.txt`:
[releases](https://github.com/GlueOps/evac/releases).

## Use

```sh
evac nodes                 # inventory table
evac nodes -i              # --interactive: pick nodes, write a node file
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

### Timeouts

| Flag | Default | Bounds |
|---|---|---|
| `--eviction-timeout` | 10m | One pod, from the first eviction attempt to the pod object being gone |
| `--job-deadline` | 30m | Per node, waiting for Job pods to finish on their own |
| `--pvc-timeout` | 5m | How long a claim may stay `Terminating` before the run reports it stuck |

Each expiry has its own exit code, so a wrapper can tell them apart — see below.

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
| **One drain at a time** | A `flock` on `$XDG_RUNTIME_DIR/evac.lock`, falling back to `$TMPDIR` then `/tmp` as `evac-<uid>.lock` — on macOS, where `XDG_RUNTIME_DIR` is normally unset, the fallback is the usual path. `nodes` and `plan` never take it, so you can always look at a cluster mid-drain. |
| **Idempotent** | No checkpoints. Every operation is convergent and scope is re-derived from live state, so a failed run is recovered by running it again. |
| **Audited by default** | Every run writes a timestamped `./evac-<context>-<time>.log` in addition to stdout, unless `--no-log-file` is passed or `--log-file` redirects it. `--output=json` emits the same events. |

Preflight blocks the drain on capacity shortfalls, on affinity traps — the case where the
nodes you selected are the only ones a workload is allowed to run on — and on a selection that
would leave a control plane node as the only place anything can be scheduled. That last one
matters on k3s, where the server node is untainted and schedulable: draining every agent
otherwise relocates the whole cluster onto the node running the API server, and the capacity
arithmetic happily reports it as fine.

`--yes` skips the confirmation prompt but **does not** bypass preflight; `--ignore-preflight`
does, and logs loudly what it overrode.

## Exit codes

| Code | Meaning | Retry sensible? |
|---|---|---|
| `0` | Drain completed | — |
| `1` | Error — API failure, unexpected condition, or pods that cannot be drained | No |
| `2` | Usage error — bad flags, unknown command, unreadable node file | No |
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

## Releases

Releases are cut by [release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/) on `main`. Merging a `feat:` or
`fix:` opens a release PR that bumps the version and updates `CHANGELOG.md`; merging *that*
tags `vX.Y.Z`, cuts the GitHub release, and triggers goreleaser to attach binaries to it.

Nothing is released by pushing a tag by hand, and nothing is released from a branch.

The first release will be `0.1.0`. This is pre-1.0 deliberately: the tool has only ever run
against k3s, and the concurrent drain path behind `--parallel` is exercised only by the
integration suite, never by a unit test.

## Development

Go 1.26.6 or newer is required, and `client-go` is held at v0.35.x deliberately — it
supports Kubernetes 1.34 to 1.36 under the ±1 skew policy, which is what the integration
matrix tests. Raising it narrows which clusters are supported.

The Go floor is a security one rather than a feature one: earlier 1.26 patches carry
stdlib vulnerabilities reachable through client-go's TLS paths, and CI fails on them via
`govulncheck`.

There is no need for a local Go toolchain — every target runs in a container:

```sh
make docker-build      # build the binary
make docker-test       # unit tests
make docker-vet        # go vet, including the integration-tagged tests
make docker-lint       # golangci-lint
make docker-snapshot   # binaries for every release target, into dist/
```

CI runs the same targets natively on every pull request, against every Kubernetes minor in
the supported skew window (1.34, 1.35, 1.36). Integration tests are gated three ways and are **not** run by
`go test ./...`:

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

### PR builds

Every pull request that touches the build — Go sources, `go.mod`/`go.sum`, the
`Makefile` or the goreleaser config — gets binaries built for it automatically:
linux and darwin, amd64 and arm64. A comment on the PR links
them; they are attached to the workflow run and expire after 7 days.

They are versioned `0.0.0-<branch>.<sha>`, so `evac version` reports which
branch and commit you are running and cannot be mistaken for a release.

To build the same thing locally:

```sh
make snapshot          # needs goreleaser
make docker-snapshot   # or in a container, like every other target
```

These are deliberately not tag pushes. release-please derives its baseline from
tags, and a tag would also cut a real GitHub release.

## License

See [LICENSE](LICENSE).
