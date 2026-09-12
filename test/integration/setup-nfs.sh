#!/usr/bin/env bash
# Sets up a real CSI driver over a real NFS server, so the volume-source guard
# can be tested against a genuine pv.spec.csi volume rather than a hand-built
# fixture. The guard's whole job is refusing volumes that are not node-local,
# and a unit test can only assert that against a struct someone wrote; driving
# an actual driver proves the field paths are right against objects Kubernetes
# produced itself.
#
# Everything runs in Docker. The NFS server is nfs-ganesha, which serves NFS
# entirely in userspace — kernel NFS servers need the host's nfsd module, which
# GitHub Actions runners do not reliably have, and depending on the host kernel
# would make this pass locally and fail in CI. Two capabilities are enough; it
# does not need --privileged.
#
# Usage: test/integration/setup-nfs.sh <kubecontext> [docker-network]
set -euo pipefail

CONTEXT="${1:?usage: setup-nfs.sh <kubecontext> [docker-network]}"
NETWORK="${2:-k3d-${CONTEXT#k3d-}}"
NFS_CONTAINER="evac-nfs-${CONTEXT#k3d-}"
# Pinned by digest: ":latest" for a test fixture means a green suite can turn
# red with no change to this repository.
# renovate: datasource=docker depName=janeczku/nfs-ganesha versioning=docker
NFS_IMAGE="janeczku/nfs-ganesha:latest@sha256:17fe1813fd20d9fdfa497a26c8a2e39dd49748cd39dbb0559df7627d9bcf4c53"
K=(kubectl --context "${CONTEXT}")

echo "==> nfs-ganesha on network ${NETWORK}"
docker rm -f "${NFS_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${NFS_CONTAINER}" --network "${NETWORK}" \
  --cap-add SYS_ADMIN --cap-add DAC_READ_SEARCH \
  "${NFS_IMAGE}" >/dev/null

NFS_IP="$(docker inspect "${NFS_CONTAINER}" \
  -f '{{range $k, $v := .NetworkSettings.Networks}}{{$v.IPAddress}}{{end}}')"
if [ -z "${NFS_IP}" ]; then
  echo "could not determine the NFS server IP" >&2
  docker logs "${NFS_CONTAINER}" >&2 || true
  exit 1
fi

# Wait on the port actually accepting connections rather than on a log line:
# ganesha logs "INITIALIZED" before it is necessarily serving, and a mount that
# races the server produces a confusing provisioning error much later.
echo "    waiting for ${NFS_IP}:2049"
for _ in $(seq 1 30); do
  if docker run --rm --network "${NETWORK}" alpine \
      sh -c "nc -z -w2 ${NFS_IP} 2049" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
if ! docker run --rm --network "${NETWORK}" alpine \
    sh -c "nc -z -w2 ${NFS_IP} 2049" >/dev/null 2>&1; then
  echo "NFS server never started serving on 2049" >&2
  docker logs "${NFS_CONTAINER}" >&2 || true
  exit 1
fi
echo "    serving at ${NFS_IP}"

echo "==> csi-driver-nfs"
# This has to be a real CSI driver rather than a bare pv.spec.nfs volume: the
# k3s node image ships no mount.nfs, and the CSI node plugin brings its own.
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
# Pinned to a release rather than the default branch: this applies manifests
# with cluster-wide RBAC, so what it installs must not change between runs.
# renovate: datasource=github-releases depName=kubernetes-csi/csi-driver-nfs
CSI_NFS_VERSION="v4.13.4"
git clone -q --depth 1 --branch "${CSI_NFS_VERSION}" \
  https://github.com/kubernetes-csi/csi-driver-nfs.git "${TMP}/csi"
for f in rbac-csi-nfs csi-nfs-driverinfo csi-nfs-controller csi-nfs-node; do
  "${K[@]}" apply -f "${TMP}/csi/deploy/${f}.yaml" >/dev/null
done
"${K[@]}" -n kube-system rollout status daemonset/csi-nfs-node --timeout=180s
"${K[@]}" -n kube-system rollout status deployment/csi-nfs-controller --timeout=180s

echo "==> StorageClass nfs-csi"
# share is ganesha's pseudo-root, not its on-disk path. nolock is required
# because rpc.statd is not running in the node plugin, and without it the mount
# fails with a message about remote locking that has nothing to do with the
# actual problem.
"${K[@]}" apply -f - >/dev/null <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: nfs-csi
provisioner: nfs.csi.k8s.io
parameters:
  server: ${NFS_IP}
  share: /
reclaimPolicy: Delete
volumeBindingMode: Immediate
mountOptions:
  - nfsvers=4.1
  - nolock
EOF

echo "==> ready. Tests keyed on StorageClass 'nfs-csi' will now run."
