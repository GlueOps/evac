#!/usr/bin/env bash
# Sets up a real CSI driver backed by a real NFS server, so the volume-source
# guard can be tested against a genuine pv.spec.csi volume rather than a
# hand-built fixture.
#
# This exists because the guard's whole job is refusing volumes that are not
# node-local, and a unit test can only assert that against a struct someone
# wrote by hand. Installing an actual driver proves the field paths are right
# against objects Kubernetes produced itself.
#
# Usage: test/integration/setup-nfs.sh <kubecontext> [docker-network]
set -euo pipefail

CONTEXT="${1:?usage: setup-nfs.sh <kubecontext> [docker-network]}"
NETWORK="${2:-k3d-${CONTEXT#k3d-}}"
NFS_CONTAINER="evac-nfs-${CONTEXT#k3d-}"
K="kubectl --context ${CONTEXT}"

echo "==> NFS server container on network ${NETWORK}"
docker rm -f "${NFS_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${NFS_CONTAINER}" --network "${NETWORK}" --privileged \
  -e SHARED_DIRECTORY=/data \
  itsthenetwork/nfs-server-alpine:latest >/dev/null

# Wait for the exports to come up before anything tries to mount them.
for _ in $(seq 1 30); do
  if docker logs "${NFS_CONTAINER}" 2>&1 | grep -q "Startup successful"; then break; fi
  sleep 1
done

NFS_IP="$(docker inspect "${NFS_CONTAINER}" \
  -f '{{range $k, $v := .NetworkSettings.Networks}}{{$v.IPAddress}}{{end}}')"
if [ -z "${NFS_IP}" ]; then
  echo "could not determine the NFS server IP" >&2
  exit 1
fi
echo "    server at ${NFS_IP}"

echo "==> csi-driver-nfs"
# The k3s node image ships no mount.nfs; the CSI node plugin brings its own,
# which is precisely why this has to be a real driver rather than a bare NFS PV.
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
git clone -q --depth 1 https://github.com/kubernetes-csi/csi-driver-nfs.git "${TMP}/csi"
for f in rbac-csi-nfs csi-nfs-driverinfo csi-nfs-controller csi-nfs-node; do
  ${K} apply -f "${TMP}/csi/deploy/${f}.yaml" >/dev/null
done
${K} -n kube-system rollout status daemonset/csi-nfs-node --timeout=180s
${K} -n kube-system rollout status deployment/csi-nfs-controller --timeout=180s

echo "==> StorageClass nfs-csi"
${K} apply -f - >/dev/null <<EOF
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
EOF

echo "==> ready. Tests keyed on StorageClass 'nfs-csi' will now run."
