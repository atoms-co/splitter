# Splitter Helm Chart

Deploys [Splitter](https://github.com/atoms-co/splitter) as a Raft cluster on Kubernetes via a
`StatefulSet` + headless `Service`, plus an optional client-facing `Service`.

## Why a StatefulSet

Raft needs **stable network identities**. The StatefulSet gives each pod a fixed name
(`<release>-splitter-0`, `-1`, ...) and the headless service gives each a stable DNS record
(`<pod>.<release>-splitter-headless`). Raft IDs and peer addresses are derived from these.

## Ports

| Port  | Name          | Purpose                                                        |
|-------|---------------|----------------------------------------------------------------|
| 50051 | public-grpc   | Client-facing gRPC                                             |
| 50052 | internal-grpc | Pod-to-pod gRPC: `ClusterService` (Notify / AddNode / forward) |
| 50053 | raft          | hashicorp/raft TCP transport                                  |
| 9090  | metrics       | Prometheus                                                     |
| 8081  | health        | HTTP health check                                             |

> **Important — join peers use the internal gRPC port, not the raft port.**
> Bootstrap discovery (`Notify`) is a gRPC call served on the internal port (50052). The chart
> builds `--raft_join_peers` from `ports.internal`. Pointing it at the raft port (50053) is a
> gRPC-vs-raw-TCP mismatch and the cluster will never elect a leader. This chart sets it correctly.

## Quick start (k3d, local)

Create a cluster with a local registry and forward the public + metrics ports:

```bash
k3d cluster create splitter-dev \
  --servers 1 --agents 2 \
  --api-port 6550 \
  --registry-create splitter-registry:0.0.0.0:5111 \
  --port "50051:50051@loadbalancer" \
  --port "9090:9090@loadbalancer"
```

Build and push the image to the local registry. Using ko:

```bash
export KO_DOCKER_REPO=k3d-splitter-registry:5111
ko build ./cmd/splitter --bare
```

Or using the repo's Bazel target (loads `atoms.co/splitter:latest` into Docker; retag/push to the
registry afterward):

```bash
bazel run //:load
docker tag atoms.co/splitter:latest k3d-splitter-registry:5111/splitter:latest
docker push k3d-splitter-registry:5111/splitter:latest
```

Install the chart:

```bash
helm install splitter ./helm/splitter \
  --set image.repository=k3d-splitter-registry:5111/splitter \
  --set image.tag=latest
```

## Verify

```bash
# Watch all 3 pods come up
kubectl get pods -l app.kubernetes.io/name=splitter -w

# Confirm leader election
kubectl logs splitter-splitter-0 | grep -E "bootstrap|leader"
# Expect: "Reached expected bootstrap count 3" then "Cluster bootstrap successful"
#         and a "New leader observation" with a non-empty LeaderID.

# Raft metrics (state, applied index, last contact)
kubectl port-forward svc/splitter-splitter 9090:9090
curl -s localhost:9090/metrics | grep raft_
```

## Operate

```bash
# splitterctl is baked into the image at /usr/local/bin/splitterctl
kubectl exec -it splitter-splitter-0 -- /usr/local/bin/splitterctl operations coordinator info <tenant>/<service>
kubectl exec -it splitter-splitter-0 -- /usr/local/bin/splitterctl join <tenant>/<service>
```

## Common operations

```bash
# Scale the Raft cluster (use odd numbers; re-run install/upgrade so peers regenerate)
helm upgrade splitter ./helm/splitter --set replicas=5

# Tail a node
kubectl logs -f splitter-splitter-1

# Reset state for a clean re-bootstrap (DESTRUCTIVE — wipes the raft log)
kubectl delete statefulset splitter-splitter
kubectl delete pvc -l app.kubernetes.io/name=splitter
helm upgrade --install splitter ./helm/splitter

# Uninstall (PVCs from a StatefulSet are retained; delete them separately)
helm uninstall splitter
kubectl delete pvc -l app.kubernetes.io/name=splitter
```

## Key values

| Key                    | Default                                  | Description                                  |
|------------------------|------------------------------------------|----------------------------------------------|
| `replicas`             | `3`                                      | Raft node count (odd for quorum)             |
| `image.repository`     | `k3d-splitter-registry:5111/splitter`    | Image repo                                   |
| `image.tag`            | `latest`                                 | Image tag                                    |
| `image.pullPolicy`     | `Always`                                 | Catches local rebuilds                       |
| `binaryPath`           | `/usr/local/bin/splitter`                | In-image binary path (set `/ko-app/splitter` for ko images) |
| `raft.fastBootstrap`   | `true`                                   | Skip bootstrap jitter (test/local)           |
| `fastActivation`       | `true`                                   | Skip recovery period (unsafe in prod)        |
| `persistence.enabled`  | `true`                                   | Mount a PVC at `/data`                       |
| `persistence.size`     | `1Gi`                                    | PVC size                                     |
| `persistence.storageClass` | `local-path`                         | StorageClass (k3d ships `local-path`)        |
| `service.enabled`      | `true`                                   | Public ClusterIP on the public gRPC port     |
| `logLevel`             | `INFO`                                   | `--log-level`                                |

Render the manifests without installing:

```bash
helm template splitter ./helm/splitter
```
