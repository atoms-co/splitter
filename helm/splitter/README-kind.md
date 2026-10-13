# Running Splitter on a kind cluster

Step-by-step `kubectl` + `helm` commands for deploying the Splitter Raft cluster on
[kind](https://kind.sigs.k8s.io/). No registry required — images are loaded straight into the
kind nodes.

## kind-specific gotchas

- **Image loading, not a registry.** kind can't pull from your host Docker daemon. Load the image
  into the nodes with `kind load docker-image`, and set `image.pullPolicy=IfNotPresent` so kubelet
  uses the loaded image instead of trying to pull `latest` from a registry (which would fail).
- **StorageClass is `standard`.** kind ships the rancher local-path provisioner as the default
  StorageClass named `standard` (not `local-path`). Override `persistence.storageClass=standard`.
- **No LoadBalancer.** Reach services with `kubectl port-forward`.
- **Image entrypoint.** The bazel `oci_image` sets no ENTRYPOINT and puts the binary at
  `/usr/local/bin/splitter`, so the chart runs it via an explicit `command` (`binaryPath`, default
  `/usr/local/bin/splitter`). If you build with `ko` instead, set `--set binaryPath=/ko-app/splitter`.

## 1. Create the cluster

```bash
kind create cluster --name splitter-dev
kubectl cluster-info --context kind-splitter-dev
```

## 2. Build and load the image

Build the image with the repo's Bazel target (loads `atoms.co/splitter:latest` into Docker):

```bash
bazel run //:load
```

Tag it to a simple local name and load it into the kind nodes:

```bash
docker tag atoms.co/splitter:latest splitter:latest
kind load docker-image splitter:latest --name splitter-dev
```

Confirm it landed on the node:

```bash
docker exec splitter-dev-control-plane crictl images | grep splitter
```

## 3. Install the chart

```bash
helm install splitter ./helm/splitter \
  --set image.repository=splitter \
  --set image.tag=latest \
  --set image.pullPolicy=IfNotPresent \
  --set persistence.storageClass=standard \
  --set replicas=3
```

## 4. Verify the cluster formed

```bash
# Watch all 3 pods come up
kubectl get pods -l app.kubernetes.io/name=splitter -w

# Confirm leader election
kubectl logs splitter-splitter-0 | grep -E "bootstrap|leader"
# Expect: "Reached expected bootstrap count 3" then "Cluster bootstrap successful",
#         and a "New leader observation" with a non-empty LeaderID.

# Check PVCs got bound (standard StorageClass)
kubectl get pvc -l app.kubernetes.io/name=splitter
```

## 5. Reach it / operate it

```bash
# Client-facing gRPC
kubectl port-forward svc/splitter-splitter 50051:50051

# Prometheus metrics
kubectl port-forward svc/splitter-splitter-headless 9090:9090   # or port-forward a pod
curl -s localhost:9090/metrics | grep raft_

# splitterctl is baked into the image
kubectl exec -it splitter-splitter-0 -- /usr/local/bin/splitterctl operations coordinator info <tenant>/<service>
kubectl exec -it splitter-splitter-0 -- /usr/local/bin/splitterctl join <tenant>/<service>
```

## Iterating on code changes

After rebuilding, reload the image and restart the pods:

```bash
bazel run //:load
docker tag atoms.co/splitter:latest splitter:latest
kind load docker-image splitter:latest --name splitter-dev
kubectl rollout restart statefulset splitter-splitter
```

## Common operations

```bash
# Scale the Raft cluster (odd numbers; upgrade so the peer list regenerates)
helm upgrade splitter ./helm/splitter \
  --set image.repository=splitter --set image.tag=latest \
  --set image.pullPolicy=IfNotPresent --set persistence.storageClass=standard \
  --set replicas=5

# Tail a node
kubectl logs -f splitter-splitter-1

# Render manifests without installing
helm template splitter ./helm/splitter --set persistence.storageClass=standard

# Clean re-bootstrap (DESTRUCTIVE — wipes the raft log)
kubectl delete statefulset splitter-splitter
kubectl delete pvc -l app.kubernetes.io/name=splitter
helm upgrade --install splitter ./helm/splitter \
  --set image.repository=splitter --set image.tag=latest \
  --set image.pullPolicy=IfNotPresent --set persistence.storageClass=standard
```

## Uninstall

```bash
helm uninstall splitter
kubectl delete pvc -l app.kubernetes.io/name=splitter   # StatefulSet PVCs are retained otherwise
kind delete cluster --name splitter-dev
```

## Pin these in values instead of repeating --set

To avoid repeating flags, drop a `values-kind.yaml`:

```yaml
image:
  repository: splitter
  tag: latest
  pullPolicy: IfNotPresent
persistence:
  storageClass: standard
replicas: 3
```

```bash
helm install splitter ./helm/splitter -f values-kind.yaml
```
