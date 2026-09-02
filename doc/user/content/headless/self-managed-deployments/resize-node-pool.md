---
headless: true
---
The VM type of a Kubernetes node pool is immutable on EKS, AKS, and GKE, so
changing it triggers a `destroy + create` that fails while Materialize pods are
still running on the pool. The supported pattern is to add a second pool with
the new VM type, roll out the Materialize instance so new pods land on it, and
then drop the old pool.

For the full procedure, see
[Resize node pools](/self-managed-deployments/deployment-guidelines/resize-node-pools/).
