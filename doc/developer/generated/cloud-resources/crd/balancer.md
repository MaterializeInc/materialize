---
source: src/cloud-resources/src/crd/balancer.rs
revision: 5912b36bab
---

# cloud-resources::crd::balancer

Defines the `Balancer` Kubernetes custom resource (group `materialize.cloud`, version `v1alpha1`) representing a balancerd deployment, along with its spec, status, and routing configuration types (`StaticRoutingConfig`, `FronteggRoutingConfig`).
This CRD is managed by the orchestratord alongside the `Materialize` CRD.

`BalancerSpec` includes an optional `configmap_name` field that names an externally managed ConfigMap in the same namespace. The ConfigMap must contain a `config.json` key holding dynamic configuration as a JSON object. The operator mounts this ConfigMap into the balancerd pod but does not own its lifecycle; the ConfigMap must exist at pod startup or balancerd skips its sync loop.
