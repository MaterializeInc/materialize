---
source: src/cloud-resources/src/crd/balancer.rs
revision: 4267863081
---

# cloud-resources::crd::balancer

Defines the `Balancer` Kubernetes custom resource (group `materialize.cloud`, version `v1alpha1`) representing a balancerd deployment, along with its spec, status, and routing configuration types (`StaticRoutingConfig`, `FronteggRoutingConfig`).
This CRD is managed by the orchestratord alongside the `Materialize` CRD.
