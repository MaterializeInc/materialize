---
source: src/orchestratord/src/controller/materialize.rs
revision: 927cd62f6b
---

# mz-orchestratord::controller::materialize

Main reconciliation controller for `Materialize` custom resources.
`Config` captures all operator-level configuration (cloud provider, region, image pull policy, feature flags, TLS specs, resource limits, network policies, cluster sizing, console image tags, etc.).
`Context` implements the `k8s_controller::Context` trait: the `apply` method coordinates `global` resources (RBAC, network policies, certificates) with per-generation `environmentd` StatefulSet deployments, applies rollout strategies (immediate promotion or manual), validates license keys and environment ID uniqueness, enforces upgrade windows, tracks update status via Kubernetes conditions, and optionally creates companion `Balancer` and `Console` CRs.

Submodules:

* `generation` — per-generation Kubernetes resource construction and version-gated configuration.
* `global` — cluster-wide resources (RBAC, network policies, certificates) shared across generations.
