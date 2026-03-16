---
source: src/orchestratord/src/controller/materialize.rs
revision: 82d92a7fad
---

# mz-orchestratord::controller::materialize

Main reconciliation controller for `Materialize` custom resources.
`Config` captures all operator-level configuration (cloud provider, region, image pull policy, feature flags, TLS specs, resource limits, etc.).
The reconciler coordinates `global` resources (RBAC, network policies, certificates) with per-generation `environmentd` StatefulSet deployments, applies rollout strategies, tracks update status, and optionally creates companion `Balancer` and `Console` CRs.
