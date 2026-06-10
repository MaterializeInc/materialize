---
source: src/orchestratord/src/controller/materialize/global.rs
revision: 82d92a7fad
---

# mz-orchestratord::controller::materialize::global

Manages global (non-generation-specific) Kubernetes resources for a `Materialize` CR.
`Resources` encapsulates the ServiceAccount, Role, RoleBinding, NetworkPolicies, and optional cert-manager `Certificate` for `environmentd`, created once per environment rather than per rolling generation.
