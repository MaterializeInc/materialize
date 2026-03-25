---
source: src/cloud-resources/src/crd/materialize.rs
revision: fd1ac54b05
---

# cloud-resources::crd::materialize

Defines the `Materialize` Kubernetes custom resource (group `materialize.cloud`, version `v1alpha1`) and all its spec/status types, including `MaterializeSpec`, `MaterializeStatus`, rollout strategies (`MaterializeRolloutStrategy`), and TLS/cert configuration.
This CRD models a Materialize environment deployment and is reconciled by the orchestratord.
