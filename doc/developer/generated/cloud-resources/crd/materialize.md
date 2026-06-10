---
source: src/cloud-resources/src/crd/materialize.rs
revision: 152e10735f
---

# cloud-resources::crd::materialize

Defines the `Materialize` Kubernetes custom resource (group `materialize.cloud`, version `v1alpha1`) and all its spec/status types, including `MaterializeSpec`, `MaterializeStatus`, rollout strategies (`MaterializeRolloutStrategy`), and TLS/cert configuration.
This CRD models a Materialize environment deployment and is reconciled by the orchestratord.
`MaterializeSpec` includes a `rollout_request_timeout` field (type `RolloutRequestTimeout`, a newtype over a duration string, defaulting to `"24h"`) that caps how long a rollout may remain un-promoted. The `ManuallyPromote` strategy variant documents that a rollout exceeding this timeout is automatically cancelled by the controller to release the read holds held by the un-promoted generation.
