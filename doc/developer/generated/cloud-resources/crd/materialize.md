---
source: src/cloud-resources/src/crd/materialize.rs
revision: 8a4a8121df
---

# cloud-resources::crd::materialize

Defines the `Materialize` Kubernetes custom resource in two API versions (`v1alpha1` and `v1`, both group `materialize.cloud`) and all shared spec/status types, including `MaterializeRolloutStrategy`, `RolloutRequestTimeout`, and TLS/cert configuration.
This CRD models a Materialize environment deployment and is reconciled by the orchestratord.

The `v1alpha1` submodule contains the storage version of the CRD. Its `MaterializeSpec` includes a `request_rollout` UUID field that must be changed to trigger a rollout, along with `rollout_request_timeout` (type `RolloutRequestTimeout`, a newtype over a duration string, defaulting to `"24h"`) that caps how long a rollout may remain un-promoted. The `v1alpha1::Materialize` implementation of `ManagedResource` returns `app_name() = Some("environmentd")`, so `managed_resource_meta` attaches the standard `app.kubernetes.io/name: environmentd` label to owned resources.

The `v1` submodule contains a simplified API version that removes the `request_rollout` field. Instead, `v1::Materialize::generate_rollout_hash` computes a SHA-256 hash over a subset of spec fields to determine when a rollout is needed; the `v1::MaterializeStatus` tracks `requested_rollout_hash` and `last_completed_rollout_hash` rather than UUIDs. `From` conversions exist in both directions between `v1` and `v1alpha1`: when converting from `v1` to `v1alpha1`, a deterministic UUIDv5 derived from the rollout hash is injected as `request_rollout` so that unchanged specs produce the same UUID (no spurious rollout) and changed specs produce a new UUID (rollout triggered immediately).

`MaterializeRolloutStrategy` and `RolloutRequestTimeout` are defined at the module level and shared by both API versions. The `ManuallyPromote` strategy variant documents that a rollout can be force-promoted at any time by setting `forcePromote` to the current rollout identifier (in `v1`, the value of `status.requestedRolloutHash`; in `v1alpha1`, the `requestRollout` value in the spec), and that a rollout exceeding the timeout is automatically cancelled by the controller to release the read holds held by the un-promoted generation. When cancelled, a new rollout can be triggered by setting `forceRollout` to a new value (in `v1`).
