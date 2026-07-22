---
source: src/catalog-protos/src/objects_v89.rs
revision: 6c07c975be
---

# mz-catalog-protos::objects_v89

Frozen snapshot of catalog object type definitions at schema version 89, identical to `objects.rs` at the time v89 was declared current.
This snapshot serves as the migration target for v88->v89 upgrades.
Relative to v88, this snapshot adds a `status` field to `ReconfigurationState` of type `ReconfigurationStatus` (an enum with variants `InProgress`, `Finalized`, `TimedOut`, `Cancelled`, `ResourceExhausted`). The serde serialization of `ReconfigurationStatus` variant names is what the `mz_internal.mz_cluster_reconfigurations` builtin view matches on in its `status` CASE mapping; similarly, `OnTimeoutAction` variant names feed the `on_timeout` CASE mapping in that view. Adding a variant to either enum requires extending the corresponding CASE mapping in that view, or the new variant surfaces verbatim instead of snake_case. All other catalog types are identical to v88.
The audit log additions are additive and confined to the append-only log: `AlterClusterReconfigurationV1` gains an optional `forced` field (indicating whether a finalized reconfiguration was forced by the timeout path rather than reached by hydration) and new `ReconfigurationLifecycleV1` transition variants; `ClusterHydrationBurstV1` is a new event detail variant for burst lifecycle events with `HydrationBurstLifecycleV1` and `BurstFinishCauseV1`.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
