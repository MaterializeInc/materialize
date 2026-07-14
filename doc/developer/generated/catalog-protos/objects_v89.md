---
source: src/catalog-protos/src/objects_v89.rs
revision: 8598d82c1c
---

# mz-catalog-protos::objects_v89

Frozen snapshot of catalog object type definitions at schema version 89, identical to `objects.rs` at the time v89 was declared current.
This snapshot serves as the migration target for v88->v89 upgrades.
Relative to v88, this snapshot adds a `status` field to `ReconfigurationState` of type `ReconfigurationStatus` (an enum with variants `InProgress`, `Finalized`, `TimedOut`, `Cancelled`, `ResourceExhausted`). All other catalog types are identical to v88.
The audit log additions are additive and confined to the append-only log: `AlterClusterReconfigurationV1` gains an optional `forced` field (indicating whether a finalized reconfiguration was forced by the timeout path rather than reached by hydration) and new `ReconfigurationLifecycleV1` transition variants; `ClusterHydrationBurstV1` is a new event detail variant for burst lifecycle events with `HydrationBurstLifecycleV1` and `BurstFinishCauseV1`.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
