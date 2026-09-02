---
source: src/catalog-protos/src/objects_v87.rs
revision: 80f8711523
---

# mz-catalog-protos::objects_v87

Frozen snapshot of catalog object type definitions at schema version 87, identical to `objects.rs` at the time v87 was declared current.
This snapshot serves as the migration target for v86->v87 upgrades.
Relative to v86, this snapshot extends `ManagedCluster` with three optional fields (`auto_scaling_strategy`, `reconfiguration`, `burst`) and adds the supporting types `AutoScalingStrategy`, `OnHydration`, `ReconfigurationState`, `ReconfigurationTarget`, `OnTimeoutAction`, and `BurstState`. It also replaces the managed `ReplicaLocation`'s single optional `availability_zone` field with an `availability_zones: Vec<String>` list.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
