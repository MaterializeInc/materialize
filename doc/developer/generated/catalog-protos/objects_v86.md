---
source: src/catalog-protos/src/objects_v86.rs
revision: 3953456a45
---

# mz-catalog-protos::objects_v86

Frozen snapshot of catalog object type definitions at schema version 86, identical to `objects.rs` at the time v86 was declared current.
This snapshot serves as the migration target for v85->v86 upgrades.
Relative to v85, this snapshot adds two durable collections for scoped feature flags: `ClusterSystemConfigurationKey`/`ClusterSystemConfigurationValue` (keyed by `ClusterId` and parameter name, with a string value) and `ReplicaSystemConfigurationKey`/`ReplicaSystemConfigurationValue` (keyed by `ReplicaId` and parameter name, with a string value), along with their `ClusterSystemConfiguration` and `ReplicaSystemConfiguration` wrapper structs and the corresponding `ClusterSystemConfiguration` and `ReplicaSystemConfiguration` variants in `StateUpdateKind`.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
