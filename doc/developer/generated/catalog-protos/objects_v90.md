---
source: src/catalog-protos/src/objects_v90.rs
revision: fca741734d
---

# mz-catalog-protos::objects_v90

Frozen snapshot of catalog object type definitions at schema version 90, identical to `objects.rs` at the time v90 was declared current.
This snapshot serves as the migration target for v89->v90 upgrades and as the migration source for v90->v91.

Relative to v89, this snapshot adds an `arrangement_compression: bool` field in three places:

- `ManagedCluster` — controls whether arrangement compression is enabled for the cluster's managed replicas.
- `ReconfigurationTarget` — the in-flight reconfiguration target for a managed cluster inherits the same field, so the target state is complete.
- `ReplicaConfig` — each cluster replica's configuration independently carries the flag.

All three fields are backfilled as `false` by the v89->v90 migration (`catalog::durable::upgrade::v89_to_v90`). Unmanaged clusters have no `ManagedCluster` variant and are not rewritten. All other catalog types are identical to v89.

`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
