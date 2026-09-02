---
source: src/catalog-protos/src/objects_v88.rs
revision: 3dc710f9b1
---

# mz-catalog-protos::objects_v88

Frozen snapshot of catalog object type definitions at schema version 88, identical to `objects.rs` at the time v88 was declared current.
This snapshot serves as the migration target for v87->v88 upgrades.
Relative to v87, this snapshot extends the audit log with two new event detail variants (`AlterClusterReconfigurationV1` and `ClusterHydrationBurstV1`) and three new `CreateOrDropClusterReplicaReasonV1Reason` enum variants (`Reconfiguration`, `HydrationBurst`, `Retired`). All other types are byte-identical to v87; no existing record shape changes.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
