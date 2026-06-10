---
source: src/catalog-protos/src/objects_v85.rs
revision: 9ba00bc4c0
---

# mz-catalog-protos::objects_v85

Frozen snapshot of catalog object type definitions at schema version 85, identical to `objects.rs` at the time v85 was declared current.
This snapshot serves as the migration target for v84->v85 upgrades.
Relative to v84, this snapshot adds two audit log event detail structs inside the `audit_log_event_v1` module: `AlterAddColumnV1` (recording the target relation id, column name, column type, and nullability) and `AlterSourceTimestampIntervalV1` (recording the source id and the old and new timestamp intervals), along with their corresponding `Details` enum variants.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
