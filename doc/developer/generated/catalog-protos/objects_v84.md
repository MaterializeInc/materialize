---
source: src/catalog-protos/src/objects_v84.rs
revision: 9ba00bc4c0
---

# mz-catalog-protos::objects_v84

Frozen snapshot of catalog object type definitions at schema version 84, identical to `objects.rs` at the time v84 was declared current.
This snapshot serves as the migration target for v83->v84 upgrades and as the migration source for v84->v85 upgrades.
The schema is identical to v83; the v83->v84 migration normalizes Role row byte forms without changing the type definitions.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
