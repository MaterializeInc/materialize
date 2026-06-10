---
source: src/catalog-protos/src/objects_v83.rs
revision: 9ba00bc4c0
---

# mz-catalog-protos::objects_v83

Frozen snapshot of catalog object type definitions at schema version 83, identical to `objects.rs` at the time v83 was declared current.
This snapshot serves as the migration target for v82->v83 upgrades and as the migration source for v83->v84 upgrades.
The schema is identical to v82; the v82->v83 migration repairs Role row byte-form drift without changing the type definitions.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
