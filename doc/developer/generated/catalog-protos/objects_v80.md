---
source: src/catalog-protos/src/objects_v80.rs
revision: 9ba00bc4c0
---

# mz-catalog-protos::objects_v80

Frozen snapshot of catalog object type definitions at schema version 80, identical to `objects.rs` at the time v80 was declared current.
Serves as the migration source for v80→v81 upgrades.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
