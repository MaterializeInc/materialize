---
source: src/catalog-protos/src/objects_v81.rs
revision: 9ba00bc4c0
---

# mz-catalog-protos::objects_v81

Frozen snapshot of catalog object type definitions at schema version 81, identical to `objects.rs` at the time v81 was declared current.
This snapshot serves as the migration target for v80->v81 upgrades and as the migration source for v81->v82 upgrades.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
