---
source: src/catalog-protos/src/objects_v82.rs
revision: 9ba00bc4c0
---

# mz-catalog-protos::objects_v82

Frozen snapshot of catalog object type definitions at schema version 82, identical to `objects.rs` at the time v82 was declared current.
This snapshot serves as the migration target for v81->v82 upgrades and as the migration source for v82->v83 upgrades.
Relative to v81, this snapshot removes the `ContinualTask` variant from `CommentObject`, `CatalogItemType`, and `ObjectType`.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
