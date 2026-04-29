---
source: src/catalog-protos/src/objects_v82.rs
revision: 9d0a7c3c6f
---

# mz-catalog-protos::objects_v82

Frozen snapshot of catalog object type definitions at schema version 82, identical to `objects.rs` at the time v82 was declared current.
This snapshot serves as the migration target for v81->v82 upgrades and as the migration source for future v82->v83 upgrades.
Relative to v81, this snapshot removes the `ContinualTask` variant from `CommentObject`, `CatalogItemType`, and `ObjectType`.
