---
source: src/catalog-protos/src/objects_v74.rs
revision: 9ba00bc4c0
---

# mz-catalog-protos::objects_v74

Frozen snapshot of catalog object type definitions at schema version 74.
Used as the migration source when upgrading catalogs from v74 to later versions.
Content is identical in structure to `objects.rs` as it existed when v74 was current.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
