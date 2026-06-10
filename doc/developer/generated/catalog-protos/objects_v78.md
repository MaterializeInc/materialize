---
source: src/catalog-protos/src/objects_v78.rs
revision: 9ba00bc4c0
---

# mz-catalog-protos::objects_v78

Frozen snapshot of catalog object type definitions at schema version 78.
Used as the migration source when upgrading catalogs from v78.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
