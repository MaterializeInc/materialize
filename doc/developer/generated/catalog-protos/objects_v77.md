---
source: src/catalog-protos/src/objects_v77.rs
revision: 9ba00bc4c0
---

# mz-catalog-protos::objects_v77

Frozen snapshot of catalog object type definitions at schema version 77.
Used as the migration source when upgrading catalogs from v77.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.
