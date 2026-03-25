---
source: src/catalog/src/durable/traits.rs
revision: 929ea2b3c5
---

# catalog::durable::traits

Defines `UpgradeFrom<T>` and `UpgradeInto<U>`, local copies of `From`/`Into` used to work around Rust's orphan rules when implementing conversions between protobuf types from `mz_catalog_protos` and the catalog's own types.
