---
source: src/catalog-protos/src/serialization.rs
revision: aa7a1afd31
---

# mz-catalog-protos::serialization

Implements `RustType` conversions between catalog-relevant Rust types from `mz_repr`, `mz_sql`, `mz_compute_types`, and `mz_storage_types` and their protobuf counterparts in `crate::objects`.
Like `audit_log`, this module lives outside `mz_catalog` to satisfy orphan rules.
Covered types include `RoleId`, `DatabaseId`, `SchemaId`, `GlobalId`, `CatalogItemId`, `AclMode`, `MzAclItem`, `CommentObjectId`, `ClusterSchedule`, `NetworkPolicyRule`, `AutoProvisionSource`, and others.
