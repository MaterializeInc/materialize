---
source: src/catalog-protos/src/audit_log.rs
revision: 7f722d60df
---

# mz-catalog-protos::audit_log

Implements `RustType` conversions between `mz_audit_log` Rust types and their protobuf representations in `crate::objects`.
This module exists as a separate crate from `mz_catalog` to work around Rust's orphan rules, which prevent implementing foreign traits on foreign types.
It covers all versioned `EventDetails` variants (including `CreateRoleV1`, `AlterAddColumnV1`, and `AlterSourceTimestampIntervalV1`), `VersionedEvent`, and related enum types.
