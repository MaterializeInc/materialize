---
source: src/catalog-protos/src/objects.rs
revision: aa7a1afd31
---

# mz-catalog-protos::objects

Defines the current (v81) set of Rust structs and enums that represent all durably persisted catalog objects, generated from protobuf definitions.
This file is the canonical snapshot of the current catalog schema; `objects_v<N>.rs` files are frozen snapshots used as migration sources.
Key types include `ConfigKey`, `ConfigValue`, `SettingKey`, `SettingValue`, `IdAllocKey`, `RoleId`, `DatabaseId`, `SchemaId`, `AutoProvisionSource`, `RoleAttributes`, and many more covering every catalog entity.
The `audit_log_event_v1` submodule includes `CreateRoleV1` for audit-logging role creation with auto-provision metadata.
