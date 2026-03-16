---
source: src/catalog-protos/src/objects.rs
revision: 82d92a7fad
---

# mz-catalog-protos::objects

Defines the current (v80) set of Rust structs and enums that represent all durably persisted catalog objects, generated from protobuf definitions.
This file is the canonical snapshot of the current catalog schema; `objects_v<N>.rs` files are frozen snapshots used as migration sources.
Key types include `ConfigKey`, `ConfigValue`, `SettingKey`, `SettingValue`, `IdAllocKey`, `RoleId`, `DatabaseId`, `SchemaId`, and many more covering every catalog entity.
