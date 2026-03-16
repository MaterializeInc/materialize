---
source: src/catalog/src/durable/objects/state_update.rs
revision: f7b6cc21ec
---

# catalog::durable::objects::state_update

Defines the multi-stage representation of a single catalog update as it flows through the system: `PersistStateUpdate` (raw bytes in persist) → `StateUpdate<StateUpdateKindJson>` (JSON) → `StateUpdate<StateUpdateKind>` (strongly typed) → `memory::objects::StateUpdate`.
`StateUpdateKind` enumerates every possible catalog collection update (cluster, item, role, schema, audit log, etc.).
`StateUpdateKindJson` provides an intermediate JSON form used for protobuf migration steps; `IntoStateUpdateKindJson` and `TryIntoStateUpdateKind` traits mediate the conversions.
