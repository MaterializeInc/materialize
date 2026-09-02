---
source: src/catalog/src/durable/objects.rs
revision: 39dcae2fba
---

# catalog::durable::objects

Defines the on-disk representation of every catalog entity as split key-value pairs (e.g., `DatabaseKey`/`DatabaseValue`) and their combined Rust structs (e.g., `Database`).
The `DurableType` trait converts between the combined struct and its key-value pair, enabling encoding and decoding for persist.
Public re-exports (via `crate::durable`) expose the combined structs (`Cluster`, `ClusterConfig`, `ClusterVariant`, `ClusterVariantManaged`, `ClusterSystemConfiguration`, `Item`, `Role`, `ReplicaSystemConfiguration`, `Schema`, `BurstState`, `ReconfigurationState`, `ReconfigurationStatus`, `ReconfigurationTarget`, etc.) to other modules; the key-value types remain internal. The `Item` struct carries an `ephemeral_owner_session: Option<Uuid>` field; `Some(uuid)` identifies a temporary item visible only to the owning session, while `None` denotes a normal durable item.
The `serialization` and `state_update` submodules handle protobuf conversion and the update-lifecycle pipeline respectively.
