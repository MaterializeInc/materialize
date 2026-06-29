---
source: src/catalog/src/durable/objects.rs
revision: 80f8711523
---

# catalog::durable::objects

Defines the on-disk representation of every catalog entity as split key-value pairs (e.g., `DatabaseKey`/`DatabaseValue`) and their combined Rust structs (e.g., `Database`).
The `DurableType` trait converts between the combined struct and its key-value pair, enabling encoding and decoding for persist.
Public re-exports (via `crate::durable`) expose the combined structs (`Cluster`, `ClusterSystemConfiguration`, `Item`, `Role`, `ReplicaSystemConfiguration`, `Schema`, `BurstState`, `ReconfigurationState`, `ReconfigurationTarget`, etc.) to other modules; the key-value types remain internal.
The `serialization` and `state_update` submodules handle protobuf conversion and the update-lifecycle pipeline respectively.
