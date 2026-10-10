---
source: src/catalog/src/durable/objects/serialization.rs
revision: 70b83779a4
---

# catalog::durable::objects::serialization

Implements `RustType` conversions between all durable catalog Rust types and their protobuf representations from `mz_catalog_protos`.
The module splits its implementation across three private submodules (`audit_log`, `foreign`, `rust_type`) and re-exports `ProtoMapEntry`, `ProtoType`, and `RustType` from `rust_type` as the module's public API; callers import these traits from here rather than from `mz_proto` directly.
Re-exports the generated protobuf types under `pub mod proto` for use by the rest of the durable module.
Also implements `From<proto::StateUpdateKind> for StateUpdateKindJson` and related conversions used during the persist read/write pipeline.
Covered durable cluster shape types include `ClusterConfig`, `ClusterVariant`, `ClusterVariantManaged`, `ReconfigurationState`, `ReconfigurationStatus`, `ReconfigurationTarget`, `BurstState`, `ReplicaConfig`, and `ReplicaLocation`.
