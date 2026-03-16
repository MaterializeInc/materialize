---
source: src/catalog/src/durable/objects/serialization.rs
revision: f7b6cc21ec
---

# catalog::durable::objects::serialization

Implements `mz_proto::RustType` conversions between all durable catalog Rust types and their protobuf representations from `mz_catalog_protos`.
Re-exports the generated protobuf types under `pub mod proto` for use by the rest of the durable module.
Also implements `From<proto::StateUpdateKind> for StateUpdateKindJson` and related conversions used during the persist read/write pipeline.
