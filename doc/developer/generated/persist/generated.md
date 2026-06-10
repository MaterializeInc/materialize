---
source: src/persist/src/generated.rs
revision: 6df3ec9286
---

# persist::generated

Thin wrapper that `include!`s the protobuf-generated Rust code from `OUT_DIR`.
Exposes the `persist` sub-module containing all generated proto types (e.g. `ProtoBatchFormat`, `ProtoBatchPartInline`, `ProtoColumnarRecords`).
Contains no hand-written logic; exists only to surface generated code through the crate's module tree.
