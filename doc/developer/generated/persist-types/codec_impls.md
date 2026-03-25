---
source: src/persist-types/src/codec_impls.rs
revision: 901d0526a1
---

# persist-types::codec_impls

Provides `Codec` and `Codec64` implementations for standard types: `()`, `String`, `Vec<u8>`, `bytes::Bytes`, `ShardId`, `i64`, and `u64`.
Also defines `UnitSchema`, `StringSchema`, `VecU8Schema`, and `ShardIdSchema` as their associated `Schema` implementations, plus the `SimpleColumnarData` trait and `SimpleColumnarEncoder`/`SimpleColumnarDecoder` generics that reduce boilerplate for simple column types.
`TodoSchema`/`TodoColumnarEncoder`/`TodoColumnarDecoder` serve as unimplemented placeholders during incremental schema migration.
