---
source: src/avro/src/codec.rs
revision: e757b4d11b
---

Defines the `Codec` enum representing the compression algorithms supported for Avro Object Container Files: `Null` (no compression), `Deflate` (RFC 1951), and optionally `Snappy` (behind the `snappy` feature flag).
Each variant implements `compress` and `decompress` methods that operate in-place on a byte buffer.
Snappy compression appends a big-endian CRC32 checksum and verifies it on decompression.
`Codec` also implements `ToAvro` (for embedding the codec name in file headers) and `FromStr` (for parsing it back).
