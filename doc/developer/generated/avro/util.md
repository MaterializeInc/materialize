---
source: src/avro/src/util.rs
revision: de06eb12d9
---

Contains low-level encoding and decoding primitives shared across the crate.
`zig_i32`/`zig_i64` encode signed integers using zigzag encoding into a buffer; `zag_i32`/`zag_i64` decode them back.
`decode_variable` reads a variable-length unsigned integer from a reader, guarding against overflow beyond 64 bits.
`safe_len` enforces a 512 MiB cap on allocation requests to protect against malformed length fields; `MAX_ALLOCATION_BYTES` exports that constant.
`TsUnit` and `MapHelper` are minor helpers for timestamp unit representation and JSON map access during schema parsing.
