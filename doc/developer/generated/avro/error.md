---
source: src/avro/src/error.rs
revision: 53536c9e74
---

Defines the crate-wide error types.
`DecodeError` is a detailed, `Clone`-able enum covering every distinct failure mode during binary decoding, including bad codec names, checksum mismatches, out-of-range indices, UTF-8 failures, and corrupt timestamps.
The top-level `Error` enum wraps `DecodeError`, `ParseSchemaError`, `SchemaResolutionError`, and `std::io::ErrorKind` under a single type; it is `Clone` because `io::Error` is stored only as its `ErrorKind`.
`From` impls allow the `?` operator to propagate all constituent error kinds transparently.
