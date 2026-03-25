---
source: src/avro/src/reader.rs
revision: e757b4d11b
---

Provides the high-level `Reader` type for reading Avro Object Container Files (OCF).
`Reader` parses the file header (magic bytes, embedded schema, codec, sync marker), optionally resolves the embedded writer schema against a caller-supplied reader schema, and exposes an `Iterator<Item = Result<Value, AvroError>>` over decoded records.
`Block` and `BlockIter` give lower-level access to raw compressed blocks for callers that need finer control over I/O or custom decoding.
`from_avro_datum` decodes a single bare datum (no OCF framing) from a byte slice using a provided schema.
`SchemaResolver` is an internal struct used by `schema::resolve_schemas` to walk writer/reader schema pairs and produce resolved `SchemaPiece` trees.
