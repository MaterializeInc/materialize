---
source: src/avro/src/reader.rs
revision: aeab441868
---

Provides the high-level `Reader` type for reading Avro Object Container Files (OCF).
`Reader` parses the file header (magic bytes, embedded schema, codec, sync marker), optionally resolves the embedded writer schema against a caller-supplied reader schema, and exposes an `Iterator<Item = Result<Value, AvroError>>` over decoded records.
`Block` and `BlockIter` give lower-level access to raw compressed blocks for callers that need finer control over I/O or custom decoding.
`from_avro_datum` decodes a single bare datum (no OCF framing) from a byte slice using a provided schema.
`SchemaResolver` is an internal struct used by `schema::resolve_schemas` to walk writer/reader schema pairs and produce resolved `SchemaPiece` trees.
Block object counts and byte lengths read from the wire are validated through `util::safe_len` before use, rejecting negative values (which wrap to a huge `usize`) and counts that exceed a safe bound; this prevents a crafted Avro file with a large block count and a zero-byte schema from causing the reader to spin decoding billions of empty values.
When union resolution fails for a nested named type, the resolver rolls back all named nodes at indices `>= resolved_idx` (truncating `named` and removing entries from `reader_to_resolved_names` and `indices`), rather than only popping the last entry; this avoids leaving `None` placeholder entries that would panic on a later `Option::unwrap`.
