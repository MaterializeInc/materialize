---
source: src/avro/src/schema.rs
revision: b0c4a9ca44
---

Parses JSON Avro schemas into an Arena-based `Schema` struct and performs schema resolution.
`SchemaPiece` is the main schema variant enum, covering all primitive, logical, and complex types as well as a suite of `Resolve*` variants that encode how writer and reader schemas differ (e.g., `ResolveIntLong`, `ResolveUnionUnion`, `ResolveRecord`); these resolution nodes guide the decoder in translating writer-encoded data into reader-expected types.
Named types (records, enums, fixed) are interned in a flat `Vec<NamedSchemaPiece>` and referenced by index via `SchemaPieceOrNamed`, avoiding recursive ownership.
`SchemaNode` pairs an index-table root with a borrowed `SchemaPiece` reference, providing the traversal handle used throughout the encode/decode paths.
`resolve_schemas` builds a fully resolved `Schema` from a writer/reader pair, and `SchemaFingerprint` supports SHA-256 fingerprinting for schema identity checks.
