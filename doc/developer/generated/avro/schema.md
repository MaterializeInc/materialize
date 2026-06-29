---
source: src/avro/src/schema.rs
revision: 548ddc2a3f
---

Parses JSON Avro schemas into an Arena-based `Schema` struct and performs schema resolution.
`SchemaPiece` is the main schema variant enum, covering all primitive, logical, and complex types as well as a suite of `Resolve*` variants that encode how writer and reader schemas differ (e.g., `ResolveIntLong`, `ResolveUnionUnion`, `ResolveRecord`); these resolution nodes guide the decoder in translating writer-encoded data into reader-expected types.
Named types (records, enums, fixed) are interned in a flat `Vec<NamedSchemaPiece>` and referenced by index via `SchemaPieceOrNamed`, avoiding recursive ownership.
`SchemaNode` pairs an index-table root with a borrowed `SchemaPiece` reference, providing the traversal handle used throughout the encode/decode paths.
`resolve_schemas` builds a fully resolved `Schema` from a writer/reader pair, and `SchemaFingerprint` supports SHA-256 fingerprinting for schema identity checks.
`SchemaParser` tracks a `depth` counter incremented on each recursive `parse_inner` call and enforced against `MAX_SCHEMA_DEPTH` (128); schemas nested deeper than this limit return a `ParseSchemaError` rather than overflowing the stack.
`UnionSchema` exposes `match_promote_writer`, `match_ref_promote_writer`, and `match_ref_promote_reader` methods that extend exact-match union variant lookup with Avro numeric promotion: `int` → `long`/`float`/`double`, `long` → `float`/`double`, and `float` → `double`, as defined by the free function `can_promote`; exact matches are always preferred over promotion matches.
The module's tests include `test_union_promotion_agrees_with_resolution`, which pins the agreement between `can_promote` and the schema resolver over every primitive pair, ensuring that any promotion union matching accepts is also decodable by resolution.
