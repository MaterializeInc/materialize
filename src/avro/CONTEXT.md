# mz-avro

Materialize's fork of the `avro-rs` crate. Full Apache Avro binary format
support with Materialize-specific extensions (Debezium JSON, UUID, logical
types) and a zero-copy visitor decode path that bypasses `Value` materialization.

## Files (LOC ≈ 9,022)

| File | What it owns |
|---|---|
| `schema.rs` | `Schema` arena; `SchemaPiece` / `SchemaPieceOrNamed`; `resolve_schemas()` — builds writer-to-reader resolution schema baking field reorder/widen/default-fill instructions into the schema tree itself; `ParseSchemaError` |
| `decode.rs` | `AvroDecode` visitor trait; `AvroDeserializer`, `GeneralDeserializer`; per-type decoder methods; `TrivialDecoder`, `ValueDecoder`; zero-copy path that decodes directly into caller types without materializing `Value` |
| `reader.rs` | `Reader` — OCF container file parser and iterator; `BlockIter`; `from_avro_datum`; `SchemaResolver` used by `resolve_schemas` |
| `types.rs` | `Value` enum (in-memory Avro datum); `Scalar`; `DecimalValue`; `SchemaResolutionError`; MZ additions: `Value::Json`, `Value::Uuid`, `Value::Date`, `Value::Timestamp{Milli,Micro}`, `Value::Decimal` |
| `writer.rs` | `Writer` — OCF container file producer; `to_avro_datum` / `write_avro_datum`; `ValidationError` |
| `encode.rs` | Low-level datum encoder (zigzag integers, blocks); `encode_unchecked` pub re-export |
| `codec.rs` | `Codec` enum (Null, Deflate, Snappy); compress/decompress impls |
| `error.rs` | `Error` enum unifying `DecodeError`, `SchemaError`, `ValidationError`, and IO errors |
| `util.rs` | Zigzag encode/decode (`zag_i32`, `zag_i64`); `safe_len` allocation guard; `TsUnit`, `MapHelper` |

## Key interfaces (exported via `lib.rs`)

- **`AvroDecode` + `AvroDeserializer` + `GeneralDeserializer`** — visitor-based zero-copy decode; primary interface for `mz-interchange`.
- **`Schema`** — parsed Avro schema arena; `resolve_schemas()` merges writer and reader schemas.
- **`Value`** — in-memory Avro datum with MZ extensions (`Json`, `Uuid`, logical types).
- **`Reader` / `Writer`** — OCF container file reader/writer.
- **`Codec`** — compression codec enum (Null, Deflate, Snappy).
- **`encode_unchecked`** — low-level datum encoder for advanced use.

## Key concepts

- **`AvroDecode` visitor trait** — allows callers to bypass `Value` materialization and decode binary data directly into Rust types. The primary hot-path used by `mz-interchange` for Debezium ingestion.
- **Schema resolution baked into `SchemaPiece`** — `resolve_schemas` produces a `Schema` whose `SchemaPiece` nodes carry `Resolve*` variants (`ResolveRecord`, `ResolveUnion`, etc.) that instruct the decoder how to transform writer data to the reader schema on the fly, without a separate resolution pass per datum.
- **`Value` enum MZ extensions** — `Json` (Debezium embedded JSON), `Uuid`, logical-type variants (`Date`, `TimestampMilli`, `TimestampMicro`, `Decimal`). These variants are not in standard `avro-rs`.
- **`safe_len`** — allocation cap guard in `util.rs`; prevents OOM from malformed length-prefixed fields (verified by `test_malformed_length` in `lib.rs`).
- **`lib.rs` pub surface** — `decode`, `reader`, `writer`, `codec`, `util` are crate-private; only `encode`, `error`, `schema`, `types` are pub modules. External users must go through the re-exported items in `lib.rs`.

## Architecture notes

- Three-layer structure: `(util, error)` → `(types, schema)` → `(encode, decode, codec, reader, writer)`.
- Schema resolution is baked into the `SchemaPiece` tree at parse time, not per-datum at decode time — this is the key performance optimization over vanilla `avro-rs`.
- `decode`, `reader`, `writer`, `codec`, `util` are private modules; callers depend only on re-exported items in `lib.rs`.
- Near-leaf crate: depends only on `mz-ore`; no `mz-repr` or `mz-expr` dependency.

## What to bubble up to src/CONTEXT.md

- `mz-avro` is the Avro codec seam; `mz-interchange` implements `AvroDecode` here for Debezium ingestion. No other Materialize crate implements Avro parsing.
- Schema resolution baked into `SchemaPiece` is a performance-critical design; any Avro schema evolution work must account for this representation.

## Cross-references

- Generated developer docs: `doc/developer/generated/avro/`.
- `mz-interchange` — sole production consumer; implements `AvroDecode` for CDC row decoding.
