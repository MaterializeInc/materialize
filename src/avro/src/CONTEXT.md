# avro::src (mz-avro)

Materialize's fork of `avro-rs`. Full read/write support for Apache Avro binary
format with Materialize-specific extensions and a zero-copy visitor decode path.

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

## Key concepts

- **`AvroDecode` visitor trait** — allows callers to bypass `Value` materialization and decode binary data directly into Rust types. The primary hot-path used by `mz-interchange` for Debezium ingestion.
- **Schema resolution baked into `SchemaPiece`** — `resolve_schemas` produces a `Schema` whose `SchemaPiece` nodes carry `Resolve*` variants (`ResolveRecord`, `ResolveUnion`, etc.) that instruct the decoder how to transform writer data to the reader schema on the fly, without a separate resolution pass per datum.
- **`Value` enum MZ extensions** — `Json` (Debezium embedded JSON), `Uuid`, logical-type variants (`Date`, `TimestampMilli`, `TimestampMicro`, `Decimal`). These variants are not in standard `avro-rs`.
- **`safe_len`** — allocation cap guard in `util.rs`; prevents OOM from malformed length-prefixed fields (verified by `test_malformed_length` in `lib.rs`).
- **`lib.rs` pub surface** — `decode`, `reader`, `writer`, `codec`, `util` are crate-private; only `encode`, `error`, `schema`, `types` are pub modules. External users must go through the re-exported items in `lib.rs`.

## Cross-references

- `mz-interchange` — primary consumer; implements `AvroDecode` for Debezium/CDC row decoding.
- `mz-avro` depends only on `mz-ore`; it is a near-leaf crate with no Materialize type dependencies.
- Generated developer docs: `doc/developer/generated/avro/`.
