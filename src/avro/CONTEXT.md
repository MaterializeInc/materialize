# mz-avro

Materialize's fork of the `avro-rs` crate. Full Apache Avro binary format
support with Materialize-specific extensions (Debezium JSON, UUID, logical
types) and a zero-copy visitor decode path that bypasses `Value` materialization.

## Structure

| Path | LOC | What it owns |
|---|---|---|
| `src/` | 9,022 | All modules — see [`src/CONTEXT.md`](src/CONTEXT.md) |

## Key interfaces (exported via `lib.rs`)

- **`AvroDecode` + `AvroDeserializer` + `GeneralDeserializer`** — visitor-based zero-copy decode; primary interface for `mz-interchange`.
- **`Schema`** — parsed Avro schema arena; `resolve_schemas()` merges writer and reader schemas.
- **`Value`** — in-memory Avro datum with MZ extensions (`Json`, `Uuid`, logical types).
- **`Reader` / `Writer`** — OCF container file reader/writer.
- **`Codec`** — compression codec enum (Null, Deflate, Snappy).
- **`encode_unchecked`** — low-level datum encoder for advanced use.

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
