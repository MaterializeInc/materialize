# persist-types (mz-persist-types)

Foundational shared-type crate for the persist subsystem. Intentionally
dependency-free of other Materialize crates (only `mz-ore`, `mz-proto`);
everything that interacts with persist imports this.

## Module surface (LOC ≈ 6,297)

| Module | LOC | Purpose |
|---|---|---|
| `lib.rs` | 297 | `Codec`, `Codec64`, `ShardId`, `PersistLocation`, `StepForward` |
| `arrow.rs` | 920 | Proto representation of Arrow `ArrayData`; `ArrayOrd`/`ArrayIdx` for columnar comparison; `ArrayBound` for budget-trimmed lower bounds |
| `schema.rs` | 845 | `SchemaId`, `Migration`, `backward_compatible` — schema evolution for shard key/value types |
| `stats.rs` | 803 | `ColumnarStats`, `PartStats`, `DynStats`/`ColumnStats` trait hierarchy; entry point for pushdown filter statistics |
| `stats/primitive.rs` | 786 | `PrimitiveStats<T>`, `TruncateBound`, `truncate_bytes`/`truncate_string` |
| `codec_impls.rs` | 666 | `Codec`/`Codec64` impls for stdlib types; columnar helpers |
| `stats/json.rs` | 416 | `JsonStats`, `JsonMapElementStats` |
| `stats/bytes.rs` | 325 | `BytesStats`, `AtomicBytesStats`, `FixedSizeBytesStats` |
| `stats/structured.rs` | 308 | `StructStats` — columnar struct-level statistics |
| `part.rs` | 288 | `Part`, `PartBuilder`, `PartOrd`, `Codec64Mut` — in-memory columnar blob |
| `parquet.rs` | 247 | `encode_arrays`/`decode_arrays`, `EncodingConfig`, `CompressionFormat` |
| `columnar.rs` | 201 | `Schema`, `ColumnEncoder`, `ColumnDecoder`, `FixedSizeCodec` traits |
| `timestamp.rs` | 117 | `try_parse_monotonic_iso8601_timestamp` for lexicographic timestamp pushdown |
| `txn.rs` | 78 | `TxnsEntry`, `TxnsCodec` — txn-wal shard encoding abstraction |

## Package identity

Crate name: `mz-persist-types`. No binary. Consumed by `mz-persist-client`,
`mz-txn-wal`, `mz-storage-operators`, `mz-repr`, and every crate that touches
persist shards.

## Key interfaces (exported)

- `Codec` trait — encode/decode for persist keys/values; carries associated
  `Schema` type and `Storage` for allocation reuse in `decode_from`.
- `Codec64` trait — 8-byte fixed encoding for timestamps and diffs.
- `ShardId` — UUID-backed opaque shard identifier; `Display`/`FromStr` with
  `s`-prefix; durable interchange format.
- `PersistLocation` — blob URI + consensus URI pair; the only address type for
  a persist deployment.
- `StepForward` — minimal timestamp advancement trait (used by txn-wal).
- `columnar::{Schema, ColumnEncoder, ColumnDecoder, FixedSizeCodec}` — columnar
  encoding traits that `Codec::Schema` must implement.
- `stats::{ColumnStats, DynStats, PartStats, ColumnarStats}` — pushdown filter
  statistics hierarchy (primitive, bytes, json, structured sub-modules).
- `schema::{SchemaId, Migration, backward_compatible}` — schema evolution seam.
- `part::{Part, PartBuilder, PartOrd}` — in-memory columnar blob.
- `txn::{TxnsEntry, TxnsCodec}` — txn-wal shard encoding abstraction.

## Downstream consumers

`mz-persist-client` (primary), `mz-txn-wal`, `mz-storage-operators`,
`mz-repr` (via `Codec` impls for `Row` / `SourceData`).

## Dependencies

`mz-ore`, `mz-proto`, `arrow`, `parquet`, `prost`.

## Bubbled findings for src/CONTEXT.md

- **`Codec::encode_schema` placement**: the TODO in `lib.rs` calls out that
  `encode_schema`/`decode_schema` should move to the `Schema` trait — tracked
  but not yet done.
- **`StepForward` duplication**: the trait comment says "TODO: Unify this with
  repr's `TimestampManipulation`" — two parallel timestamp-step abstractions
  exist.
- **`stats` module depth**: pushdown stats (`DynStats`/`ColumnStats` hierarchy,
  four sub-modules, proto generation) is the largest single concern (>2K LOC)
  and a frequent change target; it is the Seam between persist and the storage
  pushdown optimizer.
