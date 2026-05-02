# persist-types::src

See [`../CONTEXT.md`](../CONTEXT.md) for crate-level overview.

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

## Key interfaces

- **`Codec`** — primary extension point; `encode`/`decode`/`decode_from` (with
  `Storage` reuse), `encode_schema`/`decode_schema`, `validate`. `type Schema`
  must implement `columnar::Schema<Self>`.
- **`Codec64`** — fixed 8-byte encode/decode for timestamps and diffs.
- **`stats` hierarchy** — `DynStats` is the dyn-dispatch root; `ColumnStats<T>`
  is the generic form; `PartStats` aggregates per-column stats for a Part.
  The Seam between persist internals and the storage pushdown layer.
- **`schema::Migration`** — encodes forward and backward Arrow schema
  migrations; `backward_compatible` checks compatibility without a full
  migration object.

## Cross-references

- `mz-ore` — `SensitiveUrl`, retry, metrics
- `mz-proto` — `RustType`/`ProtoType` for proto roundtrip
- `arrow` / `parquet` — columnar I/O substrate
- Generated docs: `doc/developer/generated/persist-types/`
