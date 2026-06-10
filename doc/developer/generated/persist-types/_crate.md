---
source: src/persist-types/src/lib.rs
revision: 901d0526a1
---

# persist-types

Defines the foundational types and traits shared across all persist crates, intentionally kept free of other Materialize package dependencies.

The crate exposes the `Codec` trait (encode/decode for persist keys and values), `Codec64` (8-byte encoding for timestamps and diffs), `ShardId` (opaque UUID-based shard identifier), and `PersistLocation` (blob + consensus URIs).

## Module structure

* `arrow` — protobuf representation of Arrow `ArrayData`, `ArrayOrd`/`ArrayIdx` for columnar comparison, and `ArrayBound` for budget-trimmed lower bounds.
* `codec_impls` — `Codec`/`Codec64` implementations for standard library types and columnar helpers.
* `columnar` — `Schema`, `ColumnEncoder`, `ColumnDecoder`, and `FixedSizeCodec` traits for columnar encoding.
* `parquet` — `encode_arrays`/`decode_arrays` and `EncodingConfig`/`CompressionFormat` for Parquet I/O.
* `part` — `Part`, `PartBuilder`, `PartOrd`, and `Codec64Mut` for in-memory columnar blobs.
* `schema` — `SchemaId` and `Migration`/`backward_compatible` for schema evolution.
* `stats` — `ColumnarStats`, `PartStats`, and the `DynStats`/`ColumnStats` trait hierarchy for pushdown filtering, plus the `primitive`, `bytes`, `json`, and `structured` sub-modules.
* `timestamp` — `try_parse_monotonic_iso8601_timestamp` for lexicographic timestamp pushdown.
* `txn` — `TxnsEntry` and `TxnsCodec` for the txn-wal shard encoding abstraction.

## Key dependencies

`mz-ore`, `mz-proto`, `arrow`, `parquet`, `prost`.
Consumed by `mz-persist-client`, `mz-txn-wal`, `mz-storage-operators`, and virtually every crate that interacts with persist.
