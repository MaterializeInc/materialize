# persist-types (mz-persist-types)

Foundational shared-type crate for the persist subsystem. Intentionally
dependency-free of other Materialize crates (only `mz-ore`, `mz-proto`);
everything that interacts with persist imports this.

## Subtree (≈ 6,324 LOC total)

| Path | LOC | What it owns |
|---|---|---|
| `src/` | 6,297 | All modules — see [`src/CONTEXT.md`](src/CONTEXT.md) |

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
