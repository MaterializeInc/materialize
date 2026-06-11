# persist::indexed

Columnar in-memory and on-disk representation of persist batch data.

## Files (LOC ≈ 2,119 across this directory)

| File | What it owns |
|---|---|
| `columnar.rs` | `ColumnarRecords` — Arrow `BinaryArray`-backed ((K, V), Time, Diff) columnar record set; `ColumnarRecordsBuilder`; `KEY_VAL_DATA_MAX_LEN` (i32::MAX bound imposed by Parquet page size) |
| `encoding.rs` | `BlobTraceBatchPart` — on-disk batch format; Arrow/Parquet serialization of `ColumnarRecords`; proto encode/decode for batch metadata; `Description`-aware compaction metadata |
| `columnar/arrow.rs` | Arrow array re-allocation helpers; `ENABLE_ARROW_LGALLOC_CC_SIZES` / `ENABLE_ARROW_LGALLOC_NONCC_SIZES` dyn configs; lgalloc-backed buffer management |
| `columnar/parquet.rs` | Parquet encode/decode wrappers over the `arrow` crate; per-column statistics extraction |

## Key concepts

- **`ColumnarRecords`** — the central batch type; stores K, V, T, Diff in four parallel Arrow arrays. All higher-level batch math (`merge_batches`, compaction) operates on this type.
- **`BlobTraceBatchPart`** — the serialized blob format. Uses protobuf headers + Parquet body. `encoding.rs` owns the codec seam between Arrow in-memory and Parquet on-disk.
- **`KEY_VAL_DATA_MAX_LEN`** — `i32::MAX` bound on key/val data per batch. Exists because Parquet pages use i32 size fields; the comment documents a known workaround (page vs. column-chunk granularity).
- **lgalloc integration** — Arrow buffers are optionally backed by lgalloc for large-allocation management; controlled by dyn configs in `arrow.rs`.

## Cross-references

- `mz-persist-types::columnar` — `Schema`, `ColumnEncoder`, `ColumnDecoder` codec traits consumed here.
- `mz-persist-types::parquet::EncodingConfig` — encoding configuration passed in from `mz-persist-client`.
- Parent `indexed.rs` re-exports this module; `cfg.rs` registers the lgalloc dyn configs.
- Generated developer docs: `doc/developer/generated/persist/indexed/`.
