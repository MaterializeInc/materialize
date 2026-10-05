---
source: src/timely-util/src/columnar/chunk/metrics.rs
revision: 24a45d84c0
---

# timely-util::columnar::chunk::metrics

Process-wide work counters for columnar chunk maintenance, organized by pipeline stage.

## Stages

Four stages are tracked: `merge`, `advance`, `commit`, and `batch`. Each stage has its own set of atomic counters, and counters at different stages must not be added together.

- **`merge`** — records processed during sorted merge operations.
- **`advance`** — records processed during time-advancement passes.
- **`commit`** — records processed at the commit (settle) point; this is the only stage where `bytes` is recorded, as uncompressed serialized sizes.
- **`batch`** — upsert feedback arrangement batches published (including empty batches); recorded by storage but registered with compute's pool metrics because clusterd runs storage and compute in one process with one registry.

## Metrics registered

Each stage exposes the following `ComputedUIntGauge` metrics, labeled by `stage`:

| Name | Description |
|---|---|
| `mz_column_chunk_work_calls_total` | Columnar work operations, by stage |
| `mz_column_chunk_work_rows_total` | Rows processed, including repeated visits |
| `mz_column_chunk_work_bytes_total` | Uncompressed serialized bytes processed (commit stage only records non-zero values) |
| `mz_column_chunk_work_size_total` | Per row-size bucket counts, labeled by `log2_rows` |

Row-size buckets are disjoint and indexed by `ceil(log2(rows))`, with zero and one sharing bucket zero and the last bucket (index 24) absorbing everything larger. Chunks are bounded to 2 MiB, so only published batches can reach the last bucket.

## `record_batch(rows: usize)`

Public function for recording one batch an upsert feedback arrangement published.
