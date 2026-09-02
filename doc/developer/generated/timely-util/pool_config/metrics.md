---
source: src/timely-util/src/pool_config/metrics.rs
revision: 93dcb0ef5a
---

# timely-util::pool_config::metrics

Prometheus metrics for the process-wide buffer pool.

## `register(registry: &MetricsRegistry)`

Installs all buffer-pool gauges into `registry`. Idempotent via a `OnceLock`: repeated calls after the first are no-ops.

All metrics are `ComputedUIntGauge` instances that peek at the pool's `PoolStats` at scrape time via `global_pool_peek()`. They report `0` until something initializes the pool. This design ensures that monitoring a process that never uses the pool does not trigger the pool's virtual address reservation as a side effect.

## Metrics registered

| Name | Type | Description |
|---|---|---|
| `mz_column_pool_resident_bytes` | gauge | Uncompressed bytes resident in the pool |
| `mz_column_pool_oversize_bytes` | gauge | Bytes held by oversize chunks that bypass pool paging |
| `mz_column_pool_inserts_total` | monotone gauge | Chunks inserted into the pool |
| `mz_column_pool_frees_total` | monotone gauge | Chunks freed from the pool |
| `mz_column_pool_writes_elided_total` | monotone gauge | Backing writes elided (chunks freed while queued for a spill thread) |
| `mz_column_pool_evictions_compress_total` | monotone gauge | Evictions that compressed a chunk into a new swap-backed extent |
| `mz_column_pool_evictions_cheap_total` | monotone gauge | Evictions of already-backed chunks (physical pages released with no I/O) |
| `mz_column_pool_extent_bytes_written_total` | monotone gauge | Compressed bytes written into swap-backed extents |
| `mz_column_pool_spill_scheduled_total` | monotone gauge | Evictions handed to spill threads |
| `mz_column_pool_spill_cancelled_total` | monotone gauge | Compressions cancelled by a concurrent free |
| `mz_column_pool_spill_in_flight` | gauge | Spill entries currently queued or being processed |
| `mz_column_pool_admissions_budget_total` | monotone gauge | Evicted chunks re-admitted via free budget headroom |
| `mz_column_pool_admissions_steal_total` | monotone gauge | Evicted chunks re-admitted by stealing a clean backed victim slot |
| `mz_column_pool_admissions_denied_total` | monotone gauge | Admitting reads served as plain decompresses (no budget or victim) |
| `mz_column_pool_extent_pageout_incomplete_total` | monotone gauge | Pageout passes finding pages still resident |
| `mz_column_pool_extent_arena_fallbacks_total` | monotone gauge | Extent writes falling back to heap (no free extent-arena slot) |
| `mz_column_pool_slot_exhausted_fallbacks_total` | monotone gauge | Inserts falling back to heap (no free size-class slot) |
| `mz_column_pool_oversize_payloads_total` | monotone gauge | Inserts going to heap (payload exceeds largest size class) |
| `mz_column_pool_live_chunks` | gauge | Live pool chunks across all residency states |
| `mz_column_pool_warm_bytes` | gauge | Free slot bytes kept warm for fault-free reuse |
| `mz_column_pool_warm_reuses_total` | monotone gauge | Slot allocations served from the warm list |
| `mz_column_pool_eager_backs_total` | monotone gauge | Chunks eagerly compressed to compressed-but-resident by idle spill threads |
| `mz_column_pool_extent_resident_bytes` | gauge | Allocation bytes of compressed extents currently resident |
| `mz_column_pool_extent_unreclaimable_bytes` | gauge | Resident extent bytes the RSS target cannot push out |
| `mz_column_pool_extent_pageouts_total` | monotone gauge | Extents pushed to the swap device by RSS-target enforcement |

The `_total` name suffix marks monotone fields; the underlying metric type for all is `ComputedUIntGauge` because the pool owns the atomics.
