---
source: src/timely-util/src/column_pager/metrics.rs
revision: 3506b9aee8
---

# timely-util::column_pager::metrics

Prometheus metrics for the column pager.

A single process-wide `PagerMetrics` singleton is installed by compute init via `register`. The `observe_*` helper functions are no-ops until that call completes, so tests and benchmarks that do not wire a `MetricsRegistry` incur no bookkeeping overhead.

## `PagerMetrics`

Holds `IntCounter`s tracking cumulative event counts and byte volumes since process start:

| Field | Metric name | Description |
|---|---|---|
| `skip_decisions_total` | `mz_column_pager_skip_decisions_total` | Decisions that kept the chunk resident |
| `skip_bytes_total` | `mz_column_pager_skip_bytes_total` | Bytes kept resident by skip decisions |
| `pageouts_total` | `mz_column_pager_pageouts_total` | Decisions that paged the chunk out |
| `paged_bytes_in_total` | `mz_column_pager_paged_bytes_in_total` | Uncompressed bytes handed to the pager |
| `paged_bytes_out_total` | `mz_column_pager_paged_bytes_out_total` | On-storage bytes after codec and padding |
| `pageins_total` | `mz_column_pager_pageins_total` | Successful page-ins from `ColumnPager::take` |
| `pagein_bytes_total` | `mz_column_pager_pagein_bytes_total` | Uncompressed bytes delivered by page-in |
| `resident_released_total` | `mz_column_pager_resident_released_total` | Resident-ticket drops returning budget |
| `resident_released_bytes_total` | `mz_column_pager_resident_released_bytes_total` | Bytes returned to the budget by ticket drops |

Two `ComputedUIntGauge`s are registered but not stored in the struct — their closures capture a `&'static TieredPolicy` reference and read the live budget atomics at scrape time:

- `mz_column_pager_budget_remaining_bytes` — bytes currently available for resident columns.
- `mz_column_pager_budget_configured_bytes` — most-recently-configured total budget.

## `register`

`register(registry, policy)` is idempotent; repeated calls after the first are no-ops. The `policy` argument must be the `&'static TieredPolicy` singleton from `column_pager::tiered_policy()` so the computed gauges reflect the live policy state regardless of whether the column-paged batcher is currently enabled.

## Internal helpers

`observe_skip`, `observe_pageout`, `observe_pagein`, and `observe_resident_released` are `pub(crate)` functions called directly by `ColumnPager`. Each checks `METRICS.get()` before incrementing, making them zero-cost when the registry has not been installed.
