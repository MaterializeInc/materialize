---
source: src/timely-util/src/column_pager/metrics.rs
revision: bfa6499c3b
---

# timely-util::column_pager::metrics

Prometheus metrics for the column pager subsystem.

A single process-wide `PagerMetrics` singleton is stored in a `OnceLock`. It is installed by compute initialization via `register`, which takes a `MetricsRegistry` and a `&'static TieredPolicy`. Calls to `register` after the first are no-ops. The observer functions (`observe_skip`, `observe_pageout`, `observe_pagein`, `observe_resident_released`) are no-ops until `register` is called, so tests and benchmarks that do not wire a `MetricsRegistry` require no extra setup.

`PagerMetrics` holds the following `IntCounter` fields:

| Counter | Meaning |
|---|---|
| `skip_decisions_total` | Decisions that kept the chunk resident |
| `skip_bytes_total` | Bytes kept resident by skip decisions |
| `pageouts_total` | Decisions that paged the chunk out |
| `paged_bytes_in_total` | Uncompressed bytes handed to the pager before encoding |
| `paged_bytes_out_total` | On-storage bytes after codec / padding |
| `pageins_total` | Page-ins from `ColumnPager::take` |
| `pagein_bytes_total` | Uncompressed bytes delivered by page-in |
| `resident_released_total` | Resident-ticket drops that returned bytes to the budget |
| `resident_released_bytes_total` | Bytes returned to the budget by ticket drops |

Two computed gauges (`mz_column_pager_budget_remaining_bytes` and `mz_column_pager_budget_configured_bytes`) are registered with the registry at `register` time and read the live `TieredPolicy` atomics at scrape time; their collectors are owned by the Prometheus registry, not by `PagerMetrics`.
