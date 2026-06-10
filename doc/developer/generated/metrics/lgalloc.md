---
source: src/metrics/src/lgalloc.rs
revision: 1e3922ec98
---

# mz-metrics::lgalloc

Exposes lgalloc allocator statistics as Prometheus gauges labeled by size class, using two macro-generated metric structs.

`LgMetrics` (produced by `metrics_size_class!`) tracks per-size-class counters for allocations, deallocations, area counts, region states (free, clean, global, thread), and file-backed sizes (file size, allocated size) obtained from `lgalloc::lgalloc_stats()`.
`LgMapMetrics` (produced by `map_metrics!`) reports NUMA-mapping statistics (mapped, active, dirty bytes) from `/proc/self/numa_maps` via `lgalloc::lgalloc_stats_with_mapping()` on Linux, with a no-op fallback on other platforms.
Both implement `MetricsUpdate`, allowing the crate's scheduler to drive them at independently configurable refresh intervals.

Also defines `Error` / `ErrorKind` for file-stats retrieval failures.
