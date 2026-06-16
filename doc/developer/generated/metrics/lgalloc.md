---
source: src/metrics/src/lgalloc.rs
revision: 98260415d5
---

# mz-metrics::lgalloc

Exposes lgalloc allocator statistics as Prometheus gauges labeled by size class, using two macro-generated metric structs.

`LgMetrics` (produced by `metrics_size_class!`) tracks per-size-class counters for allocations, deallocations, area counts, region states (free, clean, global, thread), and file-backed sizes (file size, allocated size) obtained from `lgalloc::lgalloc_stats()`.
`LgMapMetrics` (produced by `map_metrics!`) reports NUMA-mapping statistics (mapped, active, dirty bytes) from `/proc/self/numa_maps` via `lgalloc::lgalloc_stats_with_mapping()` on Linux, with a no-op fallback on other platforms.
Both implement `MetricsUpdate`, allowing the crate's scheduler to drive them at independently configurable refresh intervals.

Both structs expose a `descs()` method returning `Vec<(String, String)>` of `(name, help)` pairs for every metric they define; this is used by `mz-metrics-catalog` to document metrics whose names are assembled at macro-expansion time and invisible to its source scraper.
The `SOURCE` constant (`pub(crate) const SOURCE: &str = file!()`) records the source file path for the same purpose.
Also defines `Error` / `ErrorKind` for file-stats retrieval failures.
