---
source: src/clusterd/src/usage_metrics.rs
revision: 780c9c1add
---

# clusterd::usage_metrics

Provides the `Collector` struct and `Usage` type for gathering and serializing system resource metrics exposed via the `/api/usage-metrics` HTTP endpoint.
`Collector` samples disk usage by delegating to `mz_metrics::usage::disk_usage`, and on Linux reads memory and swap via `mz_metrics::usage::ProcStatus` (moved from `mz_compute::memory_limiter`) and derives the heap limit from `/proc/meminfo` and cgroup v2 files.
