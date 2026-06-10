---
source: src/clusterd/src/usage_metrics.rs
revision: 16cf40e976
---

# clusterd::usage_metrics

Provides the `Collector` struct and `Usage` type for gathering and serializing system resource metrics exposed via the `/api/usage-metrics` HTTP endpoint.
`Collector` samples disk usage via `statvfs`, and on Linux reads memory and swap from `/proc/self/status` and derives the heap limit from `/proc/meminfo` and cgroup v2 files.
