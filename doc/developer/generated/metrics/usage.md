---
source: src/metrics/src/usage.rs
revision: 780c9c1add
---

# `metrics::usage`

Resource usage observations for the current process.

## Overview

This module samples resource usage from multiple independent sources and publishes them as a unified snapshot. It reports what each source says under that source's own name without combining or synthesizing figures across sources. The discrepancies between sources carry diagnostic value: for example, cgroup memory far above `VmRSS` means page cache or kernel memory is charged to the replica, which a single fused figure would hide. Deciding which source answers "how much memory is this replica using" belongs to the SQL views built on top.

## Sources

| Constant | Description |
|---|---|
| `source::CGROUP` | This process's cgroup v2 interface files |
| `source::RUSAGE` | `getrusage(RUSAGE_SELF)` |
| `source::PROC_STATUS` | `/proc/self/status` |
| `source::STATVFS` | `statvfs` on the scratch directory's filesystem |

## Key Types

**`MetricKey`** — `(&'static str, &'static str)`, a `(source, metric)` pair identifying one observation.

**`UsageMetrics`** — The sampler, implementing `MetricsUpdate`. Constructed by `register_metrics_into`. Holds the optional `CgroupV2` handle, optional disk root path, derived-peak state, and a `UIntGaugeVec` for Prometheus.

**`ProcStatus`** — Parsed fields from `/proc/self/status`: `vm_rss`, `vm_swap`, `rss_anon`, `rss_file`, `rss_shmem`. Provides helpers `rss()`, `swap()`, `heap()` (RSS + swap), `rss_anon()`, `rss_file()`, `rss_shmem()`.

## Shared Observation State

`OBSERVATIONS` is a process-global `Mutex<Option<BTreeMap<MetricKey, u64>>>`. The compute logging dataflow reads it via `observations()`. Publishing the whole map as a unit ensures readers always see a self-consistent snapshot. An observation the sampler could not read is absent from the map, never present as zero.

## Derived Peaks

`DERIVED_PEAKS` lists sources that have no kernel-side high-water mark. For each entry the sampler folds a running maximum across calls to `fold_derived_peaks`, publishing it under a distinct metric name (e.g., `"fs_used_peak"` next to `"fs_used"`). This state survives across logging dataflow rebuilds because it lives in the sampler rather than in a timely operator.

## `UsageMetrics::update`

Called by the metrics update task on each tick. Calls `sample()`, folds derived peaks, updates Prometheus gauges, and stores the result in `OBSERVATIONS`.

## Functions

- `register_metrics_into` — constructs and returns a `UsageMetrics`, logging which cgroup path (if any) was detected
- `disk_usage` — returns used bytes of the filesystem containing `root` via `statvfs`
- `observations` — returns a clone of the most recent snapshot, or `None` if no sample has been taken
