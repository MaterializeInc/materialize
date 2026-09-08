---
source: src/compute/src/logging/resource_usage.rs
revision: 780c9c1add
---

# `compute::logging::resource_usage`

Logging dataflow fragment for the resource usage of the replica's processes.

## Overview

This module constructs the timely dataflow fragment that feeds the `ComputeLog::ResourceUsage` introspection source. Observations are read from `mz_metrics::usage`, which samples resource metrics on a task that runs independently of the timely workers. Because sampling is decoupled, a saturated worker does not lose an observation; it merely delays reporting it. All metric values are either kernel-maintained high-water marks or the sampler's own folded peaks, so a late read cannot miss a spike.

## Structure

One row per `(process_id, source, metric)`. Keeping metrics independent means a metric that changes every sample does not retract and re-assert stable metrics alongside it.

## `construct`

The sole entry point. Takes a timely `Scope`, the `LoggingConfig`, the start `Instant`, a `Duration` start offset, and the number of workers per process. Returns a `Return` struct holding the `LogCollection` map.

Only one worker per process reports resource usage (`scope.index() % workers_per_process == 0`). Workers not responsible for reporting drop their capability immediately so the collection frontier can advance without waiting on them.

The operator reads `observations()` on each activation. If the current snapshot equals the previous one, it emits nothing. Otherwise it calls `emit_snapshot_diff` to retract old rows and assert new ones, then downgrades the capability to the current interval boundary via `downgrade_to_interval_boundary`.

The resulting stream is arranged via `mz_arrange_core` with a columnar exchange and returned as a `LogCollection`.

## `pack_row`

Packs one `(process_id, source, metric, value)` tuple into key/value row pairs using `PermutedRowPacker`.
