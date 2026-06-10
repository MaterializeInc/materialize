---
source: src/cluster-client/src/metrics.rs
revision: c8a90af6a2
---

# mz_cluster_client::metrics

Prometheus metrics shared by both compute and storage controllers for tracking dataflow wallclock lag.

## Key types

- **`ControllerMetrics`** -- Registers and owns the metric vectors that emulate a Prometheus summary (min gauge, max gauge, sum counter, count counter) for `mz_dataflow_wallclock_lag_seconds`. Each metric vector carries `Rule`s for `ClusterNameLookup`, `ReplicaNameLookup`, and `ObjectNameLookup` so that the public scrape endpoint can attach `cluster_name`, `replica_name`, and `collection_name` labels. Provides `wallclock_lag_metrics()` to create per-collection metric handles.
- **`WallclockLagMetrics`** -- Per-collection metric handle that tracks wallclock lag observations. Uses a `SlidingMinMax` window (60 samples) to maintain rolling min/max values, and exposes them as the 0- and 1-quantiles of the emulated summary. The `observe()` method updates all four metric series on each call.
