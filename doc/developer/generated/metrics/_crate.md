---
source: src/metrics/src/lib.rs
revision: 780c9c1add
---

# mz-metrics

Collects and periodically refreshes internal Materialize process metrics — lgalloc allocator stats, POSIX rusage stats, and process resource usage observations — publishing them into a `MetricsRegistry`.

The crate's entry point is `register_metrics_into(registry, config_set, disk_root)`, which creates independent tokio tasks for each subsystem via the internal `MetricsTask` helper. `disk_root` is an `Option<PathBuf>` identifying a directory whose filesystem usage should be tracked; pass `None` for processes that do not use disk (e.g. `environmentd`).
Each task runs a `tokio::select!` loop that ticks on a `tokio::time::Interval` (which may be zero / disabled) and receives interval-update messages over an unbounded channel.
`update_dyncfg` allows live reconfiguration of refresh intervals via Materialize's `dyncfg` system without restarting tasks.

The `MetricsUpdate` trait is the common interface for each metrics subsystem; implementations must supply a `NAME` constant, an `Error` type, and an `update()` method.

Modules:
* `lgalloc` — lgalloc size-class and NUMA-mapping gauges.
* `rusage` — POSIX `getrusage` gauges.
* `usage` — multi-source resource usage sampler: cgroup v2 interface files, `getrusage` peak RSS, `/proc/self/status` fields, and `statvfs` disk usage. Publishes a `mz_metrics_resource_usage` gauge vec labeled by source and metric name, and exposes `observations()` for callers that need a self-consistent snapshot of all readings.
* `dyncfgs` — dynamic configuration constants for refresh intervals; re-exports `all_dyncfgs`.

The public `describe_metrics()` function returns `Vec<(String, String, Vec<String>, &'static str)>` 4-tuples of `(name, help, label_keys, source_file)` for every metric registered through a `metric!`-wrapping macro in `lgalloc` and `rusage`. These metrics are invisible to the `mz-metrics-catalog` source scraper because their names are assembled at macro-expansion time; the catalog imports them by calling `describe_metrics()` and reading back their descriptors from a throwaway registry. Label keys are extracted from Prometheus descriptors via the `desc_labels` helper, which merges variable labels and constant label keys into a sorted, deduplicated `Vec<String>`.

Key dependencies: `lgalloc`, `libc`, `mz-dyncfg`, `mz-ore`, `prometheus`, `tokio`.
