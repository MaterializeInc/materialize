---
source: src/metrics/src/lib.rs
revision: 98260415d5
---

# mz-metrics

Collects and periodically refreshes internal Materialize process metrics — lgalloc allocator stats and POSIX rusage stats — publishing them into a `MetricsRegistry`.

The crate's entry point is `register_metrics_into(registry, config_set)`, which creates independent tokio tasks for each subsystem via the internal `MetricsTask` helper.
Each task runs a `tokio::select!` loop that ticks on a `tokio::time::Interval` (which may be zero / disabled) and receives interval-update messages over an unbounded channel.
`update_dyncfg` allows live reconfiguration of refresh intervals via Materialize's `dyncfg` system without restarting tasks.

The `MetricsUpdate` trait is the common interface for each metrics subsystem; implementations must supply a `NAME` constant, an `Error` type, and an `update()` method.

Modules:
* `lgalloc` — lgalloc size-class and NUMA-mapping gauges.
* `rusage` — POSIX `getrusage` gauges.
* `dyncfgs` — dynamic configuration constants for refresh intervals; re-exports `all_dyncfgs`.

The public `describe_metrics()` function returns `Vec<(String, String, &'static str)>` triples of `(name, help, source_file)` for every metric registered through a `metric!`-wrapping macro in `lgalloc` and `rusage`. These metrics are invisible to the `mz-metrics-catalog` source scraper because their names are assembled at macro-expansion time; the catalog imports them by calling `describe_metrics()` and reading back their descriptors from a throwaway registry.

Key dependencies: `lgalloc`, `libc`, `mz-dyncfg`, `mz-ore`, `prometheus`, `tokio`.
