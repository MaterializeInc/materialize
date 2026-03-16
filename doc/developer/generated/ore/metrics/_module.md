---
source: src/ore/src/metrics.rs
revision: 96c13302b6
---

# mz-ore::metrics

Provides the Materialize-wide Prometheus metrics infrastructure: a registry, metric construction macros, delete-on-drop wrappers, and Future instrumentation.

Key types and traits:

* `MetricsRegistry` — wraps `prometheus::Registry` and adds support for postprocessors (called on every `gather()`), computed gauges, and a typed `register` method driven by the `MakeCollector` trait.
* `metric!` macro / `MakeCollectorOpts` — ergonomic DSL for declaring metric options (name, help, labels, buckets) that feed into `MetricsRegistry::register`.
* `MakeCollector` — trait implemented for all standard Prometheus metric types; enables generic registration.
* `DeleteOnDropWrapper<M>` — wraps a `MetricVec` so that only delete-on-drop child metrics can be created from it, preventing label leaks; re-exported type aliases (`CounterVec`, `GaugeVec`, `IntCounterVec`, etc.) shadow the raw Prometheus types.
* `ComputedGenericGauge` / `ComputedGauge` / `ComputedIntGauge` / `ComputedUIntGauge` — gauges whose value is recomputed from a closure on every scrape.
* `MetricsFutureExt` — extension trait adding `wall_time()` and `exec_time()` combinators to any `Future`; the resulting `WallTimeFuture` / `ExecTimeFuture` record elapsed or CPU time to a Histogram or Counter.
* `delete_on_drop` submodule — underlying RAII machinery for `DeleteOnDropMetric` and related types.

The module is the single integration point between Materialize subsystems and Prometheus: every subsystem defines a struct that calls `registry.register(metric!(...))` for each metric, and uses delete-on-drop wrappers for any per-label-set child metric.
