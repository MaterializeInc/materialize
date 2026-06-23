---
source: src/ore/src/metrics.rs
revision: 83e4f88e27
---

# mz-ore::metrics

Provides the Materialize-wide Prometheus metrics infrastructure: a registry, metric construction macros, delete-on-drop wrappers, and Future instrumentation.

Key types and traits:

* `MetricsRegistry` — wraps `prometheus::Registry` and adds support for postprocessors (called on every `gather()`), computed gauges, and a typed `register` method driven by the `MakeCollector` trait.
* `metric!` macro / `MakeCollectorOpts` — ergonomic DSL for declaring metric options (name, help, labels, buckets, and an optional `visibility:` field) that feed into `MetricsRegistry::register`.
* `MetricVisibility` — documentation-metadata enum with variants `Internal` (the default) and `Public`. Set via the `visibility:` field of the `metric!` macro and consumed by `bin/gen-metrics-catalog` to produce the user-facing metrics reference. It has no effect on metric behavior at runtime; the macro only type-checks the value at compile time.
* `MakeCollector` — trait implemented for all standard Prometheus metric types; enables generic registration.
* `DeleteOnDropWrapper<M>` — wraps a `MetricVec` so that only delete-on-drop child metrics can be created from it, preventing label leaks; re-exported type aliases (`CounterVec`, `GaugeVec`, `IntCounterVec`, etc.) shadow the raw Prometheus types.
* `ComputedGenericGauge` / `ComputedGauge` / `ComputedIntGauge` / `ComputedUIntGauge` — gauges whose value is recomputed from a closure on every scrape.
* `MetricsFutureExt` — extension trait adding `wall_time()` and `exec_time()` combinators to any `Future`; the resulting `WallTimeFuture` / `ExecTimeFuture` record elapsed or CPU time to a Histogram or Counter.
* `delete_on_drop` submodule — underlying RAII machinery for `DeleteOnDropMetric` and related types.

The `describe_runtime_metrics()` function (feature-gated on `async`) returns a `Vec<(String, String, Vec<String>, &'static str)>` of `(name, help, label_keys, source_file)` 4-tuples for every Tokio runtime metric registered by `register_runtime_metrics`; it builds a throwaway current-thread runtime and registry to enumerate metric descriptors without recording values. Label keys are extracted from the first metric in each family's label set, sorted and deduplicated.
The module is the single integration point between Materialize subsystems and Prometheus: every subsystem defines a struct that calls `registry.register(metric!(...))` for each metric, and uses delete-on-drop wrappers for any per-label-set child metric.
