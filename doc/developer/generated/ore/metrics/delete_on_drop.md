---
source: src/ore/src/metrics/delete_on_drop.rs
revision: 7fad528e70
---

# mz-ore::metrics::delete_on_drop

Provides RAII wrappers that automatically remove a labeled metric from its parent `MetricVec` when dropped.

Key types and traits:

* `DeleteOnDropMetric<V, L>` — holds a concrete metric and a shared `Arc<DeleteOnDropCleanup>` that owns the labels and parent vector reference. The labeled metric is removed from the Prometheus registry exactly once, when the last clone of the metric drops (ref-counted via the `Arc`). Cloning is always safe and does not trigger premature removal.
* `DeleteOnDropCleanup<V, L>` — internal struct held inside the `Arc`; its `Drop` implementation calls `remove_label_values` or `remove` to clean up the Prometheus registry entry.
* `DeleteOnDropCounter` / `DeleteOnDropGauge` / `DeleteOnDropHistogram` — type aliases for common metric kinds.
* `MetricVecExt` — extension trait on any `MetricVec_` that adds `get_delete_on_drop_metric(labels)` as the primary construction path.
* `MetricVec_` — abstraction over `MetricVec<P>` for use in generic code.
* `PromLabelsExt` — abstraction over the various label types (`&[&str]`, `Vec<String>`, `Vec<&str>`, `BTreeMap<String, String>`, `BTreeMap<&str, &str>`) accepted by Prometheus, enabling generic label lookup and removal.

This module solves the common problem of leaked per-dimension counters/gauges when the subsystem that owns them is torn down.
