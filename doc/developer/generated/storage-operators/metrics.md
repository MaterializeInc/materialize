---
source: src/storage-operators/src/metrics.rs
revision: 11e6a79394
---

# storage-operators::metrics

Defines `BackpressureOperatorMetrics`, a struct holding metric handles for one instance of the `backpressure` operator: `emitted_bytes` (`IntCounter`), `last_backpressured_bytes` (`GaugeContribution`), and `retired_bytes` (`IntCounter`).
Also defines `GaugeContribution`, a type that represents one contributor's additive share of a shared `UIntGauge`. Each contributor tracks its last-set value and withdraws it on drop, so the gauge always reads as the sum of live contributions. This is used so multiple operator instances can share a single gauge without overcounting or leaving stale values when one instance exits.
