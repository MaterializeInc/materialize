---
source: src/storage-operators/src/metrics.rs
revision: cc81d0177d
---

# storage-operators::metrics

Defines `BackpressureMetrics`, a small struct holding three `DeleteOnDropCounter`/`DeleteOnDropGauge` handles used by the backpressure operator to track emitted bytes, currently backpressured bytes, and retired bytes.
