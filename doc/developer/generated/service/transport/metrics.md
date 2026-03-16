---
source: src/service/src/transport/metrics.rs
revision: 82d92a7fad
---

# mz-service::transport::metrics

Defines the `Metrics` trait for observing CTP connection events (bytes sent/received, messages sent/received), a `NoopMetrics` no-op implementation, and `Reader`/`Writer` wrappers that transparently call `bytes_received`/`bytes_sent` on every async I/O operation.
