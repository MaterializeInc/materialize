---
source: src/service/src/transport/metrics.rs
revision: 2bb8e26dbb
---

# mz-service::transport::metrics

Defines the `Metrics` trait for observing CTP connection events (bytes sent/received, messages sent/received), a `NoopMetrics` no-op implementation, `ClusterServerMetrics` (registers the `mz_cluster_server_last_command_received` gauge vector), `PerClusterServerMetrics` (a per-named-server `Metrics` impl that updates the last-received timestamp on every inbound byte including keepalives, acting as a connection-liveness signal), and `Reader`/`Writer` wrappers that transparently call `bytes_received`/`bytes_sent` on every async I/O operation.
