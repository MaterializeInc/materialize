---
source: src/postgres-client/src/metrics.rs
revision: 0346970958
---

# mz-postgres-client::metrics

Defines `PostgresClientMetrics`, a struct of Prometheus counters and gauges that track connection pool behavior: pool size, connection acquisition count and latency, connection errors, and TTL-driven reconnections.
Metrics are registered with a caller-supplied `MetricsRegistry` using a configurable prefix string, allowing multiple pool instances to coexist in the same process without name collisions.
