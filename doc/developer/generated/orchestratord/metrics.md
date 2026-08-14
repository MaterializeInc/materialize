---
source: src/orchestratord/src/metrics.rs
revision: 8dad2dbf43
---

# mz-orchestratord::metrics

Defines `Metrics` with two gauges: `is_leader` (whether this operator replica holds the controller leadership lease) and `environmentd_needs_update` (count of organizations running outdated pod templates, meaningful only on the replica currently holding the lease). Registers an Axum HTTP router at `/metrics` that serves Prometheus text-format output.
Also provides `add_tracing_layer`, which attaches a tower-http `TraceLayer` to emit structured tracing spans per HTTP request; health-check and metrics endpoints and OPTIONS requests use `DEBUG`-level spans while all other requests use `INFO`-level spans, and server errors emit a `WARN` event.
