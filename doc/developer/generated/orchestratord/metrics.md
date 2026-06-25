---
source: src/orchestratord/src/metrics.rs
revision: c6f1655719
---

# mz-orchestratord::metrics

Defines `Metrics` (currently a single `environmentd_needs_update` gauge counting organizations running outdated pod templates) and registers an Axum HTTP router at `/metrics` that serves Prometheus text-format output.
Also provides `add_tracing_layer`, which attaches a tower-http `TraceLayer` to emit structured tracing spans per HTTP request; health-check and metrics endpoints and OPTIONS requests use `DEBUG`-level spans while all other requests use `INFO`-level spans, and server errors emit a `WARN` event.
