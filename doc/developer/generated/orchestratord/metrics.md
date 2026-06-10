---
source: src/orchestratord/src/metrics.rs
revision: 82d92a7fad
---

# mz-orchestratord::metrics

Defines `Metrics` (currently a single `environmentd_needs_update` gauge counting organizations running outdated pod templates) and registers an Axum HTTP router at `/metrics` that serves Prometheus text-format output.
Also provides `add_tracing_layer`, which attaches a tower-http `TraceLayer` to emit structured tracing spans per HTTP request and warn on server errors.
