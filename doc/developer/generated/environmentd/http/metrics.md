---
source: src/environmentd/src/http/metrics.rs
revision: ff5e83bfeb
---

# environmentd::http::metrics

Defines the `Metrics` struct (request counters, active-request gauge, latency histogram) and a Tower `PrometheusLayer`/`PrometheusService` middleware pair that automatically instruments every HTTP handler with those metrics.
The `PrometheusFuture` wrapper correctly decrements the active-request count and discards incomplete timings when a request future is dropped before completion.
