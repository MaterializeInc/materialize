---
source: src/frontegg-auth/src/metrics.rs
revision: ff5e83bfeb
---

# frontegg-auth::metrics

Defines `Metrics`, which registers Prometheus counters and histograms for Frontegg authentication: HTTP request counts and durations, active refresh task gauge, session request counts, and session refresh counts.
`Metrics::register_into` populates all metrics from a `MetricsRegistry` and is called once during crate initialization.
