---
source: src/pgwire/src/metrics.rs
revision: e757b4d11b
---

# pgwire::metrics

Defines `MetricsConfig` (shareable configuration holding a `mz_connection_status` counter vector) and `Metrics` (a per-label handle for incrementing that counter).
`Metrics::connection_status(is_ok)` returns the appropriate `IntCounter` for tracking successful or failed connection completions.
Pre-initializes both status label values on construction so they are always emitted as time series.
