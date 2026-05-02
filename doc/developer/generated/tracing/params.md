---
source: src/tracing/src/params.rs
revision: 379b173f11
---

# mz-tracing::params

Defines `TracingParameters`, a serializable struct that bundles runtime-adjustable tracing configuration: `log_filter` and `opentelemetry_filter` (each a `CloneableEnvFilter`), plus per-backend default directive lists and Sentry-specific directives.
`TracingParameters::apply` pushes the current configuration into a `mz_ore::tracing::TracingHandle`, reloading the stderr, OpenTelemetry, and Sentry filters in place.
`TracingParameters::update` performs a partial-update merge, overwriting only the fields that are `Some` in the incoming value.
