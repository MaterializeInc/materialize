---
source: src/ore/src/tracing.rs
revision: 8ef14a9866
---

# mz-ore::tracing

Provides the application-wide tracing and observability initialization infrastructure for Materialize binaries.
`configure` wires together a layered `tracing_subscriber` stack from a `TracingConfig`: a stderr log layer (text or JSON format with optional prefix), an optional OpenTelemetry OTLP export layer (using a custom TLS/h2 channel), an optional Tokio console layer, and an optional Sentry error-reporting layer.
All filters in the stack support dynamic reload via the returned `TracingHandle`, which exposes `reload_stderr_log_filter`, `reload_opentelemetry_filter`, and `reload_sentry_directives`.
`OpenTelemetryContext` serializes and deserializes W3C trace context across task/thread/RPC boundaries by implementing the OpenTelemetry `Injector`/`Extractor` traits over a `BTreeMap<String, String>`.
`OpenTelemetryRateLimitingFilter` suppresses duplicate log messages from OpenTelemetry-internal targets (those starting with `"opentelemetry"`) within a configurable backoff window to prevent log spam.
Crate-level default filter directives (e.g., silencing noisy `kube_client` and `aws_config` targets) are exported as `LOGGING_DEFAULTS`, `OPENTELEMETRY_DEFAULTS`, and `SENTRY_DEFAULTS`.
