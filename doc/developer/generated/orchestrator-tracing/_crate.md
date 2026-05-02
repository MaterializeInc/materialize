---
source: src/orchestrator-tracing/src/lib.rs
revision: 6df3ec9286
---

# mz-orchestrator-tracing

Provides `TracingOrchestrator`, a decorator around any `Orchestrator` implementation that automatically injects tracing CLI arguments into every spawned service.
`TracingCliArgs` is a clap-parseable struct covering stderr log format, OpenTelemetry OTLP export, optional Tokio console support, and Sentry error reporting.
Services must embed `TracingCliArgs` in their own argument parsers to receive the injected flags.
