---
source: src/tracing/src/lib.rs
revision: 379b173f11
---

# mz-tracing

Provides Materialize-specific wrappers around `tracing-subscriber` types to make them cloneable and serializable, and defines the `TracingParameters` struct for runtime-adjustable filter configuration.

## Module structure

* `lib.rs` — `CloneableEnvFilter` (cloneable, serde-aware wrapper for `tracing_subscriber::EnvFilter`) and `SerializableDirective` (serde wrapper for `tracing_subscriber::filter::Directive`).
* `params` — `TracingParameters` with `apply` and `update` methods for pushing filter changes into a live `mz_ore::tracing::TracingHandle`.

## Key types

* `CloneableEnvFilter` — reconstructs an `EnvFilter` from its validated string representation on clone, avoiding the need for upstream `Clone` support.
* `SerializableDirective` — thin newtype over `Directive` adding `Display`, `FromStr`, and serde impls.
* `TracingParameters` — serializable bundle of optional log, OpenTelemetry, and Sentry filter settings; supports partial merging via `update`.

## Dependencies and consumers

Key dependencies: `tracing`, `tracing-subscriber`, `serde`, `mz-ore` (tracing feature).
Consumers: components that propagate runtime tracing configuration changes from the catalog or system parameters to running services.
