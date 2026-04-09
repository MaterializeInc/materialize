---
source: src/adapter/src/config/frontend.rs
revision: 4bdebeddb9
---

# adapter::config::frontend

Defines `SystemParameterFrontend`, which pulls `SynchronizedParameters` from either a LaunchDarkly SDK client or a local JSON config file, depending on `SystemParameterSyncClientConfig`.
The frontend maps LaunchDarkly feature-flag keys to system variable names using `key_map`, then updates the `SynchronizedParameters` set with fresh values on each `pull` call.
The `SystemParameterFrontendClient` enum encapsulates the two backend variants: `LaunchDarkly` (holding an `ld::Client` and a scoped `ld::Context`) and `File` (holding a path to a local JSON config file).
`MetricsTransport<T>` wraps an `HttpTransport` implementation and records a Unix-second timestamp on each successful HTTP response into a `UIntGauge`; two instances are used — one for the event processor (CSE metric) and one for the streaming data source (SSE metric) — to track LaunchDarkly connectivity health.
