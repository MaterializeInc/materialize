---
source: src/adapter/src/config/frontend.rs
revision: b17d5ffb6f
---

# adapter::config::frontend

Defines `SystemParameterFrontend`, which pulls `SynchronizedParameters` from either a LaunchDarkly SDK client or a local JSON config file, depending on `SystemParameterSyncClientConfig`.
The frontend maps LaunchDarkly feature-flag keys to system variable names using `key_map`, then updates the `SynchronizedParameters` set with fresh values on each `pull` call.
The `SystemParameterFrontendClient` enum encapsulates the two backend variants: `LaunchDarkly` (holding an `ld::Client` and a scoped `ld::Context`) and `File` (holding a path to a local JSON config file).
`MetricsTransport<T>` is a generic wrapper around any `launchdarkly_sdk_transport::HttpTransport` that records a Unix-second timestamp into a `UIntGauge` on each successful HTTP response head and on each body chunk received from the stream; two instances are used — one wrapping the event-processor transport (CSE metric) and one wrapping the polling data-source transport (SSE metric).
The LaunchDarkly client is built with `ld::PollingDataSourceBuilder` and `ld::EventProcessorBuilder`, both configured with their respective `MetricsTransport` instances, and is started via `start_with_default_executor` (without a per-event callback). Client initialization waits in a loop using `ld::Client::wait_for_initialization` with exponential backoff.
