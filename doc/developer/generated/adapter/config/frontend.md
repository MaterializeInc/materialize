---
source: src/adapter/src/config/frontend.rs
revision: 002ac45886
---

# adapter::config::frontend

Defines `SystemParameterFrontend`, which pulls `SynchronizedParameters` from either a LaunchDarkly SDK client or a local JSON config file, depending on `SystemParameterSyncClientConfig`.
The frontend maps LaunchDarkly feature-flag keys to system variable names using `key_map`, then updates the `SynchronizedParameters` set with fresh values on each `pull` call.
The `SystemParameterFrontendClient` enum encapsulates the two backend variants: `LaunchDarkly` (holding an `ld::Client` and a scoped `ld::Context`) and `File` (holding a path to a local JSON config file).
CSE connectivity health is tracked via an `.on_success` callback on the event processor, and SSE connectivity health is tracked via a callback passed to `start_with_default_executor_and_callback`; both record a Unix-second timestamp into a `UIntGauge` metric.
