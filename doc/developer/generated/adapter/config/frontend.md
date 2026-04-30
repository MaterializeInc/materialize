---
source: src/adapter/src/config/frontend.rs
revision: 815c551264
---

# adapter::config::frontend

Defines `SystemParameterFrontend`, which pulls `SynchronizedParameters` from either a LaunchDarkly SDK client or a local JSON config file, depending on `SystemParameterSyncClientConfig`.
The frontend maps LaunchDarkly feature-flag keys to system variable names using `key_map`, then updates the `SynchronizedParameters` set with fresh values on each `pull` call.
The `SystemParameterFrontendClient` enum encapsulates the two backend variants: `LaunchDarkly` (holding an `ld::Client` and a scoped `ld::Context`) and `File` (holding a path to a local JSON config file).
The LaunchDarkly client is built with `ld::StreamingDataSourceBuilder` and `ld::EventProcessorBuilder`, both configured with `HttpsConnector`. The event processor records a Unix-second CSE timestamp via an `on_success` callback into `Metrics`. The client is started via `start_with_default_executor_and_callback`, where the callback records the SSE timestamp on each event using `now_fn`. Client initialization waits in a loop using `ld::Client::wait_for_initialization` with exponential backoff.
The LaunchDarkly context (`ld_ctx`) is a multi-context that registers `environment`, `organization`, and `build` contexts. For non-local environments the `environment` context carries the cloud provider, region, organization ID, and ordinal; for local environments anonymous contexts with fixed keys are used to avoid billable context proliferation. The `build` context carries the commit SHA and semver version.
