---
source: src/adapter/src/config/frontend.rs
revision: 7258dad07f
---

# adapter::config::frontend

Defines `SystemParameterFrontend`, which pulls `SynchronizedParameters` from either a LaunchDarkly SDK client or a local JSON config file, depending on `SystemParameterSyncClientConfig`.
The frontend maps LaunchDarkly feature-flag keys to system variable names using `key_map`, then updates the `SynchronizedParameters` set with fresh values on each `pull` call.
The `SystemParameterFrontendClient` enum encapsulates the two backend variants: `LaunchDarkly` (holding an `ld::Client` and an environment-wide `ld::Context`) and `File` (holding a path to a local JSON config file).
The LaunchDarkly client is built with `ld::StreamingDataSourceBuilder` and `ld::EventProcessorBuilder`, both configured with `HttpsConnector`. The event processor records a Unix-second CSE timestamp via an `on_success` callback into `Metrics`. The client is started via `start_with_default_executor_and_callback`, where the callback records the SSE timestamp on each event using `now_fn`. Client initialization waits in a loop using `ld::Client::wait_for_initialization` with exponential backoff.
The LaunchDarkly context (`ld_ctx`) is a multi-context that registers `environment`, `organization`, and `build` contexts. For non-local environments the `environment` context carries the cloud provider, region, organization ID, and ordinal; for local environments anonymous contexts with fixed keys are used to avoid billable context proliferation. The `build` context carries the commit SHA and semver version. For scoped evaluation, `ld_ctx` accepts optional `cluster` and `replica` arguments that add the corresponding context kinds to the multi-context, enabling cluster-coherent and replica-local flag resolution.
`SystemParameterFrontend` exposes `pull_replica_overrides` and `pull_cluster_overrides` for scoped (per-replica and per-cluster) parameter evaluation. Both methods evaluate each object against LaunchDarkly using a context that includes a `cluster` or `replica` kind, compare the result against the environment-wide baseline via `evaluate_scoped_overrides`, and return a sparse map of overriding values. Only values that differ from the environment-wide value after canonical encoding comparison are recorded, and unparseable values are silently dropped to prevent corrupt overrides reaching the optimizer.
`ClusterScopeContext` and `ReplicaScopeContext` carry the identity attributes (id, name, is_builtin, size, size_family, cluster linkage) used to populate LD context attributes. `ClusterEvalContext` and `ReplicaEvalContext` bundle these scope contexts with their catalog IDs for use as inputs to the scoped evaluation pass.
