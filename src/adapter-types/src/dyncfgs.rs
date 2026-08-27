// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the adapter layer.

use std::time::Duration;

use mz_dyncfg::{Config, ConfigSet, ParameterScope};

pub const ALLOW_USER_SESSIONS: Config<bool> = Config::new(
    "allow_user_sessions",
    true,
    "Whether to allow user roles to create new sessions. When false, only system roles will be permitted to create new sessions.",
    ParameterScope::Environment,
);

// Slightly awkward with the WITH prefix, but we can't start with a 0..
pub const WITH_0DT_DEPLOYMENT_MAX_WAIT: Config<Duration> = Config::new(
    "with_0dt_deployment_max_wait",
    // One year, which in practice makes it so we never cut over when not
    // hydrated. To prevent cutting over unilaterally when there is an issue.
    Duration::from_hours(365 * 24),
    "How long to wait at most for clusters to be hydrated, when doing a zero-downtime deployment.",
    ParameterScope::Environment,
);

pub const WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL: Config<Duration> = Config::new(
    "with_0dt_deployment_ddl_check_interval",
    Duration::from_secs(5 * 60),
    "How often to check for DDL changes during zero-downtime deployment.",
    ParameterScope::Environment,
);

pub const ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT: Config<bool> = Config::new(
    "enable_0dt_deployment_panic_after_timeout",
    false,
    "Whether to panic if the maximum wait time is reached but preflight checks have not succeeded.",
    ParameterScope::Environment,
);

pub const WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL: Config<Duration> = Config::new(
    // The feature flag name is historical.
    "0dt_deployment_hydration_check_interval",
    Duration::from_secs(10),
    "Interval at which to check whether clusters are caught up, when doing zero-downtime deployment.",
    ParameterScope::Environment,
);

pub const WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG: Config<Duration> = Config::new(
    "with_0dt_caught_up_check_allowed_lag",
    Duration::from_secs(60),
    "Maximum allowed lag when determining whether collections are caught up for 0dt deployments.",
    ParameterScope::Environment,
);

pub const WITH_0DT_CAUGHT_UP_CHECK_CUTOFF: Config<Duration> = Config::new(
    "with_0dt_caught_up_check_cutoff",
    Duration::from_secs(2 * 60 * 60), // 2 hours
    "Collections whose write frontier is behind 'now' by more than the cutoff are ignored when doing caught-up checks for 0dt deployments.",
    ParameterScope::Environment,
);

pub const ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK: Config<bool> = Config::new(
    "enable_0dt_caught_up_replica_status_check",
    true,
    "Enable checking for crash/OOM-looping replicas during 0dt caught-up checks. Emergency break-glass flag to disable this feature if needed.",
    ParameterScope::Environment,
);

// TODO(aljoscha): Remove this break-glass flag after a couple of releases, once
// the sustained-health gate has proven itself in production. It only exists as a
// fleet-wide automatic revert to the prior "caught-up implies ready" behavior.
pub const ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK: Config<bool> = Config::new(
    "enable_0dt_caught_up_stability_check",
    true,
    "Require clusters to stay caught-up and healthy for a stability period before being considered ready during 0dt deployments. Emergency break-glass flag: disabling reverts to treating a caught-up cluster as ready with no replica-health requirement, which differs from setting the stability period to zero (a zero period still requires all replicas to be healthy).",
    ParameterScope::Environment,
);

pub const WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD: Config<Duration> = Config::new(
    "with_0dt_caught_up_check_stability_period",
    Duration::from_secs(10 * 60), // 10 minutes
    "How long a cluster must continuously be caught-up and have all replicas healthy before it is considered ready to cut over during a 0dt deployment.",
    ParameterScope::Environment,
);

pub const ENABLE_0DT_HYDRATE_MIGRATED_BUILTIN_MVS: Config<bool> = Config::new(
    "enable_0dt_hydrate_migrated_builtin_mvs",
    true,
    "Write-enable replacement-migrated builtin materialized views while read-only during a 0dt \
     deployment, so they hydrate before cut-over and keep gating promotion. Emergency break-glass \
     flag: disabling excludes migrated MVs (and their dependents) from the caught-up check again, \
     so promotion proceeds with them unhydrated. Not an exact revert: a collection with no live \
     leader frontier must be hydrated either way. Only takes effect when the leader is new enough \
     for the write to make progress, and is read once at startup, so changing it means setting it \
     on the leader and restarting the new deployment.",
    ParameterScope::Environment,
);

/// Enable logging of statement lifecycle events in mz_internal.mz_statement_lifecycle_history.
pub const ENABLE_STATEMENT_LIFECYCLE_LOGGING: Config<bool> = Config::new(
    "enable_statement_lifecycle_logging",
    true,
    "Enable logging of statement lifecycle events in mz_internal.mz_statement_lifecycle_history.",
    ParameterScope::Environment,
);

/// Enable installation of introspection subscribes.
pub const ENABLE_INTROSPECTION_SUBSCRIBES: Config<bool> = Config::new(
    "enable_introspection_subscribes",
    true,
    "Enable installation of introspection subscribes.",
    ParameterScope::Environment,
);

/// Enable sending subscribes down the new frontend-peek path.
pub const ENABLE_FRONTEND_SUBSCRIBES: Config<bool> = Config::new(
    "enable_frontend_subscribes",
    true,
    "Enable sending subscribes down the new frontend-peek path.",
    ParameterScope::Environment,
);

/// The plan insights notice will not investigate fast path clusters if plan optimization took longer than this.
pub const PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION: Config<Duration> = Config::new(
    "plan_insights_notice_fast_path_clusters_optimize_duration",
    // Looking at production values of the mz_optimizer_e2e_optimization_time_seconds metric, most
    // optimizations run faster than 10ms, so this should still work well for most queries. We want
    // to avoid the case where an optimization took just under this value and there are lots of
    // clusters, so the extra delay to produce the plan insights notice will take the optimization
    // time * the number of clusters longer.
    Duration::from_millis(10),
    "Enable plan insights fast path clusters calculation if the optimize step took less than this duration.",
    ParameterScope::Environment,
);

/// Whether to use an expression cache on boot.
pub const ENABLE_EXPRESSION_CACHE: Config<bool> = Config::new(
    "enable_expression_cache",
    true,
    "Use a cache to store optimized expressions to help speed up start times. \
     Read at startup, so changing it takes effect on the next restart.",
    ParameterScope::Environment,
);

/// Whether to enable password authentication.
pub const ENABLE_PASSWORD_AUTH: Config<bool> = Config::new(
    "enable_password_auth",
    false,
    "Enable password authentication.",
    ParameterScope::Environment,
);

/// Upper bound on the number of transitive dependencies validated for a
/// read-then-write statement (e.g. `DELETE ... WHERE ... IN (SELECT ...)`).
/// Validation walks the read set's dependency graph, which is user controlled
/// and can be arbitrarily large. The bound rejects pathological graphs with a
/// clean error instead of consuming unbounded time and memory.
pub const READ_THEN_WRITE_MAX_DEPENDENCIES: Config<usize> = Config::new(
    "read_then_write_max_dependencies",
    100_000,
    "Maximum number of transitive dependencies validated for a read-then-write \
     statement before it is rejected.",
    ParameterScope::Environment,
);

/// OIDC issuer URL.
pub const OIDC_ISSUER: Config<Option<&'static str>> = Config::new(
    "oidc_issuer",
    None,
    "OIDC issuer URL.",
    ParameterScope::Environment,
);

/// OIDC audience (client IDs). When empty, audience validation is skipped.
/// Validates that the JWT's `aud` claim contains at least one of these values.
/// It is insecure to skip validation because it is the only
/// mechanism preventing attackers from authenticating using a JWT
/// issued by a dummy application, but from the same identity provider.
pub const OIDC_AUDIENCE: Config<fn() -> serde_json::Value> = Config::new(
    "oidc_audience",
    || serde_json::json!([]),
    "OIDC audience (client IDs). A JSON array of strings. When empty, audience validation is skipped.",
    ParameterScope::Environment,
);

/// OIDC authentication claim to use as username
pub const OIDC_AUTHENTICATION_CLAIM: Config<&'static str> = Config::new(
    "oidc_authentication_claim",
    "sub",
    "OIDC authentication claim to use as username.",
    ParameterScope::Environment,
);

/// Whether OIDC group-to-role sync is enabled.
/// When true, JWT group claims are used to sync role memberships on login.
pub const OIDC_GROUP_ROLE_SYNC_ENABLED: Config<bool> = Config::new(
    "oidc_group_role_sync_enabled",
    false,
    "Enable OIDC JWT group-to-role membership sync on login.",
    ParameterScope::Environment,
);

/// The JWT claim path that contains group memberships. May be a bare claim
/// name (e.g. `groups`) or a dot-separated path into nested objects (e.g.
/// `customClaims.groups`).
pub const OIDC_GROUP_CLAIM: Config<&'static str> = Config::new(
    "oidc_group_claim",
    "groups",
    "JWT claim path containing group memberships for role sync. Supports dot-separated paths into nested objects (e.g. customClaims.groups).",
    ParameterScope::Environment,
);

/// Whether to reject login when group sync fails (strict/fail-closed mode).
/// When false (default), sync failures are logged but login proceeds (fail-open).
pub const OIDC_GROUP_ROLE_SYNC_STRICT: Config<bool> = Config::new(
    "oidc_group_role_sync_strict",
    false,
    "When true, reject login if OIDC group-to-role sync fails (fail-closed).",
    ParameterScope::Environment,
);

pub const PERSIST_FAST_PATH_ORDER: Config<bool> = Config::new(
    "persist_fast_path_order",
    false,
    "If set, send queries with a compatible literal constraint or ordering clause down the Persist fast path.",
    ParameterScope::Environment,
);

/// Whether to enforce that S3 Tables connections are in the same region as the Materialize
/// environment.
pub const ENABLE_S3_TABLES_REGION_CHECK: Config<bool> = Config::new(
    "enable_s3_tables_region_check",
    false,
    "Whether to enforce that S3 Tables connections are in the same region as the environment.",
    ParameterScope::Environment,
);

/// Whether the MCP agent endpoint is enabled.
pub const ENABLE_MCP_AGENT: Config<bool> = Config::new(
    "enable_mcp_agent",
    true,
    "Whether the MCP agent HTTP endpoint is enabled. When false, requests to /api/mcp/agent return 503 Service Unavailable.",
    ParameterScope::Environment,
);

/// Whether the MCP agent query tool is enabled.
/// When false, the `query` tool is hidden from tools/list and calls to it return an error.
/// Agents can still use `get_data_products` and `get_data_product_details`.
pub const ENABLE_MCP_AGENT_QUERY_TOOL: Config<bool> = Config::new(
    "enable_mcp_agent_query_tool",
    true,
    "Whether the MCP agent query tool is enabled. When false, the query tool is not advertised and calls to it are rejected. Agents can still discover and inspect data products.",
    ParameterScope::Environment,
);

/// Whether the MCP agent read_data_product tool is enabled.
/// When false, the `read_data_product` tool is hidden from tools/list and calls to it return an error.
/// The `query` tool is the general-purpose alternative for reading data products.
pub const ENABLE_MCP_AGENT_READ_DATA_PRODUCT_TOOL: Config<bool> = Config::new(
    "enable_mcp_agent_read_data_product_tool",
    true,
    "Whether the MCP agent read_data_product tool is enabled. When false, the read_data_product tool is not advertised and calls to it are rejected. Agents can use the query tool to read data products.",
    ParameterScope::Environment,
);

/// Whether the MCP developer endpoint is enabled.
pub const ENABLE_MCP_DEVELOPER: Config<bool> = Config::new(
    "enable_mcp_developer",
    true,
    "Whether the MCP developer HTTP endpoint is enabled. When false, requests to /api/mcp/developer return 503 Service Unavailable.",
    ParameterScope::Environment,
);

/// Whether the MCP developer query tool is enabled.
/// When false, the `query` tool is hidden from tools/list and calls to it return an error.
/// Developers can still use `query_system_catalog`.
pub const ENABLE_MCP_DEVELOPER_QUERY_TOOL: Config<bool> = Config::new(
    "enable_mcp_developer_query_tool",
    true,
    "Whether the MCP developer query tool is enabled. When false, the query tool is not advertised and calls to it are rejected. Developers can still use query_system_catalog.",
    ParameterScope::Environment,
);

/// Whether the external metrics endpoint on environmentd is enabled.
pub const ENABLE_PUBLIC_METRICS_ENDPOINT: Config<bool> = Config::new(
    "enable_public_metrics_endpoint",
    true,
    "Whether the external metrics endpoint on environmentd is enabled. When false, requests return 503.",
    ParameterScope::Environment,
);

/// Maximum size (in bytes) of MCP tool response content after JSON serialization.
/// Responses exceeding this limit are rejected with a clear error telling the
/// agent to narrow its query. Keeps responses within LLM context window limits.
pub const MCP_MAX_RESPONSE_SIZE: Config<usize> = Config::new(
    "mcp_max_response_size",
    1_000_000,
    "Maximum size in bytes of MCP tool response content. Responses exceeding this limit are rejected with an error telling the agent to narrow its query.",
    ParameterScope::Environment,
);

/// Maximum time an MCP request may run before it is aborted and a timeout
/// error is returned to the client.
pub const MCP_REQUEST_TIMEOUT: Config<Duration> = Config::new(
    "mcp_request_timeout",
    Duration::from_secs(60),
    "Maximum time an MCP request may run before it is aborted with a timeout error.",
    ParameterScope::Environment,
);

/// Maximum size (in bytes) of a webhook request body, measured after
/// decompression. Requests whose body exceeds this limit are rejected with
/// HTTP 413. Applies only to the webhook route; other HTTP routes use a
/// separate static limit.
pub const WEBHOOK_MAX_REQUEST_SIZE_BYTES: Config<usize> = Config::new(
    "webhook_max_request_size_bytes",
    // Matches `MAX_REQUEST_SIZE`, the static limit the other environmentd HTTP routes use.
    5 * 1024 * 1024,
    "The maximum size in bytes of a webhook request body, measured after decompression.",
    ParameterScope::Environment,
);

/// Maximum temporary storage a webhook `CHECK` expression may allocate while
/// validating one request. A `CHECK` that exceeds it fails the request with HTTP
/// 400 rather than holding the memory.
///
/// A `CHECK` can allocate a multiple of the request body, and `environmentd`
/// evaluates one per in-flight request. Without a bound proportionate to the
/// request, bounded network input becomes unbounded heap on a process shared by
/// every connection. The default is 4x `WEBHOOK_MAX_REQUEST_SIZE_BYTES`, well
/// above what a realistic `CHECK` (an HMAC, a `decode`, a `concat` with a
/// secret) needs and well below the 100 MiB per-call ceiling used in a cluster.
///
/// NOTE: this is runtime-reconfigurable, so it must only bound a single webhook
/// validation. Do not feed it (or any mutable budget) to a `RowArena` used in a
/// compute dataflow (see `mz_repr::RowArena::with_budget`).
pub const WEBHOOK_VALIDATION_MEMORY_BUDGET_BYTES: Config<usize> = Config::new(
    "webhook_validation_memory_budget_bytes",
    20 * 1024 * 1024,
    "The maximum bytes of temporary storage a webhook CHECK expression may allocate while validating one request.",
    ParameterScope::Environment,
);

/// Budget for the backlog a `SUBSCRIBE` (or `COPY (SUBSCRIBE ...) TO STDOUT`)
/// may accumulate in environmentd while waiting for a slow client to read.
///
/// The subscribe producer runs on the non-blockable coordinator loop, so it
/// cannot apply backpressure to a slow client. Instead the coordinator retires
/// the subscribe once its buffered backlog exceeds this budget, bounding the
/// memory a slow client can make the shared process hold.
///
/// The backlog excludes the message the client is currently draining, so this
/// bounds the accumulation of messages, not the size of any single one.
/// `max_result_size` is what bounds an individual message. A client that keeps
/// up holds at most one message at a time, so it stays at a zero backlog and a
/// large snapshot batch is delivered rather than retired.
pub const SUBSCRIBE_MAX_BUFFERED_BYTES: Config<usize> = Config::new(
    "subscribe_max_buffered_bytes",
    128 * 1024 * 1024,
    "Maximum bytes a SUBSCRIBE may buffer in environmentd for a slow client before it is retired with an error.",
    ParameterScope::Environment,
);

/// Number of user IDs to pre-allocate in a batch. Pre-allocating IDs avoids
/// a persist write + oracle call per DDL statement.
pub const USER_ID_POOL_BATCH_SIZE: Config<u32> = Config::new(
    "user_id_pool_batch_size",
    512,
    "Number of user IDs to pre-allocate in a batch for DDL operations.",
    ParameterScope::Environment,
);

/// Maximum number of txns-shard write attempts before rebuilding `environmentd`.
///
/// The effective minimum is one attempt.
pub const GROUP_COMMIT_MAX_ATTEMPTS: Config<usize> = Config::new(
    "group_commit_max_attempts",
    100,
    "Maximum number of txns-shard write attempts before rebuilding environmentd. Values below 1 are treated as 1.",
    ParameterScope::Environment,
);

/// OIDC client ID for the web console.
pub const CONSOLE_OIDC_CLIENT_ID: Config<&'static str> = Config::new(
    "console_oidc_client_id",
    "",
    "OIDC client ID for the web console.",
    ParameterScope::Environment,
);

/// Space-separated OIDC scopes requested by the web console.
pub const CONSOLE_OIDC_SCOPES: Config<&'static str> = Config::new(
    "console_oidc_scopes",
    "",
    "Space-separated OIDC scopes requested by the web console.",
    ParameterScope::Environment,
);

/// Interval at which to collect per-object arrangement size snapshots for the history table.
pub const ARRANGEMENT_SIZE_HISTORY_COLLECTION_INTERVAL: Config<Duration> = Config::new(
    "arrangement_size_history_collection_interval",
    // Disabled by default until https://github.com/MaterializeInc/materialize/pull/37455 lands.
    Duration::ZERO,
    "Interval at which to collect and snapshot per-object arrangement sizes \
     into mz_internal.mz_object_arrangement_size_history.",
    ParameterScope::Environment,
);

/// How long to retain per-object arrangement size history.
pub const ARRANGEMENT_SIZE_HISTORY_RETENTION_PERIOD: Config<Duration> = Config::new(
    "arrangement_size_history_retention_period",
    Duration::from_hours(7 * 24),
    "How long to retain rows in mz_internal.mz_object_arrangement_size_history.",
    ParameterScope::Environment,
);

/// How often to sweep replicas for completed object and replica hydration episodes.
pub const HYDRATION_HISTORY_COLLECTION_INTERVAL: Config<Duration> = Config::new(
    "hydration_history_collection_interval",
    Duration::ZERO,
    "How often to record completed object and replica hydration episodes. A zero duration disables collection.",
    ParameterScope::Environment,
);

/// How long to retain completed object and replica hydration episodes.
pub const HYDRATION_HISTORY_RETENTION_PERIOD: Config<Duration> = Config::new(
    "hydration_history_retention_period",
    Duration::from_hours(30 * 24),
    "How long to retain rows in mz_internal.mz_object_hydration_history and mz_internal.mz_replica_hydration_history.",
    ParameterScope::Environment,
);

/// How frequently the catalog `*_info` metrics (`mz_object_info`,
/// `mz_cluster_info`, …) are reconciled with the catalog. A zero duration
/// disables reconciliation.
pub const CATALOG_INFO_METRICS_RECONCILE_INTERVAL: Config<Duration> = Config::new(
    "catalog_info_metrics_reconcile_interval",
    Duration::from_secs(30),
    "How frequently to reconcile the catalog `*_info` metrics with the catalog. A zero duration disables reconciliation.",
    ParameterScope::Environment,
);

/// Server-side `statement_timeout` to set on Postgres/CRDB connections used by
/// the Postgres/CRDB timestamp oracle. A zero value leaves the statement
/// timeout unset.
pub const PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT: Config<Duration> = Config::new(
    "pg_timestamp_oracle_statement_timeout",
    crate::timestamp_oracle::DEFAULT_PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT,
    "The server-side statement timeout to set on Postgres/CRDB connections used by the \
    Postgres/CRDB timestamp oracle. A value of zero leaves the statement timeout unset.",
    ParameterScope::Environment,
);

/// Cadence of the cluster controller's reconcile tick.
pub const CLUSTER_CONTROLLER_TICK_INTERVAL: Config<Duration> = Config::new(
    "cluster_controller_tick_interval",
    Duration::from_secs(5),
    "How often the cluster controller runs a reconcile tick.",
    ParameterScope::Environment,
);

/// Whether a config-shape `ALTER CLUSTER` returns immediately, with the
/// controller converging in the background, or blocks the session on a
/// wait-shim until the reconfiguration completes or its deadline passes.
///
/// Defaults on. This is the break-glass switch back to the blocking wait-shim
/// if returning immediately causes trouble.
pub const ENABLE_BACKGROUND_ALTER_CLUSTER: Config<bool> = Config::new(
    "enable_background_alter_cluster",
    true,
    "Whether a config-shape ALTER CLUSTER returns immediately (true) or the session blocks on a wait-shim over the durable reconfiguration record (false).",
    ParameterScope::Environment,
);

/// The reconfiguration deadline written when a config-shape `ALTER CLUSTER`
/// omits `WITH (WAIT ...)`. What happens when the deadline passes un-hydrated is
/// the record's `on_timeout` action.
pub const DEFAULT_CLUSTER_RECONFIGURATION_TIMEOUT: Config<Duration> = Config::new(
    "default_cluster_reconfiguration_timeout",
    Duration::from_secs(60 * 60 * 24),
    "The reconfiguration deadline written when a config-shape ALTER CLUSTER omits WITH (WAIT ...).",
    ParameterScope::Environment,
);

/// Break-glass for the hydration-burst strategy: when off the controller never
/// runs a burst replica; graceful reconfiguration and `ON REFRESH` scheduling
/// are unaffected.
///
/// A cluster can only carry an `AUTO SCALING STRATEGY` while its SQL acceptance
/// feature flag is on, so this is the second of the two gates burst sits
/// behind.
pub const ENABLE_HYDRATION_BURST: Config<bool> = Config::new(
    "enable_hydration_burst",
    true,
    "Whether the cluster controller's hydration-burst strategy may run a burst replica (break-glass; leaves graceful reconfiguration and ON REFRESH untouched).",
    ParameterScope::Environment,
);

/// The burst-replica linger duration written into a new `burst` record when the
/// cluster's `AUTO SCALING STRATEGY` omits `LINGER DURATION`. The burst replica
/// stays up this long after the steady-state replicas first hydrate.
pub const DEFAULT_HYDRATION_BURST_LINGER: Config<Duration> = Config::new(
    "default_hydration_burst_linger",
    Duration::from_secs(0),
    "The burst-replica linger duration written when an AUTO SCALING STRATEGY omits LINGER DURATION.",
    ParameterScope::Environment,
);

pub const FRONTEND_READ_THEN_WRITE: Config<bool> = Config::new(
    "enable_adapter_frontend_occ_read_then_write",
    false,
    "Use frontend sequencing (with optimistic concurrency control) for \
     DELETE, UPDATE, and INSERT operations. Read at startup, so changing it \
     takes effect on the next restart.",
    ParameterScope::Environment,
);

/// Adds the full set of all adapter `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&ALLOW_USER_SESSIONS)
        .add(&CLUSTER_CONTROLLER_TICK_INTERVAL)
        .add(&ENABLE_BACKGROUND_ALTER_CLUSTER)
        .add(&DEFAULT_CLUSTER_RECONFIGURATION_TIMEOUT)
        .add(&ENABLE_HYDRATION_BURST)
        .add(&DEFAULT_HYDRATION_BURST_LINGER)
        .add(&WITH_0DT_DEPLOYMENT_MAX_WAIT)
        .add(&WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL)
        .add(&ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT)
        .add(&WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_CUTOFF)
        .add(&ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK)
        .add(&ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD)
        .add(&ENABLE_0DT_HYDRATE_MIGRATED_BUILTIN_MVS)
        .add(&ENABLE_STATEMENT_LIFECYCLE_LOGGING)
        .add(&ENABLE_INTROSPECTION_SUBSCRIBES)
        .add(&ENABLE_FRONTEND_SUBSCRIBES)
        .add(&PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION)
        .add(&ENABLE_EXPRESSION_CACHE)
        .add(&ENABLE_PASSWORD_AUTH)
        .add(&READ_THEN_WRITE_MAX_DEPENDENCIES)
        .add(&OIDC_ISSUER)
        .add(&OIDC_AUDIENCE)
        .add(&OIDC_AUTHENTICATION_CLAIM)
        .add(&OIDC_GROUP_ROLE_SYNC_ENABLED)
        .add(&OIDC_GROUP_CLAIM)
        .add(&OIDC_GROUP_ROLE_SYNC_STRICT)
        .add(&PERSIST_FAST_PATH_ORDER)
        .add(&ENABLE_S3_TABLES_REGION_CHECK)
        .add(&ENABLE_MCP_AGENT)
        .add(&ENABLE_MCP_AGENT_QUERY_TOOL)
        .add(&ENABLE_MCP_AGENT_READ_DATA_PRODUCT_TOOL)
        .add(&ENABLE_MCP_DEVELOPER)
        .add(&ENABLE_MCP_DEVELOPER_QUERY_TOOL)
        .add(&ENABLE_PUBLIC_METRICS_ENDPOINT)
        .add(&MCP_MAX_RESPONSE_SIZE)
        .add(&MCP_REQUEST_TIMEOUT)
        .add(&WEBHOOK_MAX_REQUEST_SIZE_BYTES)
        .add(&WEBHOOK_VALIDATION_MEMORY_BUDGET_BYTES)
        .add(&SUBSCRIBE_MAX_BUFFERED_BYTES)
        .add(&USER_ID_POOL_BATCH_SIZE)
        .add(&GROUP_COMMIT_MAX_ATTEMPTS)
        .add(&CONSOLE_OIDC_CLIENT_ID)
        .add(&CONSOLE_OIDC_SCOPES)
        .add(&ARRANGEMENT_SIZE_HISTORY_COLLECTION_INTERVAL)
        .add(&ARRANGEMENT_SIZE_HISTORY_RETENTION_PERIOD)
        .add(&HYDRATION_HISTORY_COLLECTION_INTERVAL)
        .add(&HYDRATION_HISTORY_RETENTION_PERIOD)
        .add(&CATALOG_INFO_METRICS_RECONCILE_INTERVAL)
        .add(&PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT)
        .add(&FRONTEND_READ_THEN_WRITE)
}
