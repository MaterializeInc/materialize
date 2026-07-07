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

use mz_dyncfg::{Config, ConfigSet};

pub const ALLOW_USER_SESSIONS: Config<bool> = Config::new(
    "allow_user_sessions",
    true,
    "Whether to allow user roles to create new sessions. When false, only system roles will be permitted to create new sessions.",
);

// Slightly awkward with the WITH prefix, but we can't start with a 0..
pub const WITH_0DT_DEPLOYMENT_MAX_WAIT: Config<Duration> = Config::new(
    "with_0dt_deployment_max_wait",
    // One year, which in practice makes it so we never cut over when not
    // hydrated. To prevent cutting over unilaterally when there is an issue.
    Duration::from_hours(365 * 24),
    "How long to wait at most for clusters to be hydrated, when doing a zero-downtime deployment.",
);

pub const WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL: Config<Duration> = Config::new(
    "with_0dt_deployment_ddl_check_interval",
    Duration::from_secs(5 * 60),
    "How often to check for DDL changes during zero-downtime deployment.",
);

pub const ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT: Config<bool> = Config::new(
    "enable_0dt_deployment_panic_after_timeout",
    false,
    "Whether to panic if the maximum wait time is reached but preflight checks have not succeeded.",
);

pub const WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL: Config<Duration> = Config::new(
    // The feature flag name is historical.
    "0dt_deployment_hydration_check_interval",
    Duration::from_secs(10),
    "Interval at which to check whether clusters are caught up, when doing zero-downtime deployment.",
);

pub const WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG: Config<Duration> = Config::new(
    "with_0dt_caught_up_check_allowed_lag",
    Duration::from_secs(60),
    "Maximum allowed lag when determining whether collections are caught up for 0dt deployments.",
);

pub const WITH_0DT_CAUGHT_UP_CHECK_CUTOFF: Config<Duration> = Config::new(
    "with_0dt_caught_up_check_cutoff",
    Duration::from_secs(2 * 60 * 60), // 2 hours
    "Collections whose write frontier is behind 'now' by more than the cutoff are ignored when doing caught-up checks for 0dt deployments.",
);

pub const ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK: Config<bool> = Config::new(
    "enable_0dt_caught_up_replica_status_check",
    true,
    "Enable checking for crash/OOM-looping replicas during 0dt caught-up checks. Emergency break-glass flag to disable this feature if needed.",
);

// TODO(aljoscha): Remove this break-glass flag after a couple of releases, once
// the sustained-health gate has proven itself in production. It only exists as a
// fleet-wide automatic revert to the prior "caught-up implies ready" behavior.
pub const ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK: Config<bool> = Config::new(
    "enable_0dt_caught_up_stability_check",
    true,
    "Require clusters to stay caught-up and healthy for a stability period before being considered ready during 0dt deployments. Emergency break-glass flag: disabling reverts to treating a caught-up cluster as ready with no replica-health requirement, which differs from setting the stability period to zero (a zero period still requires all replicas to be healthy).",
);

pub const WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD: Config<Duration> = Config::new(
    "with_0dt_caught_up_check_stability_period",
    Duration::from_secs(10 * 60), // 10 minutes
    "How long a cluster must continuously be caught-up and have all replicas healthy before it is considered ready to cut over during a 0dt deployment.",
);

/// Enable logging of statement lifecycle events in mz_internal.mz_statement_lifecycle_history.
pub const ENABLE_STATEMENT_LIFECYCLE_LOGGING: Config<bool> = Config::new(
    "enable_statement_lifecycle_logging",
    true,
    "Enable logging of statement lifecycle events in mz_internal.mz_statement_lifecycle_history.",
);

/// Enable installation of introspection subscribes.
pub const ENABLE_INTROSPECTION_SUBSCRIBES: Config<bool> = Config::new(
    "enable_introspection_subscribes",
    true,
    "Enable installation of introspection subscribes.",
);

/// Enable sending subscribes down the new frontend-peek path.
pub const ENABLE_FRONTEND_SUBSCRIBES: Config<bool> = Config::new(
    "enable_frontend_subscribes",
    true,
    "Enable sending subscribes down the new frontend-peek path.",
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
);

/// Whether to use an expression cache on boot.
pub const ENABLE_EXPRESSION_CACHE: Config<bool> = Config::new(
    "enable_expression_cache",
    true,
    "Use a cache to store optimized expressions to help speed up start times.",
);

/// Whether to enable password authentication.
pub const ENABLE_PASSWORD_AUTH: Config<bool> = Config::new(
    "enable_password_auth",
    false,
    "Enable password authentication.",
);

/// OIDC issuer URL.
pub const OIDC_ISSUER: Config<Option<&'static str>> =
    Config::new("oidc_issuer", None, "OIDC issuer URL.");

/// OIDC audience (client IDs). When empty, audience validation is skipped.
/// Validates that the JWT's `aud` claim contains at least one of these values.
/// It is insecure to skip validation because it is the only
/// mechanism preventing attackers from authenticating using a JWT
/// issued by a dummy application, but from the same identity provider.
pub const OIDC_AUDIENCE: Config<fn() -> serde_json::Value> = Config::new(
    "oidc_audience",
    || serde_json::json!([]),
    "OIDC audience (client IDs). A JSON array of strings. When empty, audience validation is skipped.",
);

/// OIDC authentication claim to use as username
pub const OIDC_AUTHENTICATION_CLAIM: Config<&'static str> = Config::new(
    "oidc_authentication_claim",
    "sub",
    "OIDC authentication claim to use as username.",
);

/// Whether OIDC group-to-role sync is enabled.
/// When true, JWT group claims are used to sync role memberships on login.
pub const OIDC_GROUP_ROLE_SYNC_ENABLED: Config<bool> = Config::new(
    "oidc_group_role_sync_enabled",
    false,
    "Enable OIDC JWT group-to-role membership sync on login.",
);

/// The JWT claim path that contains group memberships. May be a bare claim
/// name (e.g. `groups`) or a dot-separated path into nested objects (e.g.
/// `customClaims.groups`).
pub const OIDC_GROUP_CLAIM: Config<&'static str> = Config::new(
    "oidc_group_claim",
    "groups",
    "JWT claim path containing group memberships for role sync. Supports dot-separated paths into nested objects (e.g. customClaims.groups).",
);

/// Whether to reject login when group sync fails (strict/fail-closed mode).
/// When false (default), sync failures are logged but login proceeds (fail-open).
pub const OIDC_GROUP_ROLE_SYNC_STRICT: Config<bool> = Config::new(
    "oidc_group_role_sync_strict",
    false,
    "When true, reject login if OIDC group-to-role sync fails (fail-closed).",
);

pub const PERSIST_FAST_PATH_ORDER: Config<bool> = Config::new(
    "persist_fast_path_order",
    false,
    "If set, send queries with a compatible literal constraint or ordering clause down the Persist fast path.",
);

/// Whether to enforce that S3 Tables connections are in the same region as the Materialize
/// environment.
pub const ENABLE_S3_TABLES_REGION_CHECK: Config<bool> = Config::new(
    "enable_s3_tables_region_check",
    false,
    "Whether to enforce that S3 Tables connections are in the same region as the environment.",
);

/// Whether the MCP agent endpoint is enabled.
pub const ENABLE_MCP_AGENT: Config<bool> = Config::new(
    "enable_mcp_agent",
    true,
    "Whether the MCP agent HTTP endpoint is enabled. When false, requests to /api/mcp/agent return 503 Service Unavailable.",
);

/// Whether the MCP agent query tool is enabled.
/// When false, the `query` tool is hidden from tools/list and calls to it return an error.
/// Agents can still use `get_data_products` and `get_data_product_details`.
pub const ENABLE_MCP_AGENT_QUERY_TOOL: Config<bool> = Config::new(
    "enable_mcp_agent_query_tool",
    true,
    "Whether the MCP agent query tool is enabled. When false, the query tool is not advertised and calls to it are rejected. Agents can still discover and inspect data products.",
);

/// Whether the MCP agent read_data_product tool is enabled.
/// When false, the `read_data_product` tool is hidden from tools/list and calls to it return an error.
/// The `query` tool is the general-purpose alternative for reading data products.
pub const ENABLE_MCP_AGENT_READ_DATA_PRODUCT_TOOL: Config<bool> = Config::new(
    "enable_mcp_agent_read_data_product_tool",
    true,
    "Whether the MCP agent read_data_product tool is enabled. When false, the read_data_product tool is not advertised and calls to it are rejected. Agents can use the query tool to read data products.",
);

/// Whether the MCP developer endpoint is enabled.
pub const ENABLE_MCP_DEVELOPER: Config<bool> = Config::new(
    "enable_mcp_developer",
    true,
    "Whether the MCP developer HTTP endpoint is enabled. When false, requests to /api/mcp/developer return 503 Service Unavailable.",
);

/// Whether the MCP developer query tool is enabled.
/// When false, the `query` tool is hidden from tools/list and calls to it return an error.
/// Developers can still use `query_system_catalog`.
pub const ENABLE_MCP_DEVELOPER_QUERY_TOOL: Config<bool> = Config::new(
    "enable_mcp_developer_query_tool",
    true,
    "Whether the MCP developer query tool is enabled. When false, the query tool is not advertised and calls to it are rejected. Developers can still use query_system_catalog.",
);

/// Whether the external metrics endpoint on environmentd is enabled.
pub const ENABLE_PUBLIC_METRICS_ENDPOINT: Config<bool> = Config::new(
    "enable_public_metrics_endpoint",
    true,
    "Whether the external metrics endpoint on environmentd is enabled. When false, requests return 503.",
);

/// Maximum size (in bytes) of MCP tool response content after JSON serialization.
/// Responses exceeding this limit are rejected with a clear error telling the
/// agent to narrow its query. Keeps responses within LLM context window limits.
pub const MCP_MAX_RESPONSE_SIZE: Config<usize> = Config::new(
    "mcp_max_response_size",
    1_000_000,
    "Maximum size in bytes of MCP tool response content. Responses exceeding this limit are rejected with an error telling the agent to narrow its query.",
);

/// Maximum time an MCP request may run before it is aborted and a timeout
/// error is returned to the client.
pub const MCP_REQUEST_TIMEOUT: Config<Duration> = Config::new(
    "mcp_request_timeout",
    Duration::from_secs(60),
    "Maximum time an MCP request may run before it is aborted with a timeout error.",
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
);

/// Number of user IDs to pre-allocate in a batch. Pre-allocating IDs avoids
/// a persist write + oracle call per DDL statement.
pub const USER_ID_POOL_BATCH_SIZE: Config<u32> = Config::new(
    "user_id_pool_batch_size",
    512,
    "Number of user IDs to pre-allocate in a batch for DDL operations.",
);

/// OIDC client ID for the web console.
pub const CONSOLE_OIDC_CLIENT_ID: Config<&'static str> = Config::new(
    "console_oidc_client_id",
    "",
    "OIDC client ID for the web console.",
);

/// Space-separated OIDC scopes requested by the web console.
pub const CONSOLE_OIDC_SCOPES: Config<&'static str> = Config::new(
    "console_oidc_scopes",
    "",
    "Space-separated OIDC scopes requested by the web console.",
);

/// Interval at which to collect per-object arrangement size snapshots for the history table.
pub const ARRANGEMENT_SIZE_HISTORY_COLLECTION_INTERVAL: Config<Duration> = Config::new(
    "arrangement_size_history_collection_interval",
    // Disabled by default until https://github.com/MaterializeInc/materialize/pull/37455 lands.
    Duration::ZERO,
    "Interval at which to collect and snapshot per-object arrangement sizes \
     into mz_internal.mz_object_arrangement_size_history.",
);

/// How long to retain per-object arrangement size history.
pub const ARRANGEMENT_SIZE_HISTORY_RETENTION_PERIOD: Config<Duration> = Config::new(
    "arrangement_size_history_retention_period",
    Duration::from_hours(7 * 24),
    "How long to retain rows in mz_internal.mz_object_arrangement_size_history.",
);

/// How frequently the catalog `*_info` metrics (`mz_object_info`,
/// `mz_cluster_info`, …) are reconciled with the catalog. A zero duration
/// disables reconciliation.
pub const CATALOG_INFO_METRICS_RECONCILE_INTERVAL: Config<Duration> = Config::new(
    "catalog_info_metrics_reconcile_interval",
    Duration::from_secs(30),
    "How frequently to reconcile the catalog `*_info` metrics with the catalog. A zero duration disables reconciliation.",
);

/// Server-side `statement_timeout` to set on Postgres/CRDB connections used by
/// the Postgres/CRDB timestamp oracle. A zero value leaves the statement
/// timeout unset.
pub const PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT: Config<Duration> = Config::new(
    "pg_timestamp_oracle_statement_timeout",
    crate::timestamp_oracle::DEFAULT_PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT,
    "The server-side statement timeout to set on Postgres/CRDB connections used by the \
    Postgres/CRDB timestamp oracle. A value of zero leaves the statement timeout unset.",
);

/// Whether per-cluster and per-replica scoped system parameters are evaluated.
/// Off by default: the parameter sync loop evaluates no cluster/replica
/// contexts and resolution falls back to the environment-wide value everywhere
/// (the pre-scoped behavior). Enabling it (e.g. from LaunchDarkly) turns on
/// scoped evaluation without a deploy.
pub const ENABLE_SCOPED_SYSTEM_PARAMETERS: Config<bool> = Config::new(
    "enable_scoped_system_parameters",
    false,
    "Whether per-cluster and per-replica scoped system parameters are evaluated and applied.",
);

/// Top-level gate for the cluster controller. When on, the controller owns the
/// managed-cluster replica set and the legacy paths (the graceful 3-stage
/// machine and `cluster_scheduling.rs`) are bypassed. The replica set cannot
/// have two writers, so this is a clean switch, not a per-strategy toggle.
pub const ENABLE_CLUSTER_CONTROLLER: Config<bool> = Config::new(
    "enable_cluster_controller",
    false,
    "Whether the cluster controller owns the managed-cluster replica set. When false, the legacy scheduling and graceful-reconfiguration paths run instead.",
);

/// Cadence of the cluster controller's reconcile tick.
///
/// Replaces `cluster_check_scheduling_policies_interval` once the controller is
/// the sole owner; while the controller is dark both intervals exist.
pub const CLUSTER_CONTROLLER_TICK_INTERVAL: Config<Duration> = Config::new(
    "cluster_controller_tick_interval",
    Duration::from_secs(5),
    "How often the cluster controller runs a reconcile tick.",
);

/// Adds the full set of all adapter `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&ALLOW_USER_SESSIONS)
        .add(&ENABLE_CLUSTER_CONTROLLER)
        .add(&CLUSTER_CONTROLLER_TICK_INTERVAL)
        .add(&WITH_0DT_DEPLOYMENT_MAX_WAIT)
        .add(&WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL)
        .add(&ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT)
        .add(&WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_CUTOFF)
        .add(&ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK)
        .add(&ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD)
        .add(&ENABLE_STATEMENT_LIFECYCLE_LOGGING)
        .add(&ENABLE_INTROSPECTION_SUBSCRIBES)
        .add(&ENABLE_FRONTEND_SUBSCRIBES)
        .add(&PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION)
        .add(&ENABLE_EXPRESSION_CACHE)
        .add(&ENABLE_PASSWORD_AUTH)
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
        .add(&USER_ID_POOL_BATCH_SIZE)
        .add(&CONSOLE_OIDC_CLIENT_ID)
        .add(&CONSOLE_OIDC_SCOPES)
        .add(&ARRANGEMENT_SIZE_HISTORY_COLLECTION_INTERVAL)
        .add(&ARRANGEMENT_SIZE_HISTORY_RETENTION_PERIOD)
        .add(&CATALOG_INFO_METRICS_RECONCILE_INTERVAL)
        .add(&PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT)
        .add(&ENABLE_SCOPED_SYSTEM_PARAMETERS)
}
