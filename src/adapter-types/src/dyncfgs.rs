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
    Duration::from_secs(60 * 60 * 72),
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

/// Whether we allow sources in multi-replica clusters.
pub const ENABLE_MULTI_REPLICA_SOURCES: Config<bool> = Config::new(
    "enable_multi_replica_sources",
    true,
    "Enable multi-replica sources.",
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

/// The JWT claim name that contains group memberships.
pub const OIDC_GROUP_CLAIM: Config<&'static str> = Config::new(
    "oidc_group_claim",
    "groups",
    "JWT claim name containing group memberships for role sync.",
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
    false,
    "Whether the MCP agent query tool is enabled. When false, the query tool is not advertised and calls to it are rejected. Agents can still discover and inspect data products.",
);

/// Whether the MCP developer endpoint is enabled.
pub const ENABLE_MCP_DEVELOPER: Config<bool> = Config::new(
    "enable_mcp_developer",
    true,
    "Whether the MCP developer HTTP endpoint is enabled. When false, requests to /api/mcp/developer return 503 Service Unavailable.",
);

/// Maximum size (in bytes) of MCP tool response content after JSON serialization.
/// Responses exceeding this limit are rejected with a clear error telling the
/// agent to narrow its query. Keeps responses within LLM context window limits.
pub const MCP_MAX_RESPONSE_SIZE: Config<usize> = Config::new(
    "mcp_max_response_size",
    1_000_000,
    "Maximum size in bytes of MCP tool response content. Responses exceeding this limit are rejected with an error telling the agent to narrow its query.",
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
    Duration::from_hours(1),
    "Interval at which to collect and snapshot per-object arrangement sizes \
     into mz_internal.mz_object_arrangement_size_history.",
);

/// How long to retain per-object arrangement size history.
pub const ARRANGEMENT_SIZE_HISTORY_RETENTION_PERIOD: Config<Duration> = Config::new(
    "arrangement_size_history_retention_period",
    Duration::from_hours(7 * 24),
    "How long to retain rows in mz_internal.mz_object_arrangement_size_history.",
);

/// Adds the full set of all adapter `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&ALLOW_USER_SESSIONS)
        .add(&WITH_0DT_DEPLOYMENT_MAX_WAIT)
        .add(&WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL)
        .add(&ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT)
        .add(&WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_CUTOFF)
        .add(&ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK)
        .add(&ENABLE_STATEMENT_LIFECYCLE_LOGGING)
        .add(&ENABLE_INTROSPECTION_SUBSCRIBES)
        .add(&ENABLE_FRONTEND_SUBSCRIBES)
        .add(&PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION)
        .add(&ENABLE_EXPRESSION_CACHE)
        .add(&ENABLE_MULTI_REPLICA_SOURCES)
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
        .add(&ENABLE_MCP_DEVELOPER)
        .add(&MCP_MAX_RESPONSE_SIZE)
        .add(&USER_ID_POOL_BATCH_SIZE)
        .add(&CONSOLE_OIDC_CLIENT_ID)
        .add(&CONSOLE_OIDC_SCOPES)
        .add(&ARRANGEMENT_SIZE_HISTORY_COLLECTION_INTERVAL)
        .add(&ARRANGEMENT_SIZE_HISTORY_RETENTION_PERIOD)
}
