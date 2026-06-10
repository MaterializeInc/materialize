# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Verify that the LaunchDarkly project and the set of synchronized system
parameters in Materialize are kept in sync.

Every synchronized system parameter (system variables, dyncfgs and feature
flags -- everything returned by `SystemVars::iter_synced()`) is pulled from
LaunchDarkly at runtime by the `SystemParameterFrontend`, keyed by the
parameter name (see `src/adapter/src/config/frontend.rs`). If a parameter has
no corresponding LaunchDarkly flag, `client.variation` silently falls back to
the compiled-in default, which means the flag can never be controlled in
production. This check fails on four kinds of discrepancy, each gated by a
curated known-exceptions allowlist so that only a *new* discrepancy turns the
build red:

  * a synchronized parameter exists in Materialize but has no flag in
    LaunchDarkly -- unless it is in `KNOWN_MISSING_FROM_LD`.
  * a flag exists in LaunchDarkly but is no longer a synchronized parameter in
    the current build or the last published release -- unless it is in
    `KNOWN_STALE_LD_FLAGS`. (The last release is considered so a flag a deployed
    older version still needs is not flagged.)
  * a flag's production LaunchDarkly default differs from Materialize's
    compiled-in default. Self-managed deployments have no LaunchDarkly and run
    on the compiled-in default, so this means cloud and self-managed behave
    differently (e.g. a feature enabled in cloud but off by default in
    self-managed) -- unless it is deliberate cloud-only tuning listed in
    `INTENTIONAL_LD_OVERRIDES`.
  * a flag's served default differs between LaunchDarkly environments (e.g.
    production vs staging), which usually only happens during a staged rollout
    -- unless it is in `KNOWN_CROSS_ENV_DIVERGENCES`.

The allowlists capture the accepted baseline and are expected to shrink over
time; the check prints any entry that is no longer a discrepancy so it can be
pruned. `--no-fail` downgrades failures to warnings for local runs.

The set of synchronized parameters (and their defaults) is obtained by booting
Materialize -- with an empty `system_parameter_defaults` and no LaunchDarkly SDK
key, so that `SHOW ALL` reports the compiled-in defaults rather than mzcompose's
CI overrides or LaunchDarkly-synced values -- and running `SHOW ALL` as
`mz_system`, then filtering out the per-session variables (which are not
synchronized) and `enable_launchdarkly` (the kill switch, which is explicitly
excluded from `iter_synced`).

Caveat: `SHOW ALL` hides a parameter that is gated behind a *disabled*
feature flag (`VarDefinition::require_feature_flag`). No system parameter uses
that today, but if one is ever added it would be silently omitted here and
should be enabled before running this check (or added to a small allowlist).
"""

import itertools
import math
import os
import re
from collections.abc import Iterable
from typing import Any

import launchdarkly_api  # type: ignore
from launchdarkly_api.api import feature_flags_api  # type: ignore

from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.ui import UIError
from materialize.version_list import get_latest_published_version

# Access token required for reading the LaunchDarkly configuration.
LAUNCHDARKLY_API_TOKEN = os.environ.get("LAUNCHDARKLY_API_TOKEN")

# The LaunchDarkly project that mirrors Materialize's system parameters.
# Overridable so the same check can be pointed at a different project.
LAUNCHDARKLY_PROJECT = os.environ.get("LAUNCHDARKLY_PROJECT", "default")

# The LaunchDarkly environments to check default divergence against. Flag
# *existence* is project-level (identical across environments), so the
# missing/stale checks do not depend on the environment; only the served
# default value can differ per environment. The first environment is treated as
# the production / cloud baseline that is compared against the compiled-in
# default (i.e. what self-managed deployments use).
LAUNCHDARKLY_ENVIRONMENTS = [
    env.strip()
    for env in os.environ.get("LAUNCHDARKLY_ENVIRONMENTS", "production,staging").split(
        ","
    )
    if env.strip()
]
PRODUCTION_ENVIRONMENT = (
    LAUNCHDARKLY_ENVIRONMENTS[0] if LAUNCHDARKLY_ENVIRONMENTS else ""
)

SERVICES = [
    # Booted in unsafe mode so that internal and unsafe parameters are also
    # visible via `SHOW ALL` as `mz_system`. The image is overridden per run to
    # also collect parameters from the last published release.
    Materialized(),
]

# Per-session variables returned by `SHOW ALL` that are *not* synchronized from
# LaunchDarkly and therefore must not be expected to have a flag.
#
# Keep this in sync with the session variables defined in
# `src/sql/src/session/vars.rs`:
#   * the `vars` array in `SessionVars::new`,
#   * the `SESSION_SYSTEM_VARS` set, and
#   * the `mz_version` / `is_superuser` pseudo-variables added by
#     `SessionVars::iter`.
# Matched case-insensitively, so the historical capitalization of e.g.
# `DateStyle` does not matter.
SESSION_VARIABLES = {
    # SessionVars::new pure-session variables.
    "failpoints",
    "server_version",
    "server_version_num",
    "sql_safe_updates",
    "real_time_recency",
    "emit_plan_insights_notice",
    "emit_timestamp_notice",
    "emit_trace_id_notice",
    "auto_route_catalog_queries",
    "enable_session_rbac_checks",
    "restrict_to_user_objects",
    "enable_session_cardinality_estimates",
    "max_identifier_length",
    "statement_logging_sample_rate",
    "emit_introspection_query_notice",
    "unsafe_new_transaction_wall_time",
    "welcome_message",
    # SESSION_SYSTEM_VARS.
    "application_name",
    "client_encoding",
    "client_min_messages",
    "cluster",
    "cluster_replica",
    "default_cluster_replication_factor",
    "current_object_missing_warnings",
    "database",
    "datestyle",
    "extra_float_digits",
    "integer_datetimes",
    "intervalstyle",
    "real_time_recency_timeout",
    "search_path",
    "standard_conforming_strings",
    "statement_timeout",
    "idle_in_transaction_session_timeout",
    "timezone",
    "transaction_isolation",
    "max_query_result_size",
    # Pseudo-variables added by SessionVars::iter.
    "mz_version",
    "is_superuser",
    # The LaunchDarkly kill switch, explicitly excluded from iter_synced.
    "enable_launchdarkly",
}

# Tag used by `test/launchdarkly` and `ci/cleanup/launchdarkly.py` for the
# throwaway flags created during the integration test. These are never real
# system parameters, so we ignore them when warning about stale flags.
CI_TEST_TAG = "ci-test"

# Known synchronized parameters that intentionally have no LaunchDarkly flag.
# Should normally be empty; add a name here (with a comment explaining why) only
# if a parameter is deliberately not managed via LaunchDarkly.
IGNORED_MZ_PARAMETERS: set[str] = set()

# --- Known-exceptions allowlists ---------------------------------------------
#
# The check fails on every discrepancy that is NOT in one of these allowlists,
# so the lists capture the accepted baseline and the build only goes red on a
# *new* discrepancy. They are expected to shrink over time as flags are created
# in LaunchDarkly or removed. Regenerate from a nightly run when they drift; the
# check prints any allowlist entry that is no longer a discrepancy so it can be
# pruned.

# Parameters that exist in Materialize but have no LaunchDarkly flag (and are
# knowingly left unsynchronized). A newly added parameter that is missing from
# LaunchDarkly fails the build unless it is added here.
KNOWN_MISSING_FROM_LD: set[str] = set("""
    0dt_deployment_hydration_check_interval
    arrangement_exert_proportionality
    arrangement_size_history_retention_period
    cluster_alter_check_ready_interval
    cluster_check_scheduling_policies_interval
    cluster_enable_topology_spread
    cluster_multi_process_replica_az_affinity_weight
    cluster_soften_az_affinity
    cluster_soften_az_affinity_weight
    cluster_soften_replication_anti_affinity
    cluster_soften_replication_anti_affinity_weight
    cluster_topology_spread_ignore_non_singular_scale
    cluster_topology_spread_max_skew
    cluster_topology_spread_soft
    compute_correction_v2_chain_proportionality
    compute_correction_v2_chunk_size
    compute_flat_map_fuel
    compute_logical_backpressure_max_retained_capabilities
    compute_mv_sink_advance_persist_frontiers
    compute_peek_response_stash_batch_max_runs
    compute_peek_response_stash_read_batch_size_bytes
    compute_peek_response_stash_read_memory_budget_bytes
    compute_temporal_bucketing_summary
    console_oidc_client_id
    console_oidc_scopes
    controller_past_generation_replica_cleanup_retry_interval
    coord_slow_message_warn_threshold
    copy_to_s3_arrow_builder_buffer_ratio
    copy_to_s3_multipart_part_size_bytes
    copy_to_s3_parquet_row_group_file_ratio
    crdb_connect_timeout
    crdb_keepalives_idle
    crdb_keepalives_interval
    crdb_keepalives_retries
    crdb_tcp_user_timeout
    default_timestamp_interval
    disallow_unmaterializable_functions_as_of
    enable_0dt_caught_up_replica_status_check
    enable_0dt_deployment_panic_after_timeout
    enable_alter_table_add_column
    enable_arrangement_dictionary_compression_alpha
    enable_binary_date_bin
    enable_coalesce_case_transform
    enable_compute_half_join2
    enable_compute_render_fueled_as_specific_collection
    enable_date_bin_hopping
    enable_default_connection_validation
    enable_dequadratic_eqprop_map
    enable_envelope_materialize
    enable_eq_classes_withholding_errors
    enable_frontend_subscribes
    enable_glue_schema_registry
    enable_introspection_subscribes
    enable_kafka_broker_matching_rules
    enable_less_reduce_in_eqprop
    enable_list_length_max
    enable_list_n_layers
    enable_list_remove
    enable_load_generator_clock
    enable_load_generator_counter
    enable_load_generator_datums
    enable_managed_cluster_availability_zones
    enable_notices_for_equals_null
    enable_notices_for_index_already_exists
    enable_notices_for_index_empty_key
    enable_off_thread_optimization
    enable_password_auth
    enable_paused_cluster_readhold_downgrade
    enable_persist_streaming_compaction
    enable_persist_streaming_snapshot_and_fetch
    enable_primary_key_not_enforced
    enable_projection_pushdown_after_relation_cse
    enable_public_metrics_endpoint
    enable_raise_statement
    enable_rbac_checks
    enable_redacted_test_option
    enable_repeat_row
    enable_repeat_row_non_negative
    enable_replica_targeted_materialized_views
    enable_s3_tables_region_check
    enable_session_timelines
    enable_simplify_quantified_comparisons
    enable_time_at_time_zone
    enable_unlimited_retain_history
    enable_will_distinct_propagation
    enable_with_ordinality_legacy_fallback
    grpc_client_connect_timeout
    kafka_buffered_event_resize_threshold_elements
    kafka_default_aws_privatelink_endpoint_identification_algorithm
    kafka_poll_max_wait
    kafka_reconnect_backoff
    kafka_reconnect_backoff_max
    kafka_retry_backoff
    kafka_retry_backoff_max
    kafka_sink_batch_num_messages
    kafka_socket_keepalive
    keep_n_privatelink_status_history_entries
    keep_n_sink_status_history_entries
    keep_n_source_status_history_entries
    log_filter_defaults
    max_copy_from_row_size
    max_network_policies
    max_rules_per_network_policy
    max_sql_server_connections
    max_timestamp_interval
    mcp_max_response_size
    memory_limiter_usage_bias
    mysql_replication_heartbeat_interval
    mysql_source_connect_timeout
    mysql_source_snapshot_lock_wait_timeout
    mysql_source_snapshot_max_execution_time
    mysql_source_tcp_keepalive
    network_policy
    oidc_audience
    oidc_authentication_claim
    oidc_group_claim
    oidc_group_role_sync_strict
    oidc_issuer
    opentelemetry_filter_defaults
    optimizer_e2e_latency_warning_threshold
    persist_blob_cache_scale_factor_bytes
    persist_blob_cache_scale_with_threads
    persist_blob_connect_timeout
    persist_blob_operation_attempt_timeout
    persist_blob_operation_timeout
    persist_blob_read_timeout
    persist_catalog_force_compaction_wait
    persist_claim_compaction_percent
    persist_compaction_check_process_flag
    persist_compaction_heuristic_min_inputs
    persist_compaction_heuristic_min_parts
    persist_compaction_heuristic_min_updates
    persist_consensus_connection_pool_max_wait
    persist_consensus_connection_pool_ttl
    persist_consensus_connection_pool_ttl_stagger
    persist_expression_cache_force_compaction_fuel
    persist_expression_cache_force_compaction_wait
    persist_fast_path_order
    persist_fetch_semaphore_cost_adjustment
    persist_fetch_semaphore_permit_adjustment
    persist_gc_fallback_threshold_ms
    persist_gc_min_versions
    persist_pubsub_client_receiver_channel_size
    persist_pubsub_client_sender_channel_size
    persist_pubsub_connect_attempt_timeout
    persist_pubsub_connect_max_backoff
    persist_pubsub_reconnect_backoff
    persist_pubsub_request_timeout
    persist_pubsub_same_process_delegate_enabled
    persist_pubsub_server_connection_channel_size
    persist_pubsub_state_cache_shard_ref_channel_size
    persist_rollup_fallback_threshold_ms
    persist_state_update_lease_timeout
    persist_state_versions_recent_live_diffs_limit
    persist_stats_audit_panic
    persist_stats_budget_bytes
    persist_stats_untrimmable_columns_equals
    persist_stats_untrimmable_columns_prefix
    persist_txns_data_shard_retryer_clamp
    persist_txns_data_shard_retryer_multiplier
    persist_usage_state_fetch_concurrency_limit
    persist_use_critical_since_txn
    persist_use_postgres_tuned_queries
    persist_write_combine_inline_writes
    pg_source_connect_timeout
    pg_source_snapshot_statement_timeout
    pg_source_tcp_configure_server
    pg_source_tcp_keepalives_idle
    pg_source_tcp_keepalives_interval
    pg_source_tcp_keepalives_retries
    pg_source_validate_timeline
    pg_source_wal_sender_timeout
    pg_timestamp_oracle_connection_pool_max_size
    pg_timestamp_oracle_connection_pool_max_wait
    pg_timestamp_oracle_connection_pool_ttl
    pg_timestamp_oracle_connection_pool_ttl_stagger
    plan_insights_notice_fast_path_clusters_optimize_duration
    postgres_fetch_slot_resume_lsn_interval
    privatelink_status_update_quota_per_minute
    replica_metrics_history_retention_interval
    replica_status_history_retention_window
    scram_iterations
    sentry_filters
    sql_server_cdc_cleanup_change_table
    sql_server_cdc_cleanup_change_table_max_deletes
    sql_server_max_lsn_wait
    sql_server_snapshot_progress_report_interval
    sql_server_source_validate_restore_history
    ssh_check_interval
    ssh_connect_timeout
    ssh_keepalives_idle
    statement_logging_use_reproducible_rng
    storage_cluster_shutdown_grace_period
    storage_downgrade_since_during_finalization
    storage_record_source_sink_namespaced_errors
    storage_rocksdb_cleanup_tries
    storage_server_maintenance_interval
    storage_sink_ensure_topic_config
    storage_sink_progress_search
    storage_statistics_retention_duration
    storage_suspend_and_restart_delay
    storage_upsert_max_snapshot_batch_buffering
    storage_upsert_prevent_snapshot_buffering
    superuser_reserved_connections
    txn_wal_apply_ensure_schema_match
    unsafe_enable_table_check_constraint
    unsafe_enable_table_foreign_key
    unsafe_enable_table_keys
    unsafe_enable_unorchestrated_cluster_replicas
    unsafe_enable_unsafe_functions
    unsafe_enable_unstable_dependencies
    unsafe_mock_audit_event_timestamp
    upsert_rocksdb_compaction_style
    upsert_rocksdb_compression_type
    upsert_rocksdb_level_compaction_dynamic_level_bytes
    upsert_rocksdb_point_lookup_block_cache_size_mb
    upsert_rocksdb_retry_duration
    upsert_rocksdb_stats_log_interval_seconds
    upsert_rocksdb_stats_persist_interval_seconds
    upsert_rocksdb_universal_compaction_ratio
    upsert_rocksdb_write_buffer_manager_allow_stall
    upsert_rocksdb_write_buffer_manager_cluster_memory_fraction
    upsert_rocksdb_write_buffer_manager_memory_bytes
    user_id_pool_batch_size
    wallclock_global_lag_histogram_retention_interval
    wallclock_lag_histogram_period_interval
    wallclock_lag_history_retention_interval
    webhooks_secrets_caching_ttl_secs
    with_0dt_caught_up_check_allowed_lag
    with_0dt_caught_up_check_cutoff
    with_0dt_deployment_ddl_check_interval
    """.split())

# LaunchDarkly flags that are no longer synchronized parameters (in the current
# build or last release) but are knowingly kept (e.g. awaiting archival). A new
# stale flag fails the build unless it is added here.
KNOWN_STALE_LD_FLAGS: set[str] = set("""
    allowed_cloud_regions
    balancerd_inject_proxy_protocol_header_http
    balancerd_log_filter
    cluster_always_use_disk
    clusterd_malloc_conf
    constraint_based_timestamp_selection
    enable_0dt_deployment
    enable_aws_msk_iam_auth
    enable_consolidate_after_union_negate
    enable_continual_task_builtins
    enable_continual_task_transform
    enable_copy_from_remote
    enable_copy_to_expr
    enable_explain_broken
    enable_iceberg_sink
    enable_kafka_sink_partition_by
    enable_mcp_agents
    enable_mcp_agents_query_tool
    enable_mcp_observatory
    enable_multi_replica_sources
    enable_reduce_reduction
    enable_repr_typecheck
    enable_unified_cluster_arrangment
    enable_yugabyte_connection
    kafka_default_metadata_fetch_interval
    mysql_offset_known_interval
    persist_enable_arrow_lgalloc_noncc_sizes
    persist_enable_s3_lgalloc_cc_sizes
    persist_enable_s3_lgalloc_noncc_sizes
    persist_incremental_compaction_disabled
    persist_sink_minimum_batch_updates
    pg_offset_known_interval
    storage_reclock_to_latest
    use_global_txn_cache_source
    wait_catalog_consolidation_on_startup
    """.split())

# Parameters whose production LaunchDarkly value is *deliberately* different from
# the compiled-in default, because they tune cloud-specific infrastructure that
# does not apply to self-managed deployments. Excluded from the "cloud vs
# self-managed" divergence failure (but still subject to the cross-environment
# check). Everything else that diverges fails, so a feature accidentally left
# off in self-managed is caught.
INTENTIONAL_LD_OVERRIDES: set[str] = {
    # Cloud replica expiration (confirmed cloud-only behavior).
    "compute_replica_expiration_offset",
    "enable_compute_replica_expiration",
    # Cloud resource quotas / available replica sizes.
    "allowed_cluster_replica_sizes",
    "max_clusters",
    "max_connections",
    "max_credit_consumption_rate",
    "max_aws_privatelink_connections",
    "max_materialized_views",
    "max_sources",
    "max_tables",
}

# Flags whose served default knowingly differs between LaunchDarkly environments
# (e.g. a long-running staged rollout). A new cross-environment difference fails
# the build unless it is added here.
KNOWN_CROSS_ENV_DIVERGENCES: set[str] = set()


def synced_parameters(c: Composition) -> dict[str, str]:
    """Return the synchronized system parameters of the running `materialized`
    service as a mapping from name to its (default) value, derived from
    `SHOW ALL` as `mz_system`."""
    # 6877 is the internal SQL port, on which we can connect as `mz_system`.
    rows = c.sql_query("SHOW ALL", user="mz_system", port=6877)
    return {
        name: value
        for name, value, *_ in rows
        if name.lower() not in SESSION_VARIABLES and name not in IGNORED_MZ_PARAMETERS
    }


def collect_synced_parameters(
    c: Composition, image: str | None, label: str
) -> dict[str, str] | None:
    """Boot `materialized` (optionally pinned to `image`) and collect its
    synchronized parameters. Returns `None` if the service could not be booted,
    which is treated as best-effort for older releases."""
    try:
        # Start from a clean slate: the previous-release binary cannot read
        # persist/catalog state written by the newer current build (and vice
        # versa), so destroy any volumes left over from an earlier collection.
        c.down(destroy_volumes=True, sanity_restart_mz=False)
        with c.override(
            Materialized(
                image=image,
                sanity_restart=False,
                # Read Materialize's *compiled-in* defaults: pass an empty
                # `system_parameter_defaults` so mzcompose does not apply its CI
                # test overrides, and don't set an SDK key so the instance does
                # not sync from LaunchDarkly. `SHOW ALL` then reflects the value
                # environmentd falls back to when a flag is absent from
                # LaunchDarkly, which is what the divergence check compares
                # against.
                system_parameter_defaults={},
            )
        ):
            c.up("materialized")
            params = synced_parameters(c)
            c.stop("materialized")
        print(f"Collected {len(params)} synchronized parameters from {label}")
        return params
    except Exception as e:
        print(f"WARNING: could not collect parameters from {label}: {e}")
        return None


def ld_served_default(flag: dict[str, Any], environment: str) -> Any | None:
    """Return the value LaunchDarkly serves to an unmatched context in the given
    environment, or `None` if it cannot be determined (e.g. a percentage
    rollout). Mirrors what `client.variation` returns for a context that matches
    no specific targeting rule."""
    variations = flag.get("variations") or []
    env = (flag.get("environments") or {}).get(environment)
    if env is None:
        return None
    if env.get("on"):
        index = (env.get("fallthrough") or {}).get("variation")
    else:
        index = env.get("off_variation")
    if index is None or not (0 <= index < len(variations)):
        return None
    return variations[index].get("value")


def launchdarkly_flag_tags(api: Any) -> dict[str, list[str]]:
    """Return every flag in the configured LaunchDarkly project as a mapping
    from flag key to its tags. Uses the (summary) list endpoint, which is cheap
    but does not include per-environment targeting."""
    flags: dict[str, list[str]] = {}
    limit = 100
    offset = 0
    while True:
        page = api.get_feature_flags(
            LAUNCHDARKLY_PROJECT,
            summary=True,
            limit=limit,
            offset=offset,
        ).to_dict()
        items = page.get("items") or []
        for flag in items:
            flags[flag["key"]] = list(flag.get("tags") or [])
        total = page.get("total_count")
        offset += len(items)
        if not items or (total is not None and offset >= total):
            break
    return flags


def launchdarkly_served_defaults(
    api: Any, keys: Iterable[str]
) -> tuple[dict[str, dict[str, Any | None]], set[str]]:
    """Return, for each requested flag key, the value LaunchDarkly serves by
    default per checked environment. Fetches each flag individually because the
    list endpoint only returns summaries, not the per-environment targeting
    needed to determine the served default. Also returns the set of environment
    keys actually seen, so a misconfigured environment name can be detected."""
    defaults: dict[str, dict[str, Any | None]] = {}
    env_keys_seen: set[str] = set()
    for key in sorted(keys):
        try:
            flag = api.get_feature_flag(LAUNCHDARKLY_PROJECT, key).to_dict()
        except launchdarkly_api.ApiException as e:
            print(f"WARNING: could not fetch LaunchDarkly flag {key!r}: {e}")
            continue
        env_keys_seen.update((flag.get("environments") or {}).keys())
        defaults[key] = {
            environment: ld_served_default(flag, environment)
            for environment in LAUNCHDARKLY_ENVIRONMENTS
        }
    return defaults, env_keys_seen


# Time units accepted in duration values, expressed in seconds. Covers both the
# spaced form printed by Materialize's SHOW (e.g. "1 min", "30 d") and the
# humantime form stored in LaunchDarkly (e.g. "60s", "525600min", "1200ms").
_DURATION_UNITS_SECONDS = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "sec": 1.0,
    "secs": 1.0,
    "second": 1.0,
    "seconds": 1.0,
    "m": 60.0,
    "min": 60.0,
    "mins": 60.0,
    "minute": 60.0,
    "minutes": 60.0,
    "h": 3600.0,
    "hr": 3600.0,
    "hour": 3600.0,
    "hours": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
    "days": 86400.0,
}

_DURATION_TOKEN = re.compile(r"(\d+(?:\.\d+)?)\s*([a-zµ]+)")


def parse_duration_seconds(
    value: str, assume_bare_millis: bool = False
) -> float | None:
    """Parse a duration string into seconds, or return `None` if it is not a
    duration. Handles Materialize's spaced form ("1 min"), humantime's compact
    form ("525600min", "1h30m"), and -- when `assume_bare_millis` is set -- a
    bare number, which Materialize interprets as milliseconds for duration
    parameters (this is how some durations are stored in LaunchDarkly, e.g.
    "600000" for 10 minutes)."""
    v = value.strip().lower()
    if not v:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", v):
        return float(v) / 1000.0 if assume_bare_millis else None
    tokens = _DURATION_TOKEN.findall(v)
    # Require the whole string to be made of duration tokens, so that e.g. a
    # cluster size like "M.1-8xlarge" is not mistaken for a duration.
    if not tokens or "".join(f"{n}{u}" for n, u in tokens) != v.replace(" ", ""):
        return None
    total = 0.0
    for number, unit in tokens:
        if unit not in _DURATION_UNITS_SECONDS:
            return None
        total += float(number) * _DURATION_UNITS_SECONDS[unit]
    return total


# Byte units used by Materialize's ByteSize (1024-based, units B/kB/MB/GB/TB;
# see src/repr/src/bytes.rs). LaunchDarkly stores byte sizes as the raw integer.
_BYTE_UNITS = {
    "b": 1,
    "kb": 1024,
    "mb": 1024**2,
    "gb": 1024**3,
    "tb": 1024**4,
}

_BYTE_TOKEN = re.compile(r"(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|tb)", re.IGNORECASE)


def parse_bytes(value: str, assume_bare_bytes: bool = False) -> int | None:
    """Parse a byte size into a number of bytes, or return `None` if it is not a
    byte size. Handles Materialize's `ByteSize` form ("1GB", "4294967295B";
    1024-based) and -- when `assume_bare_bytes` is set -- a bare number, which is
    how LaunchDarkly stores byte sizes (e.g. 1073741824 for "1GB")."""
    v = value.strip()
    if not v:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", v):
        return round(float(v)) if assume_bare_bytes else None
    m = _BYTE_TOKEN.fullmatch(v)
    if not m:
        return None
    return round(float(m.group(1)) * _BYTE_UNITS[m.group(2).lower()])


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"on", "true", "t", "yes", "1"}:
        return True
    if s in {"off", "false", "f", "no", "0"}:
        return False
    return None


def values_equivalent(a: Any, b: Any) -> bool | None:
    """Whether two values represent the same setting. Durations ("1 min" vs
    "60s", "10 s" vs the raw-milliseconds "10000") and byte sizes ("1GB" vs the
    raw byte count 1073741824) are compared by value, so representation
    differences are not treated as differences. `a` and `b` may be Materialize's
    `SHOW` strings or LaunchDarkly's typed values.

    Returns `None` when the two cannot be compared confidently: when one side is
    missing, or one side is a bool/duration/byte-size the other cannot be parsed
    as."""
    if a is None or b is None:
        return None
    if isinstance(a, bool) or isinstance(b, bool):
        ba, bb = _as_bool(a), _as_bool(b)
        if ba is None or bb is None:
            return None
        return ba == bb
    sa, sb = str(a).strip(), str(b).strip()
    if sa == "" or sb == "":
        # Only equal if both are empty; an empty default vs a set value differs.
        return sa == sb
    da, db = parse_duration_seconds(sa), parse_duration_seconds(sb)
    if da is not None or db is not None:
        # At least one side is a duration with a unit; parse a bare number on
        # the other side as milliseconds (how Materialize reads it).
        if da is None:
            da = parse_duration_seconds(sa, assume_bare_millis=True)
        if db is None:
            db = parse_duration_seconds(sb, assume_bare_millis=True)
        if da is None or db is None:
            return None
        return math.isclose(da, db, rel_tol=1e-9, abs_tol=1e-12)
    ya, yb = parse_bytes(sa), parse_bytes(sb)
    if ya is not None or yb is not None:
        # At least one side is a byte size with a unit; parse a bare number on
        # the other side as raw bytes (how LaunchDarkly stores it).
        if ya is None:
            ya = parse_bytes(sa, assume_bare_bytes=True)
        if yb is None:
            yb = parse_bytes(sb, assume_bare_bytes=True)
        if ya is None or yb is None:
            return None
        return ya == yb
    try:
        return float(sa) == float(sb)
    except ValueError:
        return sa == sb


def report(title: str, names: Iterable[str]) -> None:
    print(f"--- {title}")
    for name in sorted(names):
        print(f"  {name}")


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--no-fail",
        action="store_true",
        help="Report discrepancies but do not fail (warnings only). For local "
        "runs; CI relies on the failure to catch regressions.",
    )
    args = parser.parse_args()

    if LAUNCHDARKLY_API_TOKEN is None:
        raise UIError("Missing LAUNCHDARKLY_API_TOKEN environment variable")

    # 1. Synchronized parameters (and defaults) of the current build. Mandatory:
    #    this drives the error direction of the check.
    current = collect_synced_parameters(c, image=None, label="current build")
    if current is None:
        raise UIError("Could not boot the current build to collect parameters")

    # 2. Synchronized parameters of the last published release. Best-effort:
    #    used only to avoid warning about flags that a deployed older version
    #    still relies on. `MzVersion`'s string already carries the `v` prefix.
    try:
        previous_version = get_latest_published_version()
        previous_image = f"materialize/materialized:{previous_version}"
        previous = collect_synced_parameters(
            c, image=previous_image, label=f"release {previous_version}"
        )
    except Exception as e:
        print(f"WARNING: could not determine the last published release: {e}")
        previous = None

    known = set(current)
    if previous is not None:
        known |= set(previous)

    # 3. Flags in LaunchDarkly: keys+tags from the list endpoint, and served
    #    defaults (per environment) fetched individually for the flags present
    #    in both -- the list endpoint only returns summaries.
    try:
        with launchdarkly_api.ApiClient(
            launchdarkly_api.Configuration(api_key=dict(ApiKey=LAUNCHDARKLY_API_TOKEN))
        ) as api_client:
            api = feature_flags_api.FeatureFlagsApi(api_client)
            ld_tags = launchdarkly_flag_tags(api)
            ld_keys = set(ld_tags)
            in_both = sorted(
                name
                for name in current
                if name in ld_tags and CI_TEST_TAG not in ld_tags[name]
            )
            served_defaults, env_keys_seen = launchdarkly_served_defaults(api, in_both)
    except launchdarkly_api.ApiException as e:
        raise UIError(
            f"Error calling the LaunchDarkly API (status={e.status}, reason={e.reason})"
        )
    print(
        f"Found {len(ld_keys)} flags in LaunchDarkly project "
        f"'{LAUNCHDARKLY_PROJECT}'; {len(in_both)} present in both Materialize "
        f"and LaunchDarkly"
    )

    # Surface a misconfigured environment name: if none of the environments we
    # check exist on the fetched flags, the default-divergence result is
    # meaningless.
    missing_envs = [e for e in LAUNCHDARKLY_ENVIRONMENTS if e not in env_keys_seen]
    if env_keys_seen and missing_envs:
        print(
            f"WARNING: environment(s) {', '.join(missing_envs)} not found in "
            f"LaunchDarkly; available environments: "
            f"{', '.join(sorted(env_keys_seen))}. Set LAUNCHDARKLY_ENVIRONMENTS "
            f"to the correct keys for the default-divergence check."
        )

    # --- Compare, then fail on anything outside the known-exceptions lists ---

    # Parameters in Materialize that are missing in LaunchDarkly.
    missing_in_ld = set(current) - ld_keys
    unexpected_missing = missing_in_ld - KNOWN_MISSING_FROM_LD

    # Flags in LaunchDarkly that are no longer synchronized parameters in the
    # current build or the last release (ignoring throwaway CI test flags).
    stale_in_ld = {key for key in ld_keys - known if CI_TEST_TAG not in ld_tags[key]}
    unexpected_stale = stale_in_ld - KNOWN_STALE_LD_FLAGS

    # Cloud vs self-managed: the production LaunchDarkly value differs from the
    # compiled-in default that self-managed deployments run on. Deliberate
    # cloud-only tuning is allowlisted via INTENTIONAL_LD_OVERRIDES; everything
    # else is unexpected (e.g. a feature enabled in cloud but off by default in
    # self-managed, which should be reconciled in the compiled-in default).
    cloud_vs_self_managed: dict[str, tuple[str, Any]] = {}
    for name in in_both:
        if name in INTENTIONAL_LD_OVERRIDES:
            continue
        prod_value = served_defaults.get(name, {}).get(PRODUCTION_ENVIRONMENT)
        if values_equivalent(current[name], prod_value) is False:
            cloud_vs_self_managed[name] = (current[name], prod_value)

    # Flags whose served default differs *between* LaunchDarkly environments
    # (e.g. production vs staging), which usually only happens during a staged
    # rollout. Long-running ones are allowlisted via KNOWN_CROSS_ENV_DIVERGENCES.
    env_divergences: dict[str, dict[str, Any]] = {}
    for name in in_both:
        if name in KNOWN_CROSS_ENV_DIVERGENCES:
            continue
        per_env = served_defaults.get(name, {})
        differs = any(
            values_equivalent(per_env.get(a), per_env.get(b)) is False
            for a, b in itertools.combinations(LAUNCHDARKLY_ENVIRONMENTS, 2)
        )
        if differs:
            env_divergences[name] = {
                e: per_env.get(e) for e in LAUNCHDARKLY_ENVIRONMENTS
            }

    print(
        "Discrepancies beyond the known-exceptions allowlists: "
        f"{len(unexpected_missing)} missing, {len(unexpected_stale)} stale, "
        f"{len(cloud_vs_self_managed)} cloud-vs-self-managed, "
        f"{len(env_divergences)} cross-environment."
    )

    if unexpected_missing:
        report(
            "ERROR: synchronized parameters missing in LaunchDarkly "
            "(add an LD flag, or add to KNOWN_MISSING_FROM_LD)",
            unexpected_missing,
        )

    if unexpected_stale:
        report(
            "ERROR: stale LaunchDarkly flags -- no longer a synchronized "
            "parameter in the current build or last release (archive in "
            "LaunchDarkly, or add to KNOWN_STALE_LD_FLAGS)",
            unexpected_stale,
        )

    if cloud_vs_self_managed:
        print(
            f"--- ERROR: flags whose cloud default ('{PRODUCTION_ENVIRONMENT}') "
            f"differs from the self-managed compiled-in default"
        )
        for name in sorted(cloud_vs_self_managed):
            mz_value, prod_value = cloud_vs_self_managed[name]
            print(f"  {name}: self-managed={mz_value!r} cloud={prod_value!r}")
        print(
            "Reconcile the compiled-in default, or -- if this is intentional "
            "cloud-only tuning -- add the flag to INTENTIONAL_LD_OVERRIDES."
        )

    if env_divergences:
        print(
            "--- ERROR: flags whose LaunchDarkly default differs between "
            f"environments ({', '.join(LAUNCHDARKLY_ENVIRONMENTS)})"
        )
        for name in sorted(env_divergences):
            rendered = ", ".join(
                f"{env}={value!r}" for env, value in env_divergences[name].items()
            )
            print(f"  {name}: {rendered}")
        print(
            "Make the environments agree, or -- if this is a deliberate staged "
            "rollout -- add the flag to KNOWN_CROSS_ENV_DIVERGENCES."
        )

    # Allowlist entries that are no longer discrepancies, so they can be pruned.
    resolved_missing = sorted(KNOWN_MISSING_FROM_LD - missing_in_ld)
    resolved_stale = sorted(KNOWN_STALE_LD_FLAGS - stale_in_ld)
    if resolved_missing or resolved_stale:
        print(
            "--- NOTE: known-exception entries that no longer apply and can be "
            "pruned from the allowlists"
        )
        for name in resolved_missing:
            print(f"  KNOWN_MISSING_FROM_LD: {name}")
        for name in resolved_stale:
            print(f"  KNOWN_STALE_LD_FLAGS: {name}")

    failures = {
        "missing": unexpected_missing,
        "stale": unexpected_stale,
        "cloud-vs-self-managed": set(cloud_vs_self_managed),
        "cross-environment": set(env_divergences),
    }
    total = sum(len(v) for v in failures.values())
    if total:
        summary = ", ".join(f"{len(v)} {k}" for k, v in failures.items() if v)
        message = (
            f"{total} unexpected LaunchDarkly discrepancy/ies ({summary}). "
            "Resolve them, or add to the appropriate known-exceptions allowlist."
        )
        if args.no_fail:
            print(f"WARNING: {message}")
        else:
            raise UIError(message)
    else:
        print(
            "No unexpected discrepancies: every difference is covered by the "
            "known-exceptions allowlists."
        )
