# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The implementation of the mzcompose system for Docker compositions.

For an overview of what mzcompose is and why it exists, see the [user-facing
documentation][user-docs].

[user-docs]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/mzbuild.md
"""

import os
import random
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

import psycopg

from materialize import spawn, ui
from materialize.mz_version import MzVersion
from materialize.ui import UIError

T = TypeVar("T")
say = ui.speaker("C> ")


DEFAULT_CONFLUENT_PLATFORM_VERSION = "7.9.0"

DEFAULT_MZ_VOLUMES = [
    "mzdata:/mzdata",
    "mydata:/var/lib/mysql-files",
    "tmp:/share/tmp",
    "scratch:/scratch",
]


# Parameters which disable systems that periodically/unpredictably impact performance
ADDITIONAL_BENCHMARKING_SYSTEM_PARAMETERS = {
    "enable_statement_lifecycle_logging": "false",
    "persist_catalog_force_compaction_fuel": "0",
    "statement_logging_default_sample_rate": "0",
    "statement_logging_max_sample_rate": "0",
    # Default of 128 MB increases memory usage by a lot for some small
    # performance in benchmarks, see for example FastPathLimit scenario: 55%
    # more memory, 5% faster
    "persist_blob_cache_mem_limit_bytes": "1048576",
    # This would increase the memory usage of many tests, making it harder to
    # tell small memory increase regressions
    "persist_blob_cache_scale_with_threads": "false",
    # The peek response stash kicks in when results get larger, and it
    # increases query latency. Which in turn makes benchmarking more
    # unpredictable.
    "enable_compute_peek_response_stash": "false",
}


def get_minimal_system_parameters(
    version: MzVersion,
    zero_downtime: bool = False,
) -> dict[str, str]:
    """Settings we need in order to have tests run at all, but otherwise stay
    with the defaults: not changing performance or increasing coverage."""

    return {
        # -----
        # Unsafe functions
        "unsafe_enable_unsafe_functions": "true",
        # -----
        # Others (ordered by name)
        "allow_real_time_recency": "true",
        "constraint_based_timestamp_selection": "verify",
        "enable_compute_peek_response_stash": "true",
        "enable_0dt_deployment": "true" if zero_downtime else "false",
        "enable_0dt_deployment_panic_after_timeout": "true",
        "enable_0dt_deployment_sources": (
            "true" if version >= MzVersion.parse_mz("v0.132.0-dev") else "false"
        ),
        "enable_alter_swap": "true",
        "enable_columnation_lgalloc": "true",
        "enable_compute_active_dataflow_cancelation": "true",
        "enable_compute_correction_v2": "true",
        "enable_compute_logical_backpressure": "true",
        "enable_connection_validation_syntax": "true",
        "enable_continual_task_create": "true",
        "enable_continual_task_retain": "true",
        "enable_continual_task_transform": "true",
        "enable_copy_to_expr": "true",
        "enable_create_table_from_source": "true",
        "enable_disk_cluster_replicas": "true",
        "enable_eager_delta_joins": "true",
        "enable_envelope_debezium_in_subscribe": "true",
        "enable_expressions_in_limit_syntax": "true",
        "enable_introspection_subscribes": "true",
        "enable_kafka_sink_partition_by": "true",
        "enable_logical_compaction_window": "true",
        "enable_multi_worker_storage_persist_sink": "true",
        "enable_multi_replica_sources": "true",
        "enable_rbac_checks": "true",
        "enable_reduce_mfp_fusion": "true",
        "enable_refresh_every_mvs": "true",
        "enable_cluster_schedule_refresh": "true",
        "enable_statement_lifecycle_logging": "true",
        "enable_variadic_left_join_lowering": "true",
        "enable_worker_core_affinity": "true",
        "grpc_client_http2_keep_alive_timeout": "5",
        "ore_overflowing_behavior": "panic",
        "persist_stats_audit_percent": "100",
        "unsafe_enable_table_keys": "true",
        "with_0dt_deployment_max_wait": "1800s",
        # End of list (ordered by name)
    }


@dataclass
class VariableSystemParameter:
    key: str
    default: str
    values: list[str]


# TODO: The linter should check this too
def get_variable_system_parameters(
    version: MzVersion,
    zero_downtime: bool,
    force_source_table_syntax: bool,
) -> list[VariableSystemParameter]:
    return [
        # -----
        # To reduce CRDB load as we are struggling with it in CI (values based on load test environment):
        VariableSystemParameter(
            "persist_next_listen_batch_retryer_clamp",
            "16s",
            ["100ms", "1s", "10s", "100s"],
        ),
        VariableSystemParameter(
            "persist_next_listen_batch_retryer_initial_backoff",
            "100ms",
            ["10ms", "100ms", "1s", "10s"],
        ),
        VariableSystemParameter(
            "persist_next_listen_batch_retryer_fixed_sleep",
            "1200ms",
            ["100ms", "1s", "10s"],
        ),
        # -----
        # Persist internals changes, advance coverage
        VariableSystemParameter(
            "persist_enable_arrow_lgalloc_noncc_sizes", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "persist_enable_s3_lgalloc_noncc_sizes", "true", ["true", "false"]
        ),
        # -----
        # Others (ordered by name),
        VariableSystemParameter("cluster_always_use_disk", "true", ["true", "false"]),
        VariableSystemParameter(
            "compute_dataflow_max_inflight_bytes",
            "134217728",
            ["1048576", "4194304", "16777216", "67108864"],
        ),  # 128 MiB
        VariableSystemParameter("compute_hydration_concurrency", "2", ["1", "2", "4"]),
        VariableSystemParameter(
            "compute_replica_expiration_offset", "3d", ["3d", "10d"]
        ),
        VariableSystemParameter(
            "compute_apply_column_demands", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "compute_peek_response_stash_threshold_bytes",
            # 1 MiB, an in-between value
            "1048576",
            # force-enabled, the in-between, and the production value
            ["0", "1048576", "314572800", "67108864"],
        ),
        VariableSystemParameter(
            "disk_cluster_replicas_default", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "kafka_default_metadata_fetch_interval",
            "1s",
            ["100ms", "1s"],
        ),
        VariableSystemParameter("mysql_offset_known_interval", "1s", ["100ms", "1s"]),
        VariableSystemParameter(
            "force_source_table_syntax",
            "true" if force_source_table_syntax else "false",
            ["true", "false"] if force_source_table_syntax else ["false"],
        ),
        VariableSystemParameter(
            "persist_batch_columnar_format",
            "structured" if version > MzVersion.parse_mz("v0.135.0-dev") else "both_v2",
            ["row", "both_v2", "both", "structured"],
        ),
        VariableSystemParameter(
            "persist_batch_delete_enabled", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "persist_batch_structured_order", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "persist_batch_builder_structured", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "persist_batch_structured_key_lower_len",
            "256",
            ["0", "1", "512", "1000"],
        ),
        VariableSystemParameter(
            "persist_batch_max_run_len", "4", ["2", "3", "4", "16"]
        ),
        VariableSystemParameter(
            "persist_catalog_force_compaction_fuel",
            "1024",
            ["256", "1024", "4096"],
        ),
        VariableSystemParameter(
            "persist_catalog_force_compaction_wait",
            "1s",
            ["100ms", "1s", "10s"],
        ),
        VariableSystemParameter(
            "persist_encoding_enable_dictionary", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "persist_fast_path_limit",
            "1000",
            ["100", "1000", "10000"],
        ),
        VariableSystemParameter("persist_fast_path_order", "true", ["true", "false"]),
        VariableSystemParameter(
            "persist_gc_use_active_gc",
            ("true" if version > MzVersion.parse_mz("v0.143.0-dev") else "false"),
            (
                ["true", "false"]
                if version > MzVersion.parse_mz("v0.127.0-dev")
                else ["false"]
            ),
        ),
        VariableSystemParameter(
            "persist_inline_writes_single_max_bytes",
            "4096",
            ["256", "1024", "4096", "16384"],
        ),
        VariableSystemParameter(
            "persist_inline_writes_total_max_bytes",
            "1048576",
            ["65536", "262144", "1048576", "4194304"],
        ),
        VariableSystemParameter(
            "persist_pubsub_client_enabled", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "persist_pubsub_push_diff_enabled", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "persist_record_compactions", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "persist_record_schema_id",
            ("true" if version > MzVersion.parse_mz("v0.127.0-dev") else "false"),
            (
                ["true", "false"]
                if version > MzVersion.parse_mz("v0.127.0-dev")
                else ["false"]
            ),
        ),
        VariableSystemParameter(
            "persist_rollup_use_active_rollup",
            ("true" if version > MzVersion.parse_mz("v0.143.0-dev") else "false"),
            (
                ["true", "false"]
                if version > MzVersion.parse_mz("v0.127.0-dev")
                else ["false"]
            ),
        ),
        # 16 MiB - large enough to avoid a big perf hit, small enough to get more coverage...
        VariableSystemParameter(
            "persist_blob_target_size",
            "16777216",
            ["4096", "1048576", "16777216", "134217728"],
        ),
        # 5 times the default part size - 4 is the bare minimum.
        VariableSystemParameter(
            "persist_compaction_memory_bound_bytes",
            "83886080",
            ["67108864", "134217728", "536870912", "1073741824"],
        ),
        VariableSystemParameter(
            "persist_use_critical_since_catalog", "true", ["true", "false"]
        ),
        VariableSystemParameter(
            "persist_use_critical_since_snapshot",
            "false" if zero_downtime else "true",
            ["false"] if zero_downtime else ["true", "false"],
        ),
        VariableSystemParameter(
            "persist_use_critical_since_source",
            "false" if zero_downtime else "true",
            ["false"] if zero_downtime else ["true", "false"],
        ),
        VariableSystemParameter(
            "persist_part_decode_format", "arrow", ["arrow", "row_with_validate"]
        ),
        VariableSystemParameter(
            "persist_blob_cache_scale_with_threads", "true", ["true", "false"]
        ),
        VariableSystemParameter("pg_offset_known_interval", "1s", ["100ms", "1s"]),
        VariableSystemParameter(
            "statement_logging_default_sample_rate", "0.01", ["0", "0.01"]
        ),
        VariableSystemParameter(
            "statement_logging_max_sample_rate", "0.01", ["0", "0.01"]
        ),
        VariableSystemParameter("storage_reclock_to_latest", "true", ["true", "false"]),
        VariableSystemParameter(
            "storage_source_decode_fuel",
            "100000",
            ["10000", "100000", "1000000"],
        ),
        VariableSystemParameter(
            "storage_statistics_collection_interval",
            "1000",
            ["100", "1000", "10000"],
        ),
        VariableSystemParameter(
            "storage_statistics_interval", "2000", ["100", "1000", "10000"]
        ),
        VariableSystemParameter(
            "storage_use_continual_feedback_upsert", "true", ["true", "false"]
        ),
        # End of list (ordered by name)
    ]


def get_default_system_parameters(
    version: MzVersion | None = None,
    zero_downtime: bool = False,
    force_source_table_syntax: bool = False,
) -> dict[str, str]:
    """For upgrade tests we only want parameters set when all environmentd /
    clusterd processes have reached a specific version (or higher)
    """

    if not version:
        version = MzVersion.parse_cargo()

    params = get_minimal_system_parameters(version, zero_downtime)

    system_param_setting = os.getenv("CI_SYSTEM_PARAMETERS", "")
    variable_params = get_variable_system_parameters(
        version, zero_downtime, force_source_table_syntax
    )

    if system_param_setting == "":
        for param in variable_params:
            params[param.key] = param.default
    elif system_param_setting == "random":
        seed = os.getenv("CI_SYSTEM_PARAMETERS_SEED", os.getenv("BUILDKITE_JOB_ID", 1))
        rng = random.Random(seed)
        for param in variable_params:
            params[param.key] = rng.choice(param.values)
        print(
            f"System parameters with seed CI_SYSTEM_PARAMETERS_SEED={seed}: {params}",
            file=sys.stderr,
        )
    elif system_param_setting == "minimal":
        pass
    else:
        raise ValueError(
            f"Unknown value for CI_SYSTEM_PARAMETERS: {system_param_setting}"
        )

    return params


# If you are adding a new config flag in Materialize, consider setting values
# for it in get_variable_system_parameters if it can be varied in tests. Set it
# in get_minimal_system_parameters if it's required for tests to succeed at
# all. Only add it in UNINTERESTING_SYSTEM_PARAMETERS if none of the above
# apply.
UNINTERESTING_SYSTEM_PARAMETERS = [
    "enable_mz_join_core",
    "enable_compute_mv_append_smearing",
    "linear_join_yielding",
    "enable_lgalloc",
    "enable_lgalloc_eager_reclamation",
    "lgalloc_background_interval",
    "lgalloc_file_growth_dampener",
    "lgalloc_local_buffer_bytes",
    "lgalloc_slow_clear_bytes",
    "lgalloc_limiter_interval",
    "lgalloc_limiter_usage_factor",
    "lgalloc_limiter_usage_bias",
    "lgalloc_limiter_burst_factor",
    "enable_columnar_lgalloc",
    "compute_server_maintenance_interval",
    "compute_dataflow_max_inflight_bytes_cc",
    "compute_flat_map_fuel",
    "consolidating_vec_growth_dampener",
    "copy_to_s3_parquet_row_group_file_ratio",
    "copy_to_s3_arrow_builder_buffer_ratio",
    "copy_to_s3_multipart_part_size_bytes",
    "enable_compute_replica_expiration",
    "enable_compute_render_fueled_as_specific_collection",
    "compute_logical_backpressure_max_retained_capabilities",
    "compute_logical_backpressure_inflight_slack",
    "persist_fetch_semaphore_cost_adjustment",
    "persist_fetch_semaphore_permit_adjustment",
    "persist_optimize_ignored_data_fetch",
    "persist_pubsub_same_process_delegate_enabled",
    "persist_pubsub_connect_attempt_timeout",
    "persist_pubsub_request_timeout",
    "persist_pubsub_connect_max_backoff",
    "persist_pubsub_client_sender_channel_size",
    "persist_pubsub_client_receiver_channel_size",
    "persist_pubsub_server_connection_channel_size",
    "persist_pubsub_state_cache_shard_ref_channel_size",
    "persist_pubsub_reconnect_backoff",
    "persist_encoding_compression_format",
    "persist_batch_max_runs",
    "persist_write_combine_inline_writes",
    "persist_reader_lease_duration",
    "persist_consensus_connection_pool_max_size",
    "persist_consensus_connection_pool_max_wait",
    "persist_consensus_connection_pool_ttl",
    "persist_consensus_connection_pool_ttl_stagger",
    "crdb_connect_timeout",
    "crdb_tcp_user_timeout",
    "persist_use_critical_since_txn",
    "use_global_txn_cache_source",
    "persist_batch_builder_max_outstanding_parts",
    "persist_compaction_heuristic_min_inputs",
    "persist_compaction_heuristic_min_parts",
    "persist_compaction_heuristic_min_updates",
    "persist_gc_blob_delete_concurrency_limit",
    "persist_state_versions_recent_live_diffs_limit",
    "persist_usage_state_fetch_concurrency_limit",
    "persist_blob_operation_timeout",
    "persist_blob_operation_attempt_timeout",
    "persist_blob_connect_timeout",
    "persist_blob_read_timeout",
    "persist_stats_collection_enabled",
    "persist_stats_filter_enabled",
    "persist_stats_budget_bytes",
    "persist_stats_untrimmable_columns_equals",
    "persist_stats_untrimmable_columns_prefix",
    "persist_stats_untrimmable_columns_suffix",
    "persist_expression_cache_force_compaction_fuel",
    "persist_expression_cache_force_compaction_wait",
    "persist_blob_cache_mem_limit_bytes",
    "persist_blob_cache_scale_factor_bytes",
    "persist_claim_unclaimed_compactions",
    "persist_claim_compaction_percent",
    "persist_claim_compaction_min_version",
    "persist_next_listen_batch_retryer_multiplier",
    "persist_rollup_threshold",
    "persist_rollup_fallback_threshold_ms",
    "persist_gc_fallback_threshold_ms",
    "persist_compaction_minimum_timeout",
    "persist_compaction_use_most_recent_schema",
    "persist_compaction_check_process_flag",
    "balancerd_sigterm_connection_wait",
    "balancerd_sigterm_listen_wait",
    "balancerd_inject_proxy_protocol_header_http",
    "balancerd_log_filter",
    "balancerd_opentelemetry_filter",
    "balancerd_log_filter_defaults",
    "balancerd_opentelemetry_filter_defaults",
    "balancerd_sentry_filters",
    "persist_enable_s3_lgalloc_cc_sizes",
    "persist_enable_arrow_lgalloc_cc_sizes",
    "controller_past_generation_replica_cleanup_retry_interval",
    "wallclock_lag_recording_interval",
    "enable_wallclock_lag_histogram_collection",
    "wallclock_lag_histogram_period_interval",
    "enable_timely_zero_copy",
    "enable_timely_zero_copy_lgalloc",
    "timely_zero_copy_limit",
    "arrangement_exert_proportionality",
    "txn_wal_apply_ensure_schema_match",
    "persist_txns_data_shard_retryer_initial_backoff",
    "persist_txns_data_shard_retryer_multiplier",
    "persist_txns_data_shard_retryer_clamp",
    "storage_cluster_shutdown_grace_period",
    "storage_dataflow_delay_sources_past_rehydration",
    "storage_dataflow_suspendable_sources",
    "storage_downgrade_since_during_finalization",
    "replica_metrics_history_retention_interval",
    "wallclock_lag_history_retention_interval",
    "wallclock_global_lag_histogram_retention_interval",
    "kafka_client_id_enrichment_rules",
    "kafka_poll_max_wait",
    "kafka_default_aws_privatelink_endpoint_identification_algorithm",
    "kafka_buffered_event_resize_threshold_elements",
    "mysql_replication_heartbeat_interval",
    "postgres_fetch_slot_resume_lsn_interval",
    "pg_schema_validation_interval",
    "storage_enforce_external_addresses",
    "storage_upsert_prevent_snapshot_buffering",
    "storage_rocksdb_use_merge_operator",
    "storage_upsert_max_snapshot_batch_buffering",
    "storage_rocksdb_cleanup_tries",
    "storage_suspend_and_restart_delay",
    "storage_sink_snapshot_frontier",
    "storage_server_maintenance_interval",
    "storage_sink_progress_search",
    "storage_sink_ensure_topic_config",
    "sql_server_snapshot_max_lsn_wait",
    "sql_server_snapshot_progress_report_interval",
    "sql_server_cdc_poll_interval",
    "sql_server_cdc_cleanup_change_table",
    "sql_server_cdc_cleanup_change_table_max_deletes",
    "sql_server_offset_known_interval",
    "allow_user_sessions",
    "with_0dt_deployment_ddl_check_interval",
    "enable_0dt_caught_up_check",
    "with_0dt_caught_up_check_allowed_lag",
    "with_0dt_caught_up_check_cutoff",
    "plan_insights_notice fast_path_clusters_optimize_duration",
    "enable_continual_task_builtins",
    "enable_expression_cache",
    "enable_password_auth",
    "mz_metrics_lgalloc_map_refresh_interval",
    "mz_metrics_lgalloc_refresh_interval",
    "mz_metrics_rusage_refresh_interval",
    "compute_peek_response_stash_batch_max_runs",
    "compute_peek_response_stash_read_batch_size_bytes",
    "compute_peek_response_stash_read_memory_budget_bytes",
    "compute_peek_stash_num_batches",
    "compute_peek_stash_batch_size",
    "enable_timely_init_at_process_startup",
]


DEFAULT_CRDB_ENVIRONMENT = [
    "COCKROACH_ENGINE_MAX_SYNC_DURATION_DEFAULT=120s",
    "COCKROACH_LOG_MAX_SYNC_DURATION=120s",
]


# TODO(benesch): change to `docker-mzcompose` once v0.39 ships.
DEFAULT_CLOUD_PROVIDER = "mzcompose"
DEFAULT_CLOUD_REGION = "us-east-1"
DEFAULT_ORG_ID = "00000000-0000-0000-0000-000000000000"
DEFAULT_ORDINAL = "0"
DEFAULT_MZ_ENVIRONMENT_ID = f"{DEFAULT_CLOUD_PROVIDER}-{DEFAULT_CLOUD_REGION}-{DEFAULT_ORG_ID}-{DEFAULT_ORDINAL}"


# TODO(benesch): replace with Docker health checks.
def _check_tcp(
    cmd: list[str], host: str, port: int, timeout_secs: int, kind: str = ""
) -> list[str]:
    cmd.extend(
        [
            "timeout",
            str(timeout_secs),
            "bash",
            "-c",
            f"until [ cat < /dev/null > /dev/tcp/{host}/{port} ] ; do sleep 0.1 ; done",
        ]
    )
    try:
        spawn.capture(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        ui.log_in_automation(
            "wait-for-tcp ({}{}:{}): error running {}: {}, stdout:\n{}\nstderr:\n{}".format(
                kind, host, port, ui.shell_quote(cmd), e, e.stdout, e.stderr
            )
        )
        raise
    return cmd


# TODO(benesch): replace with Docker health checks.
def _wait_for_pg(
    timeout_secs: int,
    query: str,
    dbname: str,
    port: int,
    host: str,
    user: str,
    password: str | None,
    expected: Iterable[Any] | Literal["any"],
    print_result: bool = False,
    sslmode: str = "disable",
) -> None:
    """Wait for a pg-compatible database (includes materialized)"""
    obfuscated_password = password[0:1] if password is not None else ""
    args = f"dbname={dbname} host={host} port={port} user={user} password='{obfuscated_password}...'"
    ui.progress(f"waiting for {args} to handle {query!r}", "C")
    error = None
    for remaining in ui.timeout_loop(timeout_secs, tick=0.5):
        try:
            conn = psycopg.connect(
                dbname=dbname,
                host=host,
                port=port,
                user=user,
                password=password,
                connect_timeout=1,
                sslmode=sslmode,
            )
            # The default (autocommit = false) wraps everything in a transaction.
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(query.encode())
                if expected == "any" and cur.rowcount == -1:
                    ui.progress(" success!", finish=True)
                    return
                result = list(cur.fetchall())
                if expected == "any" or result == expected:
                    if print_result:
                        say(f"query result: {result}")
                    else:
                        ui.progress(" success!", finish=True)
                    return
                else:
                    say(
                        f"host={host} port={port} did not return rows matching {expected} got: {result}"
                    )
        except Exception as e:
            ui.progress(f"{e if print_result else ''} {int(remaining)}")
            error = e
    ui.progress(finish=True)
    raise UIError(f"never got correct result for {args}: {error}")


def bootstrap_cluster_replica_size() -> str:
    return "bootstrap"


def cluster_replica_size_map() -> dict[str, dict[str, Any]]:
    def replica_size(
        workers: int,
        scale: int,
        disabled: bool = False,
        is_cc: bool = True,
        memory_limit: str | None = None,
    ) -> dict[str, Any]:
        return {
            "cpu_exclusive": False,
            "cpu_limit": None,
            "credits_per_hour": f"{workers * scale}",
            "disabled": disabled,
            "disk_limit": None,
            "is_cc": is_cc,
            "memory_limit": memory_limit or "4Gi",
            "scale": scale,
            "workers": workers,
            # "selectors": {},
        }

    replica_sizes = {
        bootstrap_cluster_replica_size(): replica_size(1, 1),
        "2-4": replica_size(4, 2),
        "free": replica_size(0, 0, disabled=True),
        "1cc": replica_size(1, 1),
        "1C": replica_size(1, 1),
        "1-no-disk": replica_size(1, 1, is_cc=False),
        "2-no-disk": replica_size(2, 1, is_cc=False),
    }

    for i in range(0, 6):
        workers = 1 << i
        replica_sizes[f"{workers}"] = replica_size(workers, 1)
        for mem in [4, 8, 16, 32]:
            replica_sizes[f"{workers}-{mem}G"] = replica_size(
                workers, 1, memory_limit=f"{mem} GiB"
            )

        replica_sizes[f"{workers}-1"] = replica_size(1, workers)
        replica_sizes[f"{workers}-{workers}"] = replica_size(workers, workers)
        replica_sizes[f"mem-{workers}"] = replica_size(
            workers, 1, memory_limit=f"{workers} GiB"
        )

    return replica_sizes
