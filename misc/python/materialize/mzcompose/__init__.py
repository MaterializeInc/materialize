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

import subprocess
from collections.abc import Iterable
from typing import Any, Literal, TypeVar

import psycopg

from materialize import spawn, ui
from materialize.mz_version import MzVersion
from materialize.ui import UIError

T = TypeVar("T")
say = ui.speaker("C> ")


DEFAULT_CONFLUENT_PLATFORM_VERSION = "7.7.0"

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
}


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

    return {
        # -----
        # Unsafe functions
        "enable_unsafe_functions": "true",
        "unsafe_enable_unsafe_functions": "true",
        # -----
        # To reduce CRDB load as we are struggling with it in CI (values based on load test environment):
        "persist_next_listen_batch_retryer_clamp": "16s",
        "persist_next_listen_batch_retryer_initial_backoff": "100ms",
        "persist_next_listen_batch_retryer_fixed_sleep": "1200ms",
        # -----
        # Persist internals changes: advance coverage
        "persist_enable_arrow_lgalloc_noncc_sizes": "true",
        "persist_enable_s3_lgalloc_noncc_sizes": "true",
        "persist_enable_one_alloc_per_request": "true",
        # -----
        # Others (ordered by name)
        "allow_real_time_recency": "true",
        "cluster_always_use_disk": "true",
        "compute_dataflow_max_inflight_bytes": "134217728",  # 128 MiB
        "compute_hydration_concurrency": "2",
        "compute_replica_expiration_offset": "3d",
        "compute_apply_column_demands": "true",
        "disk_cluster_replicas_default": "true",
        "enable_0dt_deployment": "true" if zero_downtime else "false",
        "enable_0dt_deployment_panic_after_timeout": "true",
        "enable_0dt_deployment_sources": (
            "true" if version >= MzVersion.parse_mz("v0.132.0-dev") else "false"
        ),
        "enable_alter_swap": "true",
        "enable_columnation_lgalloc": "true",
        "enable_compute_chunked_stack": "true",
        "enable_connection_validation_syntax": "true",
        "enable_continual_task_builtins": (
            "true" if version > MzVersion.parse_mz("v0.127.0-dev") else "false"
        ),
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
        "enable_rbac_checks": "true",
        "enable_reduce_mfp_fusion": "true",
        "enable_refresh_every_mvs": "true",
        "enable_cluster_schedule_refresh": "true",
        "enable_statement_lifecycle_logging": "true",
        "enable_table_keys": "true",
        "unsafe_enable_table_keys": "true",
        "enable_variadic_left_join_lowering": "true",
        "enable_worker_core_affinity": "true",
        "kafka_default_metadata_fetch_interval": "1s",
        "mysql_offset_known_interval": "1s",
        "persist_record_schema_id": (
            "true" if version > MzVersion.parse_mz("v0.127.0-dev") else "false"
        ),
        "force_source_table_syntax": "true" if force_source_table_syntax else "false",
        "persist_batch_columnar_format": "both_v2",
        "persist_batch_delete_enabled": "true",
        "persist_batch_structured_order": "true",
        "persist_batch_structured_key_lower_len": "256",
        "persist_batch_max_run_len": "4",
        "persist_catalog_force_compaction_fuel": "1024",
        "persist_catalog_force_compaction_wait": "1s",
        "persist_fast_path_limit": "1000",
        "persist_inline_writes_single_max_bytes": "4096",
        "persist_inline_writes_total_max_bytes": "1048576",
        "persist_pubsub_client_enabled": "true",
        "persist_pubsub_push_diff_enabled": "true",
        "persist_record_compactions": "true",
        # 16 MiB - large enough to avoid a big perf hit, small enough to get more coverage...
        "persist_blob_target_size": "16777216",
        "persist_stats_audit_percent": "100",
        "persist_use_critical_since_catalog": "true",
        "persist_use_critical_since_snapshot": "false" if zero_downtime else "true",
        "persist_use_critical_since_source": "false" if zero_downtime else "true",
        "persist_part_decode_format": "row_with_validate",
        "pg_offset_known_interval": "1s",
        "statement_logging_default_sample_rate": "0.01",
        "statement_logging_max_sample_rate": "0.01",
        "storage_reclock_to_latest": "true",
        "storage_source_decode_fuel": "100000",
        "storage_statistics_collection_interval": "1000",
        "storage_statistics_interval": "2000",
        "storage_use_continual_feedback_upsert": "true",
        "storage_use_reclock_v2": "true",
        "with_0dt_deployment_max_wait": "1800s",
        # End of list (ordered by name)
    }


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
        is_cc: bool = False,
        memory_limit: str | None = None,
    ) -> dict[str, Any]:
        return {
            "cpu_exclusive": False,
            "cpu_limit": None,
            "credits_per_hour": f"{workers * scale}",
            "disabled": disabled,
            "disk_limit": None,
            "is_cc": is_cc,
            "memory_limit": memory_limit,
            "scale": scale,
            "workers": workers,
            # "selectors": {},
        }

    replica_sizes = {
        bootstrap_cluster_replica_size(): replica_size(1, 1),
        "2-4": replica_size(4, 2),
        "free": replica_size(0, 0, disabled=True),
        "1cc": replica_size(1, 1, is_cc=True),
        "1C": replica_size(1, 1, is_cc=True),
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
