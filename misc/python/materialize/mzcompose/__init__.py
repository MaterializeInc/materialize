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
from ssl import SSLContext
from typing import Any, Literal, TypeVar

import pg8000

from materialize import spawn, ui
from materialize.ui import UIError

T = TypeVar("T")
say = ui.speaker("C> ")


DEFAULT_CONFLUENT_PLATFORM_VERSION = "7.5.2"

DEFAULT_MZ_VOLUMES = [
    "mzdata:/mzdata",
    "mydata:/var/lib/mysql-files",
    "tmp:/share/tmp",
    "scratch:/scratch",
]

DEFAULT_SYSTEM_PARAMETERS = {
    # -----
    # Unsafe functions
    "enable_unsafe_functions": "true",
    "enable_dangerous_functions": "true",  # former name of 'enable_unsafe_functions'
    # -----
    # To reduce CRDB load as we are struggling with it in CI (values based on load test environment):
    "persist_next_listen_batch_retryer_clamp": "16s",
    "persist_next_listen_batch_retryer_initial_backoff": "100ms",
    "persist_next_listen_batch_retryer_fixed_sleep": "1200ms",
    # -----
    # Persist internals changes: advance coverage
    "persist_enable_arrow_lgalloc_noncc_sizes": "true",
    "persist_enable_s3_lgalloc_noncc_sizes": "true",
    # -----
    # Others (ordered by name)
    "cluster_always_use_disk": "true",
    "compute_dataflow_max_inflight_bytes": "134217728",  # 128 MiB
    "compute_hydration_concurrency": 2,
    "disk_cluster_replicas_default": "true",
    "enable_alter_swap": "true",
    "enable_assert_not_null": "true",
    "enable_aws_connection": "true",
    "enable_columnation_lgalloc": "true",
    "enable_comment": "true",
    "enable_compute_chunked_stack": "true",
    "enable_connection_validation_syntax": "true",
    "enable_copy_to_expr": "true",
    "enable_disk_cluster_replicas": "true",
    "enable_eager_delta_joins": "true",
    "enable_envelope_debezium_in_subscribe": "true",
    "enable_equivalence_propagation": "true",
    "enable_expressions_in_limit_syntax": "true",
    "enable_logical_compaction_window": "true",
    "enable_multi_worker_storage_persist_sink": "true",
    "enable_mysql_source": "true",
    "enable_rbac_checks": "true",
    "enable_reduce_mfp_fusion": "true",
    "enable_refresh_every_mvs": "true",
    "enable_cluster_schedule_refresh": "true",
    "enable_sink_doc_on_option": "true",
    "enable_statement_lifecycle_logging": "true",
    "enable_table_keys": "true",
    "enable_variadic_left_join_lowering": "true",
    "enable_worker_core_affinity": "true",
    "persist_batch_delete_enabled": "true",
    "persist_fast_path_limit": "1000",
    "persist_inline_writes_single_max_bytes": "4096",
    "persist_inline_writes_total_max_bytes": "1048576",
    "persist_pubsub_client_enabled": "true",
    "persist_pubsub_push_diff_enabled": "true",
    "persist_sink_minimum_batch_updates": "128",
    "persist_stats_audit_percent": "100",
    "persist_txn_tables": "lazy",
    "statement_logging_default_sample_rate": "0.01",
    "statement_logging_max_sample_rate": "0.01",
    "storage_persist_sink_minimum_batch_updates": "100",
    "storage_source_decode_fuel": "100000",
    "timestamp_oracle": "postgres",
    "wait_catalog_consolidation_on_startup": "true",
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
    ssl_context: SSLContext | None = None,
) -> None:
    """Wait for a pg-compatible database (includes materialized)"""
    obfuscated_password = password[0:1] if password is not None else ""
    args = f"dbname={dbname} host={host} port={port} user={user} password='{obfuscated_password}...'"
    ui.progress(f"waiting for {args} to handle {query!r}", "C")
    error = None
    for remaining in ui.timeout_loop(timeout_secs, tick=0.5):
        try:
            conn = pg8000.connect(
                database=dbname,
                host=host,
                port=port,
                user=user,
                password=password,
                timeout=1,
                ssl_context=ssl_context,
            )
            # The default (autocommit = false) wraps everything in a transaction.
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(query)
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
