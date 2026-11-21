# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""System parameters changed while the workload runs.

A rollout of a feature flag changes a parameter under load, so code that
reads one twice in an operation can see two values, and state written under
one value is read under another. This component has no check of its own: the
other components' checks hold under every value here.

The parameters and their values are a subset of what CI varies
(`mzcompose.get_variable_system_parameters`): the ones that change how
persist, compute and storage write, compact, fetch and hand over state. Half
the timelines change none, so the defaults keep most of the exploration.
"""

from __future__ import annotations

import threading
import time

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    reachable,
)

from materialize.antithesis_driver.client import Op, Outcome, Status
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

MIN_INTERVAL_SECONDS = 5.0

ON_OFF = ("true", "false")
PARAMETERS: dict[str, tuple[str, ...]] = {
    "compute_apply_column_demands": ON_OFF,
    "compute_correction_v2_chunk_size": ("8192", "2097152"),
    "compute_dataflow_max_inflight_bytes": ("1048576", "134217728"),
    "compute_hydration_concurrency": ("1", "4"),
    "compute_mv_sink_shared_batch_max_bytes": ("0", "65536", "1048576"),
    "compute_peek_response_stash_threshold_bytes": ("0", "1048576", "314572800"),
    "compute_subscribe_snapshot_optimization": ON_OFF,
    "default_timestamp_interval": ("100ms", "1s"),
    "enable_case_literal_transform": ON_OFF,
    "enable_cast_elimination": ON_OFF,
    "enable_coalesce_case_transform": ON_OFF,
    "enable_columnar_accumulable_diff": ON_OFF,
    "enable_columnar_merge_batcher": ON_OFF,
    "enable_compute_sync_mv_sink": ON_OFF,
    "enable_fixed_correlated_cte_lowering": ON_OFF,
    "enable_simplify_from_less_existence": ON_OFF,
    "enable_union_cancellation_after_relation_cse": ON_OFF,
    "enable_upsert_v2": ON_OFF,
    "kafka_offset_commit_refresh_interval": ("1s", "10s"),
    "mysql_source_snapshot_parallelism": ON_OFF,
    "mysql_source_snapshot_partition_min_rows": ("2", "50000"),
    "persist_batch_delete_enabled": ON_OFF,
    "persist_batch_max_run_len": ("2", "4", "16"),
    "persist_blob_hedged_get_delay": ("0s", "10ms", "2s"),
    "persist_blob_hedged_get_enabled": ON_OFF,
    "persist_blob_target_size": ("65536", "1048576", "16777216"),
    "persist_enable_incremental_compaction": ON_OFF,
    "persist_encoding_enable_dictionary": ON_OFF,
    "persist_fast_path_limit": ("100", "1000"),
    "persist_fast_path_order": ON_OFF,
    "persist_gc_max_versions": ("256", "128000"),
    "persist_gc_min_versions": ("16", "256"),
    "persist_gc_remove_finalized_rollups": ON_OFF,
    "persist_gc_use_active_gc": ON_OFF,
    "persist_inline_writes_single_max_bytes": ("256", "4096", "16384"),
    "persist_inline_writes_total_max_bytes": ("65536", "1048576"),
    "persist_next_listen_batch_retryer_clamp": ("100ms", "1s", "16s"),
    "persist_part_decode_format": ("arrow", "row_with_validate"),
    "persist_pubsub_client_enabled": ON_OFF,
    "persist_pubsub_push_diff_enabled": ON_OFF,
    "persist_rollup_use_active_rollup": ON_OFF,
    "persist_source_hydration_frontier_coalesce_bytes": ("0", "1048576"),
    "persist_state_update_lease_timeout": ("0s", "1s", "10s"),
    "persist_validate_part_bounds_on_read": ON_OFF,
    "persist_validate_part_bounds_on_write": ON_OFF,
    "storage_source_decode_fuel": ("10000", "1000000"),
    "storage_statistics_interval": ("100", "2000"),
    "storage_use_continual_feedback_upsert": ON_OFF,
}


class Flags:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self.enabled = False
        self._last = 0.0
        self.changed: dict[str, str] = {}
        """Parameters set to a value, for the log."""

    def configure(self, d: Draw) -> None:
        self.enabled = d.boolean()

    def flip(self, d: Draw) -> Status:
        """Set one parameter to one of its values, or back to its default."""
        name = d.choice(sorted(PARAMETERS))
        value = d.choice((None, *PARAMETERS[name]))
        with self._lock:
            now = time.monotonic()
            if not self.enabled or now - self._last < MIN_INTERVAL_SECONDS:
                return Status.NOT_SENT
            self._last = now
        statement = (
            f"ALTER SYSTEM RESET {name}"
            if value is None
            else f"ALTER SYSTEM SET {name} = '{value}'"
        )

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            if not outcome.ok:
                return
            with self._lock:
                if value is None:
                    self.changed.pop(name, None)
                else:
                    self.changed[name] = value
                changed = dict(self.changed)
            log(f"system parameters: {statement}, now changed: {changed}")
            reachable(
                "driver: a system parameter was changed while the workload ran",
                {"statement": statement},
            )

        return self.ctx.system.call(
            d.integer(0, len(self.ctx.system.sessions) - 1),
            Op("flip system parameter", execute, complete),
            RULE_WAIT_SECONDS,
        )
