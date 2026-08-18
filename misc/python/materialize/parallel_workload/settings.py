# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from enum import Enum


class Complexity(Enum):
    Read = "read"
    DML = "dml"
    DDL = "ddl"
    DDLOnly = "ddl-only"

    @classmethod
    def _missing_(cls, value):
        if value == "random":
            return cls(random.choice([elem.value for elem in cls]))


class Scenario(Enum):
    Regression = "regression"
    Cancel = "cancel"
    Kill = "kill"
    Rename = "rename"
    BackupRestore = "backup-restore"
    ZeroDowntimeDeploy = "0dt-deploy"
    RepeatRow = "repeat-row"

    @classmethod
    def _missing_(cls, value):
        if value == "random":
            return cls(random.choice([elem.value for elem in cls]))


# Scenarios that need durable state across restarts/promotions and therefore run
# against an external CockroachDB metadata and consensus backend. Every other
# scenario uses the in-process Postgres metadata store. This drives both the
# `external` service wiring and which scenarios must keep
# `persist_pg_consensus_read_committed` off (the CRDB_* consensus queries are
# only correct under SERIALIZABLE).
COCKROACH_SCENARIOS = frozenset(
    {Scenario.Kill, Scenario.BackupRestore, Scenario.ZeroDowntimeDeploy}
)


ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS = {
    # Uses a lot of memory, hard to predict how much
    "memory_limiter_interval": "0",
    # See https://materializeinc.slack.com/archives/CTESPM7FU/p1758195280629909, should reenable when it performs better
    "enable_compute_logical_backpressure": "false",
    # Allows the `Scenario.RepeatRow` scenario to call `repeat_row`. Having
    # it on outside that scenario is harmless: no Parallel Workload codegen
    # emits `repeat_row` unless the scenario is active.
    "enable_repeat_row": "true",
    # 64 MiB, down from the 1 GiB default. A peek's result is materialized in
    # memory on the replica serving it before this bound errors it
    # (`to_error_if_exceeds`, compute_state.rs), and measured on a workload-like
    # relation a 300 MiB result costs ~1 GiB of replica RSS plus ~0.5 GiB in
    # environmentd. The ceiling is per in-flight statement and every worker
    # holds a pg and a WebSocket session, so at the default a handful of
    # concurrent wide reads can claim tens of GiB on the default cluster, where
    # every peek lands. All containers of a run share one cgroup budget (24 GiB
    # on the CI agent, environmentd plus every replica process plus Kafka,
    # Postgres, MySQL, SQL Server), so that ends in the kernel OOM-killer
    # picking a victim and taking unrelated processes down with it. Runs do
    # reach the cap ("result exceeds max size of" shows up in the ignored-error
    # statistics), which is what makes the tail dangerous. Query shapes are
    # unaffected: an oversized result errors instead of being buffered, and that
    # error is already tolerated for every action.
    "max_result_size": "67108864",
}
