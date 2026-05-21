#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Single first command for the workload-replay template.

Antithesis runs exactly one `first_*` command per execution history
before any driver fires (see
https://antithesis.com/docs/test_templates/test_composer_reference/).
For workload-replay the setup is minimal — every captured query
targets the built-in `mz_catalog_server` cluster and the built-in
`materialize` database, so we just need to widen a few catalog limits
that the workload-replay framework's own `objects.py` would otherwise
push up.

The workload's `quickstart` cluster (in the bundled capture YAML) is
*not* recreated here: the captured queries all reference
`mz_catalog_server` instead, so the captured cluster is dead state.
Skipping it also sidesteps the SIZE = '800cc' mismatch (Materialize's
default sizes in the Antithesis topology don't include that cloud
size).

Idempotent across Antithesis-injected restarts: every statement uses
`ALTER SYSTEM SET` which is a tolerant no-op when the value is
already at the requested level.
"""

from __future__ import annotations

import sys

import helper_logging
from helper_pg import execute_internal_retry

LOG = helper_logging.setup_logging("first.workload_replay_setup")


_ALTER_SYSTEM_KNOBS: tuple[str, ...] = (
    # Mirrors the `objects.run_create_objects_part_1` widenings so the
    # captured workload's object count never trips a default limit.
    # Conservative bound (1e6) so even with concurrent driver runs
    # claiming the same shared catalog we never hit it.
    "ALTER SYSTEM SET max_schemas_per_database = 1000000",
    "ALTER SYSTEM SET max_tables = 1000000",
    "ALTER SYSTEM SET max_materialized_views = 1000000",
    "ALTER SYSTEM SET max_sources = 1000000",
    "ALTER SYSTEM SET max_sinks = 1000000",
    "ALTER SYSTEM SET max_roles = 1000000",
    "ALTER SYSTEM SET max_clusters = 1000000",
    "ALTER SYSTEM SET max_replicas_per_cluster = 1000000",
    "ALTER SYSTEM SET max_secrets = 1000000",
    # SUBSCRIBE replay needs an isolation override; without this,
    # SUBSCRIBE against the captured isolation level fails because
    # idle-in-transaction timeout kicks in during the per-query budget.
    "ALTER SYSTEM SET idle_in_transaction_session_timeout = 0",
)


def main() -> int:
    LOG.info("workload-replay first command: widening catalog limits")
    for stmt in _ALTER_SYSTEM_KNOBS:
        try:
            execute_internal_retry(stmt)
        except Exception as exc:  # noqa: BLE001
            # ALTER SYSTEM SET is unusually robust (no upstream
            # dependencies, no catalog races), so a failure here is
            # likely a fault-injection window the helper's retry budget
            # didn't outlast. Log and continue — drivers absorb the
            # resulting limit-hit errors via
            # `is_expected_replay_error`, and the first command
            # rerunning on the next setup phase will get another shot.
            LOG.warning("ALTER SYSTEM tolerated: %s (%s)", stmt, exc)
    LOG.info("workload-replay first command: complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
