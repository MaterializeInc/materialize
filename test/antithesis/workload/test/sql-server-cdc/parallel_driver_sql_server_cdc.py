#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for property `sql-server-source-no-data-loss`.

Every row inserted into the upstream SQL Server `dbo.cdc_test` must
eventually appear — with the correct value — in the Materialize source
that reads from it via the SQL Server CDC capture-instance polling
path.

Each invocation:
  1. Checks the SQL Server CDC source exists (created by
     first_sql_server_cdc_setup).
  2. Picks a per-invocation `batch_id` prefix so concurrent drivers
     don't collide.
  3. Inserts ROWS_PER_INVOCATION rows on the upstream SQL Server,
     recording the expected {id → value} map locally.
  4. Polls the Materialize source table until all expected rows appear
     (or the catchup budget expires).
  5. Asserts correctness via `always(...)` on count and per-row values.
     A `sometimes(...)` liveness anchor fires on successful catchup.

Mirrors `parallel_driver_pg_cdc.py` and `parallel_driver_mysql_cdc.py`
so triage reports can compare the three source families side-by-side.
"""

from __future__ import annotations

import sys
import time

import helper_logging
import helper_random
import helper_sql_server_upstream as sql_server
from antithesis.assertions import always, sometimes
from helper_pg import query_retry
from helper_sql_server_source import (
    SOURCE_BASENAME,
    SOURCE_NAME,
    TABLE_NAME,
    UPSTREAM_SCHEMA,
    UPSTREAM_TABLE,
)

LOG = helper_logging.setup_logging("driver.sql_server_cdc")

ROWS_PER_INVOCATION = 20
# Sized to span at least one MAX_OFF window from the global fault-
# orchestrator (default 40s) plus catchup time. SQL Server CDC is a
# polling code path (not push), so the catchup floor is bounded below
# by the source's poll interval — add headroom on top of MAX_ON+MAX_OFF.
CATCHUP_TIMEOUT_S = 120.0
POLL_INTERVAL_S = 1.0


def _source_exists() -> bool:
    rows = query_retry("SELECT 1 FROM mz_sources WHERE name = %s", (SOURCE_BASENAME,))
    return bool(rows)


def _insert_rows(batch_id: str) -> dict[str, str]:
    """Insert ROWS_PER_INVOCATION rows into the upstream SQL Server.

    Returns {id → value} for every successfully inserted row. SQL
    Server has no native `ON CONFLICT` — but batch_id makes our id
    space disjoint per invocation, so plain INSERT is safe.
    """
    expected: dict[str, str] = {}
    for i in range(ROWS_PER_INVOCATION):
        row_id = f"{batch_id}:{i}"
        value = f"v{helper_random.random_int(0, 9999):04d}"
        try:
            sql_server.execute(
                f"INSERT INTO {UPSTREAM_SCHEMA}.{UPSTREAM_TABLE} "
                f"(id, batch_id, value) VALUES (%s, %s, %s)",
                (row_id, batch_id, value),
            )
            expected[row_id] = value
        except Exception as exc:  # noqa: BLE001
            # Under fault injection a write to the upstream may fail. Skip
            # the row rather than crashing so the driver keeps inserting
            # others.
            LOG.info("insert failed for row %s: %s; skipping", row_id, exc)
    return expected


def _wait_for_catchup(batch_id: str, expected_count: int) -> bool:
    """Poll Materialize until all expected rows for `batch_id` appear."""
    deadline = time.monotonic() + CATCHUP_TIMEOUT_S
    last_seen = -1
    while time.monotonic() < deadline:
        try:
            rows = query_retry(
                f"SELECT COUNT(*)::bigint FROM {TABLE_NAME} WHERE batch_id = %s",
                (batch_id,),
            )
            count = int(rows[0][0]) if rows and rows[0][0] is not None else 0
        except Exception as exc:  # noqa: BLE001
            LOG.info("catchup poll failed: %s; retrying", exc)
            time.sleep(POLL_INTERVAL_S)
            continue

        if count != last_seen:
            LOG.info(
                "sql-server cdc catchup: batch=%s observed=%d target=%d",
                batch_id,
                count,
                expected_count,
            )
            last_seen = count

        if count >= expected_count:
            return True
        time.sleep(POLL_INTERVAL_S)

    LOG.warning(
        "sql-server cdc catchup timeout: batch=%s last_seen=%d target=%d",
        batch_id,
        last_seen,
        expected_count,
    )
    return False


def _check_rows(expected: dict[str, str]) -> None:
    """Assert every expected row has the correct value in the Materialize source."""
    for row_id, want in expected.items():
        # real_time_recency: pushes chosen-ts to the upstream's real-time
        # frontier so per-row SELECTs don't race a too-eager catchup-count
        # clear.
        rows = query_retry(
            f"SELECT value FROM {TABLE_NAME} WHERE id = %s",
            (row_id,),
            real_time_recency=True,
        )
        found = bool(rows)
        observed = rows[0][0] if found else None
        always(
            found and observed == want,
            "sql-server: CDC source row has correct value after catchup",
            {
                "source": TABLE_NAME,
                "id": row_id,
                "expected_value": want,
                "observed_present": found,
                "observed_value": observed,
            },
        )


def main() -> int:
    if not _source_exists():
        # first_sql_server_cdc_setup must run before this driver. Outside
        # Antithesis (e.g. snouty validate) the source may not exist yet —
        # exit cleanly rather than erroring so validate can still proceed.
        LOG.warning(
            "sql-server cdc source %s not found; skipping "
            "(first_sql_server_cdc_setup must run first)",
            SOURCE_NAME,
        )
        return 0

    batch_id = f"ss{helper_random.random_u64():016x}"
    LOG.info("driver starting; batch_id=%s", batch_id)

    expected = _insert_rows(batch_id)
    if not expected:
        LOG.info("no rows inserted successfully this invocation; exiting cleanly")
        return 0

    LOG.info("inserted %d rows; waiting for catchup", len(expected))
    caught_up = _wait_for_catchup(batch_id, len(expected))

    sometimes(
        caught_up,
        "sql-server: CDC source caught up to all upstream inserts within catchup budget",
        {
            "source": TABLE_NAME,
            "batch_id": batch_id,
            "rows_inserted": len(expected),
        },
    )

    if not caught_up:
        LOG.info("catchup did not complete in budget; skipping per-row assertions")
        return 0

    _check_rows(expected)

    rows = query_retry(
        f"SELECT COUNT(*)::bigint FROM {TABLE_NAME} WHERE batch_id = %s",
        (batch_id,),
        real_time_recency=True,
    )
    count_in_mz = int(rows[0][0]) if rows and rows[0][0] is not None else 0
    always(
        count_in_mz == len(expected),
        "sql-server: CDC source row count matches inserted count after catchup",
        {
            "source": TABLE_NAME,
            "batch_id": batch_id,
            "expected_count": len(expected),
            "observed_count": count_in_mz,
        },
    )

    LOG.info(
        "driver done; asserted on %d rows for batch_id=%s", len(expected), batch_id
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
