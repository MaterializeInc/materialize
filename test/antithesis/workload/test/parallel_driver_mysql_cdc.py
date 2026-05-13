#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for property `mysql-source-no-data-loss`.

Every row inserted to the MySQL primary must eventually appear — with the
correct value — in the Materialize CDC source that reads from the
multithreaded replica.

Each invocation:
  1. Checks the MySQL CDC source exists (created by first_mysql_replica_setup).
  2. Picks a per-invocation `batch_id` prefix so concurrent drivers don't
     collide.
  3. Inserts ROWS_PER_INVOCATION rows to the MySQL primary, recording the
     expected {id → value} map locally.
  4. Requests an Antithesis quiet period and polls the Materialize source
     table until all expected rows appear (or the budget expires).
  5. Asserts correctness via `always(...)` on count and per-row values.
     A `sometimes(...)` liveness anchor fires on successful catchup.

This is a `parallel_driver_` — Antithesis runs many concurrent instances.
Each assigns itself a fresh prefix from the Antithesis-seeded RNG so
parallel drivers exercise the MySQL CDC path simultaneously without
interfering with each other's expected-state model.
"""

from __future__ import annotations

import logging
import sys
import time

import helper_mysql
import helper_random
from antithesis.assertions import always, sometimes
from helper_mysql_source import SOURCE_NAME, TABLE_NAME
from helper_pg import query_retry
from helper_quiet import request_quiet_period

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
LOG = logging.getLogger("driver.mysql_cdc")

ROWS_PER_INVOCATION = 20
QUIET_PERIOD_S = 25
CATCHUP_TIMEOUT_S = 90.0
POLL_INTERVAL_S = 1.0


def _source_exists() -> bool:
    rows = query_retry("SELECT 1 FROM mz_sources WHERE name = %s", (SOURCE_NAME,))
    return bool(rows)


def _insert_rows(batch_id: str) -> dict[str, str]:
    """Insert ROWS_PER_INVOCATION rows to the MySQL primary.

    Returns {id → value} for every successfully inserted row.
    """
    expected: dict[str, str] = {}
    for i in range(ROWS_PER_INVOCATION):
        row_id = f"{batch_id}:{i}"
        value = f"v{helper_random.random_int(0, 9999):04d}"
        try:
            helper_mysql.execute_primary(
                "INSERT INTO antithesis.cdc_test (id, batch_id, value) "
                "VALUES (%s, %s, %s) "
                "ON DUPLICATE KEY UPDATE value = VALUES(value), batch_id = VALUES(batch_id)",
                (row_id, batch_id, value),
                database="antithesis",
            )
            expected[row_id] = value
        except Exception as exc:  # noqa: BLE001
            # Under fault injection a write to the primary may fail. Skip the
            # row rather than crashing so the driver keeps inserting others.
            LOG.info("insert failed for row %s: %s; skipping", row_id, exc)
    return expected


def _wait_for_catchup(batch_id: str, expected_count: int) -> bool:
    """Poll Materialize until all expected rows for `batch_id` appear.

    Returns True when `COUNT(*) WHERE batch_id = ?` reaches expected_count,
    False on timeout.
    """
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
                "mysql cdc catchup: batch=%s observed=%d target=%d",
                batch_id,
                count,
                expected_count,
            )
            last_seen = count

        if count >= expected_count:
            return True
        time.sleep(POLL_INTERVAL_S)

    LOG.warning(
        "mysql cdc catchup timeout: batch=%s last_seen=%d target=%d",
        batch_id,
        last_seen,
        expected_count,
    )
    return False


def _check_rows(expected: dict[str, str]) -> None:
    """Assert every expected row has the correct value in the Materialize source."""
    for row_id, want in expected.items():
        # real_time_recency: the count-based catchup above can clear at a
        # chosen-ts that just barely satisfies the COUNT, leaving a per-row
        # SELECT moments later to race. RTR pushes chosen-ts to the mysql
        # upstream's real-time frontier; see helper_pg.query_retry.
        rows = query_retry(
            f"SELECT value FROM {TABLE_NAME} WHERE id = %s",
            (row_id,),
            real_time_recency=True,
        )
        found = bool(rows)
        observed = rows[0][0] if found else None
        always(
            found and observed == want,
            "mysql: CDC source row has correct value after catchup",
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
        # first_mysql_replica_setup must run before this driver. Outside
        # Antithesis (e.g. snouty validate) the source may not exist yet —
        # exit cleanly rather than erroring so validate can still proceed.
        LOG.warning(
            "mysql cdc source %s not found; skipping "
            "(first_mysql_replica_setup must run first)",
            SOURCE_NAME,
        )
        return 0

    batch_id = f"p{helper_random.random_u64():016x}"
    LOG.info("driver starting; batch_id=%s", batch_id)

    expected = _insert_rows(batch_id)
    if not expected:
        LOG.info("no rows inserted successfully this invocation; exiting cleanly")
        return 0

    LOG.info("inserted %d rows; requesting quiet period", len(expected))
    request_quiet_period(QUIET_PERIOD_S)

    caught_up = _wait_for_catchup(batch_id, len(expected))

    # Liveness anchor: at least one invocation should fully catch up. If this
    # never fires across an entire run the safety assertions below are vacuous.
    sometimes(
        caught_up,
        "mysql: CDC source caught up to all primary inserts after quiet period",
        {
            "source": TABLE_NAME,
            "batch_id": batch_id,
            "rows_inserted": len(expected),
        },
    )

    if not caught_up:
        # Don't run per-row safety assertions on stale data — a slow catchup
        # is a separate concern from row-level correctness.
        LOG.info("catchup did not complete in budget; skipping per-row assertions")
        return 0

    # Safety: every row we inserted must be present with the correct value.
    _check_rows(expected)

    # Count-level safety check: no extra rows for our batch_id should exist.
    rows = query_retry(
        f"SELECT COUNT(*)::bigint FROM {TABLE_NAME} WHERE batch_id = %s",
        (batch_id,),
        real_time_recency=True,
    )
    count_in_mz = int(rows[0][0]) if rows and rows[0][0] is not None else 0
    always(
        count_in_mz == len(expected),
        "mysql: CDC source row count matches inserted count after catchup",
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
