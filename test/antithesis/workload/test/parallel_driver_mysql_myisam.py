#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for property `mysql-myisam-cdc-no-data-loss`.

Sibling driver `parallel_driver_mysql_cdc.py` exercises the same property
shape against the InnoDB-backed `antithesis.cdc_test`. This driver
exercises the non-transactional flavor against the MyISAM-backed
`antithesis.cdc_test_myisam`. The Materialize MySQL source code path
doesn't distinguish engines — the binlog/CDC contract is engine-agnostic
— so this is a check that the engine-agnostic behavior actually holds
under non-transactional upstream DML.

What's different about MyISAM in the binlog:
  * BEGIN/COMMIT around MyISAM statements is silently ignored — each
    statement commits immediately.
  * Every MyISAM statement gets its own GTID (one transaction per
    statement, not per BEGIN/COMMIT block).
  * No rollback semantics: a statement that fails partway through leaves
    whatever rows it managed to insert committed and visible to the
    binlog / replica / Materialize source.
  * No table-locking deadlock recovery: the storage engine takes
    table-level locks, so concurrent writers serialize rather than
    abort-and-retry.

These differences shouldn't affect Materialize's view of the data: every
acknowledged INSERT must appear in the CDC source with the right value.
That's the property this driver asserts, with the same shape as
`parallel_driver_mysql_cdc.py`.

Each invocation:
  1. Checks the MySQL CDC source and the MyISAM reference table exist.
  2. Picks a per-invocation `batch_id` prefix so concurrent drivers
     (including the InnoDB sibling) don't collide.
  3. Inserts ROWS_PER_INVOCATION rows to the MyISAM table on the primary.
  4. Polls the Materialize source
     table until all expected rows appear (or the budget expires).
  5. Asserts correctness via `always(...)` on count and per-row values.
"""

from __future__ import annotations

import logging
import sys
import time

import helper_mysql
import helper_random
from helper_mysql_source import (
    MYSQL_DATABASE,
    MYSQL_TABLE_MYISAM,
    SOURCE_NAME,
    TABLE_NAME_MYISAM,
)
from helper_pg import query_retry

from antithesis.assertions import always, sometimes

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
LOG = logging.getLogger("driver.mysql_myisam")

ROWS_PER_INVOCATION = 20
# Sized to span at least one MAX_OFF window from the global fault-
# orchestrator (default 40s) plus replica → source → MZ catchup time.
CATCHUP_TIMEOUT_S = 120.0
POLL_INTERVAL_S = 1.0


def _source_ready() -> bool:
    """Source + MyISAM reference table both exist in Materialize."""
    src = query_retry("SELECT 1 FROM mz_sources WHERE name = %s", (SOURCE_NAME,))
    tbl = query_retry("SELECT 1 FROM mz_tables WHERE name = %s", (TABLE_NAME_MYISAM,))
    return bool(src) and bool(tbl)


def _insert_rows(batch_id: str) -> dict[str, str]:
    """Insert ROWS_PER_INVOCATION rows to the MyISAM table.

    Each insert is its own implicit transaction (MyISAM ignores BEGIN/
    COMMIT). Returns {id → value} for every successfully inserted row.
    Failures are logged and skipped: under fault injection the primary
    may be unreachable mid-loop, and the property is "every
    acknowledged INSERT shows up," not "every attempted INSERT shows up."
    """
    expected: dict[str, str] = {}
    for i in range(ROWS_PER_INVOCATION):
        row_id = f"{batch_id}:{i}"
        value = f"v{helper_random.random_int(0, 9999):04d}"
        try:
            helper_mysql.execute_primary(
                f"INSERT INTO {MYSQL_DATABASE}.{MYSQL_TABLE_MYISAM} "
                "(id, batch_id, value) VALUES (%s, %s, %s) "
                "ON DUPLICATE KEY UPDATE value = VALUES(value), batch_id = VALUES(batch_id)",
                (row_id, batch_id, value),
                database=MYSQL_DATABASE,
            )
            expected[row_id] = value
        except Exception as exc:  # noqa: BLE001
            LOG.info("MyISAM insert failed for row %s: %s; skipping", row_id, exc)
    return expected


def _wait_for_catchup(batch_id: str, expected_count: int) -> bool:
    """Poll Materialize until all expected rows for `batch_id` appear in
    the MyISAM-referenced subsource.
    """
    deadline = time.monotonic() + CATCHUP_TIMEOUT_S
    last_seen = -1
    while time.monotonic() < deadline:
        try:
            rows = query_retry(
                f"SELECT COUNT(*)::bigint FROM {TABLE_NAME_MYISAM} WHERE batch_id = %s",
                (batch_id,),
            )
            count = int(rows[0][0]) if rows and rows[0][0] is not None else 0
        except Exception as exc:  # noqa: BLE001
            LOG.info("catchup poll failed: %s; retrying", exc)
            time.sleep(POLL_INTERVAL_S)
            continue

        if count != last_seen:
            LOG.info(
                "mysql myisam catchup: batch=%s observed=%d target=%d",
                batch_id,
                count,
                expected_count,
            )
            last_seen = count

        if count >= expected_count:
            return True
        time.sleep(POLL_INTERVAL_S)

    LOG.warning(
        "mysql myisam catchup timeout: batch=%s last_seen=%d target=%d",
        batch_id,
        last_seen,
        expected_count,
    )
    return False


def _check_rows(expected: dict[str, str]) -> None:
    """Assert every expected row has the correct value in the Materialize
    MyISAM-referenced subsource. Uses real_time_recency so the per-row
    SELECT chosen-ts waits for the MySQL source's real-time upstream
    frontier; the count-based catchup above can clear at a chosen-ts that
    just barely satisfies the COUNT, leaving a per-row SELECT moments
    later to race.
    """
    for row_id, want in expected.items():
        rows = query_retry(
            f"SELECT value FROM {TABLE_NAME_MYISAM} WHERE id = %s",
            (row_id,),
            real_time_recency=True,
        )
        found = bool(rows)
        observed = rows[0][0] if found else None
        always(
            found and observed == want,
            "mysql myisam: CDC source row has correct value after catchup",
            {
                "source": TABLE_NAME_MYISAM,
                "id": row_id,
                "expected_value": want,
                "observed_present": found,
                "observed_value": observed,
            },
        )


def main() -> int:
    if not _source_ready():
        # first_mysql_replica_setup must run before this driver. Outside
        # Antithesis (e.g. snouty validate) the source / MyISAM table may
        # not exist yet — exit cleanly rather than erroring so validate
        # can still proceed.
        LOG.warning(
            "mysql cdc source %s or MyISAM table %s not found; skipping "
            "(first_mysql_replica_setup must run first)",
            SOURCE_NAME,
            TABLE_NAME_MYISAM,
        )
        return 0

    batch_id = f"myi-p{helper_random.random_u64():016x}"
    LOG.info("driver starting; batch_id=%s", batch_id)

    expected = _insert_rows(batch_id)
    if not expected:
        LOG.info("no rows inserted successfully this invocation; exiting cleanly")
        return 0

    LOG.info("inserted %d rows; waiting for catchup", len(expected))
    caught_up = _wait_for_catchup(batch_id, len(expected))

    sometimes(
        caught_up,
        "mysql myisam: CDC source caught up to all primary inserts within catchup budget",
        {
            "source": TABLE_NAME_MYISAM,
            "batch_id": batch_id,
            "rows_inserted": len(expected),
        },
    )

    if not caught_up:
        LOG.info("catchup did not complete in budget; skipping per-row assertions")
        return 0

    _check_rows(expected)

    rows = query_retry(
        f"SELECT COUNT(*)::bigint FROM {TABLE_NAME_MYISAM} WHERE batch_id = %s",
        (batch_id,),
        real_time_recency=True,
    )
    count_in_mz = int(rows[0][0]) if rows and rows[0][0] is not None else 0
    always(
        count_in_mz == len(expected),
        "mysql myisam: CDC source row count matches inserted count after catchup",
        {
            "source": TABLE_NAME_MYISAM,
            "batch_id": batch_id,
            "expected_count": len(expected),
            "observed_count": count_in_mz,
        },
    )

    LOG.info(
        "driver done; asserted on %d MyISAM rows for batch_id=%s",
        len(expected),
        batch_id,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
