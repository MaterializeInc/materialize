#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `mysql-source-gtid-monotonicity-violation`.

The MySQL CDC source must never enter an errored state because of
"received out of order gtids".  This error (BinlogGtidMonotonicityViolation)
fires when the multithreaded replica delivers a GTID with a lower
transaction-id than what was already observed for that UUID — permanently
erroring the source with no self-recovery path.

With 4 parallel replica workers and `replica_preserve_commit_order=ON`,
this should be impossible: the commit-order protocol guarantees that GTIDs
arrive in primary-commit order.  But under Antithesis fault injection
(scheduling jitter, container kills, network delays) the commit-order
guarantee could be tested.  This driver records an `always()` failure the
moment the errored state is observed, giving Antithesis a reportable
property violation with a deterministic replay anchor.

This is an `anytime_` driver — it runs continuously throughout the timeline
so faults active during its polling window can be correlated with the first
observed error.  A bounded run budget (`RUN_BUDGET_S`) prevents one instance
from pinning resources; Antithesis re-launches it freely.

Error-state detection is workload-observable: `mz_internal.mz_source_statuses`
reports `status = 'errored'` with `error` containing the error message.  We
check for the specific substring "out of order gtids" so the assertion is
tight and won't fire on unrelated source errors.

The complementary SUT-side assertion lives in
`src/storage/src/source/mysql/replication/partitions.rs`:
`assert_unreachable!("mysql: BinlogGtidMonotonicityViolation …")`.
"""

from __future__ import annotations

import sys
import time

import helper_logging
from helper_mysql_source import SOURCE_NAME
from helper_pg import query_retry

from antithesis.assertions import always

LOG = helper_logging.setup_logging("driver.mysql_source_no_gtid_errors")

# Knobs.
POLL_INTERVAL_S = 2.0
RUN_BUDGET_S = 60.0

# Substring that identifies the specific error this property targets.
_GTID_ORDER_ERROR = "out of order gtids"


def _source_status() -> tuple[str, str | None] | None:
    """Query status and error for the MySQL CDC source.

    Returns (status, error_message) or None if the source doesn't exist yet
    or the query fails (both are expected early in a timeline).
    """
    try:
        rows = query_retry(
            """
            SELECT ss.status, ss.error
            FROM mz_internal.mz_source_statuses ss
            JOIN mz_sources s ON s.id = ss.id
            WHERE s.name = %s
            """,
            (SOURCE_NAME,),
        )
    except Exception as exc:  # noqa: BLE001
        LOG.info("source status query failed: %s", exc)
        return None
    if not rows:
        return None
    status, error = rows[0]
    return (status, error)


def main() -> int:
    deadline = time.monotonic() + RUN_BUDGET_S
    checks = 0

    while time.monotonic() < deadline:
        result = _source_status()
        if result is None:
            time.sleep(POLL_INTERVAL_S)
            continue

        status, error = result
        checks += 1

        is_gtid_error = (
            status == "errored"
            and error is not None
            and _GTID_ORDER_ERROR in error.lower()
        )

        always(
            not is_gtid_error,
            "mysql: source must not enter errored state due to out-of-order GTIDs",
            {
                "source": SOURCE_NAME,
                "status": status,
                "error": error,
                "note": (
                    "BinlogGtidMonotonicityViolation fired — multithreaded replica "
                    "delivered a GTID with lower txn-id than previously observed; "
                    "replica_preserve_commit_order protocol violated under fault injection"
                ),
            },
        )

        if is_gtid_error:
            LOG.error(
                "gtid monotonicity violation detected: status=%s error=%s",
                status,
                error,
            )

        time.sleep(POLL_INTERVAL_S)

    LOG.info(
        "mysql-source-no-gtid-errors done; %d status checks over %.0fs",
        checks,
        RUN_BUDGET_S,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
