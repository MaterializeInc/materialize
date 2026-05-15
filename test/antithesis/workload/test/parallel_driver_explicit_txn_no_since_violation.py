#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for property `peek-no-since-violation`.

Targets the bug family at database-issues#11224 ("Various queries
sometimes end up with insufficient read holds") and its peers (#9510,
#10011, #10050, #11200). Every occurrence in the issue carried
`in_immediate_multi_stmt_txn: true` — i.e. the bug only fires on
*subsequent* queries inside an explicit `BEGIN…COMMIT` transaction,
where Materialize uses the read holds the first query acquired.

The bug surfaces in two code paths that share a root cause (read holds
re-resolved by ID returning an already-downgraded view):

  - **New peek sequencing** (`frontend_peek.rs`), used by direct SELECT
    inside a `BEGIN..COMMIT`. Fixed by PR #36403 ("thread bare
    ReadHolds through ComputeController::peek"). **This is what this
    driver targets.**
  - **Old peek sequencing** (`sequence_plan → sequence_execute →
    handle_execute`), still used by `EXECUTE` of prepared statements.
    `parallel_driver_parallel_workload`'s `exe_prepared` does
    `PREPARE … EXECUTE foo … DEALLOCATE` per random SELECT and is the
    surface CI nightly has flaked on (e.g. database-issues#11200).

The two drivers are complementary, not redundant — neither
explicit-txn-SELECT nor PREPARE/EXECUTE subsumes the other code path.

Each invocation:
  1. Issues a small write against `antithesis_txn_table` (autocommit) —
     committed writes are what advance the table's persist frontier and
     keep the timestamp oracle moving, giving the explicit txn below
     real frontier work to race against.
  2. Opens a separate connection, runs an explicit transaction:
         BEGIN
         SELECT … FROM antithesis_txn_table    (1st query: acquires read holds)
         SELECT … FROM antithesis_txn_mv       (subsequent: uses stored holds)
         SELECT … FROM antithesis_txn_table
         SELECT … FROM antithesis_txn_mv
         COMMIT
     The intermixed table/MV reads keep the input_id_bundle changing,
     so the stored-holds subset / re-resolution path is exercised on
     every statement.
  3. If a `SinceViolation`/`as_of not beyond since`/`insufficient read
     holds` error escapes any of the SELECTs, fires `always(False)`
     with the offending error attached. Other errors (connection drop
     from a clusterd kill, transient broker/admission issues) are
     demoted to `sometimes(False)`.
  4. Records a `sometimes(True)` liveness anchor when the whole txn
     commits cleanly so triage can tell vacuous-pass from real coverage.
"""

from __future__ import annotations

import logging
import sys
import time

import helper_logging
import helper_random
import psycopg
from helper_pg import connect

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.explicit_txn")

CLUSTER = "antithesis_cluster"
TABLE_NAME = "antithesis_txn_table"
MV_NAME = "antithesis_txn_mv"

# Pulled from the bug surface (see frontend_peek.rs +
# compute-client/src/controller.rs around the SinceViolation returns).
# We deliberately also match the persist-side variant the related
# bug #9510 surfaces, so this driver doubles as the regression anchor
# for that one — both express the same since-was-advanced-under-us
# pattern.
_SINCE_VIOLATION_PATTERNS = (
    "sinceviolation",
    "as_of not beyond",
    "as_of of",
    "since of our read handle is merely",
    "insufficient read holds",
    "dataflow has an as_of not beyond",
)

# Other errors that we treat as transient: connection dropped because
# Antithesis killed clusterd/materialized; admission control; broker
# blips. None of these are the bug; they're noise we expect under fault
# injection.
_TRANSIENT_PATTERNS = (
    "connection refused",
    "connection reset",
    "server closed the connection",
    "is (re)initializing",
    "toomanyrequests",
    "terminating connection due to administrator command",
)

# How many SELECTs to fire inside each explicit transaction. Big enough
# that we cover the >1-subsequent-query case (which is where stored
# read holds get re-resolved); small enough that Antithesis can run
# many independent invocations concurrently.
SELECTS_PER_TXN = 8

# Bytes injected per autocommit-write to drive frontier advance. A few
# rows per invocation, multiplied by many concurrent invocations, is
# enough churn for the timestamp oracle to move between txn-statements.
WRITE_ROWS_PER_INVOCATION = 5


def _matches_any(msg: str, patterns: tuple[str, ...]) -> bool:
    lo = msg.lower()
    return any(p in lo for p in patterns)


def _drive_frontier_advance(batch_id: str) -> int:
    """Insert a handful of rows under autocommit so the table's persist
    frontier moves while the explicit txn (next step) is in flight.

    Returns the number of rows actually committed (some inserts may
    fail under fault injection; that's fine, we just want non-zero
    churn on aggregate across all invocations).
    """
    inserted = 0
    try:
        with connect(autocommit=True) as conn, conn.cursor() as cur:
            for i in range(WRITE_ROWS_PER_INVOCATION):
                # `id` carries a per-invocation high bit so concurrent
                # invocations don't collide on the same primary key.
                # 63-bit random id: collision odds across the whole
                # workload lifetime are vanishingly small; plain INSERT
                # is fine. (Materialize doesn't support
                # `ON CONFLICT DO UPDATE` — that's a Postgres extension.)
                row_id = helper_random.random_u64() & 0x7FFF_FFFF_FFFF_FFFF
                try:
                    cur.execute(
                        f"INSERT INTO {TABLE_NAME} (id, v) VALUES (%s, %s)",
                        (row_id, i),
                    )
                    inserted += 1
                except psycopg.Error as exc:
                    LOG.info("frontier-advance insert failed (%s); skipping", exc)
    except Exception as exc:  # noqa: BLE001
        LOG.info("frontier-advance: open/close failed (%s); skipping", exc)
    return inserted


def _run_explicit_txn(batch_id: str) -> tuple[bool, str | None]:
    """Open a non-autocommit connection, run BEGIN -> N SELECTs -> COMMIT.

    Returns (clean, error_message_or_None). `clean` is True iff every
    SELECT inside the txn succeeded and the COMMIT landed. The caller
    decides whether a non-clean run is a property violation
    (SinceViolation pattern) or a transient (fault-injection noise).
    """
    # Use autocommit=True at the psycopg layer and send `BEGIN`
    # explicitly. Two reasons: (a) the wire conversation matches what
    # `in_immediate_multi_stmt_txn` actually checks on Materialize's
    # side — that flag toggles on the explicit `BEGIN`, not on
    # psycopg's implicit-txn-from-non-autocommit behavior; (b) it
    # lets us be exact about transaction boundaries in logs and
    # avoid the implicit-rollback-on-error landmine when something
    # inside the txn raises.
    try:
        with connect(autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("BEGIN")
            try:
                for i in range(SELECTS_PER_TXN):
                    if i % 2 == 0:
                        cur.execute(
                            f"SELECT count(*) FROM {TABLE_NAME} WHERE id > %s",
                            (i,),
                        )
                    else:
                        cur.execute(f"SELECT n, s FROM {MV_NAME}")
                    cur.fetchall()
                cur.execute("COMMIT")
            except psycopg.Error:
                # Roll back so the connection isn't left in a wedged
                # txn state. ROLLBACK against a connection that already
                # aborted is a no-op error; ignore.
                try:
                    cur.execute("ROLLBACK")
                except psycopg.Error:
                    pass
                raise
        return True, None
    except psycopg.Error as exc:
        return False, str(exc)
    except Exception as exc:  # noqa: BLE001
        # Non-psycopg errors (e.g. the connection helper itself gave
        # up after its retry budget) are treated as transients too.
        return False, str(exc)


def main() -> int:
    batch_id = f"t{helper_random.random_u64():016x}"
    LOG.info("driver starting; batch_id=%s", batch_id)

    # Step 1: drive frontier advance.
    inserted = _drive_frontier_advance(batch_id)
    LOG.info("batch=%s inserted %d frontier-advance rows", batch_id, inserted)

    # Optional: very small sleep so the timestamp oracle has a moment
    # to publish the new frontier before the next connection opens.
    # Antithesis fault injection will stretch or compress this window
    # arbitrarily anyway; the sleep is a baseline, not a guarantee.
    time.sleep(0.05)

    # Step 2: run the explicit transaction; capture verdict.
    clean, err = _run_explicit_txn(batch_id)

    if clean:
        sometimes(
            True,
            "explicit-txn: full BEGIN..SELECTs..COMMIT cycle completes cleanly",
            {"batch_id": batch_id, "selects": SELECTS_PER_TXN},
        )
        # Safety side fires a trivially-true `always` so the assertion
        # site exists in the catalog even on no-fault runs; Antithesis
        # uses presence-of-firing as coverage signal.
        always(
            True,
            "peek: explicit-txn reads never surface a SinceViolation or "
            "since-related read-hold error",
            {"batch_id": batch_id, "verdict": "clean"},
        )
        LOG.info("batch=%s txn clean", batch_id)
        return 0

    assert err is not None
    if _matches_any(err, _SINCE_VIOLATION_PATTERNS):
        # The bug. Surface as a property violation with the offending
        # message so triage can correlate against #11224 et al.
        always(
            False,
            "peek: explicit-txn reads never surface a SinceViolation or "
            "since-related read-hold error",
            {
                "batch_id": batch_id,
                "verdict": "since_violation",
                "error": err[:1500],
            },
        )
        LOG.warning("batch=%s SINCE VIOLATION caught: %s", batch_id, err[:200])
        return 0

    if _matches_any(err, _TRANSIENT_PATTERNS):
        sometimes(
            False,
            "explicit-txn: full BEGIN..SELECTs..COMMIT cycle completes cleanly",
            {
                "batch_id": batch_id,
                "verdict": "transient",
                "error": err[:500],
            },
        )
        LOG.info("batch=%s txn transient: %s", batch_id, err[:200])
        return 0

    # Unknown error class: not a SinceViolation, not a recognized
    # transient. Record it as transient (to avoid false-positive
    # property violations) but log loud enough to surface in triage.
    sometimes(
        False,
        "explicit-txn: full BEGIN..SELECTs..COMMIT cycle completes cleanly",
        {
            "batch_id": batch_id,
            "verdict": "unknown",
            "error": err[:500],
        },
    )
    logging.getLogger("driver.explicit_txn").warning(
        "batch=%s txn unknown-error: %s", batch_id, err[:300]
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
