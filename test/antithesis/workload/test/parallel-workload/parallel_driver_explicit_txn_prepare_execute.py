#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for property `peek-no-since-violation` (PREPARE/EXECUTE arm).

Sibling of `parallel_driver_explicit_txn_no_since_violation`.  That driver
issues plain `SELECT` statements inside `BEGIN..COMMIT`, exercising the
new peek sequencing (`frontend_peek.rs`). This one issues
`PREPARE … EXECUTE p; EXECUTE p; …` inside `BEGIN..COMMIT`, exercising
the **old peek sequencing** (`sequence_plan → sequence_execute →
handle_execute`) — `EXECUTE` of a prepared statement always routes
through the old sequencer even when `enable_frontend_peek_sequencing`
is on.

The bug class (SQL-261 / database-issues#11224, #11200, #10050, #9965)
surfaces in both peek sequencings, and Gábor noted on #11200 that the
EXECUTE-from-explicit-txn surface is *not* subsumed by the direct-SELECT
driver. Hence a separate driver — small clone of the SELECT one with the
SQL changed.

Each invocation:
  1. Issues a small write against `antithesis_txn_table` (autocommit)
     to advance the table's persist frontier — same as the SELECT arm.
  2. Opens a separate connection, runs an explicit transaction:
         BEGIN
         PREPARE p_table AS SELECT count(*) FROM antithesis_txn_table WHERE id > $1
         PREPARE p_mv    AS SELECT n, s FROM <mv>
         EXECUTE p_table(0)
         EXECUTE p_mv
         SELECT count(*) FROM antithesis_txn_table          (new peek path)
         EXECUTE p_table(1)
         EXECUTE p_mv
         SELECT n, s FROM <mv>                              (new peek path)
         EXECUTE p_table(2)
         EXECUTE p_mv
         DEALLOCATE p_table
         DEALLOCATE p_mv
         COMMIT
     The mix of `EXECUTE`s and direct `SELECT`s inside the same txn
     hits both peek sequencings from a single stored-read-hold context,
     which is the surface @teskje called out on database-issues#11200
     (`PeekError` is only raised by `ComputeController::peek`, the path
     `EXECUTE` always uses; direct SELECTs go through the frontend
     sequencer's own peek path).
  3. SDK coin flip picks plain-MV vs REFRESH-EVERY-MV per invocation,
     same as the SELECT arm.
  4. SinceViolation pattern -> `always(False)` on the shared property.
     Other errors that match `helper_fault_tolerance.looks_like_fault`
     -> `sometimes(False)` (fault-injection noise, not a bug).
"""

from __future__ import annotations

import logging
import sys
import time

import helper_logging
import helper_random
import psycopg
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault
from helper_pg import connect

LOG = helper_logging.setup_logging("driver.explicit_txn_prepare_execute")

CLUSTER = "antithesis_cluster"
TABLE_NAME = "antithesis_txn_table"
MV_NAME_PLAIN = "antithesis_txn_mv"
MV_NAME_REFRESH_EVERY = "antithesis_txn_mv_refresh_every"

# Property-violation indicators: substrings the driver actively *catches*
# and fires `always(False)` on. Distinct from fault-injection noise
# (matched via `helper_fault_tolerance.looks_like_fault`) and kept local
# because they define this driver's catch surface, not what it ignores.
_SINCE_VIOLATION_PATTERNS = (
    "sinceviolation",
    "as_of not beyond",
    "as_of of",
    "since of our read handle is merely",
    "insufficient read holds",
    "dataflow has an as_of not beyond",
)

# Number of EXECUTE calls + interleaved direct SELECTs per txn. Sized
# like the SELECT driver's SELECTS_PER_TXN=8 — large enough that the
# second-and-subsequent stored-read-hold reuse path gets exercised,
# small enough that many invocations can run concurrently.
EXECUTES_PER_TXN = 6
DIRECT_SELECTS_PER_TXN = 2

# Bytes injected per autocommit-write to drive frontier advance. Matches
# the SELECT driver so both arms exert roughly the same write pressure
# on the table's persist frontier.
WRITE_ROWS_PER_INVOCATION = 5


def _matches_any(msg: str, patterns: tuple[str, ...]) -> bool:
    lo = msg.lower()
    return any(p in lo for p in patterns)


def _drive_frontier_advance() -> int:
    """Insert a handful of rows under autocommit so the table's persist
    frontier moves while the explicit txn (next step) is in flight.

    Identical shape to the SELECT driver's version — kept duplicated for
    the same per-driver-self-contained reason as the pattern lists.
    """
    inserted = 0
    try:
        with connect(autocommit=True) as conn, conn.cursor() as cur:
            for i in range(WRITE_ROWS_PER_INVOCATION):
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


def _run_explicit_txn(batch_id: str, mv_name: str) -> tuple[bool, str | None]:
    """Open a connection, run BEGIN -> PREPARE -> mixed EXECUTE/SELECT -> COMMIT.

    Returns (clean, error_message_or_None). `clean` is True iff every
    EXECUTE and SELECT inside the txn succeeded and the COMMIT landed.

    The intermixing of EXECUTE (old peek sequencing, hits
    `ComputeController::peek`) with direct SELECT (new peek sequencing,
    hits `frontend_peek.rs`) on the same set of stored read holds is
    what makes this driver complementary to its sibling — the bug has
    fired on both code paths separately in CI, and the read-hold storage
    is shared between them inside an explicit txn.
    """
    try:
        with connect(autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("BEGIN")
            try:
                # Two prepared statements: one parameterised against the
                # table (so EXECUTEs vary their predicate), one against
                # the chosen MV. Distinct names so an outer DEALLOCATE
                # at the end of the txn cleanly tears them both down.
                cur.execute(
                    f"PREPARE p_table AS "
                    f"SELECT count(*) FROM {TABLE_NAME} WHERE id > $1"
                )
                cur.execute(f"PREPARE p_mv AS " f"SELECT n, s FROM {mv_name}")

                # Interleave EXECUTEs and direct SELECTs so both peek
                # paths fire against the same stored-read-hold context.
                # Cadence: every Nth statement is a direct SELECT, the
                # rest are EXECUTEs — keeps the EXECUTE-path density
                # high (it's the under-exercised arm) while still
                # forcing at least DIRECT_SELECTS_PER_TXN trips through
                # the new peek sequencing.
                exec_idx = 0
                direct_idx = 0
                total = EXECUTES_PER_TXN + DIRECT_SELECTS_PER_TXN
                # Pick statement positions for the direct SELECTs deterministically
                # from EXECUTES_PER_TXN+DIRECT_SELECTS_PER_TXN total slots.
                # Spreading them out (not bunched at the start) matters because
                # the bug fires on "subsequent" statements that reuse stored
                # read holds — we want at least one direct SELECT *after* some
                # EXECUTEs and at least one EXECUTE *after* some SELECTs.
                direct_positions = {
                    int(i * (total / max(DIRECT_SELECTS_PER_TXN, 1)) + 1)
                    for i in range(DIRECT_SELECTS_PER_TXN)
                }
                for slot in range(total):
                    if slot in direct_positions and direct_idx < DIRECT_SELECTS_PER_TXN:
                        # Direct SELECT — new peek sequencing path.
                        if direct_idx % 2 == 0:
                            cur.execute(
                                f"SELECT count(*) FROM {TABLE_NAME} WHERE id > %s",
                                (direct_idx,),
                            )
                        else:
                            cur.execute(f"SELECT n, s FROM {mv_name}")
                        direct_idx += 1
                    else:
                        # EXECUTE — old peek sequencing path.
                        # Inline the integer rather than parameter-binding it:
                        # psycopg's %s rewrites to the extended-protocol $1,
                        # and Materialize rejects $1 inside EXECUTE's argument
                        # list with "there is no parameter $1".
                        if exec_idx % 2 == 0:
                            cur.execute(f"EXECUTE p_table({int(exec_idx)})")
                        else:
                            cur.execute("EXECUTE p_mv")
                        exec_idx += 1
                    cur.fetchall()

                cur.execute("DEALLOCATE p_table")
                cur.execute("DEALLOCATE p_mv")
                cur.execute("COMMIT")
            except psycopg.Error:
                # ROLLBACK to clear the wedged txn state. Best-effort:
                # if the connection is already gone, the ROLLBACK
                # itself will raise; swallow and propagate the outer
                # exception unchanged.
                try:
                    cur.execute("ROLLBACK")
                except psycopg.Error:
                    pass
                raise
        return True, None
    except psycopg.Error as exc:
        return False, str(exc)
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def main() -> int:
    batch_id = f"px{helper_random.random_u64():016x}"
    use_refresh_mv = (helper_random.random_u64() & 1) == 1
    mv_name = MV_NAME_REFRESH_EVERY if use_refresh_mv else MV_NAME_PLAIN
    mv_kind = "refresh_every" if use_refresh_mv else "plain"
    LOG.info(
        "driver starting; batch_id=%s mv=%s executes=%d direct=%d",
        batch_id,
        mv_name,
        EXECUTES_PER_TXN,
        DIRECT_SELECTS_PER_TXN,
    )

    inserted = _drive_frontier_advance()
    LOG.info("batch=%s inserted %d frontier-advance rows", batch_id, inserted)
    time.sleep(0.05)

    clean, err = _run_explicit_txn(batch_id, mv_name)

    sometimes(
        use_refresh_mv,
        "explicit-txn-prepare-execute: invocation reads from the REFRESH EVERY MV",
        {"batch_id": batch_id, "mv": mv_name},
    )
    sometimes(
        not use_refresh_mv,
        "explicit-txn-prepare-execute: invocation reads from the plain MV",
        {"batch_id": batch_id, "mv": mv_name},
    )

    if clean:
        sometimes(
            True,
            "explicit-txn-prepare-execute: BEGIN..(PREPARE/EXECUTE+SELECT)..COMMIT completes cleanly",
            {
                "batch_id": batch_id,
                "executes": EXECUTES_PER_TXN,
                "direct_selects": DIRECT_SELECTS_PER_TXN,
                "mv_kind": mv_kind,
            },
        )
        # Shared property with the SELECT driver — both arms attest the
        # same invariant ("explicit-txn reads never surface a
        # SinceViolation"), they just exercise different sequencers.
        always(
            True,
            "peek: explicit-txn reads never surface a SinceViolation or "
            "since-related read-hold error",
            {"batch_id": batch_id, "verdict": "clean", "arm": "prepare_execute"},
        )
        LOG.info("batch=%s txn clean", batch_id)
        return 0

    assert err is not None
    if _matches_any(err, _SINCE_VIOLATION_PATTERNS):
        always(
            False,
            "peek: explicit-txn reads never surface a SinceViolation or "
            "since-related read-hold error",
            {
                "batch_id": batch_id,
                "verdict": "since_violation",
                "arm": "prepare_execute",
                "mv_kind": mv_kind,
                "error": err[:1500],
            },
        )
        LOG.warning("batch=%s SINCE VIOLATION caught: %s", batch_id, err[:200])
        return 0

    if looks_like_fault(err):
        sometimes(
            False,
            "explicit-txn-prepare-execute: BEGIN..(PREPARE/EXECUTE+SELECT)..COMMIT completes cleanly",
            {
                "batch_id": batch_id,
                "verdict": "transient",
                "error": err[:500],
            },
        )
        LOG.info("batch=%s txn transient: %s", batch_id, err[:200])
        return 0

    sometimes(
        False,
        "explicit-txn-prepare-execute: BEGIN..(PREPARE/EXECUTE+SELECT)..COMMIT completes cleanly",
        {
            "batch_id": batch_id,
            "verdict": "unknown",
            "error": err[:500],
        },
    )
    logging.getLogger("driver.explicit_txn_prepare_execute").warning(
        "batch=%s txn unknown-error: %s", batch_id, err[:300]
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
