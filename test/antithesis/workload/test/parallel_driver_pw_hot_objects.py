#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: high-contention workload against shared hot objects.

Closes the per-object-pressure gap between Antithesis and CI nightly's
parallel-workload runs. Targets database-issues#11200 (`EXECUTE foo`
SinceViolation, **old** peek sequencing) and the broader #11224 family.

Every invocation runs a tight action-mix loop against the
`pw_hot_table` / `pw_hot_mv_count` / `pw_hot_mv_sum` set created by
`first_pw_hot_objects_setup`. Two assertion sites fire from this driver:

  * `peek-no-since-violation` — same message as the explicit-txn driver
    so Antithesis aggregates; details say `op=explicit-txn`.
  * `prepared-execute-no-since-violation` — distinct message, the
    old-peek-sequencing variant via `PREPARE … EXECUTE … DEALLOCATE`.
    Bugs here probably outlive a fix to the new-peek-sequencing variant
    so keeping the two assertion sites separate lets triage tell us
    *which* path is regressing.

Action mix (weights chosen to maximise per-MV pressure while keeping
the loop fault-tolerant):

  30  INSERT into pw_hot_table          (drives source frontier advance)
  25  PREPARE+EXECUTE on pw_hot_mv_count (old peek sequencing path)
  15  PREPARE+EXECUTE on pw_hot_mv_sum + table  (larger input bundle)
  15  BEGIN; SELECT mv; SELECT table; COMMIT   (new peek sequencing path)
  10  Single-shot SELECT                 (baseline happy path)
   5  DELETE old rows                    (frontier retreat + storage bound)

Connection: one autocommit connection per invocation, reused for every
action in the loop (PREPAREd statements are connection-scoped). The
explicit-txn action sends `BEGIN`/`COMMIT` on the wire — psycopg's
autocommit knob is intentionally not toggled, so the wire conversation
matches what Materialize's `in_immediate_multi_stmt_txn` flag tracks.
"""

from __future__ import annotations

import sys
import time

import helper_logging
import helper_pw_hot
import helper_random
import psycopg
from helper_pg import connect
from helper_pw_hot import (
    INDEX_NAME,  # noqa: F401  -- exposed for triage details
    MV_COUNT_NAME,
    MV_SUM_NAME,
    RETENTION,
    TABLE_NAME,
)

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.pw_hot")

# Total wallclock budget per invocation. Long enough to fire many
# PREPAREs and DELETEs; short enough that Antithesis can interleave
# many invocations across a run.
RUNTIME_S = 10.0

# Each action's relative weight in the picker. Tuples of (name, weight).
# Names are also the assertion-detail tag — keep them grep-friendly.
_ACTIONS: tuple[tuple[str, int], ...] = (
    ("insert", 30),
    ("prepared_count", 25),
    ("prepared_join", 15),
    ("explicit_txn", 15),
    ("select_baseline", 10),
    ("delete_old", 5),
)


def _pick_action(rng_total: int) -> str:
    """Weighted random action pick. `rng_total` is `sum(w)` precomputed."""
    pick = helper_random.random_int(1, rng_total)
    cumulative = 0
    for name, weight in _ACTIONS:
        cumulative += weight
        if pick <= cumulative:
            return name
    return _ACTIONS[-1][0]  # unreachable; defensive


# Verdict tags for assertion details. Kept aligned with the two
# property entries in the catalog.
_VERDICT_CLEAN = "clean"
_VERDICT_TRANSIENT = "transient"
_VERDICT_SINCE_VIOLATION = "since_violation"
_VERDICT_UNKNOWN = "unknown"


def _classify(err: str) -> str:
    if helper_pw_hot.is_since_violation(err):
        return _VERDICT_SINCE_VIOLATION
    if helper_pw_hot.is_transient(err):
        return _VERDICT_TRANSIENT
    return _VERDICT_UNKNOWN


# ---------------------------------------------------------------------------
# Action implementations
# ---------------------------------------------------------------------------
#
# Each action takes (cur, batch_id, op_idx) and either completes or raises
# `psycopg.Error`. The driver loop catches the error, decides whether
# to fire `always(False)`, and continues.


def _act_insert(cur: psycopg.Cursor, batch_id: str, op_idx: int) -> None:
    row_id = helper_random.random_u64() & 0x7FFF_FFFF_FFFF_FFFF
    cur.execute(
        f"INSERT INTO {TABLE_NAME} (id, v, written_by) VALUES (%s, %s, %s)",
        (row_id, op_idx, batch_id),
    )


def _act_delete_old(cur: psycopg.Cursor, batch_id: str, op_idx: int) -> None:
    # Time-based pruning — rows older than the retention window go.
    # Each invocation issues this at low rate so storage is bounded
    # without making the DELETE the dominant frontier-advance signal.
    cur.execute(f"DELETE FROM {TABLE_NAME} WHERE ts < now() - {RETENTION}")


def _act_select_baseline(cur: psycopg.Cursor, batch_id: str, op_idx: int) -> None:
    cur.execute(f"SELECT n FROM {MV_COUNT_NAME} LIMIT 1")
    cur.fetchall()


def _act_prepared_count(cur: psycopg.Cursor, batch_id: str, op_idx: int) -> None:
    """The #11200-targeting path: PREPARE+EXECUTE on the indexed MV.

    Uses an invocation-and-op unique statement name so concurrent
    invocations don't race on the same name (the connection is per-
    invocation, so cross-invocation collision is impossible anyway, but
    op-level uniqueness within the same connection matters when the
    DEALLOCATE below didn't run because the EXECUTE raised).
    """
    stmt = f"q_{batch_id}_{op_idx}"
    cur.execute(f"PREPARE {stmt} AS SELECT n FROM {MV_COUNT_NAME}")
    try:
        cur.execute(f"EXECUTE {stmt}")
        cur.fetchall()
    finally:
        try:
            cur.execute(f"DEALLOCATE {stmt}")
        except psycopg.Error:
            pass


def _act_prepared_join(cur: psycopg.Cursor, batch_id: str, op_idx: int) -> None:
    """Like _act_prepared_count but with a multi-collection input bundle.

    The EXECUTE's `input_id_bundle` includes both the MV and the
    underlying table — exercises the stored-holds-subset path in
    `frontend_peek.rs` even though this action goes through the old
    sequencing.
    """
    stmt = f"qj_{batch_id}_{op_idx}"
    cur.execute(
        f"PREPARE {stmt} AS "
        f"SELECT mv.n, mv.s, t.v "
        f"FROM {MV_SUM_NAME} mv, {TABLE_NAME} t "
        f"WHERE t.id = (SELECT max(id) FROM {TABLE_NAME})"
    )
    try:
        cur.execute(f"EXECUTE {stmt}")
        cur.fetchall()
    finally:
        try:
            cur.execute(f"DEALLOCATE {stmt}")
        except psycopg.Error:
            pass


def _act_explicit_txn(cur: psycopg.Cursor, batch_id: str, op_idx: int) -> None:
    """BEGIN -> 4 SELECTs alternating table/MV -> COMMIT.

    This is the new-peek-sequencing path. Shares the property with
    `parallel_driver_explicit_txn_no_since_violation`, but runs against
    the hot objects which have far higher write pressure than the
    explicit-txn driver's own scaffold.
    """
    cur.execute("BEGIN")
    try:
        for i in range(4):
            if i % 2 == 0:
                cur.execute(
                    f"SELECT count(*) FROM {TABLE_NAME} WHERE id > %s",
                    (i,),
                )
            else:
                cur.execute(f"SELECT n, s FROM {MV_SUM_NAME}")
            cur.fetchall()
        cur.execute("COMMIT")
    except psycopg.Error:
        try:
            cur.execute("ROLLBACK")
        except psycopg.Error:
            pass
        raise


_ACTION_FNS = {
    "insert": _act_insert,
    "delete_old": _act_delete_old,
    "select_baseline": _act_select_baseline,
    "prepared_count": _act_prepared_count,
    "prepared_join": _act_prepared_join,
    "explicit_txn": _act_explicit_txn,
}

# Which assertion site does each action's failure feed into. Actions
# with no entry (i.e. INSERT/DELETE) don't surface SinceViolation by
# design, so any error from them goes to the generic transient/unknown
# bucket without firing a property assertion.
_ACTION_ASSERTION = {
    "select_baseline": "peek-no-since-violation",
    "prepared_count": "prepared-execute-no-since-violation",
    "prepared_join": "prepared-execute-no-since-violation",
    "explicit_txn": "peek-no-since-violation",
}


def _fire_property(
    site: str, ok: bool, batch_id: str, action: str, err: str | None
) -> None:
    """Fire the appropriate `always` for a failing peek-side action.

    `ok=True` records a clean invocation (trivially-true `always`) so
    the assertion site exists in Antithesis's catalog regardless of
    whether a violation ever fires. `ok=False` is the property
    violation itself.
    """
    if site == "peek-no-since-violation":
        always(
            ok,
            "peek: explicit-txn reads never surface a SinceViolation or "
            "since-related read-hold error",
            {
                "batch_id": batch_id,
                "action": action,
                "op": "explicit-txn-on-hot-objects",
                **({"error": err[:1500]} if err else {}),
            },
        )
    elif site == "prepared-execute-no-since-violation":
        always(
            ok,
            "peek: PREPARE+EXECUTE on indexed MV never surfaces a "
            "SinceViolation or since-related read-hold error "
            "(old peek sequencing)",
            {
                "batch_id": batch_id,
                "action": action,
                "op": "prepared-execute-on-hot-objects",
                **({"error": err[:1500]} if err else {}),
            },
        )


def main() -> int:
    batch_id = f"h{helper_random.random_u64():016x}"
    LOG.info("driver starting; batch_id=%s runtime=%.1fs", batch_id, RUNTIME_S)

    weight_total = sum(w for _, w in _ACTIONS)
    deadline = time.monotonic() + RUNTIME_S

    counts: dict[str, int] = {name: 0 for name, _ in _ACTIONS}
    transient_count = 0
    unknown_count = 0
    since_violation_count = 0
    clean_count = 0

    try:
        with connect(autocommit=True) as conn, conn.cursor() as cur:
            op_idx = 0
            while time.monotonic() < deadline:
                action = _pick_action(weight_total)
                counts[action] += 1
                fn = _ACTION_FNS[action]
                try:
                    fn(cur, batch_id, op_idx)
                    clean_count += 1
                    # Fire the trivial-true `always` so the assertion
                    # site shows up in Antithesis's catalog on clean
                    # runs (otherwise it only appears on violations).
                    site = _ACTION_ASSERTION.get(action)
                    if site is not None:
                        _fire_property(site, True, batch_id, action, None)
                except psycopg.Error as exc:
                    err = str(exc)
                    verdict = _classify(err)
                    site = _ACTION_ASSERTION.get(action)
                    if verdict == _VERDICT_SINCE_VIOLATION:
                        since_violation_count += 1
                        if site is not None:
                            _fire_property(site, False, batch_id, action, err)
                        LOG.warning(
                            "batch=%s SINCE VIOLATION on %s: %s",
                            batch_id,
                            action,
                            err[:200],
                        )
                    elif verdict == _VERDICT_TRANSIENT:
                        transient_count += 1
                        LOG.info(
                            "batch=%s transient on %s: %s",
                            batch_id,
                            action,
                            err[:200],
                        )
                    else:
                        unknown_count += 1
                        LOG.warning(
                            "batch=%s unknown-error on %s: %s",
                            batch_id,
                            action,
                            err[:300],
                        )
                op_idx += 1
    except Exception as exc:  # noqa: BLE001
        # Helper-level fault (connection budget exhausted etc.). Counts
        # as a transient run — we never even got to a peek assertion site.
        LOG.info("batch=%s connection setup/loop failed: %s", batch_id, exc)
        transient_count += 1

    # Liveness anchor: at least one action completed cleanly. If this
    # never fires across the whole run, the `always(True)` sites are
    # vacuously safe and the property catalog entries lose meaning.
    sometimes(
        clean_count > 0,
        "pw-hot: at least one action completed cleanly per invocation",
        {
            "batch_id": batch_id,
            "clean_actions": clean_count,
            "transient_errors": transient_count,
            "unknown_errors": unknown_count,
            "since_violations": since_violation_count,
            "action_counts": counts,
        },
    )

    LOG.info(
        "batch=%s done: clean=%d transient=%d unknown=%d since_violation=%d actions=%s",
        batch_id,
        clean_count,
        transient_count,
        unknown_count,
        since_violation_count,
        counts,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
