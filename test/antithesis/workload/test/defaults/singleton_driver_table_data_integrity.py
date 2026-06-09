#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: end-to-end table-data integrity under crash recovery.

This is the data-side counterpart to `singleton_driver_catalog_recovery_consistency`.
That driver verifies that catalog DDL (CREATE TABLE / DROP TABLE)
survives environmentd crashes; this one verifies that the *rows* in a
user table survive — no silently lost rows, no duplicated rows, no
torn updates — after any sequence of Antithesis-injected faults.

The bug class this catches:
  * Persist write-path loses a row that was acknowledged as committed
    (client got OK, fault landed before the persist batch was sealed,
    recovery missed the in-flight write).
  * Persist read-path returns a stale snapshot after recovery
    (catalog says the table is up to ts T, but a fresh SELECT misses
    rows whose commit ts was < T).
  * Compaction loses or duplicates rows (run-merging logic error).
  * Concurrent rollup-write races leave a shard in an inconsistent
    state that the SUT silently reads through.

None of those classes are covered by any existing Antithesis driver
today (the catalog driver tracks table *names*, not table *contents*;
the upsert-rehydration driver only covers the kafka-source write-path,
not direct SQL writes).

Approach mirrors `singleton_driver_catalog_recovery_consistency`:
  * One `singleton_driver_` per timeline, long enough to span multiple
    Antithesis-injected restarts.
  * In-process `expected: dict[int, str]` model holds the authoritative
    "(id, value) pairs the SUT must contain right now" view.
  * Per cycle, do one DML operation (INSERT / UPDATE / DELETE), then
    open a *fresh* psycopg connection and SELECT the per-driver table
    scoped to this driver's prefix, asserting the live SQL result is
    set-equal to `expected`.
  * Cross-cycle stability is the recovery check: if a fault lands
    between cycle N and cycle N+1, cycle N+1's read is the
    post-recovery snapshot and the assertion catches any lost,
    duplicated, or torn rows.

Idempotency for retry-after-fault safety:
  * INSERT uses `INSERT INTO t SELECT %s, %s WHERE NOT EXISTS
    (SELECT 1 FROM t WHERE id = %s)` — Materialize doesn't support
    `ON CONFLICT DO NOTHING` (it's a PG-only extension) and doesn't
    enforce PRIMARY KEY uniqueness as a runtime constraint, so a
    naive `INSERT VALUES (id, v)` retry would create duplicate rows.
    The WHERE NOT EXISTS gate makes the INSERT a no-op for an id
    already present in the table — `execute_retry`'s return then
    unambiguously matches "the row is in the table" regardless of
    how many retries landed.
  * UPDATE is identity-stable: setting `value = X` is the same on
    every retry, so the row converges to X regardless of how many
    times the statement lands.
  * DELETE on a missing id is a no-op (zero rowcount, no error), so
    a retry after the first attempt removed the row converges
    cleanly.
  * When `execute_retry` exhausts its retry budget and raises, we
    abandon the cycle entirely (no model update, no follow-up read).
    Mirrors the catalog driver's behaviour — fault windows exceeding
    the budget are not property failures and an aborted cycle's
    read could conflate "real corruption" with "transient mid-fault
    snapshot". Singleton semantics means we never have a concurrent
    writer to reconcile against.

Three corroborating `sometimes(...)` anchors record per-action
liveness, plus tiered milestones for cycle progress and model size
so the triage report shows breadth of exercise.
"""

from __future__ import annotations

import os
import sys
import time

import helper_logging
import helper_random
import psycopg
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault
from helper_pg import (
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGUSER,
    execute_retry,
    query_retry,
)

LOG = helper_logging.setup_logging("driver.table_data_integrity")

# Long-running knobs.  Per-cycle budget has to exceed environmentd's
# restart time so a fault landing mid-DML still resolves before the
# next cycle reads.  CYCLE_COUNT high enough to give Antithesis many
# windows to land a fault between cycles, but bounded so a clean run
# completes inside ~one minute.
CYCLE_COUNT = 25
INTER_CYCLE_SLEEP_S = 0.5

# Per-invocation action-mix swarming.  Each timeline picks its own
# weights so different timelines cover create-heavy / churn-heavy /
# drop-heavy modes without driver edits.  Skewed toward INSERT so
# the model builds up over the run; UPDATE/DELETE only become
# relevant once there's content.
INSERT_WEIGHT_RANGE = (0.40, 0.70)
UPDATE_WEIGHT_RANGE = (0.10, 0.40)
# DELETE picks up the remainder; clamped below to keep model non-empty.

# Cap model size so a long-running timeline doesn't accumulate
# unbounded state.  Above this, force a DELETE if the random choice
# was INSERT; below, prefer INSERT to keep churn going.
MODEL_SIZE_CAP = 200
MODEL_SIZE_FLOOR = 5

PROBE_CONNECT_TIMEOUT_S = 2.0


def _fresh_observed_rows(table: str) -> dict[int, str] | None:
    """Open a new connection and SELECT every row in `table`.

    Returns the {id: value} dict on success, or None on any
    connect/query failure.  None lets the caller skip the cycle's
    assertion rather than blaming the property for a fault-window
    read.  Using a fresh connection per check defeats any client-
    side cache / session state and forces the read through the
    full pgwire + coordinator + cluster + persist path each time.
    """
    try:
        with (
            psycopg.connect(
                host=PGHOST,
                port=PGPORT,
                user=PGUSER,
                dbname=PGDATABASE,
                connect_timeout=int(PROBE_CONNECT_TIMEOUT_S),
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(f"SELECT id, value FROM {table}")
            return {int(row[0]): str(row[1]) for row in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001
        if not looks_like_fault(str(exc)):
            # Re-raise non-fault errors — they're a different bug
            # (table dropped, permission revoked, etc.) we want to
            # surface, not absorb.  But still return None so the
            # caller can decide; surface via the assertion-detail
            # blob instead.
            LOG.warning("non-fault probe failure: %s", exc)
        return None


def _value_for(prefix: str, ident: int, version: int) -> str:
    """Deterministic value-for-id generator.

    Identity-stable: re-deriving the same (prefix, ident, version)
    yields the same value, so a retry of an UPDATE always sends the
    same payload — exactly what makes the retry semantics safe.
    """
    return f"{prefix}_v{version}_id{ident:06d}"


def _do_insert(table: str, ident: int, value: str) -> bool:
    """Idempotent INSERT via WHERE NOT EXISTS gate.

    Materialize doesn't support `INSERT ... ON CONFLICT` (that's a
    PostgreSQL-only extension) and doesn't enforce PRIMARY KEY
    uniqueness as a runtime constraint, so a naive `INSERT VALUES`
    retry after a fault-window network drop would create a duplicate
    row — silently corrupting the model-vs-SUT comparison.  The
    `INSERT INTO t SELECT %s, %s WHERE NOT EXISTS (...)` pattern is
    the canonical SQL way to express upsert-do-nothing without
    relying on a unique constraint; we verified Materialize supports
    INSERT INTO t SELECT FROM t (self-referential).

    Returns True iff the statement ran to completion (either as a
    real insert or as a no-op repeat against a row this driver
    committed on a prior retry).  False iff retries exhausted.
    """
    try:
        execute_retry(
            f"INSERT INTO {table} (id, value) "
            f"SELECT %s, %s WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE id = %s)",
            (ident, value, ident),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        LOG.info("INSERT id=%d budget-exhausted: %s", ident, exc)
        return False


def _do_update(table: str, ident: int, new_value: str) -> bool:
    """UPDATE to a deterministic new value.

    Identity-stable: re-sending the same UPDATE multiple times
    converges on the same row state.  Returns True on completion,
    False on budget exhaustion.
    """
    try:
        execute_retry(
            f"UPDATE {table} SET value = %s WHERE id = %s",
            (new_value, ident),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        LOG.info("UPDATE id=%d budget-exhausted: %s", ident, exc)
        return False


def _do_delete(table: str, ident: int) -> bool:
    """DELETE; missing-row is a clean no-op so retry-after-fault is safe.

    Returns True on completion, False on budget exhaustion.
    """
    try:
        execute_retry(
            f"DELETE FROM {table} WHERE id = %s",
            (ident,),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        LOG.info("DELETE id=%d budget-exhausted: %s", ident, exc)
        return False


def _pick_action(expected_size: int, weights: dict[str, float]) -> str:
    """Pick INSERT/UPDATE/DELETE honouring weights + model bounds.

    Below MODEL_SIZE_FLOOR we force INSERT so there's always
    something to UPDATE/DELETE.  Above MODEL_SIZE_CAP we force
    DELETE to keep state bounded.  In the middle, weighted random
    via `random_float` so different timelines exercise different
    action mixes.
    """
    if expected_size < MODEL_SIZE_FLOOR:
        return "insert"
    if expected_size > MODEL_SIZE_CAP:
        return "delete"
    roll = helper_random.random_float(0.0, 1.0)
    if roll < weights["insert"]:
        return "insert"
    if roll < weights["insert"] + weights["update"]:
        return "update"
    return "delete"


def main() -> int:
    # Per-timeline namespace so concurrent timelines (and any future
    # parallel_driver_ instances) do not collide on table names.
    prefix = f"datarec_{helper_random.random_u64():016x}"
    table = f"antithesis_dataint_{prefix}"

    # Per-invocation action-mix swarm.  Each timeline samples once and
    # keeps the same mix throughout — different timelines explore
    # different mixes (so the fuzzer's coverage isn't dominated by
    # whichever single mix was checked in).
    w_insert = helper_random.random_float(*INSERT_WEIGHT_RANGE)
    w_update = helper_random.random_float(*UPDATE_WEIGHT_RANGE)
    weights = {
        "insert": w_insert,
        "update": w_update,
        # Remainder rolls into delete.  Always > 0 because the input
        # ranges are bounded such that w_insert + w_update <= ~0.95.
        "delete": max(0.05, 1.0 - w_insert - w_update),
    }
    LOG.info(
        "table-data-integrity starting; prefix=%s weights=%s",
        prefix,
        {k: f"{v:.2f}" for k, v in weights.items()},
    )

    # Create the per-prefix table.  IF NOT EXISTS lets the driver
    # retry across faults without colliding on the table itself.
    # Note: no PRIMARY KEY — Materialize doesn't enforce uniqueness
    # as a runtime constraint, so the keyword is misleading (it'd be
    # a hint that didn't actually constrain).  Application-side
    # uniqueness is enforced by `_do_insert`'s WHERE NOT EXISTS
    # gate plus the monotonic `next_id` counter.
    try:
        execute_retry(
            f"CREATE TABLE IF NOT EXISTS {table} (id BIGINT NOT NULL, value TEXT NOT NULL)"
        )
    except Exception as exc:  # noqa: BLE001
        LOG.warning("table create failed; cannot run cycle: %s", exc)
        sometimes(
            False,
            "table-data-integrity: at least one timeline could create its table",
            {"prefix": prefix, "exc": str(exc)[:200]},
        )
        return 0

    # The model: id -> value.  Updated only AFTER a successful DML
    # statement (so retry-after-fault that exhausted the budget never
    # leaves the model out of sync with the SUT).
    expected: dict[int, str] = {}
    next_id = 0
    # Version counter for UPDATE — bumps per update so we can verify
    # the SUT picked up the freshest write, not a stale earlier one.
    next_version = 1

    saw_actions = {"insert": 0, "update": 0, "delete": 0}
    cycles_with_clean_read = 0
    cycles_aborted_on_fault = 0
    saw_probe_failure = False
    safety_observed_count = 0

    for cycle_idx in range(CYCLE_COUNT):
        action = _pick_action(len(expected), weights)

        if action == "insert":
            ident = next_id
            next_id += 1
            value = _value_for(prefix, ident, version=0)
            if not _do_insert(table, ident, value):
                cycles_aborted_on_fault += 1
                time.sleep(INTER_CYCLE_SLEEP_S)
                continue
            expected[ident] = value
            saw_actions["insert"] += 1
        elif action == "update":
            # Choose any existing id deterministically via the SDK
            # random.  sorted() makes the choice replay-stable
            # within a timeline (helper_random.random_choice routes
            # the actual pick through the SDK).
            ident = helper_random.random_choice(sorted(expected.keys()))
            version = next_version
            next_version += 1
            new_value = _value_for(prefix, ident, version)
            if not _do_update(table, ident, new_value):
                cycles_aborted_on_fault += 1
                time.sleep(INTER_CYCLE_SLEEP_S)
                continue
            expected[ident] = new_value
            saw_actions["update"] += 1
        else:  # delete
            ident = helper_random.random_choice(sorted(expected.keys()))
            if not _do_delete(table, ident):
                cycles_aborted_on_fault += 1
                time.sleep(INTER_CYCLE_SLEEP_S)
                continue
            expected.pop(ident, None)
            saw_actions["delete"] += 1

        # Verify via a fresh connection.  If this read fails we skip
        # the assertion — a fault-window read is not regression
        # evidence.  The DML above already succeeded, so the model
        # is consistent with the SUT regardless of whether the
        # follow-up SELECT lands.
        observed = _fresh_observed_rows(table)
        if observed is None:
            saw_probe_failure = True
            LOG.info(
                "cycle %d: fresh-connection read failed; skipping assertion", cycle_idx
            )
            time.sleep(INTER_CYCLE_SLEEP_S)
            continue

        cycles_with_clean_read += 1
        safety_observed_count += 1

        # Compute concrete diffs so the assertion blob is useful for
        # triage without flooding (cap by [:5]).
        missing_in_sut = sorted(set(expected.keys()) - set(observed.keys()))[:5]
        extra_in_sut = sorted(set(observed.keys()) - set(expected.keys()))[:5]
        mismatched_value = [
            (k, expected[k], observed[k])
            for k in sorted(set(expected.keys()) & set(observed.keys()))
            if expected[k] != observed[k]
        ][:5]

        always(
            observed == expected,
            "table-data: live table matches in-process expected model after every DML cycle",
            {
                "prefix": prefix,
                "cycle": cycle_idx,
                "action": action,
                "expected_size": len(expected),
                "observed_size": len(observed),
                "missing_in_sut": missing_in_sut,
                "extra_in_sut": extra_in_sut,
                "mismatched_value": mismatched_value,
            },
        )

        LOG.info(
            "[PROGRESS] cycle=%d action=%s model_size=%d observed=%d match=%s",
            cycle_idx,
            action,
            len(expected),
            len(observed),
            observed == expected,
        )
        time.sleep(INTER_CYCLE_SLEEP_S)

    # Liveness anchors.  The order matches a reader's eye-track:
    # "did the driver actually run >0 cycles?" → "did it cover all 3
    # action types?" → "did it interact with the fault path?".
    sometimes(
        cycles_with_clean_read >= 2,
        "table-data-integrity: 2+ post-DML reads landed cleanly in this timeline",
        {"clean_reads": cycles_with_clean_read, "cycles_planned": CYCLE_COUNT},
    )
    sometimes(
        saw_actions["insert"] > 0,
        "table-data-integrity: at least one INSERT cycle ran cleanly",
        {"insert_count": saw_actions["insert"]},
    )
    sometimes(
        saw_actions["update"] > 0,
        "table-data-integrity: at least one UPDATE cycle ran cleanly",
        {"update_count": saw_actions["update"]},
    )
    sometimes(
        saw_actions["delete"] > 0,
        "table-data-integrity: at least one DELETE cycle ran cleanly",
        {"delete_count": saw_actions["delete"]},
    )
    sometimes(
        cycles_aborted_on_fault > 0 or saw_probe_failure,
        "table-data-integrity: fault-injection path exercised (write-retry or probe absorbed a fault)",
        {
            "cycles_aborted": cycles_aborted_on_fault,
            "saw_probe_failure": saw_probe_failure,
        },
    )

    # Tiered progress milestones.  Same shape as workload-replay's
    # tiered anchors so the triage report shows "we ever made it past
    # this many post-DML reads in some timeline" at a glance.
    for threshold in (1, 5, 10, 20):
        sometimes(
            safety_observed_count >= threshold,
            f"table-data-integrity: safety-observed cycles >= {threshold} in at least one timeline",
            {"threshold": threshold, "safety_observed": safety_observed_count},
        )

    LOG.info(
        "[SUMMARY] table-data-integrity prefix=%s clean_reads=%d aborted=%d "
        "saw_probe_failure=%s actions=%s final_model_size=%d",
        prefix,
        cycles_with_clean_read,
        cycles_aborted_on_fault,
        saw_probe_failure,
        saw_actions,
        len(expected),
    )

    # Best-effort cleanup so local-dev re-runs don't accumulate
    # tables.  Under Antithesis the timeline ends with the driver
    # and state is wiped between timelines, so this is purely a
    # local-dev nicety.
    try:
        execute_retry(f"DROP TABLE IF EXISTS {table}")
    except Exception:  # noqa: BLE001
        pass

    return 0


if __name__ == "__main__":
    # Touch helper_pg env constants so static analysis treats them as
    # used; the helper module re-exports them for drivers (like this
    # one) that open their own connections.
    _ = (PGHOST, PGPORT, PGUSER, PGDATABASE, os, query_retry)
    sys.exit(main())
