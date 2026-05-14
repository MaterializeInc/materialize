#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `catalog-recovery-consistency`.

After environmentd crashes and restarts, the catalog state must be
consistent with what was committed pre-crash: every previously-acknowledged
DDL operation must remain visible, and the catalog upper must not regress.
The user-visible form of this property is: "if I created a table and
received an OK, the table is still there after a restart."

Approach mirrors `singleton_driver_upsert_state_rehydration.py`:
  - One `singleton_driver_` per timeline, long enough to span multiple
    Antithesis-injected environmentd restarts.
  - In-process `expected_tables: set[str]` model holds the authoritative
    "what should be in the catalog right now" view.
  - Per cycle, do some DDL (CREATE TABLE or DROP TABLE), then open a
    *fresh* psycopg connection and SELECT from `mz_tables` scoped to the
    driver's namespace, asserting the live catalog matches `expected`.
  - Cross-cycle stability is the recovery check: if an environmentd
    restart lands between cycle N and cycle N+1, cycle N+1's read is the
    post-recovery snapshot and the assertion catches any lost or stuck
    DDL.

`helper_pg.execute_retry` retries OperationalError transparently, so when
environmentd is down mid-DDL the call will block-and-retry until the next
incarnation is reachable. That's exactly the timing we want: the DDL
either committed pre-crash (in which case it must reappear post-recovery)
or never committed (in which case we record it failed and update the
local model). When the retry budget elapses before recovery, we abandon
that cycle's DDL without updating the local model — fault windows
exceeding the budget are *not* property failures.

Two corroborating `sometimes(...)` anchors record (a) whether the driver
observed a coord-side connect failure during its run, and (b) whether at
least two assertion-bearing cycles ran (so the assertion at cycle N+1
genuinely reads post-restart state, not just the same state as N).
"""

from __future__ import annotations

import os
import sys
import time

import helper_logging
import helper_random
import psycopg
from helper_pg import (
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGUSER,
    execute_retry,
    query_retry,
)

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.catalog_recovery_consistency")

# Long-running knobs: the driver owns its timeline and the per-cycle budget
# has to comfortably exceed environmentd's restart time so a fault landing
# mid-DDL still resolves before the next cycle. CYCLE_COUNT high enough to
# give Antithesis multiple windows to land a restart between cycles.
CYCLE_COUNT = 10
INTER_CYCLE_SLEEP_S = 2.0

# Drop fraction is swarmed per-invocation in main(). Wide range so different
# timelines exercise create-heavy (catalog grows) and drop-heavy (churn-
# through-recovery) modes without rebuilding the driver.
DROP_PROBABILITY_RANGE = (0.10, 0.50)

PROBE_CONNECT_TIMEOUT_S = 2.0


def _fresh_observed_tables(name_prefix: str) -> set[str] | None:
    """Open a new connection and SELECT mz_tables filtered to `name_prefix`.

    Returns the set of observed table names on success, or `None` on any
    connect/query failure. None lets the caller skip the cycle's assertion
    rather than blaming the property for a fault-window read.
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
            cur.execute(
                "SELECT name FROM mz_tables WHERE name LIKE %s",
                (f"{name_prefix}%",),
            )
            return {row[0] for row in cur.fetchall()}
    except Exception:  # noqa: BLE001
        return None


def _saw_coord_unavailable() -> bool:
    """Best-effort one-shot probe with the same short connect timeout as
    the assertion reads. A failure here means a coord-side connection was
    refused or timed out within the last ~tick — a strong proxy for
    "environmentd is down or just restarted." This is corroborating signal
    only; it does not gate the safety assertion.
    """
    try:
        with psycopg.connect(
            host=PGHOST,
            port=PGPORT,
            user=PGUSER,
            dbname=PGDATABASE,
            connect_timeout=int(PROBE_CONNECT_TIMEOUT_S),
            autocommit=True,
        ) as _conn:
            pass
        return False
    except Exception:  # noqa: BLE001
        return True


def _run_cycle(
    expected: set[str],
    name_prefix: str,
    cycle_idx: int,
    next_id: int,
    drop_probability: float,
) -> tuple[bool, int]:
    """One create-or-drop + verify cycle.

    Returns (assertions_ran, next_id_after) where `assertions_ran` is True
    iff this cycle landed a successful post-DDL read against a fresh
    connection (i.e. the cycle contributes to the safety property). The
    `next_id` counter is monotonic across cycles so table names are unique
    even after drops.

    The DDL is run via `execute_retry`, which already retries transient
    OperationalError until the retry budget. If it raises anyway the
    cycle aborts and the local model is not updated — exactly the
    semantics needed: a DDL we never acknowledged is allowed to be
    missing from the post-recovery catalog.
    """
    new_id = next_id
    if expected and helper_random.random_bool(drop_probability):
        # Drop a random existing table. Choosing from `expected` keeps the
        # drop deterministic w.r.t. the local model.
        table = sorted(expected)[helper_random.random_int(0, len(expected) - 1)]
        try:
            execute_retry(f"DROP TABLE {table}")
        except Exception as exc:  # noqa: BLE001
            LOG.info(
                "cycle %d: DROP %s failed (%s); not updating model",
                cycle_idx,
                table,
                exc,
            )
            return False, new_id
        expected.discard(table)
    else:
        table = f"{name_prefix}_t{new_id:06d}"
        try:
            execute_retry(f"CREATE TABLE {table} (id BIGINT NOT NULL)")
        except Exception as exc:  # noqa: BLE001
            LOG.info(
                "cycle %d: CREATE %s failed (%s); not updating model",
                cycle_idx,
                table,
                exc,
            )
            return False, new_id
        expected.add(table)
        new_id += 1

    # Verify via a fresh connection. If this read fails, we skip the
    # assertion — a fault-window read is not regression evidence.
    observed = _fresh_observed_tables(name_prefix)
    if observed is None:
        LOG.info(
            "cycle %d: fresh-connection read failed; skipping assertion", cycle_idx
        )
        return False, new_id

    always(
        observed == expected,
        "catalog recovery: live catalog table set matches in-process expected model",
        {
            "cycle": cycle_idx,
            "name_prefix": name_prefix,
            "expected_count": len(expected),
            "observed_count": len(observed),
            # Cap the explicit diffs so the assertion details stay compact
            # even on a large divergence.
            "missing_from_catalog": sorted(expected - observed)[:5],
            "unexpected_in_catalog": sorted(observed - expected)[:5],
        },
    )
    return True, new_id


def main() -> int:
    # Per-timeline namespace so concurrent timelines and any future
    # parallel_driver_ instances do not collide on table names.
    name_prefix = f"catrec_{helper_random.random_u64():016x}"
    drop_probability = helper_random.random_float(*DROP_PROBABILITY_RANGE)
    LOG.info(
        "catalog recovery driver starting; name_prefix=%s drop_probability=%.3f",
        name_prefix,
        drop_probability,
    )

    expected: set[str] = set()
    next_id = 0
    cycles_ran = 0
    saw_coord_unavailable = False

    for cycle_idx in range(CYCLE_COUNT):
        ran, next_id = _run_cycle(
            expected, name_prefix, cycle_idx, next_id, drop_probability
        )
        if ran:
            cycles_ran += 1
        if _saw_coord_unavailable():
            saw_coord_unavailable = True
        time.sleep(INTER_CYCLE_SLEEP_S)

    sometimes(
        cycles_ran >= 2,
        "catalog recovery: 2+ assertion-bearing cycles ran in this timeline",
        {"cycles_ran": cycles_ran, "cycles_planned": CYCLE_COUNT},
    )
    sometimes(
        saw_coord_unavailable,
        "catalog recovery: observed environmentd connect failure during run",
        {"cycles_ran": cycles_ran, "saw_coord_unavailable": saw_coord_unavailable},
    )

    LOG.info(
        "catalog recovery driver done; cycles_ran=%d/%d expected_size=%d saw_coord_unavailable=%s",
        cycles_ran,
        CYCLE_COUNT,
        len(expected),
        saw_coord_unavailable,
    )
    return 0


if __name__ == "__main__":
    # Touch helper_pg env constants so static analysis treats them as
    # used; the helper module re-exports them for drivers (like this one)
    # that open their own connections.
    _ = (PGHOST, PGPORT, PGUSER, PGDATABASE, os, query_retry)
    sys.exit(main())
