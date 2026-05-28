#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: differential query oracle.

The existing Antithesis suite is heavy on crash/panic detection and
frontier/recovery invariants but thin on *wrong-results* oracles — the
bug class where the SUT returns the wrong answer without crashing.
That's the crown-jewel correctness concern for Materialize: an
incrementally-maintained materialized view must always equal the
batch-recomputed answer.

This driver builds a small random table, then derives three logically
equivalent computations of the same aggregate:

  * BASE  — the aggregate computed directly off the table on the fly.
  * VIEW  — a non-materialized view with the identical definition
            (re-planned + recomputed at read time).
  * MV    — a materialized view with the identical definition
            (incrementally maintained, read from its persisted output).

It then reads all three **inside a single read transaction**, so they
are evaluated at one consistent timestamp, and asserts they are
byte-for-byte identical.  A disagreement is a real correctness bug:

  * MV != BASE  → incremental maintenance diverged from batch (the
    highest-value Materialize bug class — silent wrong results in a MV).
  * VIEW != BASE → the optimizer produced different results for the
    same logical query depending on plan shape.

Under Antithesis fault injection, the same comparison runs across
kill/pause/restart windows, so it also catches a MV that returns stale
or torn results after recovery.

Property shape:
  always(all formulations agree)         — safety, the correctness oracle
  sometimes(>= 2 formulations compared)  — liveness, the oracle ran

Fault-shaped failures (connect drop, paused cluster, AS-OF-since races)
are demoted to `sometimes(False)` via `looks_like_fault`, so a fault
window never trips the safety assertion.
"""

from __future__ import annotations

import sys

import helper_logging
import helper_random
import psycopg
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault
from helper_pg import (
    CONNECT_TIMEOUT_S,
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGUSER,
)

LOG = helper_logging.setup_logging("driver.differential_query")

# Keep the table small: the oracle is about *correctness of the
# computation*, not throughput.  A few hundred rows across a handful
# of group keys exercises consolidation/aggregation while keeping the
# whole compare cheap enough to run many times per timeline.
NUM_ROWS = 200
NUM_KEYS = 16
STATEMENT_TIMEOUT_MS = 10000


def _rand_rows(rng: helper_random.AntithesisRandom) -> list[tuple[int, int]]:
    return [
        (rng.randrange(NUM_KEYS), rng.randrange(-1000, 1000)) for _ in range(NUM_ROWS)
    ]


def main() -> int:
    prefix = f"{helper_random.random_u64():016x}"
    rng = helper_random.AntithesisRandom()
    table = f"diffq_t_{prefix}"
    view = f"diffq_v_{prefix}"
    mv = f"diffq_mv_{prefix}"

    # The aggregate under test.  count(*) + sum + min + max + count
    # distinct exercises several consolidation paths at once.
    agg = (
        "SELECT k, count(*) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx, "
        "count(DISTINCT v) AS d FROM {rel} GROUP BY k"
    )

    rows = _rand_rows(rng)
    values_sql = ",".join(f"({k},{v})" for k, v in rows)

    try:
        with psycopg.connect(
            host=PGHOST,
            port=PGPORT,
            user=PGUSER,
            dbname=PGDATABASE,
            autocommit=True,
            connect_timeout=CONNECT_TIMEOUT_S,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}".encode())
                cur.execute(f"CREATE TABLE {table} (k int, v int)".encode())
                cur.execute(f"INSERT INTO {table} VALUES {values_sql}".encode())
                cur.execute(f"CREATE VIEW {view} AS {agg.format(rel=table)}".encode())
                cur.execute(
                    f"CREATE MATERIALIZED VIEW {mv} AS {agg.format(rel=table)}".encode()
                )

            # Single read transaction → one consistent timestamp for all
            # three formulations.  A read txn in Materialize picks one
            # timestamp and serves every SELECT in it from that snapshot,
            # so any disagreement is a genuine divergence rather than a
            # read-skew artifact.
            order = " ORDER BY k, c, s, mn, mx, d"
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}".encode())
                cur.execute(b"BEGIN")
                cur.execute((agg.format(rel=table) + order).encode())
                base_rows = cur.fetchall()
                cur.execute((f"SELECT k, c, s, mn, mx, d FROM {view}" + order).encode())
                view_rows = cur.fetchall()
                cur.execute((f"SELECT k, c, s, mn, mx, d FROM {mv}" + order).encode())
                mv_rows = cur.fetchall()
                cur.execute(b"COMMIT")

            # Best-effort cleanup; failures here are not correctness signals.
            with conn.cursor() as cur:
                try:
                    cur.execute(f"DROP TABLE {table} CASCADE".encode())
                except Exception as exc:  # noqa: BLE001
                    LOG.debug("cleanup tolerated: %s", exc)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if looks_like_fault(msg):
            # Fault window during setup/read — not a correctness signal.
            sometimes(
                False,
                "differential-query: comparison completed without a fault",
                {"prefix": prefix, "fault": msg[:200]},
            )
            return 0
        # A non-fault error here (e.g. a planner panic surfaced as an
        # error, an unexpected SQL failure) is itself worth surfacing.
        LOG.warning("differential-query non-fault error: %s", msg)
        always(
            False,
            "differential-query: setup/read raised no unexpected (non-fault) error",
            {"prefix": prefix, "error": msg[:300]},
        )
        return 0

    base_view_agree = base_rows == view_rows
    base_mv_agree = base_rows == mv_rows

    always(
        base_view_agree and base_mv_agree,
        "differential-query: base, view, and materialized view agree at one timestamp",
        {
            "prefix": prefix,
            "base_view_agree": base_view_agree,
            "base_mv_agree": base_mv_agree,
            "base_count": len(base_rows),
            "view_count": len(view_rows),
            "mv_count": len(mv_rows),
            # Only the first divergent rows, bounded for the payload.
            "base_sample": [list(r) for r in base_rows[:5]],
            "mv_sample": [list(r) for r in mv_rows[:5]],
        },
    )
    sometimes(
        True,
        "differential-query: oracle ran (>= 2 formulations compared)",
        {"prefix": prefix, "rows": len(base_rows)},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
