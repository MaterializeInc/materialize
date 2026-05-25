#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `strict-serializable-reads`.

Materialize's headline consistency guarantee: two reads on the same
collection at oracle-assigned timestamps t1 < t2 must observe consistent
ordering — anything visible at t1 must remain visible at t2. This driver
exercises the cross-read half of that property: a sequence of fresh-
connection reads against a materialized view, interleaved with writes,
must yield a non-decreasing count.

Approach:
  1. Reuse `helper_table_mv` (table `mv_input_table` + MV `mv_input_count`)
     so this driver does not introduce new schema. Each invocation owns a
     fresh prefix so concurrent driver instances scope to disjoint rows.
  2. For each step k = 1..N:
       - INSERT one row tagged with the prefix in autocommit mode (each
         insert is its own oracle-timestamped write).
       - Open a *fresh* psycopg connection, set `transaction_isolation`
         to `strict serializable` explicitly, and SELECT the MV's row
         count for the prefix. Record (k, observed_count).
       - Fresh connections are deliberate: a single long-lived connection
         could mask a read-regression bug behind connection-local caching.
  3. After all steps, run one more fresh-connection SELECT as the final
     observation.
  4. Assertions:
       - `always(count[k+1] >= count[k], …)` between every adjacent pair
         of recorded reads — the core strict-serializable read ordering
         invariant.
       - `always(final >= max(count), …)` for the closing observation.
       - `sometimes(...)` liveness anchor confirming the closing
         observation reached the inserted count within the final-read
         budget (which is sized to span at least one quiet window from
         the global fault-orchestrator).

Read failures (connect timeout, server unavailable mid-fault) are skipped
rather than recorded — they are not regression evidence, and a False
positive on transient unavailability would obscure real bugs.

This is a `parallel_driver_` — many concurrent instances run because the
property is about read monotonicity *within* each client's observation
stream, and prefix-scoping isolates each instance's expected count.
"""

from __future__ import annotations

import os
import sys
import time

import helper_logging
import helper_random
import psycopg
from antithesis.assertions import always, sometimes
from helper_pg import (
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGUSER,
    execute_retry,
)
from helper_table_mv import MV_NAME, TABLE_MV_INPUT, ensure_table_and_mv

LOG = helper_logging.setup_logging("driver.strict_serializable_reads")

STEPS_PER_INVOCATION = 12
# Sized to span at least one MAX_OFF window from the global fault-
# orchestrator (default 40s) plus the time the final read needs after
# the MV catches up.
FINAL_READ_TIMEOUT_S = 90.0
FINAL_READ_POLL_S = 0.5
PROBE_CONNECT_TIMEOUT_S = 5


def _fresh_select_count(prefix: str) -> int | None:
    """Open a *new* connection, force strict serializable, and SELECT the
    MV's row_count for `prefix`. Returns None on any connect/query failure
    so the caller can skip the observation without conflating fault-induced
    unavailability with a read regression.

    Setting `transaction_isolation` explicitly costs one extra round trip
    but defends against future changes to the system default.
    """
    try:
        with (
            psycopg.connect(
                host=PGHOST,
                port=PGPORT,
                user=PGUSER,
                dbname=PGDATABASE,
                connect_timeout=PROBE_CONNECT_TIMEOUT_S,
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("SET transaction_isolation TO 'strict serializable'")
            cur.execute(
                f"SELECT row_count::bigint FROM {MV_NAME} WHERE prefix = %s",
                (prefix,),
            )
            row = cur.fetchone()
    except Exception:  # noqa: BLE001
        return None
    if row is None:
        return 0  # MV has no row for this prefix yet
    return int(row[0])


def main() -> int:
    ensure_table_and_mv()

    prefix = f"p{helper_random.random_u64():016x}"
    LOG.info("strict-serializable driver starting; prefix=%s", prefix)

    # Sequence of (step_index, observed_count). Reads that failed are
    # represented as None and dropped before assertions.
    observations: list[tuple[int, int]] = []

    for step in range(1, STEPS_PER_INVOCATION + 1):
        # Each INSERT is one autocommit write; the coordinator stamps it
        # with an oracle timestamp. We INSERT before the read so the
        # *expected* monotone behaviour is that every read is >= the
        # previous one and the final read equals the total insert count
        # (modulo catchup; covered by the liveness anchor below).
        #
        # `WHERE NOT EXISTS` gates the INSERT idempotently against
        # `execute_retry`'s retry semantics: Materialize doesn't
        # enforce uniqueness as a runtime constraint, so a fault-
        # window network drop after server-side commit would cause
        # the retry to re-insert (step, prefix), pushing the MV's
        # COUNT(*) GROUP BY prefix above the recorded
        # `observations[-1][0]` and tripping
        # `final == expected_final` non-monotonicity.  See
        # `helper_table_mv` for the matching MV-side
        # `COUNT(DISTINCT id)` belt-and-braces.
        try:
            execute_retry(
                f"INSERT INTO {TABLE_MV_INPUT} (id, prefix) "
                f"SELECT %s, %s WHERE NOT EXISTS "
                f"(SELECT 1 FROM {TABLE_MV_INPUT} WHERE id = %s AND prefix = %s)",
                (step, prefix, step, prefix),
            )
        except Exception as exc:  # noqa: BLE001
            # Persistent insert failure under sustained fault — bail.
            # Already-recorded observations are still valid evidence for
            # the monotonicity assertion below.
            LOG.info("step %d: insert failed (%s); ending step loop", step, exc)
            break

        observed = _fresh_select_count(prefix)
        if observed is None:
            # Fault-window read; skip. We do NOT record it so the
            # adjacent-pair assertion below doesn't see a spurious zero.
            continue
        observations.append((step, observed))

    # Settle and take the closing observation. The driver is short and the
    # observations list is small, so a generous timeout here is fine — long
    # enough to span at least one global-orchestrator quiet window.
    expected_final = len(observations) and observations[-1][0]
    # `expected_final` is the largest step that was actually INSERTed (we
    # may have bailed early). It's an *upper bound* on the count — the
    # final count may equal it (fully caught up) or be slightly less
    # (catchup still in flight). The monotonicity assertion only cares
    # that final >= every earlier observation.

    deadline = time.monotonic() + FINAL_READ_TIMEOUT_S
    final: int | None = _fresh_select_count(prefix)
    while final is None and time.monotonic() < deadline:
        time.sleep(FINAL_READ_POLL_S)
        final = _fresh_select_count(prefix)

    sometimes(
        final is not None and final == expected_final,
        "strict-serializable reads: final fresh-connection read reached inserted count",
        {
            "prefix": prefix,
            "expected_final": expected_final,
            "final_observed": final,
            "observations": len(observations),
        },
    )

    # ----- monotonicity: adjacent-pair assertion -----
    # Across the recorded fresh-connection reads, no read may regress.
    # This is the strict-serializable read-ordering property.
    for i in range(1, len(observations)):
        prev_step, prev_count = observations[i - 1]
        curr_step, curr_count = observations[i]
        always(
            curr_count >= prev_count,
            "strict-serializable reads: fresh-connection read regressed across adjacent observations",
            {
                "prefix": prefix,
                "prev_step": prev_step,
                "prev_count": prev_count,
                "curr_step": curr_step,
                "curr_count": curr_count,
            },
        )

    # ----- monotonicity: closing observation dominates the maximum -----
    # If the closing observation succeeded, it must be >= every earlier
    # observation. (The final equality with `expected_final` is covered by
    # the `sometimes` liveness anchor above and is not asserted here.)
    if final is not None and observations:
        max_observed = max(c for _, c in observations)
        always(
            final >= max_observed,
            "strict-serializable reads: closing fresh-connection read regressed below earlier maximum",
            {
                "prefix": prefix,
                "final": final,
                "max_earlier": max_observed,
            },
        )

    LOG.info(
        "strict-serializable driver done; observations=%d final=%s expected_final=%s",
        len(observations),
        final,
        expected_final,
    )
    return 0


if __name__ == "__main__":
    # Touch the imported env constants so static analysis treats them as
    # used; helper_pg re-exports them for drivers that bypass its retry
    # helpers (as this one does for fresh connections).
    _ = (PGHOST, PGPORT, PGUSER, PGDATABASE, os)
    sys.exit(main())
