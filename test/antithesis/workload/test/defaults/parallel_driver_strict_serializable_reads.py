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
must yield a non-decreasing count *and* a non-decreasing chosen/oracle
timestamp.

Approach:
  1. Reuse `helper_table_mv` (table `mv_input_table` + MV `mv_input_count`)
     so this driver does not introduce new schema. Each invocation owns a
     fresh prefix so concurrent driver instances scope to disjoint rows.
  2. For each step k = 1..N:
       - INSERT one row tagged with the prefix in autocommit mode (each
         insert is its own oracle-timestamped write).
       - Open a *fresh* psycopg connection, set `transaction_isolation`
         to `strict serializable` explicitly, then on the same connection
         run `EXPLAIN TIMESTAMP AS JSON FOR <select>` to capture the
         SUT-chosen `chosen_ts` / `oracle_read_ts` / `since` / `upper`,
         then run the SELECT itself. Record the observation.
       - Fresh connections are deliberate: a single long-lived connection
         could mask a read-regression bug behind connection-local caching.
       - EXPLAIN and SELECT are separate statements; each gets its own
         oracle bump under strict serializable, so the SELECT's timestamp
         will be >= the EXPLAIN's. We use EXPLAIN's chosen/oracle ts as
         the observation's sample point — it's the timestamp the SUT
         would have chosen for the SELECT at that moment.
  3. After all steps, take one more fresh-connection observation as the
     closing read, retrying until the count reaches `expected_final` or
     the budget is exhausted.
  4. Assertions:
       - **Count monotonicity** (data-level): `always(count[k+1] >=
         count[k], …)` between every adjacent pair, plus `always(final
         >= max(count), …)` for the closing observation.
       - **Timestamp monotonicity** (oracle-level): `always(oracle_read_ts
         and chosen_ts non-decreasing across adjacent observations …)`.
         This is the actual strict-serializable invariant: a violation
         here would not necessarily produce a count regression (e.g. a
         read at a stale ts that coincidentally still includes the new
         rows), so this catches a strictly larger class of bugs.
       - **Per-observation invariants**: `chosen_ts >= oracle_read_ts`
         (the SUT may advance chosen_ts past the oracle if collections
         aren't yet readable at the oracle's ts, but never below it) and
         `chosen_ts >= since` (reading below the read frontier would
         mean reading a compacted timestamp).
       - `sometimes(...)` liveness anchor confirming the closing
         observation reached the inserted count within the final-read
         budget (which is sized to span at least one quiet window from
         the global fault-orchestrator).

Read failures (connect timeout, server unavailable mid-fault) are skipped
rather than recorded — they are not regression evidence, and a false
positive on transient unavailability would obscure real bugs. EXPLAIN
and SELECT are paired on the same connection so a fault-window failure
drops both halves symmetrically.

This is a `parallel_driver_` — many concurrent instances run because the
property is about read monotonicity *within* each client's observation
stream, and prefix-scoping isolates each instance's expected count.
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import NamedTuple

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


class Observation(NamedTuple):
    """One fresh-connection sample: the count the SUT would return at
    this moment plus the timestamps it chose for that read.

    `since` / `upper` are the singleton frontier elements at EXPLAIN
    time, or None if the antichain was empty (which would itself be
    unusual — we record it rather than asserting it directly here).
    """

    step: int
    count: int
    chosen_ts: int
    oracle_read_ts: int
    since: int | None
    upper: int | None


def _parse_explain_timestamp_json(
    payload: str,
) -> tuple[int, int, int | None, int | None] | None:
    """Pull (chosen_ts, oracle_read_ts, since, upper) out of an EXPLAIN
    TIMESTAMP AS JSON payload. Returns None if the payload shape is
    unexpected (e.g. `NoTimestamp` variant for a timestamp-independent
    query — not expected for an MV read, but defensive).

    The shape we rely on comes from `TimestampExplanation`'s default
    serde Serialize impl in `src/adapter/src/coord/timestamp_selection.rs`:

        {
          "determination": {
            "timestamp_context": {
              "TimelineTimestamp": {"chosen_ts": N, "oracle_ts": N, ...}
            },
            "oracle_read_ts": N,
            "since": {"elements": [N]},
            "upper": {"elements": [N]},
            ...
          },
          ...
        }
    """
    try:
        j = json.loads(payload)
        det = j["determination"]
        ctx = det["timestamp_context"]
        if not isinstance(ctx, dict) or "TimelineTimestamp" not in ctx:
            return None
        chosen_ts = int(ctx["TimelineTimestamp"]["chosen_ts"])
        oracle_read_ts = det.get("oracle_read_ts")
        if oracle_read_ts is None:
            return None
        oracle_read_ts = int(oracle_read_ts)
        since_elems = det.get("since", {}).get("elements") or []
        upper_elems = det.get("upper", {}).get("elements") or []
        since = int(since_elems[0]) if since_elems else None
        upper = int(upper_elems[0]) if upper_elems else None
        return (chosen_ts, oracle_read_ts, since, upper)
    except (KeyError, ValueError, TypeError):
        return None


def _fresh_observation(step: int, prefix: str) -> Observation | None:
    """Open a *new* strict-serializable connection, capture the SUT's
    timestamp choices via `EXPLAIN TIMESTAMP AS JSON`, then capture the
    MV count via SELECT on the same connection. Returns None on any
    connect/query/parse failure so the caller can skip without
    conflating fault-induced unavailability with a real regression.

    Setting `transaction_isolation` explicitly costs one extra round
    trip but defends against future changes to the system default.
    """
    select_sql = f"SELECT row_count::bigint FROM {MV_NAME} WHERE prefix = %s"
    explain_sql = f"EXPLAIN TIMESTAMP AS JSON FOR {select_sql}"
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
            cur.execute(explain_sql, (prefix,))
            explain_row = cur.fetchone()
            cur.execute(select_sql, (prefix,))
            select_row = cur.fetchone()
    except Exception:  # noqa: BLE001
        return None
    if explain_row is None:
        return None
    parsed = _parse_explain_timestamp_json(explain_row[0])
    if parsed is None:
        return None
    chosen_ts, oracle_read_ts, since, upper = parsed
    # MV with no row for this prefix yet → count 0; that's a valid
    # observation, not a failure.
    count = 0 if select_row is None else int(select_row[0])
    return Observation(
        step=step,
        count=count,
        chosen_ts=chosen_ts,
        oracle_read_ts=oracle_read_ts,
        since=since,
        upper=upper,
    )


def main() -> int:
    ensure_table_and_mv()

    prefix = f"p{helper_random.random_u64():016x}"
    LOG.info("strict-serializable driver starting; prefix=%s", prefix)

    # Sequence of Observation records. Failed observations are dropped
    # (not appended) so the adjacent-pair assertions below don't compare
    # against a spurious zero or a torn timestamp.
    observations: list[Observation] = []

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
            # the monotonicity assertions below.
            LOG.info("step %d: insert failed (%s); ending step loop", step, exc)
            break

        obs = _fresh_observation(step, prefix)
        if obs is None:
            # Fault-window read; skip. We do NOT record it so the
            # adjacent-pair assertions below don't see a torn observation.
            continue
        observations.append(obs)

    # Settle and take the closing observation. The driver is short and the
    # observations list is small, so a generous timeout here is fine — long
    # enough to span at least one global-orchestrator quiet window.
    expected_final = observations[-1].step if observations else 0
    # `expected_final` is the largest step that was actually INSERTed (we
    # may have bailed early). It's an *upper bound* on the count — the
    # final count may equal it (fully caught up) or be slightly less
    # (catchup still in flight). The monotonicity assertions only care
    # that final >= every earlier observation.

    deadline = time.monotonic() + FINAL_READ_TIMEOUT_S
    final: Observation | None = _fresh_observation(expected_final, prefix)
    while (
        final is None or final.count < expected_final
    ) and time.monotonic() < deadline:
        time.sleep(FINAL_READ_POLL_S)
        final = _fresh_observation(expected_final, prefix)

    sometimes(
        final is not None and final.count == expected_final,
        "strict-serializable reads: final fresh-connection read reached inserted count",
        {
            "prefix": prefix,
            "expected_final": expected_final,
            "final_observed": final.count if final is not None else None,
            "observations": len(observations),
        },
    )

    # Append the closing observation so the adjacent-pair and per-
    # observation assertions below cover it too.
    if final is not None:
        observations.append(final)

    # ----- count monotonicity: adjacent-pair assertion (data-level) -----
    # Across the recorded fresh-connection reads, no count may regress.
    # This is the data-level shadow of the strict-serializable read-
    # ordering property — kept as independent evidence alongside the
    # timestamp-level assertions below.
    for i in range(1, len(observations)):
        prev = observations[i - 1]
        curr = observations[i]
        always(
            curr.count >= prev.count,
            "strict-serializable reads: fresh-connection read regressed across adjacent observations",
            {
                "prefix": prefix,
                "prev_step": prev.step,
                "prev_count": prev.count,
                "curr_step": curr.step,
                "curr_count": curr.count,
            },
        )

    # ----- count monotonicity: closing observation dominates the maximum -----
    # If the closing observation succeeded, it must be >= every earlier
    # observation. (The final equality with `expected_final` is covered by
    # the `sometimes` liveness anchor above and is not asserted here.)
    if final is not None and len(observations) >= 2:
        max_observed = max(o.count for o in observations[:-1])
        always(
            final.count >= max_observed,
            "strict-serializable reads: closing fresh-connection read regressed below earlier maximum",
            {
                "prefix": prefix,
                "final": final.count,
                "max_earlier": max_observed,
            },
        )

    # ----- timestamp monotonicity: adjacent-pair assertion (oracle-level) -----
    # The actual strict-serializable property: oracle and chosen
    # timestamps for fresh-connection reads must be non-decreasing across
    # observations. Catches oracle regression that wouldn't surface as a
    # count regression (e.g. a read at a stale ts that coincidentally
    # still includes the new rows).
    for i in range(1, len(observations)):
        prev = observations[i - 1]
        curr = observations[i]
        always(
            curr.oracle_read_ts >= prev.oracle_read_ts,
            "strict-serializable reads: oracle_read_ts regressed across adjacent observations",
            {
                "prefix": prefix,
                "prev_step": prev.step,
                "prev_oracle_read_ts": prev.oracle_read_ts,
                "curr_step": curr.step,
                "curr_oracle_read_ts": curr.oracle_read_ts,
            },
        )
        always(
            curr.chosen_ts >= prev.chosen_ts,
            "strict-serializable reads: chosen_ts regressed across adjacent observations",
            {
                "prefix": prefix,
                "prev_step": prev.step,
                "prev_chosen_ts": prev.chosen_ts,
                "curr_step": curr.step,
                "curr_chosen_ts": curr.chosen_ts,
            },
        )

    # ----- per-observation timestamp invariants -----
    for obs in observations:
        # Per `TimestampContext` in src/adapter/src/coord/timestamp_selection.rs:
        # chosen_ts may be advanced past oracle_read_ts when collections
        # aren't yet readable at the oracle's ts (the read then blocks
        # for catchup), but must never be below it.
        always(
            obs.chosen_ts >= obs.oracle_read_ts,
            "strict-serializable reads: chosen_ts below oracle_read_ts",
            {
                "prefix": prefix,
                "step": obs.step,
                "chosen_ts": obs.chosen_ts,
                "oracle_read_ts": obs.oracle_read_ts,
            },
        )
        # `since` is the read frontier — chosen_ts below it would mean
        # reading a compacted/no-longer-retained timestamp. We do not
        # assert `chosen_ts < upper`: under strict serializable the
        # coordinator routinely picks a ts ahead of upper and blocks for
        # catchup (`respond_immediately=false`).
        if obs.since is not None:
            always(
                obs.chosen_ts >= obs.since,
                "strict-serializable reads: chosen_ts below since (read frontier)",
                {
                    "prefix": prefix,
                    "step": obs.step,
                    "chosen_ts": obs.chosen_ts,
                    "since": obs.since,
                },
            )

    LOG.info(
        "strict-serializable driver done; observations=%d final=%s expected_final=%s",
        len(observations),
        final.count if final is not None else None,
        expected_final,
    )
    return 0


if __name__ == "__main__":
    # Touch the imported env constants so static analysis treats them as
    # used; helper_pg re-exports them for drivers that bypass its retry
    # helpers (as this one does for fresh connections).
    _ = (PGHOST, PGPORT, PGUSER, PGDATABASE, os)
    sys.exit(main())
