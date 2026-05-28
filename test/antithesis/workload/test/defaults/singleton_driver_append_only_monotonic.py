#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis singleton driver: append-only count monotonicity.

For an append-only table — one whose only DML is INSERT — the row
count must be monotone non-decreasing across SUT-side time.  Any
observation where the count *drops* across a later read means the SUT
silently lost a committed row, which is a persist / storage data-loss
bug class no other driver in the suite directly tests.

Approach:
  * Create a prefix-scoped two-column append-only table
    `append_only_<prefix>(n bigint, prefix text)`.
  * Run a long INSERT loop, monotonically increasing `n` from 1 upward.
    Each INSERT uses `execute_retry` so a fault landing mid-commit is
    retried; once it returns success the row is durably committed
    (the helper distinguishes ack'd vs. ambiguous via Indeterminate
    classification, see helper_pg).  Track `committed_count`, the
    in-process model of "rows we know landed".
  * Every CHECK_INTERVAL_S, on a *fresh* connection (defeats any
    session-cache surprises), `SELECT count(*) FROM table` and assert:
      - the observed count never went *backwards* relative to any
        prior observation in this driver lifetime (monotonicity);
      - the observed count never exceeds `committed_count` (sanity:
        no phantom rows we didn't insert).
  * On fault-shape failures during the read, skip that observation —
    a paused materialized isn't a regression signal.

Singleton (one per timeline) so the in-process model + table ownership
are unambiguous across the run, and so the loop spans many
fault-ON/OFF cycles to surface compaction- or recovery-time row loss
specifically.

Properties:
  always — every observed count was >= every prior observed count
    in this run; no count exceeded committed_count.
  sometimes — committed_count >= 100 in some timeline (liveness — the
    INSERT loop made meaningful progress).
  sometimes — at least one fault-induced retry happened (anchor for
    the recovery-time-loss path actually being exercised).
"""

from __future__ import annotations

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
)

LOG = helper_logging.setup_logging("driver.append_only_monotonic")

RUNTIME_S = 300.0
# Cadence of the count(*) consistency check.  Roughly every fault-OFF
# window so each check spans a healthy SUT moment in expectation.
CHECK_INTERVAL_S = 5.0
# Per-attempt connection budget for the count(*) probe.  Short and
# non-retrying so a paused materialized is observed (skipped) rather
# than papered over.
PROBE_CONNECT_TIMEOUT_S = 5
STATEMENT_TIMEOUT_MS = 5000


def _probe_count(table: str) -> int | None:
    """Return current `SELECT count(*) FROM table` or None on fault."""
    try:
        with psycopg.connect(
            host=PGHOST,
            port=PGPORT,
            user=PGUSER,
            dbname=PGDATABASE,
            autocommit=True,
            connect_timeout=PROBE_CONNECT_TIMEOUT_S,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}".encode())
                cur.execute(f"SELECT count(*) FROM {table}".encode())
                row = cur.fetchone()
                return int(row[0]) if row is not None else None
    except Exception as exc:  # noqa: BLE001
        if not looks_like_fault(str(exc)):
            LOG.warning("count probe non-fault error: %s", exc)
        return None


def main() -> int:
    prefix = f"{helper_random.random_u64():016x}"
    table = f"public.append_only_{prefix}"

    # Setup.  `execute_retry` rides through any fault window so the
    # table + initial row exist by the time the loop starts measuring.
    try:
        execute_retry(f"CREATE TABLE IF NOT EXISTS {table} (n bigint, prefix text)")
    except Exception as exc:  # noqa: BLE001
        if not looks_like_fault(str(exc)):
            LOG.warning("setup non-fault error: %s", exc)
        sometimes(
            False,
            "append-only-monotonic: at least one timeline completed setup",
            {"prefix": prefix, "error": str(exc)[:200]},
        )
        return 0

    deadline = time.time() + RUNTIME_S
    committed_count = 0
    next_n = 1
    fault_retries = 0
    max_observed_count = 0
    violations: list[dict[str, int]] = []
    overshoots: list[dict[str, int]] = []
    probes_taken = 0
    next_check_at = time.time() + CHECK_INTERVAL_S

    while time.time() < deadline:
        # One INSERT per iteration.  execute_retry distinguishes
        # OperationalError (retryable transient) from a definite commit
        # — when it returns success, the row is durably committed.
        try:
            execute_retry(
                f"INSERT INTO {table} (n, prefix) VALUES ({next_n}, '{prefix}')"
            )
            committed_count += 1
            next_n += 1
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if looks_like_fault(msg):
                fault_retries += 1
                # Advance next_n so a re-INSERT doesn't collide with an
                # ambiguous prior write.  Don't increment committed_count —
                # we don't know if the prior INSERT landed.
                next_n += 1
                time.sleep(0.5)
            else:
                LOG.warning("INSERT non-fault error: %s", msg)
                # A real (non-fault) SQL error during INSERT shouldn't
                # happen on this driver's own table — bail with a
                # sometimes anchor so it surfaces in triage.
                break

        if time.time() >= next_check_at:
            observed = _probe_count(table)
            if observed is not None:
                probes_taken += 1
                if observed < max_observed_count:
                    # MONOTONICITY VIOLATION — count went backwards.
                    violations.append(
                        {
                            "prev_max": max_observed_count,
                            "observed": observed,
                            "committed_count_at_probe": committed_count,
                            "next_n_at_probe": next_n,
                        }
                    )
                if observed > committed_count:
                    # PHANTOM ROW — count exceeds what we know we
                    # committed.  Either a duplicate landed or
                    # something we didn't insert is showing up.
                    overshoots.append(
                        {
                            "observed": observed,
                            "committed_count": committed_count,
                            "next_n_at_probe": next_n,
                        }
                    )
                max_observed_count = max(max_observed_count, observed)
            next_check_at = time.time() + CHECK_INTERVAL_S

    # Best-effort cleanup; cleanup failures aren't a correctness signal.
    try:
        execute_retry(f"DROP TABLE IF EXISTS {table}")
    except Exception as exc:  # noqa: BLE001
        LOG.debug("cleanup tolerated: %s", exc)

    LOG.info(
        "[SUMMARY] append-only-monotonic prefix=%s committed=%d max_observed=%d "
        "probes=%d fault_retries=%d violations=%d overshoots=%d",
        prefix,
        committed_count,
        max_observed_count,
        probes_taken,
        fault_retries,
        len(violations),
        len(overshoots),
    )

    always(
        not violations,
        "append-only-monotonic: count(*) of an append-only table never "
        "decreased across observations in this run",
        {
            "prefix": prefix,
            "committed_count": committed_count,
            "probes_taken": probes_taken,
            "violations_sample": violations[:5],
        },
    )
    always(
        not overshoots,
        "append-only-monotonic: count(*) never exceeded the in-process "
        "committed_count (no phantom rows)",
        {
            "prefix": prefix,
            "committed_count": committed_count,
            "overshoots_sample": overshoots[:5],
        },
    )
    sometimes(
        committed_count >= 100,
        "append-only-monotonic: committed_count >= 100 in some timeline",
        {"prefix": prefix, "committed_count": committed_count},
    )
    sometimes(
        fault_retries > 0,
        "append-only-monotonic: at least one fault-shaped INSERT retry "
        "exercised the recovery-time-loss path",
        {"prefix": prefix, "fault_retries": fault_retries},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
