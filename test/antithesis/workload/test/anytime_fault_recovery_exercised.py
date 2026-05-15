#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `fault-recovery-exercised`.

The most fundamental liveness property in the catalog: after the system
takes a hit from Antithesis fault injection, it must eventually come back
and serve SQL again. The catalog frames this in terms of the `/health/ready`
endpoint returning 200; this workload uses `SELECT 1` (the cheapest
end-to-end pgwire round trip) as the proxy, and observes the cluster
replica status as a corroborating signal.

Approach:
  - Probe `materialized` with a *short-budget* psycopg connect on every
    tick. Long retry budgets in `helper_pg` would mask the actual
    fault-active periods we want to detect — here we want to observe the
    transitions.
  - Track per-tick state: was this probe a success or a connect/query
    failure?
  - If we observe a failure at tick T and a success at tick T+k (any k>=1)
    within this invocation, that is the recovery transition we care about,
    and we fire `sometimes("...query succeeded after observed fault")`.

  - Separately, fire `sometimes("...observed cluster replica non-online")`
    when `mz_cluster_replica_statuses` reports any antithesis replica
    `offline`. This is a corroborating signal so triage can distinguish
    "no fault ever landed" from "faults landed but no recovery observed."

This is an `anytime_` driver — Antithesis launches it many times, each
short-lived. Recovery transitions accumulate across invocations.
"""

from __future__ import annotations

import os
import sys
import time

import helper_logging
import psycopg
from helper_pg import (
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGUSER,
    query_one_retry,
)

from antithesis.assertions import sometimes

LOG = helper_logging.setup_logging("driver.fault_recovery_exercised")

POLL_INTERVAL_S = 0.5
RUN_BUDGET_S = 30.0
PROBE_CONNECT_TIMEOUT_S = 2.0

ANTITHESIS_CLUSTER = "antithesis_cluster"


def _probe_select_one() -> bool:
    """Run `SELECT 1` with a short connect timeout. Return True on success.

    Distinct from the resilient `helper_pg.query_*` paths because we *want*
    to observe transient failures here — they are the fault-active half of
    the recovery transition we are looking for.
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
            cur.execute("SELECT 1")
            row = cur.fetchone()
            return row is not None and row[0] == 1
    except Exception:  # noqa: BLE001
        return False


def _replica_non_online() -> bool:
    """Did any antithesis_cluster replica record an `offline` status at any
    point in this timeline?

    Queries `mz_cluster_replica_status_history` (audit log) rather than
    `mz_cluster_replica_statuses` (current-state view). The current-state
    view shows only the latest tick per (replica, process), so a transient
    offline window — exactly the shape Antithesis fault injection creates
    when it pauses or kills clusterd1 / clusterd2 for a few seconds — can
    open and close between two consecutive polls and the assertion never
    fires. The history table is sticky: once an offline event is recorded
    it stays observable from any later poll within the retention window.

    Uses the retry-budgeted query helper because we want a clear yes/no, not
    a probe outcome — if the helper can't get an answer we conservatively
    return False so the corroborating signal stays silent rather than
    accidentally firing on a probe-side failure.
    """
    try:
        row = query_one_retry(
            """
            SELECT EXISTS (
                SELECT 1
                FROM mz_internal.mz_cluster_replica_status_history h
                JOIN mz_cluster_replicas r ON r.id = h.replica_id
                JOIN mz_clusters c ON c.id = r.cluster_id
                WHERE c.name = %s AND h.status = 'offline'
            )
            """,
            (ANTITHESIS_CLUSTER,),
        )
    except Exception:  # noqa: BLE001
        return False
    return bool(row and row[0])


def main() -> int:
    deadline = time.monotonic() + RUN_BUDGET_S

    # Per-invocation state. The driver is short-lived; Antithesis covers the
    # full timeline by launching many invocations.
    saw_failure = False
    saw_recovery_after_failure = False
    saw_replica_non_online = False
    successes = 0
    failures = 0

    while time.monotonic() < deadline:
        ok = _probe_select_one()
        if ok:
            successes += 1
            if saw_failure:
                saw_recovery_after_failure = True
        else:
            failures += 1
            saw_failure = True

        if _replica_non_online():
            saw_replica_non_online = True

        time.sleep(POLL_INTERVAL_S)

    sometimes(
        saw_recovery_after_failure,
        "fault recovery: SELECT 1 succeeded after a previously-observed connect failure",
        {
            "successes": successes,
            "failures": failures,
            "saw_replica_non_online": saw_replica_non_online,
        },
    )
    sometimes(
        saw_replica_non_online,
        "fault recovery: observed antithesis_cluster replica non-online at least once",
        {"successes": successes, "failures": failures},
    )
    # Bare-minimum healthy-coverage signal: at least one successful probe in
    # the invocation. If this ever goes 0/N across a run, no driver was
    # ever able to talk to Materialize and the entire test is suspect —
    # downstream property assertions would be vacuous.
    sometimes(
        successes > 0,
        "fault recovery: at least one SELECT 1 succeeded this invocation",
        {"successes": successes, "failures": failures},
    )

    LOG.info(
        "fault-recovery probe done; successes=%d failures=%d recovery=%s replica_offline=%s",
        successes,
        failures,
        saw_recovery_after_failure,
        saw_replica_non_online,
    )
    return 0


if __name__ == "__main__":
    # Reference PGUSER/PGPORT/PGHOST/PGDATABASE so static analysis sees them
    # used through helper_pg's re-export rather than as dead imports.
    _ = (PGHOST, PGPORT, PGUSER, PGDATABASE, os)
    sys.exit(main())
