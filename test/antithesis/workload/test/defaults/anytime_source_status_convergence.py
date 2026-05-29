#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis continuous probe: source / sink status convergence.

Ports the test/source-sink-errors property class to Antithesis (see
test/antithesis/README.md → "Next test frameworks to port" §4).
Antithesis already injects the disruptions for free (broker kill, PG
pause, container partition); the missing piece is the *assertion* that
the SUT's status tables converge to ground truth as the disruption
clears.

Property under test: after Antithesis stalls an upstream (kafka /
postgres / mysql / sql-server) and then releases it, the affected
source's row in `mz_internal.mz_source_statuses` returns to `running`
within a bounded window.  A source that stays in `stalled` /
`failed` / `ceased` long after its upstream recovered is a real
correctness concern — downstream consumers see no progress despite the
underlying data being available.

Probe loop:
  * Every POLL_INTERVAL_S, snapshot `mz_source_statuses` +
    `mz_sink_statuses` filtered to user objects (`id LIKE 'u%'`).
  * Track per-(id, kind):
      - `last_healthy_at` — most recent observation with status in
        {running, starting}; `starting` counts because it's a
        transient pre-healthy state, not an error.
      - `last_unhealthy_at` — most recent observation with any other
        status; record the status + error message.
  * Flip `ever_unhealthy` / `ever_recovered` to support the liveness
    anchors below.

At launch end, an object is "stuck" if its latest observation was
unhealthy AND we never observed it healthy after that AND that
observation is more than MIN_RECOVERY_S in the past — i.e., the SUT
had a generous window to recover and didn't.

Properties:

  always — no user source/sink ended this probe launch stuck in an
    unhealthy status for more than MIN_RECOVERY_S without any later
    healthy observation.

  sometimes — at least one user object was observed in a non-healthy
    state.  Liveness anchor for the disruption-was-actually-injected
    path; without it the always check could be vacuously satisfied by
    a run that never faulted any upstream.

  sometimes — at least one user object recovered from unhealthy back
    to healthy during this probe launch.  Liveness anchor for the
    recovery path itself.

In groups without user sources/sinks (workload-replay, upsert-stress
in some configs), the probe finds nothing to scan and the always check
holds trivially; the sometimes anchors simply don't fire for that
timeline.  No setup required.
"""

from __future__ import annotations

import os
import sys
import time

import helper_logging
import psycopg
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault

LOG = helper_logging.setup_logging("anytime.source_status_convergence")

PGHOST = os.environ.get("PGHOST", "materialized")
PGPORT = int(os.environ.get("PGPORT", "6875"))
PGUSER = os.environ.get("PGUSER", "materialize")
PGDATABASE = os.environ.get("PGDATABASE", "materialize")

CONNECT_TIMEOUT_S = 5
STATEMENT_TIMEOUT_MS = 8000
POLL_INTERVAL_S = 5.0
RUNTIME_S = 600.0
# How long an object may remain in an unhealthy state without any
# observed recovery before we count it as "stuck".  Set comfortably
# longer than the fault-orchestrator's longest fault-ON window
# (MAX_ON=40s) so a still-faulted source isn't flagged in flight.
# 180s is short enough that 10-min probe launches leave a useful
# observation window after the most-recent possible stall.
MIN_RECOVERY_S = 180.0

# Healthy statuses.  `starting` is the transient pre-running state and
# isn't a violation; everything else (`stalled`, `failed`, `ceased`,
# `dropped`, `paused`) is unhealthy from the convergence-of-status
# perspective.
_HEALTHY = frozenset({"running", "starting"})

# Combined source + sink status scan.  Scoped to user objects (`u%`)
# so system sources never trip the safety check.
_STATUS_QUERY = (
    "SELECT 'source' AS kind, s.id, s.name, s.status, s.error "
    "FROM mz_internal.mz_source_statuses s "
    "WHERE s.id LIKE 'u%' "
    "UNION ALL "
    "SELECT 'sink' AS kind, k.id, k.name, k.status, k.error "
    "FROM mz_internal.mz_sink_statuses k "
    "WHERE k.id LIKE 'u%'"
)


def _scan_statuses() -> list[tuple[str, str, str, str, str | None]] | None:
    """Snapshot user source/sink statuses or return None on fault."""
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
                cur.execute(_STATUS_QUERY.encode())
                return [
                    (
                        str(r[0]),
                        str(r[1]),
                        str(r[2]),
                        str(r[3]),
                        str(r[4]) if r[4] is not None else None,
                    )
                    for r in cur.fetchall()
                ]
    except Exception as exc:  # noqa: BLE001
        if not looks_like_fault(str(exc)):
            LOG.warning("status scan non-fault error: %s", exc)
        return None


def main() -> int:
    deadline = time.time() + RUNTIME_S
    last_healthy_at: dict[tuple[str, str], float] = {}
    last_unhealthy_at: dict[tuple[str, str], tuple[float, str, str, str | None]] = {}
    ever_unhealthy = False
    ever_recovered = False
    polls = 0
    objects_seen: set[tuple[str, str]] = set()

    while time.time() < deadline:
        rows = _scan_statuses()
        if rows is None:
            time.sleep(POLL_INTERVAL_S)
            continue
        polls += 1
        now = time.time()
        for kind, oid, name, status, error in rows:
            key = (kind, oid)
            objects_seen.add(key)
            if status in _HEALTHY:
                # Recovery transition: previously unhealthy → now healthy.
                prev_unhealthy = last_unhealthy_at.get(key)
                if prev_unhealthy is not None and prev_unhealthy[
                    0
                ] > last_healthy_at.get(key, 0.0):
                    ever_recovered = True
                last_healthy_at[key] = now
            else:
                last_unhealthy_at[key] = (now, status, name, error)
                ever_unhealthy = True
        time.sleep(POLL_INTERVAL_S)

    # Stuck-at-launch-end check.  An object is stuck iff:
    #   * the most recent observation was unhealthy, AND
    #   * we never observed it healthy after that, AND
    #   * the unhealthy observation is far enough in the past that the
    #     SUT had a generous window to recover.
    end_time = time.time()
    stuck: list[dict[str, str | float]] = []
    for key, (ts_u, status, name, error) in last_unhealthy_at.items():
        ts_h = last_healthy_at.get(key, 0.0)
        if ts_u > ts_h and (end_time - ts_u) > MIN_RECOVERY_S:
            stuck.append(
                {
                    "kind": key[0],
                    "id": key[1],
                    "name": name,
                    "status": status,
                    "error": (error[:200] if error else None),
                    "age_s": round(end_time - ts_u, 1),
                }
            )

    LOG.info(
        "source-status probe: polls=%d objects=%d ever_unhealthy=%s ever_recovered=%s stuck=%d",
        polls,
        len(objects_seen),
        ever_unhealthy,
        ever_recovered,
        len(stuck),
    )

    always(
        not stuck,
        "source-status: no user source/sink stuck in an unhealthy state "
        "without a subsequent healthy observation",
        {
            "polls": polls,
            "objects_seen": len(objects_seen),
            "min_recovery_s": MIN_RECOVERY_S,
            "stuck_count": len(stuck),
            "stuck_sample": stuck[:5],
        },
    )
    sometimes(
        ever_unhealthy,
        "source-status: observed at least one user source/sink in an "
        "unhealthy state during the probe launch",
        {"polls": polls, "objects_seen": len(objects_seen)},
    )
    sometimes(
        ever_recovered,
        "source-status: observed at least one user source/sink recover "
        "from unhealthy back to healthy",
        {"polls": polls, "objects_seen": len(objects_seen)},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
