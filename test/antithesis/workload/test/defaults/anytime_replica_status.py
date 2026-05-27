#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis continuous probe: cluster-replica fault-recovery visibility.

The May 25 batch of runs showed two registered Sometimes-assertions as
9× unfound across every timeline:

  fault recovery: observed antithesis_cluster replica non-online at least once
  fault recovery: SELECT 1 succeeded after a previously-observed connect failure

Both are about *observability* of fault windows from the workload side:
Antithesis kills / pauses containers freely, but no driver actively
polled `mz_internal.mz_cluster_replica_statuses` or tracked the
"connect failed then later succeeded" pattern, so the assertions never
fired even though faults absolutely landed on the cluster.

This probe runs as an `anytime_*` (continuous) command and:

  1. Polls `mz_cluster_replica_statuses` joined to `mz_clusters` for
     replicas belonging to the universal `antithesis_cluster`.  Records
     any (replica_id, status) transition observed within this launch.
     Fires `sometimes(saw an `offline` row)` and the legacy-name
     `sometimes(observed antithesis_cluster replica non-online)`.

  2. Issues an unrelated `SELECT 1` round-trip on every poll.  Tracks
     whether a fault-shaped connect/query failure has been seen, and
     whether a subsequent poll succeeded — fires
     `sometimes(SELECT 1 succeeded after a previously-observed connect
     failure)` when the transition happens.

Both signals are coverage anchors, not safety invariants — these are
all `sometimes(...)` calls.  A fault that *never* lands within this
probe's launch lifetime is fine; Antithesis re-launches anytime drivers
continuously, so cumulative coverage builds across the run.
"""

from __future__ import annotations

import time

import helper_logging
import psycopg
from antithesis.assertions import sometimes
from helper_fault_tolerance import looks_like_fault
from helper_pg import (
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGUSER,
    query_retry,
)

LOG = helper_logging.setup_logging("anytime.replica_status")

# Probe cadence.  1s rather than the persist-invariants probe's 2s: a
# clusterd kill/restart can resolve inside a single 2s window, and the
# point-in-time `mz_cluster_replica_statuses` poll would miss it.  The
# durable-history query below is the real safety net (it sees offline
# events the live poll missed), but a tighter cadence still helps the
# online/offline-transition observation.
POLL_INTERVAL_S = 1.0
# Per-launch wall-clock budget.  Anytime drivers are re-launched after
# they exit; a long launch covers more of one timeline but its `seen`
# state is lost between launches.  10 minutes covers ~7+ fault-ON/OFF
# cycles, which is plenty to witness a transition across a single
# launch.
RUNTIME_S = 600.0
# Direct-connect timeout for the SELECT-1 liveness probe.  Deliberately
# short and NON-retrying (unlike helper_pg.query_retry) so the probe
# actually *observes* a connect failure during a fault window — the
# whole point of the "recovered after a previous connect failure"
# coverage anchor.  query_retry would mask the transient by retrying
# through the fault, so the recovery transition would never be seen.
PROBE_CONNECT_TIMEOUT_S = 3

# `antithesis_cluster` is the universal cluster bootstrapped by
# workload-entrypoint.sh in every group's topology.  Probes scope
# here so noise from per-driver scratch clusters doesn't show up.
CLUSTER_NAME = "antithesis_cluster"

# Point-in-time view: the replica's current status.  Used for the
# online liveness anchor and the live offline→online transition.
_QUERY_STATUSES = (
    "SELECT r.replica_id, r.process_id, r.status "
    "FROM mz_internal.mz_cluster_replica_statuses r "
    "JOIN mz_cluster_replicas cr ON r.replica_id = cr.id "
    "JOIN mz_clusters c ON cr.cluster_id = c.id "
    f"WHERE c.name = '{CLUSTER_NAME}'"
)

# Durable history: every status transition with a wall-clock
# `occurred_at`.  This is the robust offline detector — a clusterd
# blip that the point-in-time poll missed still leaves an `offline`
# row here that any later successful poll observes.  Scoped to events
# at/after this probe launch so we attribute the transition to this
# timeline rather than a stale one from before the probe started.
_QUERY_OFFLINE_HISTORY = (
    "SELECT count(*) "
    "FROM mz_internal.mz_cluster_replica_status_history h "
    "JOIN mz_cluster_replicas cr ON h.replica_id = cr.id "
    "JOIN mz_clusters c ON cr.cluster_id = c.id "
    "WHERE c.name = '{cluster}' "
    "  AND h.status != 'online' "
    "  AND h.occurred_at >= '{since}'"
)


def _poll_statuses() -> list[tuple[str, int, str]] | None:
    """Return [(replica_id, process_id, status), ...] or None on fault."""
    try:
        rows = query_retry(_QUERY_STATUSES)
    except Exception as exc:  # noqa: BLE001
        if looks_like_fault(str(exc)):
            return None
        # Non-fault error — likely a schema change or permission issue
        # this probe needs adjusting for.  Surface it.
        raise
    return [(str(r[0]), int(r[1]), str(r[2])) for r in rows]


def _count_offline_since(since_iso: str) -> int | None:
    """Count offline history events for the cluster since `since_iso`.

    Returns the count, or None on a fault-shaped failure (the caller
    just skips that poll — the durable rows are still there for the
    next one).
    """
    q = _QUERY_OFFLINE_HISTORY.format(cluster=CLUSTER_NAME, since=since_iso)
    try:
        rows = query_retry(q)
    except Exception as exc:  # noqa: BLE001
        if looks_like_fault(str(exc)):
            return None
        raise
    return int(rows[0][0]) if rows else 0


def _select_one_no_retry() -> bool:
    """Open a fresh, non-retrying connection and run SELECT 1.

    Returns True on success, False on any failure (fault-shaped or
    otherwise).  Intentionally does NOT use helper_pg.query_retry: the
    recovery anchor needs to observe the transient connect failure that
    a retry loop would paper over.
    """
    try:
        with psycopg.connect(
            host=PGHOST,
            port=PGPORT,
            user=PGUSER,
            dbname=PGDATABASE,
            connect_timeout=PROBE_CONNECT_TIMEOUT_S,
            autocommit=True,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(b"SELECT 1")
                row = cur.fetchone()
                return row is not None and int(row[0]) == 1
    except Exception:  # noqa: BLE001
        # Any failure — connect refused/timeout during a fault window,
        # or a server-initiated drop — counts as an observed failure.
        return False


def main() -> int:
    deadline = time.time() + RUNTIME_S
    # Wall-clock launch time, ISO-formatted for the history query's
    # `occurred_at >=` filter.  mz timestamps `occurred_at` in UTC.
    since_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    saw_non_online = False
    saw_online = False
    saw_offline_to_online_transition = False
    last_status: dict[tuple[str, int], str] = {}
    offline_events_seen = 0

    saw_connect_fault = False
    saw_recovery_after_connect_fault = False

    polls = 0
    while time.time() < deadline:
        polls += 1

        # (1) Point-in-time status: online liveness + live offline→online.
        status_rows = _poll_statuses()
        if status_rows is not None:
            for replica_id, process_id, status in status_rows:
                key = (replica_id, process_id)
                prev = last_status.get(key)
                if status != "online":
                    saw_non_online = True
                else:
                    saw_online = True
                    if prev is not None and prev != "online":
                        saw_offline_to_online_transition = True
                last_status[key] = status

        # (2) Durable history: catches offline events the 1s poll missed.
        # A clusterd blip that resolved between two polls still left an
        # `offline` row here, so any later successful query observes it.
        offline_count = _count_offline_since(since_iso)
        if offline_count is not None and offline_count > 0:
            saw_non_online = True
            offline_events_seen = offline_count

        # (3) Non-retrying SELECT 1 to observe connect-failure→recovery.
        select_ok = _select_one_no_retry()
        if not select_ok:
            saw_connect_fault = True
        elif select_ok and saw_connect_fault:
            saw_recovery_after_connect_fault = True

        time.sleep(POLL_INTERVAL_S)

    LOG.info(
        "replica-status probe complete: polls=%d saw_non_online=%s "
        "offline_events_seen=%d saw_offline_to_online=%s saw_connect_fault=%s "
        "saw_recovery=%s",
        polls,
        saw_non_online,
        offline_events_seen,
        saw_offline_to_online_transition,
        saw_connect_fault,
        saw_recovery_after_connect_fault,
    )

    # Legacy property name retained verbatim so Antithesis's history can
    # match it against the 9× unfound entry that prompted this probe.
    sometimes(
        saw_non_online,
        "fault recovery: observed antithesis_cluster replica non-online "
        "at least once",
        {
            "polls": polls,
            "replicas_observed": len(last_status),
            "offline_events_seen": offline_events_seen,
            "cluster_name": CLUSTER_NAME,
        },
    )
    sometimes(
        saw_offline_to_online_transition,
        "fault recovery: antithesis_cluster replica returned to online "
        "after a previously-observed non-online state",
        {
            "polls": polls,
            "replicas_observed": len(last_status),
        },
    )
    # Legacy property name retained for the same reason: it's currently
    # 9× unfound because no driver tracked the "connect failed then
    # later succeeded" pattern.  Now it's a continuous sometimes-anchor.
    sometimes(
        saw_recovery_after_connect_fault,
        "fault recovery: SELECT 1 succeeded after a previously-observed "
        "connect failure",
        {
            "polls": polls,
            "saw_connect_fault": saw_connect_fault,
        },
    )
    # Liveness: the probe itself made any progress.  Without this, a
    # bug in the probe that prevents `polls` from advancing would leave
    # the three coverage assertions above vacuously satisfied with no
    # signal.
    sometimes(
        saw_online,
        "fault recovery: antithesis_cluster replica observed online "
        "at least once in this probe launch",
        {"polls": polls},
    )

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
