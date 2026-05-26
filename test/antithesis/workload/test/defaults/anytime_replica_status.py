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
from antithesis.assertions import sometimes
from helper_fault_tolerance import looks_like_fault
from helper_pg import query_retry

LOG = helper_logging.setup_logging("anytime.replica_status")

# Probe cadence.  Short enough to catch sub-second fault windows from
# the orchestrator's `MIN_ON=20s, MAX_ON=40s` cycle; long enough that
# many concurrent invocations don't drown the SUT in introspection
# queries.  The persist-invariants probe uses 2s — same cadence keeps
# triage time-axes alignable across the two anytime probes.
POLL_INTERVAL_S = 2.0
# Per-launch wall-clock budget.  Anytime drivers are re-launched after
# they exit; a long launch covers more of one timeline but its `seen`
# state is lost between launches.  10 minutes covers ~7+ fault-ON/OFF
# cycles, which is plenty to witness a transition across a single
# launch.
RUNTIME_S = 600.0

# `antithesis_cluster` is the universal cluster bootstrapped by
# workload-entrypoint.sh in every group's topology.  Probes scope
# here so noise from per-driver scratch clusters doesn't show up.
CLUSTER_NAME = "antithesis_cluster"

_QUERY_STATUSES = (
    "SELECT r.replica_id, r.process_id, r.status "
    "FROM mz_internal.mz_cluster_replica_statuses r "
    "JOIN mz_cluster_replicas cr ON r.replica_id = cr.id "
    "JOIN mz_clusters c ON cr.cluster_id = c.id "
    f"WHERE c.name = '{CLUSTER_NAME}'"
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


def _select_one() -> bool | None:
    """Return True on success, False on fault-shape failure, None on
    non-fault error (probe-side issue worth surfacing — caller raises).
    """
    try:
        rows = query_retry("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        if looks_like_fault(str(exc)):
            return False
        raise
    return len(rows) == 1 and int(rows[0][0]) == 1


def main() -> int:
    deadline = time.time() + RUNTIME_S

    saw_non_online = False
    saw_online = False
    saw_offline_to_online_transition = False
    last_status: dict[tuple[str, int], str] = {}

    saw_connect_fault = False
    saw_recovery_after_connect_fault = False

    polls = 0
    while time.time() < deadline:
        polls += 1
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

        select_ok = _select_one()
        if select_ok is False:
            saw_connect_fault = True
        elif select_ok is True and saw_connect_fault:
            saw_recovery_after_connect_fault = True

        time.sleep(POLL_INTERVAL_S)

    LOG.info(
        "replica-status probe complete: polls=%d saw_non_online=%s "
        "saw_offline_to_online=%s saw_connect_fault=%s saw_recovery=%s",
        polls,
        saw_non_online,
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
