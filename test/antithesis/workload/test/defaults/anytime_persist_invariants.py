#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis continuous probe: persist + controller frontier invariants.

Two invariants asserted continuously, on every poll:

  1. For every (object_id, read_frontier, write_frontier) with both
     frontiers non-NULL: `read_frontier <= write_frontier`.

     `read_frontier` is the "since" (oldest timestamp still readable);
     `write_frontier` is the "upper" (timestamp not yet written to).
     `since <= upper` is the foundational state-machine invariant of
     persist + the controller.  A violation here means the SUT will
     either (a) attempt to read at a ts in the [since, upper) range
     it claims is still readable but persist has GC'd, or (b) emit a
     progress marker at a ts the writer hasn't actually reached.
     Both are silent-corruption shapes that today only surface as
     the gc.rs:417 panic (downstream cascade) — the panic fires
     because the violated invariant eventually trips an assertion
     somewhere far from the original corruption.  Asserting the
     invariant directly catches the violation at its source.

  2. `write_frontier` per object is monotone non-decreasing across
     polls.  A frontier going *backwards* means the controller has
     either rebooted with a stale view or got confused about state
     after a partial recovery — both customer-visible (downstream
     consumers see "time go backwards").

This driver is an `anytime_*` so Antithesis re-launches it
continuously; each launch runs for RUNTIME_S and exits, letting the
next launch pick up.  Polls happen every POLL_INTERVAL_S; on a
fault-shaped probe failure (connect refused, partition, etc.) we
just skip that poll without firing the safety assertion — fault
windows are not invariant violations.

Bug class this catches that no other driver catches:
  * Persist GC or rollup logic putting a shard into a state where
    since advanced past upper (the gc.rs:417 soft_assert canary,
    caught at root rather than at panic-cascade).
  * Controller restart leaving a frontier rewound (would surface as
    a non-monotone write_frontier observation).
  * Replica handoff dropping a frontier ack (since-stops-advancing
    or upper-jumps-backwards under multi-replica fault interleaving).

This complements (does not replace) the per-source `anytime_kafka_*`
frontier probes — those check kafka-source-specific properties; this
one checks the controller-wide invariant that applies to every
materialized object.
"""

from __future__ import annotations

import os
import sys
import time

import helper_logging
from helper_fault_tolerance import looks_like_fault
from helper_pg import (
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGUSER,
    query_retry,
)

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("anytime.persist_invariants")

# Probe cadence.  Short enough to land many samples inside a single
# Antithesis test run, long enough to avoid drowning the SUT in
# introspection queries.  The kafka-frontier probe uses 2s; matching
# that keeps the visualisation of "frontier behavior under fault"
# consistent across probes.
POLL_INTERVAL_S = 2.0
# Per-launch wall-clock budget.  Anytime drivers are re-launched by
# Antithesis after they exit, so a long-running launch covers more
# of one timeline; a too-long launch means the assertions in the
# *previous* launch are lost between re-launches.  10 min strikes
# a balance.
RUNTIME_S = 600.0


def _query_frontiers() -> list[tuple[str, int | None, int | None]] | None:
    """Return [(object_id, read_frontier, write_frontier), ...].

    None means "the probe couldn't reach the SUT" (fault window) —
    the caller skips the poll's safety assertion in that case.
    Non-fault exceptions re-raise so a genuine bug in the probe
    itself surfaces loudly instead of being absorbed.

    Frontier values come back as Python ints (or None for an empty-
    antichain frontier).  Casting `mz_timestamp` → `text::bigint`
    fits any value < 2^63; mz_timestamp is u64 ms-since-epoch and
    in practice always well below that.
    """
    try:
        rows = query_retry(
            "SELECT "
            "  object_id, "
            "  CASE WHEN read_frontier IS NULL "
            "       THEN NULL "
            "       ELSE read_frontier::text::bigint END, "
            "  CASE WHEN write_frontier IS NULL "
            "       THEN NULL "
            "       ELSE write_frontier::text::bigint END "
            "FROM mz_internal.mz_frontiers"
        )
    except Exception as exc:  # noqa: BLE001
        if looks_like_fault(str(exc)):
            return None
        # Genuine probe-side bug — surface it.
        raise
    return [
        (
            str(row[0]),
            int(row[1]) if row[1] is not None else None,
            int(row[2]) if row[2] is not None else None,
        )
        for row in rows
    ]


def main() -> int:
    deadline = time.time() + RUNTIME_S

    # `last_write` tracks the most recent observed write_frontier per
    # object so we can check monotonicity across polls.  None-valued
    # entries (object had NULL write_frontier in a prior poll) are
    # left in place — a subsequent non-NULL write_frontier is a fresh
    # observation and starts the chain.  NULL → non-NULL transitions
    # don't violate monotonicity by themselves (NULL means "empty
    # antichain = ∞", which any concrete value is <= to; so going
    # NULL → 1000 would technically be a "decrease" — but in
    # practice mz_timestamp NULL is also the "uninitialized" state
    # the controller emits before it has a frontier, so we treat
    # NULL → concrete as initialisation, not regression).
    last_write: dict[str, int] = {}

    polls = 0
    saw_fault = False
    total_shards_observed = 0
    write_increased_observations = 0
    safety_observed_count = 0

    while time.time() < deadline:
        rows = _query_frontiers()
        if rows is None:
            saw_fault = True
            polls += 1
            time.sleep(POLL_INTERVAL_S)
            continue

        polls += 1
        safety_observed_count += 1
        total_shards_observed = max(total_shards_observed, len(rows))

        # Invariant 1: read_frontier <= write_frontier on non-NULL pairs.
        violations: list[tuple[str, int, int]] = []
        non_null_shards = 0
        for oid, rf, wf in rows:
            if rf is None or wf is None:
                continue
            non_null_shards += 1
            if rf > wf:
                violations.append((oid, rf, wf))

        # Cap diagnostic blob.  If a violation is real, we want it
        # surfaced clearly; we don't need a thousand examples.
        violations_sample = violations[:5]

        always(
            len(violations) == 0,
            "persist: read_frontier <= write_frontier for every shard "
            "(since <= upper invariant)",
            {
                "poll": polls,
                "violations_count": len(violations),
                "violations_sample": violations_sample,
                "non_null_shards": non_null_shards,
                "total_shards": len(rows),
            },
        )

        # Invariant 2: write_frontier per object is monotone non-decreasing
        # across polls.  Compare to last_write; record regressions.
        regressions: list[tuple[str, int, int]] = []
        for oid, _rf, wf in rows:
            if wf is None:
                continue
            prev = last_write.get(oid)
            if prev is not None and wf < prev:
                regressions.append((oid, prev, wf))
            if prev is None or wf > prev:
                last_write[oid] = wf
                if prev is not None:
                    write_increased_observations += 1

        regressions_sample = regressions[:5]
        always(
            len(regressions) == 0,
            "persist: write_frontier (upper) is monotone non-decreasing per object",
            {
                "poll": polls,
                "regressions_count": len(regressions),
                "regressions_sample": regressions_sample,
                "tracked_objects": len(last_write),
            },
        )

        if polls % 10 == 0:
            LOG.info(
                "[PROGRESS] persist-invariants poll=%d shards=%d tracked=%d "
                "increased=%d saw_fault=%s",
                polls,
                len(rows),
                len(last_write),
                write_increased_observations,
                saw_fault,
            )

        time.sleep(POLL_INTERVAL_S)

    # Liveness anchors.  Order matches "did the probe actually run
    # and see live shards?" → "did it see frontier advancement?" →
    # "did it cross a fault?".
    sometimes(
        polls >= 5,
        "persist-invariants: probe ran >= 5 polls in some timeline",
        {"polls": polls, "runtime_s": RUNTIME_S},
    )
    sometimes(
        safety_observed_count >= 5,
        "persist-invariants: >= 5 polls landed cleanly (safety assertion exercised)",
        {"safety_observed": safety_observed_count, "polls": polls},
    )
    sometimes(
        total_shards_observed > 0,
        "persist-invariants: at least one shard was visible in some poll",
        {"max_shards": total_shards_observed},
    )
    sometimes(
        write_increased_observations > 0,
        "persist-invariants: write_frontier was observed advancing forward",
        {"increased_observations": write_increased_observations},
    )
    sometimes(
        saw_fault,
        "persist-invariants: fault-injection path exercised "
        "(probe failed mid-run at least once)",
        {"saw_fault": saw_fault},
    )

    LOG.info(
        "[SUMMARY] persist-invariants polls=%d safety_observed=%d "
        "max_shards=%d tracked_objects=%d advances=%d saw_fault=%s",
        polls,
        safety_observed_count,
        total_shards_observed,
        len(last_write),
        write_increased_observations,
        saw_fault,
    )
    return 0


if __name__ == "__main__":
    _ = (os, PGHOST, PGPORT, PGUSER, PGDATABASE)
    sys.exit(main())
