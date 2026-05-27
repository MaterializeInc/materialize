#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis continuous probe: internal errors must not escape to clients.

`internal error` is Materialize's wording for a bug-class failure — an
invariant the optimizer/runtime expected to hold did not, and rather
than crash it returned the error to the client.  These are
panics-adjacent: the same root cause that yields `internal error` on
one query shape often yields a hard panic on another.  They're easy to
miss because the SUT keeps running, so no crash detector fires.

This probe runs a rotating set of non-trivial *valid* queries — joins
and aggregates over the system catalog plus a few arithmetic/temporal
edge cases — and classifies any error each raises:

  * `looks_like_fault`           → fault window, skip (not a bug).
  * contains "internal error"    → `always(False)`: an internal error
                                     escaped on a query that should
                                     always succeed.
  * any other error              → logged but not asserted on (a plain
                                     user error on these queries would
                                     indicate the probe needs updating,
                                     not a SUT bug; we don't want to
                                     false-positive the safety check).

Also scans `mz_internal.mz_source_statuses` / `mz_sink_statuses` for
objects stuck in an error state whose message is an internal error
(as opposed to an upstream/connection issue).

Property shape:
  always(no internal error escaped)      — safety
  sometimes(probe executed a query)      — liveness anchor
"""

from __future__ import annotations

import os
import time

import helper_logging
import psycopg
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault

LOG = helper_logging.setup_logging("anytime.internal_error_scan")

PGHOST = os.environ.get("PGHOST", "materialized")
PGPORT = int(os.environ.get("PGPORT", "6875"))
PGUSER = os.environ.get("PGUSER", "materialize")
PGDATABASE = os.environ.get("PGDATABASE", "materialize")

CONNECT_TIMEOUT_S = 5
STATEMENT_TIMEOUT_MS = 8000
RUNTIME_S = 600.0
POLL_INTERVAL_S = 3.0

# Non-trivial but always-valid queries.  Each exercises a different
# slice of the planner/runtime (catalog joins, aggregation, window
# functions, arithmetic + temporal edges) and must never return an
# `internal error` on a healthy SUT.  Kept read-only and catalog-scoped
# so the probe needs no setup and collides with nothing.
_PROBE_QUERIES: tuple[str, ...] = (
    # Multi-way catalog join + aggregation.
    "SELECT c.name, count(r.id) "
    "FROM mz_clusters c LEFT JOIN mz_cluster_replicas r ON r.cluster_id = c.id "
    "GROUP BY c.name ORDER BY c.name",
    # Window function over the catalog.
    "SELECT id, name, row_number() OVER (ORDER BY id) "
    "FROM mz_objects ORDER BY id LIMIT 50",
    # Correlated subquery + EXISTS.
    "SELECT o.id FROM mz_objects o "
    "WHERE EXISTS (SELECT 1 FROM mz_columns c WHERE c.id = o.id) "
    "ORDER BY o.id LIMIT 50",
    # Aggregate with HAVING + DISTINCT.
    "SELECT type, count(DISTINCT id) FROM mz_objects "
    "GROUP BY type HAVING count(*) > 0 ORDER BY type",
    # Arithmetic / type edges that have historically tripped the
    # optimizer's constant folding.
    "SELECT generate_series, generate_series % 7, "
    "       (generate_series * 2)::numeric / 3 "
    "FROM generate_series(1, 100)",
    # Temporal arithmetic.
    "SELECT now() - interval '1 day', date_trunc('hour', now()), "
    "       extract(epoch FROM now())::bigint % 1000",
    # jsonb round-trip + nesting.
    "SELECT jsonb_build_object('k', g, 'sq', g*g) "
    "FROM generate_series(1, 50) AS g",
)

_STATUS_SCAN = (
    "SELECT 'source' AS kind, name, status, error FROM mz_internal.mz_source_statuses "
    "WHERE error IS NOT NULL "
    "UNION ALL "
    "SELECT 'sink' AS kind, name, status, error FROM mz_internal.mz_sink_statuses "
    "WHERE error IS NOT NULL"
)


def _is_internal_error(msg: str) -> bool:
    return "internal error" in msg.lower()


def main() -> int:
    deadline = time.time() + RUNTIME_S
    queries_run = 0
    internal_errors: list[dict[str, str]] = []
    status_internal_errors: list[dict[str, str]] = []
    qi = 0

    while time.time() < deadline:
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
                    cur.execute(
                        f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}".encode()
                    )
                    # Rotate through one probe query per poll so a single
                    # poll stays cheap; over a launch we cover them all
                    # many times.
                    q = _PROBE_QUERIES[qi % len(_PROBE_QUERIES)]
                    qi += 1
                    try:
                        cur.execute(q.encode())
                        cur.fetchall()
                        queries_run += 1
                    except Exception as exc:  # noqa: BLE001
                        msg = str(exc)
                        if _is_internal_error(msg) and not looks_like_fault(msg):
                            internal_errors.append({"query": q[:120], "error": msg[:300]})
                            LOG.error("internal error escaped on probe query: %s", msg)
                        elif not looks_like_fault(msg):
                            LOG.warning("probe query non-fault, non-internal error: %s", msg)

                    # Status-table scan for stuck-in-internal-error objects.
                    try:
                        cur.execute(_STATUS_SCAN.encode())
                        for kind, name, status, error in cur.fetchall():
                            if error and _is_internal_error(str(error)):
                                status_internal_errors.append(
                                    {
                                        "kind": str(kind),
                                        "name": str(name),
                                        "status": str(status),
                                        "error": str(error)[:200],
                                    }
                                )
                    except Exception as exc:  # noqa: BLE001
                        if not looks_like_fault(str(exc)):
                            LOG.debug("status scan error: %s", exc)
        except Exception as exc:  # noqa: BLE001
            # Connect-level failure — fault window, just retry next poll.
            if not looks_like_fault(str(exc)):
                LOG.debug("probe connect error: %s", exc)

        time.sleep(POLL_INTERVAL_S)

    LOG.info(
        "internal-error scan complete: queries_run=%d internal_errors=%d "
        "status_internal_errors=%d",
        queries_run,
        len(internal_errors),
        len(status_internal_errors),
    )

    always(
        not internal_errors,
        "internal-error: no `internal error` escaped to a client on a valid query",
        {
            "queries_run": queries_run,
            "internal_error_count": len(internal_errors),
            "sample": internal_errors[:5],
        },
    )
    always(
        not status_internal_errors,
        "internal-error: no source/sink stuck in an internal-error status",
        {
            "count": len(status_internal_errors),
            "sample": status_internal_errors[:5],
        },
    )
    sometimes(
        queries_run > 0,
        "internal-error: probe executed at least one query in this launch",
        {"queries_run": queries_run},
    )
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
