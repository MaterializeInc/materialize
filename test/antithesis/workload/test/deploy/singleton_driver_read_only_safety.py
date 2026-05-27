#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: read-only safety of the deploy-target materialized.

The `deploy` workload group brings up a second `materialized` instance
(`materialized2`) alongside the primary, sharing the same
postgres-metadata catalog and minio blob store, with the deploy-
generation knob bumped to 1.  The bump puts materialized2 into
read-only mode: it rehydrates state from the shared durable layer but
must reject every write attempt.

The driver runs as a singleton per timeline (long-lived; spans many
Antithesis-injected faults):

  * Phase A — drive a small INSERT loop against the *primary*
    (`materialized`).  Each insert uses a per-row marker derived from
    the driver's prefix so concurrent invocations / re-runs don't
    collide.  Records the set of markers successfully written.

  * Phase B — every poll, on a *fresh* connection to materialized2,
    issue both a SELECT that should observe the markers via the shared
    state AND a write that should be rejected with `cannot write in
    read-only mode`.  Records:
      - `writes_rejected`: count of attempted writes that surfaced a
        recognizable read-only error
      - `writes_unsafely_accepted`: count of attempted writes that
        returned OK — every increment of this is an `always(False)`
        violation
      - `markers_replicated`: set of markers observed via SELECT on
        materialized2

Properties asserted:

  always — no write ever succeeded against the read-only instance.
    A single succeeding write means the deploy-generation gate failed
    and two instances are racing on the same persist state — a
    dual-leader correctness violation.

  sometimes — at least one marker INSERTed on the primary was
    observed on materialized2.  Liveness anchor for the shared-state
    replication path.

  sometimes — at least one write attempt against materialized2 was
    rejected with `cannot write in read-only mode`.  Without this the
    safety check is vacuous (every poll might have hit a fault
    window).

Fault-tolerance: every primary write is wrapped in retries, every
materialized2 connect/query is allowed to fail-fault-shape, and the
driver runs for a bounded budget per invocation.  Singleton, so
Antithesis runs at most one of these per timeline.
"""

from __future__ import annotations

import os
import sys
import time

import helper_logging
import helper_random
import psycopg
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault

LOG = helper_logging.setup_logging("driver.read_only_safety")

# Primary and read-only deploy-target hostnames.  Set in the deploy
# group's docker-compose.yaml via export-compose.  6875 is the user
# pgwire port; the driver uses the default `materialize` user with no
# password (matches every other antithesis driver).
PRIMARY_HOST = os.environ.get("PGHOST", "materialized")
DEPLOY_TARGET_HOST = os.environ.get("PGHOST_DEPLOY_TARGET", "materialized2")
PORT = int(os.environ.get("PGPORT", "6875"))
USER = os.environ.get("PGUSER", "materialize")
DATABASE = os.environ.get("PGDATABASE", "materialize")

CONNECT_TIMEOUT_S = 10
STATEMENT_TIMEOUT_MS = 5000

# Bounded per-invocation lifetime.  Singleton drivers may run for a
# long time per timeline (the catalog/persist singletons run several
# minutes), but the read-only-safety check converges quickly: once we
# have ~30 polls landing both a primary write and a materialized2
# write-attempt across enough fault-OFF windows to observe the
# replication, additional time adds little.
RUNTIME_S = float(os.environ.get("DEPLOY_RUNTIME_S", "300"))
POLL_INTERVAL_S = 2.0

# Substring matches for the read-only-mode rejection.  Materialize
# emits this on every DML against an instance running with a higher
# deploy_generation than the current leader's; the wording is stable
# across versions (see `test/0dt/mzcompose.py` for the canonical
# `contains:` checks).
READ_ONLY_REJECT_PATTERNS = (
    "cannot write in read-only mode",
    "read-only",
)


def _looks_like_read_only_reject(msg: str) -> bool:
    lo = msg.lower()
    return any(pat.lower() in lo for pat in READ_ONLY_REJECT_PATTERNS)


def _connect(host: str) -> psycopg.Connection | None:
    """Open a fresh autocommit connection or return None on fault."""
    try:
        return psycopg.connect(
            host=host,
            port=PORT,
            user=USER,
            dbname=DATABASE,
            autocommit=True,
            connect_timeout=CONNECT_TIMEOUT_S,
        )
    except Exception as exc:  # noqa: BLE001
        if not looks_like_fault(str(exc)):
            LOG.warning("[%s] non-fault connect error: %s", host, exc)
        return None


def _exec(conn: psycopg.Connection, sql: str) -> tuple[bool, str | None]:
    """Execute `sql`; return (ok, err_msg_or_None)."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}".encode())
            cur.execute(sql.encode())
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    return True, None


def _select_markers(conn: psycopg.Connection, table: str) -> set[str] | None:
    """Return the marker set currently visible on `conn`, or None on fault."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}".encode())
            cur.execute(f"SELECT marker FROM {table}".encode())
            return {str(row[0]) for row in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001
        if not looks_like_fault(str(exc)):
            LOG.warning("[deploy-target] non-fault select error: %s", exc)
        return None


def _ensure_table(prefix: str) -> str | None:
    """Create the per-prefix marker table on the primary if not present.

    Returns the qualified table name on success, or None if the primary
    couldn't be reached within the connect budget (treated as a fault
    window — the singleton driver retries on the next poll).
    """
    table = f"public.deploy_safety_{prefix}"
    conn = _connect(PRIMARY_HOST)
    if conn is None:
        return None
    try:
        ok, err = _exec(conn, f"CREATE TABLE IF NOT EXISTS {table} (marker text)")
        if not ok:
            if not looks_like_fault(err or ""):
                LOG.warning("primary CREATE TABLE non-fault error: %s", err)
            return None
        return table
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def main() -> int:
    prefix = f"{helper_random.random_u64():016x}"
    table = None
    while table is None and time.time() < time.time() + 30:
        table = _ensure_table(prefix)
        if table is None:
            time.sleep(POLL_INTERVAL_S)
    if table is None:
        # Couldn't even reach the primary to set up scratch state —
        # the run was fault-stalled at startup; report nothing rather
        # than firing vacuous assertions.
        LOG.warning("could not set up scratch table within startup budget")
        return 0

    deadline = time.time() + RUNTIME_S

    written_markers: set[str] = set()
    replicated_markers: set[str] = set()
    writes_rejected = 0
    writes_unsafely_accepted = 0
    primary_write_attempts = 0
    primary_write_successes = 0
    polls = 0
    serial = 0
    # Capture (msg, sample_marker) for the *first* unsafe accept so the
    # assertion-detail payload has a concrete repro hook.  Subsequent
    # accepts are counted but not stored — first-failure detail is
    # plenty for triage.
    first_unsafe_accept: dict[str, str] | None = None

    while time.time() < deadline:
        polls += 1
        serial += 1
        marker = f"{prefix}_{serial:06d}"

        # --- Primary write ---
        primary = _connect(PRIMARY_HOST)
        if primary is not None:
            primary_write_attempts += 1
            ok, err = _exec(
                primary,
                f"INSERT INTO {table} (marker) VALUES ('{marker}')",
            )
            if ok:
                primary_write_successes += 1
                written_markers.add(marker)
            elif not looks_like_fault(err or ""):
                LOG.warning("primary INSERT non-fault error: %s", err)
            try:
                primary.close()
            except Exception:  # noqa: BLE001
                pass

        # --- Read-only target probe ---
        target = _connect(DEPLOY_TARGET_HOST)
        if target is not None:
            try:
                # Selectively poll for replicated state.  Cheap; runs
                # every iteration so we get many samples and the
                # liveness anchor fires reliably once mz2 has caught up.
                seen = _select_markers(target, table)
                if seen is not None:
                    replicated_markers |= seen & written_markers

                # Attempt a write that MUST be rejected.  This is the
                # safety probe.  Use a distinct marker namespace so we
                # never mistake an "accepted" write for a successful
                # replication observation later.
                attempt_marker = f"{prefix}_ATTEMPT_{serial:06d}"
                ok, err = _exec(
                    target,
                    f"INSERT INTO {table} (marker) VALUES ('{attempt_marker}')",
                )
                if ok:
                    writes_unsafely_accepted += 1
                    if first_unsafe_accept is None:
                        first_unsafe_accept = {
                            "marker": attempt_marker,
                            "details": "INSERT returned OK on the deploy target",
                        }
                    LOG.error(
                        "SAFETY VIOLATION: write succeeded on materialized2: %s",
                        attempt_marker,
                    )
                elif err and _looks_like_read_only_reject(err):
                    writes_rejected += 1
                elif err and looks_like_fault(err):
                    # Fault-shape error during a write attempt — neither
                    # a rejection nor a violation.  Fall through.
                    pass
                else:
                    # Unexpected non-fault, non-read-only error.  Log
                    # but don't count: triage can look at the warning
                    # and decide whether the wording shifted.
                    LOG.warning(
                        "deploy-target write returned an unexpected error: %s",
                        err,
                    )
            finally:
                try:
                    target.close()
                except Exception:  # noqa: BLE001
                    pass

        time.sleep(POLL_INTERVAL_S)

    LOG.info(
        "[SUMMARY] read-only-safety: polls=%d primary_writes=%d/%d "
        "replicated=%d/%d writes_rejected=%d writes_unsafely_accepted=%d",
        polls,
        primary_write_successes,
        primary_write_attempts,
        len(replicated_markers),
        len(written_markers),
        writes_rejected,
        writes_unsafely_accepted,
    )

    # SAFETY: writes against the read-only instance must NEVER succeed.
    # Any single increment of writes_unsafely_accepted is an always(False)
    # — dual-leader behavior on shared persist state.
    always(
        writes_unsafely_accepted == 0,
        "deploy: read-only materialized2 never accepted a write",
        {
            "prefix": prefix,
            "writes_unsafely_accepted": writes_unsafely_accepted,
            "writes_rejected": writes_rejected,
            "first_unsafe_accept": first_unsafe_accept,
            "primary_host": PRIMARY_HOST,
            "deploy_target_host": DEPLOY_TARGET_HOST,
        },
    )

    # LIVENESS: shared-state replication actually runs.  Without this
    # the safety probe could be passing vacuously (mz2 never even rehydrated
    # or all the primary writes failed under faults).
    sometimes(
        len(replicated_markers) > 0,
        "deploy: at least one primary-written marker observed on materialized2",
        {
            "prefix": prefix,
            "written": len(written_markers),
            "replicated": len(replicated_markers),
        },
    )

    # LIVENESS: the read-only rejection path is exercised.  Without
    # this the always() above could pass because no write was ever
    # attempted on mz2 — e.g. if every fault window covered mz2.
    sometimes(
        writes_rejected > 0,
        "deploy: materialized2 rejected at least one write with read-only error",
        {"prefix": prefix, "writes_rejected": writes_rejected},
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
