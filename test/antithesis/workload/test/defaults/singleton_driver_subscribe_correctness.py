#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: SUBSCRIBE stream correctness under fault injection.

SUBSCRIBE is the streaming-read API customers wire dashboards and
downstream applications against — a stream that drops or duplicates
events under network faults is the silent kind of bug nobody notices
until the dashboard's numbers don't add up.  No existing Antithesis
driver exercises this surface today.

Property statement (no faults, no faults, no faults — under faults,
no faults, no faults):
  1. Every committed INSERT eventually appears in the SUBSCRIBE stream
     as a `+1` diff for its (counter, value) — no missed events.
  2. The SUBSCRIBE stream never emits a row not committed by the
     producer — no spurious events.
  3. After a fault-injected disconnect, reconnecting with `AS OF
     <max_progress_ts_seen>` resumes cleanly with no missed events
     in the seam.

Approach:
  * Producer thread: in a loop, INSERTs (counter, value) with a
    monotone counter, using `execute_retry` so a fault-window write
    converges via retry idempotency.  Tracks the authoritative
    committed_set in shared state.
  * Consumer thread: opens `SUBSCRIBE t WITH (PROGRESS = TRUE)` via a
    DECLARE CURSOR / FETCH loop on a fresh psycopg connection.  On
    any disconnect that `looks_like_fault`, increments a reconnect
    counter, sleeps briefly, and reconnects with `SUBSCRIBE t WITH
    (SNAPSHOT = FALSE, PROGRESS = TRUE) AS OF <max_progress_ts>` —
    no SNAPSHOT to avoid re-emitting rows we've already consumed.
  * After RUNTIME_S of concurrent run:
    - stop the producer; query the authoritative committed_set from
      `SELECT counter FROM t` (fresh read).
    - query `SELECT mz_now()` for a target catch-up timestamp.
    - wait up to CATCHUP_TIMEOUT_S for the consumer's max_progress_ts
      to reach target_ts.
    - stop the consumer; compare its received_set to the authoritative
      committed_set.
  * Safety assertion: received_set ⊇ committed_set (no missed).
    Safety assertion: received_set ⊆ committed_set ∪ {producer
    attempts that didn't ack} (no spurious — the unioned set is wider
    than committed_set to absorb the ambiguous case where the producer
    got a fault on the INSERT but the SUT committed the row anyway).

Why singleton: a single producer + a single consumer with shared
in-memory state is much simpler than coordinating multiple producers
or multiple consumers against the same model.  Singleton matches the
catalog_recovery and table-data-integrity drivers' shape.

The retry-after-fault semantics for INSERTs use the same idempotent
ON CONFLICT pattern as `singleton_driver_table_data_integrity`: a
retry of an already-committed counter silently no-ops, so success
from `execute_retry` is unambiguous.

Bugs this catches that no other driver catches:
  * SUBSCRIBE stream loses an event whose commit_ts < consumer's
    progress_ts (the stream's "fence" property is broken).
  * SUBSCRIBE emits an uncommitted event after a fault recovery
    (write isolation bug surfacing only on the stream side).
  * AS OF reconnect skips events whose ts straddles the seam.
  * Progress timestamp advances past events that should still be
    pending (a frontier-correctness bug).
"""

from __future__ import annotations

import os
import sys
import threading
import time
from typing import Any

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

LOG = helper_logging.setup_logging("driver.subscribe_correctness")

# Producer/consumer run window.  Long enough to absorb at least one
# full faults-ON+OFF cycle from the global orchestrator (~80s default)
# plus enough margin that a faulted run still gets some clean cycles.
RUNTIME_S = 60.0
# Wall-clock budget for the consumer to catch up after the producer
# stops.  Sized at ~one full ON+OFF cycle so a consumer that's mid-
# fault when producer stops has a chance to reconnect and drain.
CATCHUP_TIMEOUT_S = 90.0
# Per-insert gap.  Short enough to land many commits during the run
# window, long enough to leave the SUT room for other work and to
# avoid wedging the worker pool.
INSERT_GAP_S = 0.05
# Per-FETCH timeout.  We need the consumer thread to check
# `consumer_stop` regularly; FETCH WITH (timeout = '...') makes the
# stream cooperative without us needing a separate signal channel.
FETCH_TIMEOUT_MS = 500
# Connection settings for the consumer.  Short connect_timeout so a
# faulted server fails fast and the reconnect loop spins.
PROBE_CONNECT_TIMEOUT_S = 5

# Mutex guarding shared state between threads.  CPython's GIL makes
# single-attribute reads/writes atomic but compound operations
# (read-modify-write on a dict, set.add inside a comprehension) are
# not.  Holding the lock around all mutations keeps the assertion-
# time snapshot consistent without us writing a freeze step.
_state_lock = threading.Lock()


def _producer_loop(
    table: str,
    state: dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """INSERT (counter, value) rows with a monotone counter.

    `state` is the shared producer state dict mutated under
    `_state_lock`.  Idempotent ON CONFLICT means a fault-retry that
    lands a duplicate INSERT silently no-ops at the SUT, so
    `execute_retry` returning success unambiguously means "this
    counter is in the table".
    """
    counter = 0
    while not stop_event.is_set():
        counter += 1
        with _state_lock:
            state["attempted"] += 1
        value = f"v{counter}"
        try:
            # Materialize doesn't support ON CONFLICT and doesn't
            # enforce uniqueness — use a WHERE NOT EXISTS gate to
            # make INSERT idempotent under `execute_retry`'s retry-
            # after-network-drop semantics.  Without this, a fault-
            # window write that committed at the SUT but lost the
            # client ack would re-INSERT on the retry, creating two
            # rows with the same counter and breaking the
            # subscribe-side `received_set ⊆ committed_set` check.
            execute_retry(
                f"INSERT INTO {table} (counter, value) "
                f"SELECT %s, %s "
                f"WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE counter = %s)",
                (counter, value, counter),
            )
            with _state_lock:
                state["committed_set"].add(counter)
        except Exception as exc:  # noqa: BLE001
            # Budget-exhausted retry.  The INSERT may or may not have
            # actually landed at the SUT.  Don't add to committed_set
            # — but DO add to ambiguous_set, which the assertion uses
            # to absorb the "spurious-from-stream's view" case where
            # the SUT did commit despite the client never getting an
            # ack.  Without this, a stream-side commit-but-no-ack
            # would fire `always(False, "no spurious")` even though
            # there's no real spurious; it's a coherent commit the
            # producer just didn't witness.
            with _state_lock:
                state["ambiguous_set"].add(counter)
                state["fault_attempts"] += 1
            if not looks_like_fault(str(exc)):
                LOG.warning(
                    "producer: non-fault failure on counter=%d: %s", counter, exc
                )
        time.sleep(INSERT_GAP_S)


def _consumer_loop(
    table: str,
    state: dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """Open SUBSCRIBE, FETCH rows, reconnect on fault.

    Loop invariant: at the top of each connection-open iteration,
    `state["max_progress_ts"]` is the highest ts the consumer has
    proven all events for.  On reconnect, we use that as the AS OF
    bound with `SNAPSHOT = FALSE` to avoid re-receiving events we've
    already consumed.

    Row shape with `WITH (PROGRESS = TRUE)`:
        (mz_timestamp BIGINT, mz_progressed BOOL, mz_diff BIGINT, counter BIGINT, value TEXT)
        - progressed=true marks "all events at ts < mz_timestamp emitted"
        - progressed=false is a data event with mz_diff = +/-1
    """
    while not stop_event.is_set():
        try:
            conn = psycopg.connect(
                host=PGHOST,
                port=PGPORT,
                user=PGUSER,
                dbname=PGDATABASE,
                connect_timeout=PROBE_CONNECT_TIMEOUT_S,
            )
        except Exception as exc:  # noqa: BLE001
            if stop_event.is_set():
                return
            if looks_like_fault(str(exc)):
                with _state_lock:
                    state["reconnects"] += 1
                time.sleep(0.5)
                continue
            with _state_lock:
                state["fatal_err"] = (type(exc).__name__, str(exc)[:200])
            return

        try:
            # Each SUBSCRIBE session is its own pg transaction.
            # autocommit=False is the default, but we set it
            # explicitly so the cursor + FETCH protocol is clear.
            conn.autocommit = False
            with conn.cursor() as cur:
                with _state_lock:
                    as_of = state["max_progress_ts"]
                # First connect: take a snapshot so we see existing
                # rows.  Reconnect: skip the snapshot since we
                # already consumed everything < max_progress_ts.
                if as_of > 0:
                    cur.execute(
                        f"DECLARE c CURSOR FOR "
                        f"SUBSCRIBE {table} WITH (SNAPSHOT = FALSE, PROGRESS = TRUE) "
                        f"AS OF {as_of}"
                    )
                else:
                    cur.execute(
                        f"DECLARE c CURSOR FOR "
                        f"SUBSCRIBE {table} WITH (PROGRESS = TRUE)"
                    )

                while not stop_event.is_set():
                    cur.execute(
                        f"FETCH ALL FROM c WITH (timeout = '{FETCH_TIMEOUT_MS}ms')"
                    )
                    rows = cur.fetchall()
                    if not rows:
                        # FETCH timed out with no rows.  Re-loop.
                        continue
                    for row in rows:
                        ts = int(row[0])
                        progressed = bool(row[1])
                        if progressed:
                            with _state_lock:
                                if ts > state["max_progress_ts"]:
                                    state["max_progress_ts"] = ts
                            continue
                        # Data row.  row[2] = mz_diff, row[3] = counter, row[4] = value
                        diff = int(row[2])
                        counter = int(row[3])
                        with _state_lock:
                            if diff > 0:
                                state["received_set"].add(counter)
                                state["received_events"] += diff
                            else:
                                # Negative diff would be a retraction.
                                # The producer only INSERTs, never
                                # UPDATEs or DELETEs, so a retraction
                                # from the SUT is an unexpected event.
                                # Track it for the "no spurious" check.
                                state["retracted_set"].add(counter)
        except Exception as exc:  # noqa: BLE001
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            if stop_event.is_set():
                return
            if looks_like_fault(str(exc)):
                with _state_lock:
                    state["reconnects"] += 1
                time.sleep(0.5)
                continue
            with _state_lock:
                state["fatal_err"] = (type(exc).__name__, str(exc)[:200])
            return
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def main() -> int:
    prefix = f"subcorr_{helper_random.random_u64():016x}"
    table = f"antithesis_subcorr_{prefix}"

    LOG.info("subscribe-correctness starting; prefix=%s runtime=%ss", prefix, RUNTIME_S)

    # Create the per-prefix table.  No PK — Materialize doesn't
    # enforce uniqueness anyway; the producer's WHERE NOT EXISTS
    # gate provides application-side dedup.
    try:
        execute_retry(
            f"CREATE TABLE IF NOT EXISTS {table} (counter BIGINT NOT NULL, value TEXT NOT NULL)"
        )
    except Exception as exc:  # noqa: BLE001
        LOG.warning("subscribe-correctness: table create failed: %s", exc)
        sometimes(
            False,
            "subscribe-correctness: at least one timeline could create its table",
            {"prefix": prefix, "exc": str(exc)[:200]},
        )
        return 0

    producer_state: dict[str, Any] = {
        "attempted": 0,
        "committed_set": set(),
        "ambiguous_set": set(),
        "fault_attempts": 0,
    }
    consumer_state: dict[str, Any] = {
        "received_set": set(),
        "received_events": 0,
        "retracted_set": set(),
        "max_progress_ts": 0,
        "reconnects": 0,
        "fatal_err": None,
    }

    producer_stop = threading.Event()
    consumer_stop = threading.Event()

    t_producer = threading.Thread(
        name="subcorr-producer",
        target=_producer_loop,
        args=(table, producer_state, producer_stop),
        daemon=True,
    )
    t_consumer = threading.Thread(
        name="subcorr-consumer",
        target=_consumer_loop,
        args=(table, consumer_state, consumer_stop),
        daemon=True,
    )
    t_producer.start()
    t_consumer.start()

    # Run window.
    end_time = time.time() + RUNTIME_S
    while time.time() < end_time:
        if consumer_state.get("fatal_err"):
            LOG.error(
                "subscribe-correctness: consumer fatal error: %s",
                consumer_state["fatal_err"],
            )
            break
        time.sleep(2.0)
        with _state_lock:
            LOG.info(
                "[PROGRESS] subscribe-correctness: attempted=%d committed=%d received=%d "
                "progress_ts=%d reconnects=%d",
                producer_state["attempted"],
                len(producer_state["committed_set"]),
                len(consumer_state["received_set"]),
                consumer_state["max_progress_ts"],
                consumer_state["reconnects"],
            )

    # Stop the producer first, then drive the catch-up wait.
    producer_stop.set()
    t_producer.join(timeout=15.0)

    # Read the authoritative set AND the catch-up target timestamp in a
    # single read transaction, so they share one logical time.
    #
    # This closes a snapshot-skew race that an earlier AS-OF-mz_now()
    # attempt didn't fully fix.  `mz_now()` sampled on its own
    # connection can lag the producer's last committed row's timestamp,
    # so an authoritative read AS OF that (too-early) target missed a
    # row the consumer had already streamed — producing an exact
    # off-by-one (missed *or* spurious) with `reconnects == 0`, i.e. no
    # fault involved.  Reading `SELECT counter FROM table` and
    # `SELECT mz_now()` in the same transaction guarantees:
    #   * the transaction's read timestamp T is >= every committed row
    #     (strict-serializable read picks T at/after the latest write),
    #     so `authoritative_set` is the complete committed set; and
    #   * `target_ts == T`, so once the consumer's progress reaches T it
    #     has streamed exactly the rows in `authoritative_set` — no more
    #     (the producer has stopped, nothing commits after T), no fewer.
    # missed and spurious are then both structurally empty unless the
    # SUT genuinely lost or invented a row.
    authoritative_set: set[int] = set()
    target_ts = 0
    auth_ok = False
    try:
        with psycopg.connect(
            host=PGHOST,
            port=PGPORT,
            user=PGUSER,
            dbname=PGDATABASE,
            connect_timeout=PROBE_CONNECT_TIMEOUT_S,
        ) as conn:
            with conn.cursor() as cur:
                # autocommit defaults to off → these run in one txn.
                cur.execute(f"SELECT counter FROM {table}".encode())
                authoritative_set = {int(r[0]) for r in cur.fetchall()}
                cur.execute(b"SELECT mz_now()::text::bigint")
                row = cur.fetchone()
                target_ts = int(row[0]) if row is not None else 0
                conn.commit()
        auth_ok = True
    except Exception as exc:  # noqa: BLE001
        if not looks_like_fault(str(exc)):
            LOG.warning("subscribe-correctness: authoritative read failed: %s", exc)

    if not auth_ok:
        # Couldn't take the snapshot (fault window) — can't run the
        # safety assertion; bail with a `sometimes(False, ...)`.
        consumer_stop.set()
        t_consumer.join(timeout=5.0)
        sometimes(
            False,
            "subscribe-correctness: at least one timeline could read authoritative set",
            {"prefix": prefix},
        )
        return 0

    catchup_deadline = time.time() + CATCHUP_TIMEOUT_S
    while time.time() < catchup_deadline:
        with _state_lock:
            if consumer_state["max_progress_ts"] >= target_ts:
                break
            if consumer_state.get("fatal_err"):
                break
        time.sleep(0.5)

    with _state_lock:
        consumer_caught_up = consumer_state["max_progress_ts"] >= target_ts
        received_snapshot = set(consumer_state["received_set"])
        retracted_snapshot = set(consumer_state["retracted_set"])
        reconnects = consumer_state["reconnects"]
        fatal_err = consumer_state.get("fatal_err")
        progress_ts = consumer_state["max_progress_ts"]

    consumer_stop.set()
    t_consumer.join(timeout=10.0)

    # Diffs for the assertion blobs.
    missed = sorted(authoritative_set - received_snapshot)[:20]
    spurious = sorted(
        received_snapshot - authoritative_set - producer_state["ambiguous_set"]
    )[:20]

    LOG.info(
        "[SUMMARY] subscribe-correctness prefix=%s attempted=%d committed=%d "
        "authoritative=%d received=%d retracted=%d reconnects=%d progress_ts=%d "
        "target_ts=%d caught_up=%s missed=%d spurious=%d fatal=%s",
        prefix,
        producer_state["attempted"],
        len(producer_state["committed_set"]),
        len(authoritative_set),
        len(received_snapshot),
        len(retracted_snapshot),
        reconnects,
        progress_ts,
        target_ts,
        consumer_caught_up,
        len(missed),
        len(spurious),
        fatal_err is not None,
    )

    # Safety: the consumer thread must never terminate on a non-fault
    # exception.  `_consumer_loop` already classifies via
    # `looks_like_fault` and reconnects on fault-shape errors; any
    # other exception means SUBSCRIBE itself surfaced something
    # unexpected (e.g. a row-shape regression, a `pipeline mode
    # failed` ProgrammingError, a SQL error from AS OF reconnect).
    # Surface this directly rather than letting it slip through as
    # `consumer_caught_up = False` (which would skip the missed/
    # spurious always-checks and leave the bug invisible).
    always(
        fatal_err is None,
        "subscribe: consumer thread never terminates on a non-fault exception",
        {
            "prefix": prefix,
            "fatal_err": fatal_err,
            "reconnects": reconnects,
            "received_count": len(received_snapshot),
        },
    )

    # Safety: every committed row must appear in the stream once
    # the consumer has caught up.  Guarded on `consumer_caught_up`
    # because asserting against a still-catching-up consumer would
    # surface flaky misses that aren't real bugs.  The `sometimes`
    # for caught-up below makes sure we don't trivially-pass by
    # never catching up.
    if consumer_caught_up:
        always(
            len(missed) == 0,
            "subscribe: every committed row eventually appears in the SUBSCRIBE stream "
            "(no missed events after catch-up)",
            {
                "prefix": prefix,
                "missed_sample": missed,
                "authoritative_count": len(authoritative_set),
                "received_count": len(received_snapshot),
                "reconnects": reconnects,
                "progress_ts": progress_ts,
                "target_ts": target_ts,
            },
        )
        always(
            len(spurious) == 0,
            "subscribe: no row appears in the SUBSCRIBE stream that wasn't committed "
            "(no spurious events, ambiguous producer-failures excluded)",
            {
                "prefix": prefix,
                "spurious_sample": spurious,
                "authoritative_count": len(authoritative_set),
                "received_count": len(received_snapshot),
                "ambiguous_count": len(producer_state["ambiguous_set"]),
            },
        )
        # Producer only ever INSERTs.  Any retraction from the
        # stream means the SUT emitted a -1 diff for a row that was
        # never deleted — a serious frontier / consolidation bug.
        always(
            len(retracted_snapshot) == 0,
            "subscribe: stream never emits a retraction for a row the producer didn't delete",
            {
                "prefix": prefix,
                "retracted_sample": sorted(retracted_snapshot)[:20],
            },
        )

    # Liveness anchors.
    sometimes(
        consumer_caught_up,
        "subscribe-correctness: consumer caught up to target_ts within budget",
        {
            "max_progress_ts": progress_ts,
            "target_ts": target_ts,
            "catchup_timeout_s": CATCHUP_TIMEOUT_S,
        },
    )
    sometimes(
        len(producer_state["committed_set"]) >= 10,
        "subscribe-correctness: producer committed >= 10 rows in some timeline",
        {"committed": len(producer_state["committed_set"])},
    )
    sometimes(
        len(received_snapshot) >= 10,
        "subscribe-correctness: consumer received >= 10 distinct counters in some timeline",
        {"received": len(received_snapshot)},
    )
    sometimes(
        reconnects > 0,
        "subscribe-correctness: at least one fault-induced reconnect was exercised",
        {"reconnects": reconnects},
    )
    sometimes(
        producer_state["fault_attempts"] > 0,
        "subscribe-correctness: producer encountered a fault-shaped failure",
        {"fault_attempts": producer_state["fault_attempts"]},
    )

    # Tiered milestones — same shape as the other progress-aware
    # drivers so the triage view shows "did we get past N events
    # in any timeline" at a glance.
    for threshold in (1, 10, 100, 1000):
        sometimes(
            len(received_snapshot) >= threshold,
            f"subscribe-correctness: received_set size >= {threshold} in at least one timeline",
            {"threshold": threshold, "received": len(received_snapshot)},
        )

    # Best-effort cleanup so local-dev re-runs don't accumulate
    # tables.  No harm on retry failure.
    try:
        execute_retry(f"DROP TABLE IF EXISTS {table}")
    except Exception:  # noqa: BLE001
        pass

    return 0


if __name__ == "__main__":
    _ = (os, PGHOST, PGPORT, PGUSER, PGDATABASE)
    sys.exit(main())
