#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `upsert-state-rehydrates-correctly`.

After a clusterd restart, the rehydrated upsert state — observed via
`SELECT * FROM source` — must equal the state at the most recent durable
timestamp before the restart, for every key produced so far.

Implementation strategy: a `singleton_driver_` runs exactly once per
timeline and lives long enough to span multiple produce/settle/assert
cycles. Local memory holds the authoritative "what the source should say"
model across cycles. If Antithesis kills clusterd between two cycles, the
next cycle's `SELECT` is effectively a rehydration check — and because the
local model is unchanged across the restart, any divergence in the source
output is exactly the property's failure mode.

Each cycle:
  1. Produce a batch of (key, value) and (key, null) messages, updating the
     in-memory `expected_state` model.
  2. Request a quiet period and wait for `offset_committed` to reach the
     highest produced offset.
  3. SELECT every tracked key's current source state and assert it matches
     `expected_state` via `always("upsert: rehydrated state equals
     local model", ...)`. Across-cycle stability is exactly what
     rehydration correctness is.

The driver also records one `sometimes` anchor confirming that at least
two assertion-bearing cycles ran (without this, the safety check could be
vacuously satisfied by a single early settle), and a second anchor
confirming clusterd was observed unavailable between cycles (best-effort
proxy for "restart happened" — the helper_pg retry budget makes connect
errors very rare under normal operation).

Distinct prefix per timeline keeps multiple parallel timelines independent.
"""

from __future__ import annotations

import logging
import sys
import time

import helper_random
from helper_kafka import make_producer
from helper_pg import query_one_retry
from helper_quiet import request_quiet_period
from helper_source_stats import wait_for_catchup
from helper_upsert_source import (
    SOURCE_UPSERT_TEXT,
    TOPIC_UPSERT_TEXT,
    ensure_upsert_text_source,
)

from antithesis.assertions import always, sometimes

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
LOG = logging.getLogger("driver.upsert_state_rehydration")

# Long-running knobs — this driver owns its timeline alongside parallel
# drivers, so the per-cycle budget is generous and the cycle count high
# enough that a node-termination fault has a chance to land between cycles.
CYCLE_COUNT = 8
PRODUCES_PER_CYCLE = 30
DISTINCT_KEYS = 6
DISTINCT_VALUES = 12
TOMBSTONE_PROB = 0.20

QUIET_PERIOD_S = 25
CATCHUP_TIMEOUT_S = 120.0
INTER_CYCLE_SLEEP_S = 2.0


def _select_value_for_key(key: str) -> tuple[bool, str | None]:
    """Duplicate of `_select_value_for_key` in `parallel_driver_upsert_latest_value.py`.
    Kept inline to avoid expanding helper surface for one shared private function."""
    row = query_one_retry(
        f"SELECT count(*)::bigint, max(text) FROM {SOURCE_UPSERT_TEXT} WHERE key = %s",
        (key,),
    )
    if row is None:
        return False, None
    count, value = row
    if count == 0:
        return False, None
    if count != 1:
        raise RuntimeError(
            f"upsert source has {count} rows for key {key!r}; this driver "
            "assumes the per-key uniqueness property holds (see "
            "`upsert-key-reflects-latest-value` and "
            "`kafka-source-no-data-duplication`)"
        )
    return True, value


def _saw_clusterd_unavailable() -> bool:
    """Best-effort probe: does `mz_internal.mz_cluster_replica_statuses` show
    any `antithesis_cluster` replica with `status != 'online'` right now?
    The status column reports `online` or `offline`. Catching `offline`
    in a snapshot doesn't *prove* a restart happened (we may have missed
    a transient flap entirely), but it's a noisy yes-signal that something
    disturbed the cluster during the cycle.
    """
    try:
        row = query_one_retry("""
            SELECT EXISTS (
                SELECT 1
                FROM mz_internal.mz_cluster_replica_statuses s
                JOIN mz_cluster_replicas r ON r.id = s.replica_id
                JOIN mz_clusters c ON c.id = r.cluster_id
                WHERE c.name = 'antithesis_cluster' AND s.status != 'online'
            )
            """)
    except Exception:  # noqa: BLE001
        return False
    return bool(row and row[0])


def _run_cycle(
    producer, tracker, expected: dict[str, str | None], cycle_idx: int
) -> bool:
    """Produce one batch, settle, and assert state for every tracked key.

    Returns True if assertions ran (cycle settled), False if we bailed early.
    """
    keys = [f"reh-k{i}" for i in range(DISTINCT_KEYS)]
    for _ in range(PRODUCES_PER_CYCLE):
        key = helper_random.random_choice(keys)
        if helper_random.random_bool(TOMBSTONE_PROB):
            producer.produce(
                topic=TOPIC_UPSERT_TEXT,
                key=key.encode("utf-8"),
                value=None,
                on_delivery=tracker.callback,
            )
            expected[key] = None
        else:
            value = f"reh-v{cycle_idx:02d}-{helper_random.random_int(0, DISTINCT_VALUES - 1):04d}"
            producer.produce(
                topic=TOPIC_UPSERT_TEXT,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=tracker.callback,
            )
            expected[key] = value
        producer.poll(0)

    pending = producer.flush(timeout=30)
    if pending > 0 or tracker.last_error is not None:
        LOG.info(
            "cycle %d: skipping assertions; flush pending=%d last_error=%s",
            cycle_idx,
            pending,
            tracker.last_error,
        )
        return False

    max_produced = tracker.topic_max_offset(TOPIC_UPSERT_TEXT)
    if max_produced < 0:
        LOG.info("cycle %d: no messages confirmed delivered; skipping", cycle_idx)
        return False

    request_quiet_period(QUIET_PERIOD_S)
    caught_up = wait_for_catchup(
        SOURCE_UPSERT_TEXT, max_produced, timeout_s=CATCHUP_TIMEOUT_S
    )
    if not caught_up:
        LOG.info(
            "cycle %d: catchup did not complete in budget; skipping asserts", cycle_idx
        )
        return False

    # Per-key assertion. The cross-cycle stability of `expected` is what
    # makes this a rehydration check: if a clusterd restart happened
    # between this cycle and the previous, the source has been rebuilt
    # from feedback and must agree with `expected` again.
    for key, want in expected.items():
        found, observed = _select_value_for_key(key)
        if want is None:
            always(
                not found,
                "upsert: rehydrated state matches local model (tombstoned key)",
                {
                    "source": SOURCE_UPSERT_TEXT,
                    "key": key,
                    "cycle": cycle_idx,
                    "observed_value": observed,
                },
            )
        else:
            always(
                found and observed == want,
                "upsert: rehydrated state matches local model (live key)",
                {
                    "source": SOURCE_UPSERT_TEXT,
                    "key": key,
                    "cycle": cycle_idx,
                    "expected_value": want,
                    "observed_present": found,
                    "observed_value": observed,
                },
            )
    return True


def main() -> int:
    ensure_upsert_text_source()
    LOG.info("rehydration driver starting; %d cycles planned", CYCLE_COUNT)

    producer, tracker = make_producer(client_id="antithesis-rehydration")
    expected: dict[str, str | None] = {}

    cycles_run = 0
    saw_replica_unavailable = False

    for cycle_idx in range(CYCLE_COUNT):
        if _run_cycle(producer, tracker, expected, cycle_idx):
            cycles_run += 1
        if _saw_clusterd_unavailable():
            saw_replica_unavailable = True
        time.sleep(INTER_CYCLE_SLEEP_S)

    sometimes(
        cycles_run >= 2,
        "upsert: rehydration driver ran 2+ assertion cycles",
        {"cycles_run": cycles_run, "cycles_planned": CYCLE_COUNT},
    )
    sometimes(
        saw_replica_unavailable,
        "upsert: rehydration driver observed clusterd replica non-online",
        {"cycles_run": cycles_run},
    )

    LOG.info("rehydration driver done; %d/%d cycles ran", cycles_run, CYCLE_COUNT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
