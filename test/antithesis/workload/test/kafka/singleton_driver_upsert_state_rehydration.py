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
  2. Wait for `offset_committed` to reach the highest produced offset.
     The global fault-orchestrator drives quiet/active windows on its
     own cadence; the per-cycle catchup timeout is sized to span at
     least one quiet window so settle has somewhere to land.
  3. SELECT every tracked key's current source state and assert it matches
     `expected_state` via `always("upsert: rehydrated state equals
     local model", ...)`. Across-cycle stability is exactly what
     rehydration correctness is.

The driver also records one `sometimes` anchor confirming that at least
two assertion-bearing cycles ran (without this, the safety check could be
vacuously satisfied by a single early settle).

Distinct prefix per timeline keeps multiple parallel timelines independent.
"""

from __future__ import annotations

import sys
import time

import helper_logging
import helper_random
from helper_kafka import FLUSH_TIMEOUT_S, make_producer
from helper_pg import query_one_retry
from helper_source_stats import wait_for_catchup
from helper_upsert_source import (
    SOURCE_UPSERT_TEXT,
    TOPIC_UPSERT_TEXT,
    ensure_upsert_text_source,
)

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.upsert_state_rehydration")

# Long-running knobs — this driver owns its timeline alongside parallel
# drivers, so the per-cycle budget is generous and the cycle count high
# enough that a node-termination fault has a chance to land between cycles.
CYCLE_COUNT = 8
PRODUCES_PER_CYCLE = 30
DISTINCT_KEYS = 6
DISTINCT_VALUES = 12

# Tombstone fraction is swarmed once per driver invocation (see main()) so
# different timelines exercise different live/dead mixes — heavy-tombstone
# runs stress the upsert-state-remove rehydration path, mostly-live runs
# stress value-overwrite rehydration. The choice is fixed for the whole
# driver lifetime so cross-cycle stability of `expected` still tests
# rehydration, not just per-cycle convergence.
TOMBSTONE_PROB_RANGE = (0.05, 0.50)

# Sized to span at least one MAX_OFF window from the global fault-
# orchestrator (default 40s) and survive a clusterd restart inside it;
# rehydration after a kill is the whole point of this driver.
CATCHUP_TIMEOUT_S = 180.0
INTER_CYCLE_SLEEP_S = 2.0


def _select_value_for_key(key: str) -> tuple[bool, str | None]:
    """Duplicate of `_select_value_for_key` in `parallel_driver_upsert_latest_value.py`.
    Kept inline to avoid expanding helper surface for one shared private function."""
    # See helper_pg.query_retry for why real_time_recency is required here.
    row = query_one_retry(
        f"SELECT count(*)::bigint, max(text) FROM {SOURCE_UPSERT_TEXT} WHERE key = %s",
        (key,),
        real_time_recency=True,
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


def _run_cycle(
    producer,
    tracker,
    expected: dict[str, str | None],
    cycle_idx: int,
    tombstone_prob: float,
) -> bool:
    """Produce one batch, settle, and assert state for every tracked key.

    Returns True if assertions ran (cycle settled), False if we bailed early.
    """
    keys = [f"reh-k{i}" for i in range(DISTINCT_KEYS)]
    for _ in range(PRODUCES_PER_CYCLE):
        key = helper_random.random_choice(keys)
        if helper_random.random_bool(tombstone_prob):
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

    pending = producer.flush(timeout=FLUSH_TIMEOUT_S)
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

    # The global fault-orchestrator drives quiet windows; this catchup
    # timeout is sized to span one and survive a clusterd kill in it.
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
    # Swarm once per invocation, fixed for the run so cross-cycle stability
    # of `expected` keeps testing rehydration rather than per-cycle drift.
    tombstone_prob = helper_random.random_float(*TOMBSTONE_PROB_RANGE)
    LOG.info(
        "rehydration driver starting; %d cycles planned tombstone_prob=%.3f",
        CYCLE_COUNT,
        tombstone_prob,
    )

    producer, tracker = make_producer(client_id="antithesis-rehydration")
    expected: dict[str, str | None] = {}

    cycles_run = 0

    for cycle_idx in range(CYCLE_COUNT):
        if _run_cycle(producer, tracker, expected, cycle_idx, tombstone_prob):
            cycles_run += 1
        time.sleep(INTER_CYCLE_SLEEP_S)

    # The "did this run actually span a clusterd restart" anchor is
    # deliberately not in this driver — see the module docstring. The
    # `cycles_run >= 2` check below is the rehydration-coverage anchor:
    # without two settle-then-read cycles, the safety assertions could
    # be vacuously satisfied by a single early settle.
    sometimes(
        cycles_run >= 2,
        "upsert: rehydration driver ran 2+ assertion cycles",
        {"cycles_run": cycles_run, "cycles_planned": CYCLE_COUNT},
    )

    LOG.info("rehydration driver done; %d/%d cycles ran", cycles_run, CYCLE_COUNT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
