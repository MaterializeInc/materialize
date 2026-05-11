#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for property `upsert-key-reflects-latest-value`.

For each key produced to a Kafka UPSERT-envelope source, after a quiet period
that lets Materialize catch up, the source's row for that key must reflect the
last value produced — or be absent if the last message was a tombstone.

Each invocation:
  1. Ensures the upsert source exists (idempotent CREATE ... IF NOT EXISTS).
  2. Picks a per-invocation key prefix so concurrent driver instances don't
     interfere with each other's expected-state model.
  3. Produces a deterministic mix of upserts and tombstones, tracking the
     local "what should the source say" model.
  4. Requests an Antithesis quiet period and waits for offset_committed to
     reach the highest produced offset.
  5. For every tracked key, asserts that what's in the source matches the
     local model. Live keys use one assertion message, tombstoned keys use
     another, so triage can distinguish the two failure modes.

This is a `parallel_driver_` — Antithesis runs many concurrent instances and
each one assigns itself a fresh prefix from deterministic randomness, so
multiple drivers exercise the source without colliding.
"""

from __future__ import annotations

import logging
import sys

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
LOG = logging.getLogger("driver.upsert_latest_value")

# Knobs. Kept small per-invocation because Antithesis launches the driver many
# times; total coverage comes from re-invocations, not from one huge run.
PRODUCES_PER_INVOCATION = 40
DISTINCT_KEYS = 8  # small key space so we re-write the same key often
DISTINCT_VALUES = 16
TOMBSTONE_PROB = 0.15

QUIET_PERIOD_S = 20
CATCHUP_TIMEOUT_S = 60.0


def _produce(producer, tracker, topic: str, key: str, value: str | None) -> None:
    """Encode value=None as a Kafka tombstone (null payload)."""
    payload = None if value is None else value.encode("utf-8")
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=payload,
        on_delivery=tracker.callback,
    )


def _select_value_for_key(key: str) -> tuple[bool, str | None]:
    """Return (found, value) for the single source row matching `key`.

    Returns (False, None) when no row exists (the tombstone case for an
    UPSERT source). Returns (True, value) when exactly one row exists.
    Raises if more than one row exists — that would mean the source is
    multi-rowed per key and violates the UPSERT contract itself, which is
    out of scope for this property and should be caught by
    `kafka-source-no-data-duplication`.
    """
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
            f"upsert source has {count} rows for key {key!r}; this driver assumes "
            "the per-key uniqueness property holds"
        )
    return True, value


def main() -> int:
    ensure_upsert_text_source()

    # Per-invocation prefix isolates this driver's keys from other concurrent
    # drivers and from previous invocations of this same driver.
    prefix = f"p{helper_random.random_u64():016x}"
    LOG.info("driver starting; prefix=%s", prefix)

    producer, tracker = make_producer(client_id=f"antithesis-{prefix}")

    # Local "what should the source say" model for this invocation's keys.
    # Value of None means "the last message was a tombstone".
    expected: dict[str, str | None] = {}

    keys = [f"{prefix}-k{i}" for i in range(DISTINCT_KEYS)]
    for _ in range(PRODUCES_PER_INVOCATION):
        key = helper_random.random_choice(keys)
        if helper_random.random_bool(TOMBSTONE_PROB):
            _produce(producer, tracker, TOPIC_UPSERT_TEXT, key, None)
            expected[key] = None
        else:
            value = f"v{helper_random.random_int(0, DISTINCT_VALUES - 1):04d}"
            _produce(producer, tracker, TOPIC_UPSERT_TEXT, key, value)
            expected[key] = value
        producer.poll(0)

    # Flush all pending deliveries. We poll callbacks while flushing so the
    # tracker reflects the true max produced offset.
    pending = producer.flush(timeout=30)
    if pending > 0 or tracker.last_error is not None:
        # Under sustained fault injection we cannot prove which of the just-
        # produced messages Kafka actually accepted, so `expected` may name
        # values the source never sees. Bail out before running safety
        # assertions — fault-induced delivery loss is not what this property
        # is testing. The catchup `sometimes()` is also skipped because we
        # have no trustworthy target offset.
        LOG.info(
            "skipping assertions: producer.flush pending=%d last_error=%s",
            pending,
            tracker.last_error,
        )
        return 0

    max_produced = tracker.topic_max_offset(TOPIC_UPSERT_TEXT)
    if max_produced < 0:
        LOG.info("no messages confirmed delivered this invocation; exiting cleanly")
        return 0

    # Now ask Antithesis to pause faults and wait for Materialize to catch up.
    request_quiet_period(QUIET_PERIOD_S)
    caught_up = wait_for_catchup(
        SOURCE_UPSERT_TEXT, max_produced, timeout_s=CATCHUP_TIMEOUT_S
    )

    # Liveness signal: at least one invocation should reach catchup. If this
    # never fires across an entire run, the safety assertions below would be
    # vacuous and the run is uninteresting.
    sometimes(
        caught_up,
        "upsert: source caught up to produced offsets after quiet period",
        {"source": SOURCE_UPSERT_TEXT, "target_offset": max_produced},
    )

    if not caught_up:
        # Don't run the per-key safety assertions on stale data — that would
        # blame the property for a slow catchup that's a separate concern.
        LOG.info("catchup did not complete in budget; skipping per-key assertions")
        return 0

    # Per-key safety assertions. Two distinct messages so triage reports tell
    # us *which* invariant broke: a value mismatch or a tombstone resurrection.
    for key, want in expected.items():
        found, observed = _select_value_for_key(key)

        if want is None:
            # The last produced message for this key was a tombstone; the
            # source must not contain a row for it.
            always(
                not found,
                "upsert: tombstoned key has no row in source",
                {
                    "source": SOURCE_UPSERT_TEXT,
                    "key": key,
                    "observed_value": observed,
                },
            )
        else:
            # Live key: there must be exactly one row, with the latest value.
            always(
                found and observed == want,
                "upsert: SELECT for key matches latest produced value",
                {
                    "source": SOURCE_UPSERT_TEXT,
                    "key": key,
                    "expected_value": want,
                    "observed_present": found,
                    "observed_value": observed,
                },
            )

    LOG.info("driver done; asserted on %d keys", len(expected))
    return 0


if __name__ == "__main__":
    sys.exit(main())
