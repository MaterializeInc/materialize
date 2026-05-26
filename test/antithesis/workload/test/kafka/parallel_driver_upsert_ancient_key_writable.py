#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for property `upsert-ancient-key-writable`.

The sibling driver `parallel_driver_upsert_latest_value.py` writes keys
under a fresh per-invocation prefix (`p<u64hex>-k{0..7}`) and only ever
revisits its own keys *within* an invocation. Each key it writes is
abandoned at end-of-invocation — the row persists in the upsert source,
but no future invocation of that driver ever touches it again.

This driver exercises the question: *if I write into a key that has
been resident in the upsert source for a long time, does the source
still reflect the write?* A failure here looks like an upsert state-
rehydration bug surfacing only after enough time / fault-injection has
elapsed: the source remembers the key from when it was originally
written, but a fresh write produces no observable change.

To exercise that property without interfering with the sibling driver
(whose `always("upsert: SELECT for key matches latest produced value",
…)` assumes nothing concurrent is writing to its prefixed keys), this
driver owns its own key ring `ancient-k{0..N-1}`. Each invocation picks
K of them at random, snapshots their current values, produces fresh
values, waits for catchup, and asserts that the source's view of each
key changed. Between two invocations that happen to target the same
ring slot, a lot of wall time and fault injection may elapse — the
longer the run, the more genuinely "ancient" the snapshotted value is
at the time of the next overwrite.

Assertion shape, per targeted key:
  - `always`: post-catchup, the source's view of the key is NOT the
    old value we snapshotted. The observed value can be (a) our new
    value, (b) some other invocation's cross-overwrite, or (c) absent
    (only possible if some concurrent peer tombstoned the key — this
    driver never tombstones). The one outcome we must never see is
    "row still present with the exact old value we just overwrote," which
    means the write was silently dropped while no peer interfered.
  - `sometimes`: the observed value equals OUR specific new value.
    Liveness — confirms we sometimes win the race against any concurrent
    peers and fully exercise the write+read pipeline through to query.

Initial state for each key in the ring: until the first invocation
writes to ring slot `k`, no row exists for it. We tolerate that as
old_value=None and only assert post-catchup that the row now exists.
The first invocation to write each key seeds the property for later
ones.
"""

from __future__ import annotations

import sys

import helper_logging
import helper_random
from antithesis.assertions import always, sometimes
from helper_kafka import FLUSH_TIMEOUT_S, make_producer
from helper_pg import query_retry
from helper_source_stats import wait_for_catchup
from helper_upsert_source import (
    SOURCE_UPSERT_TEXT,
    TOPIC_UPSERT_TEXT,
    ensure_upsert_text_source,
)

LOG = helper_logging.setup_logging("driver.upsert_ancient_key_writable")

# Fixed key ring owned exclusively by this driver. No other driver writes
# keys matching this prefix, so the property's assertions are race-free
# against the rest of the workload. The size sets the upper bound on how
# many distinct "ancient" rows accumulate — small enough that revisits
# happen often, large enough that two concurrent invocations of this
# driver usually don't pick the same ring slot.
ANCIENT_KEY_PREFIX = "ancient-k"
ANCIENT_KEY_RING_SIZE = 32

# Number of ancient keys to target per invocation. Small on purpose — the
# Test Composer launches this driver many times, so coverage comes from
# many short invocations rather than one big one.
ANCIENT_KEYS_PER_INVOCATION = 5

# Sized to span at least one MAX_OFF window from the global fault-
# orchestrator (default 40s) plus the time the upsert source needs to
# advance offset_committed past our produces.
CATCHUP_TIMEOUT_S = 90.0


def _produce(producer, tracker, topic: str, key: str, value: str) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=value.encode("utf-8"),
        on_delivery=tracker.callback,
    )


def _snapshot_current_value(key: str) -> tuple[bool, str | None]:
    """Return (found, value) for `key` at a real-time-recency timestamp.
    UPSERT contract: at most one row per key.
    """
    rows = query_retry(
        f"SELECT count(*)::bigint, max(text) FROM {SOURCE_UPSERT_TEXT} WHERE key = %s",
        (key,),
        real_time_recency=True,
    )
    if not rows:
        return False, None
    count, value = rows[0]
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

    # Per-invocation prefix is only used as a nonce-namespace for our
    # written values, so triage can attribute a `cross-<prefix>-<nonce>`
    # value back to a specific invocation. The KEYS we target come from
    # the shared ring, not from any per-invocation prefix.
    prefix = f"p{helper_random.random_u64():016x}"
    LOG.info("driver starting; prefix=%s", prefix)

    # Pick K distinct ring slots at random. The helper module doesn't
    # expose `random_sample`, so we do reservoir-style sampling via
    # repeated random_choice with removal.
    candidate_pool = list(range(ANCIENT_KEY_RING_SIZE))
    slot_indices: list[int] = []
    for _ in range(min(ANCIENT_KEYS_PER_INVOCATION, len(candidate_pool))):
        pick = helper_random.random_choice(candidate_pool)
        candidate_pool.remove(pick)
        slot_indices.append(pick)
    keys = [f"{ANCIENT_KEY_PREFIX}{i}" for i in slot_indices]

    # Snapshot each key's current value BEFORE producing. This is the
    # `old_value` half of the assertion.
    snapshots: list[tuple[str, bool, str | None]] = []
    for key in keys:
        found, value = _snapshot_current_value(key)
        snapshots.append((key, found, value))

    sometimes(
        any(found for _, found, _ in snapshots),
        "upsert: at least one ancient-ring key has a prior value to overwrite",
        {"keys": [k for k, _, _ in snapshots]},
    )

    producer, tracker = make_producer(client_id=f"antithesis-ancient-{prefix}")

    # Produce a fresh value to each ring slot. The value embeds our
    # prefix + a per-write nonce so triage can distinguish "our write
    # reached the source" from "some concurrent invocation wrote".
    new_values: list[tuple[str, bool, str | None, str]] = []
    for key, found, old_value in snapshots:
        nonce = helper_random.random_u64()
        new_value = f"cross-{prefix}-{nonce:016x}"
        new_values.append((key, found, old_value, new_value))
        _produce(producer, tracker, TOPIC_UPSERT_TEXT, key, new_value)
        producer.poll(0)

    pending = producer.flush(timeout=FLUSH_TIMEOUT_S)
    if pending > 0 or tracker.last_error is not None:
        # Under sustained fault injection we can't prove which produces
        # Kafka accepted. Bail before asserting — "writes that landed got
        # reflected" doesn't apply to writes that didn't land.
        LOG.info(
            "skipping assertions: producer.flush pending=%d last_error=%s",
            pending,
            tracker.last_error,
        )
        return 0

    max_produced = tracker.topic_max_offset(TOPIC_UPSERT_TEXT)
    if max_produced < 0:
        LOG.info("no produces confirmed; exiting cleanly")
        return 0

    caught_up = wait_for_catchup(
        SOURCE_UPSERT_TEXT, max_produced, timeout_s=CATCHUP_TIMEOUT_S
    )
    sometimes(
        caught_up,
        "upsert: source caught up after cross-invocation produces within catchup budget",
        {"source": SOURCE_UPSERT_TEXT, "target_offset": max_produced},
    )
    if not caught_up:
        LOG.info("catchup did not complete in budget; skipping assertions")
        return 0

    my_value_observed = 0

    for key, was_found, old_value, new_value in new_values:
        found_after, observed = _snapshot_current_value(key)

        if was_found:
            # Safety property: writing into a key that had a prior value
            # must change the source's view. Accepted outcomes:
            #   * observed == new_value (we won the race)
            #   * observed == <peer's cross-overwrite> (peer won)
            #   * not found_after (peer tombstoned — this driver never
            #     does so it'd have to be a future variant or an
            #     external producer, but the shape is legitimate)
            # The one outcome we must NEVER see is `found_after and
            # observed == old_value`, which means our write was silently
            # lost while no one else touched the key.
            violation = found_after and observed == old_value
            always(
                not violation,
                "upsert: write to ancient key changes its reflected value",
                {
                    "source": SOURCE_UPSERT_TEXT,
                    "key": key,
                    "old_value": old_value,
                    "new_value_attempted": new_value,
                    "observed_present": found_after,
                    "observed_value": observed,
                },
            )
        else:
            # First-touch path: the ring slot was empty before. After
            # producing a non-null value, the source must contain a row.
            # The row's value is either ours or a peer's cross-overwrite;
            # both are valid. The one outcome we must never see is
            # `not found_after` — meaning a non-tombstone write to an
            # empty key produced no row.
            always(
                found_after,
                "upsert: write to previously-empty ancient key creates a row",
                {
                    "source": SOURCE_UPSERT_TEXT,
                    "key": key,
                    "new_value_attempted": new_value,
                    "observed_present": found_after,
                    "observed_value": observed,
                },
            )

        if found_after and observed == new_value:
            my_value_observed += 1

    sometimes(
        my_value_observed > 0,
        "upsert: cross-invocation driver's own write reached the source",
        {
            "my_value_observed": my_value_observed,
            "ancient_keys_targeted": len(new_values),
        },
    )

    LOG.info(
        "driver done; ancient_keys=%d my_value_observed=%d",
        len(new_values),
        my_value_observed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
