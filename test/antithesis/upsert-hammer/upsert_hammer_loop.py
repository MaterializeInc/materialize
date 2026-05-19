#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Standalone continuous Kafka producer for INC-936 upsert stress testing.

This is NOT a Test Composer driver. It runs as its own long-lived
container (`upsert-hammer-{i}` in mzcompose.py) outside the Antithesis
workload service. The Antithesis engagement team's plan for hunting
INC-936 calls for "a simple script that writes to / deletes a limited
population of keys forever forever, and containerize that. Parallelize
this (either via threading, or by creating multiple replicas of the
workload container in the compose file)." This file is the script;
mzcompose.py supplies multiple replicas.

Each replica:
  * Ensures the stress topic exists with NUM_PARTITIONS partitions.
  * Loops forever, producing upserts and tombstones against a shared
    ~100-key space (sized to give each timely worker meaningful
    consolidation traffic while still piling concurrent retract+insert
    events on the same per-(key, ts) windows). Different replicas hit
    overlapping keys, so the upsert operator's consolidation window —
    the codepath where INC-936's "invalid upsert state" panic surfaces
    — sees concurrent retracts and inserts for the same key.
  * Retries Kafka connection / delivery errors silently. Faults are
    expected; the load generator must outlive any individual fault
    window, not exit on it.

The panic itself is a clusterd-side soft-assertion and Antithesis
catches it via its built-in process-termination property. No assertions
fire from this script.

Exit only on SIGTERM (compose down). Crashes propagate so a workload-
side bug is visible rather than hidden by an inner restart loop.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time

# Helpers live alongside this script in /opt/upsert-hammer/ via the
# pre-image copy step in the hammer image's mzbuild.yml. Constants
# come from `helper_upsert_stress_const` (a no-deps module split out
# of `helper_upsert_stress` exactly so the hammer image does not have
# to drag psycopg / the Materialize-pgwire surface in to read three
# integers).
from helper_kafka import ensure_topic, make_producer
from helper_random import random_bool, random_choice, random_float, random_int
from helper_upsert_stress_const import NUM_PARTITIONS, TOPIC_UPSERT_STRESS

LOG = logging.getLogger("antithesis.upsert_hammer")

# Identifier for log grep / per-replica debugging. Set by the compose
# service definition; defaults to "0" so a stray local invocation still
# logs something useful.
INSTANCE_ID = os.environ.get("HAMMER_INSTANCE_ID", "0")

# Shared key space. ~100 keys × 8 partitions ≈ 12 keys per partition,
# which with CLUSTERD_WORKERS=4 gives each timely worker ~25 keys to
# consolidate — enough breadth that the per-worker consolidation path
# gets meaningful coverage, while still small enough that 4 concurrent
# hammers writing to the same key space pile retract+insert events
# onto the same per-(key, ts) consolidation window. Smaller (e.g. 16)
# starved a couple of workers of any traffic at all.
DISTINCT_KEYS = 100
DISTINCT_VALUES = 64

# Per-process tombstone fraction, drawn once at startup from a wide
# range. Different replicas naturally explore different mixes — heavy-
# tombstone replicas stress the retract path, mostly-live replicas
# stress the overwrite path.
TOMBSTONE_PROB_RANGE = (0.10, 0.60)

# How often to log a heartbeat. Triage of a long-running stress
# container needs progress evidence; the log line carries the produced
# count and current max offset so a stalled hammer is easy to spot.
HEARTBEAT_INTERVAL_S = 30.0

# Backoff used between top-level retries when the producer itself
# explodes. Inner librdkafka retries already happen under the
# delivery.timeout.ms umbrella set in helper_kafka.make_producer.
TOPLEVEL_BACKOFF_S = 5.0


_shutdown_requested = False


def _request_shutdown(signum: int, _frame: object) -> None:
    """SIGTERM/SIGINT handler. Flag the loop to drain on the next iteration."""
    global _shutdown_requested
    _shutdown_requested = True
    LOG.info("hammer[%s]: signal %d received, draining", INSTANCE_ID, signum)


def _setup_logging() -> None:
    """Plain stderr logging — same format used by every other driver, so
    Antithesis triage UI groups log lines consistently across containers."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


def _run_one_session(tombstone_prob: float) -> None:
    """Open one producer, hammer until shutdown or an unrecoverable error.

    Wrapped in retry-on-exception by `main()` so a producer that gets
    permanently broken by a long fault window is replaced rather than
    bringing the container down.
    """
    producer, tracker = make_producer(
        client_id=f"antithesis-hammer-{INSTANCE_ID}"
    )
    keys = [f"hk{i:02d}" for i in range(DISTINCT_KEYS)]

    produced = 0
    tombstones = 0
    last_heartbeat = time.monotonic()

    while not _shutdown_requested:
        key = random_choice(keys)
        if random_bool(tombstone_prob):
            producer.produce(
                topic=TOPIC_UPSERT_STRESS,
                key=key.encode("utf-8"),
                value=None,
                on_delivery=tracker.callback,
            )
            tombstones += 1
        else:
            value = f"v{random_int(0, DISTINCT_VALUES - 1):04d}"
            producer.produce(
                topic=TOPIC_UPSERT_STRESS,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=tracker.callback,
            )
        produced += 1

        # Drain delivery callbacks without blocking. Skipping this lets
        # librdkafka's queue grow unbounded; calling it every produce
        # keeps memory bounded and lets the tracker reflect reality.
        producer.poll(0)

        now = time.monotonic()
        if now - last_heartbeat >= HEARTBEAT_INTERVAL_S:
            max_offset = tracker.topic_max_offset(TOPIC_UPSERT_STRESS)
            LOG.info(
                "hammer[%s]: heartbeat produced=%d tombstones=%d max_offset=%d last_error=%s",
                INSTANCE_ID,
                produced,
                tombstones,
                max_offset,
                tracker.last_error,
            )
            last_heartbeat = now

    # Drain on shutdown so the broker has the final state. Best-effort:
    # if the broker is gone, flush will time out and we exit anyway.
    LOG.info("hammer[%s]: draining producer before exit", INSTANCE_ID)
    producer.flush(timeout=30)


def main() -> int:
    _setup_logging()
    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    tombstone_prob = random_float(*TOMBSTONE_PROB_RANGE)
    LOG.info(
        "hammer[%s] starting: keys=%d values=%d partitions=%d tombstone_prob=%.3f",
        INSTANCE_ID,
        DISTINCT_KEYS,
        DISTINCT_VALUES,
        NUM_PARTITIONS,
        tombstone_prob,
    )

    # Ensure the topic exists before any producer is built. The
    # Materialize-side source-create driver (`first_upsert_stress_setup`)
    # also calls this; both paths are idempotent. Doing it here means
    # the hammer is independent of Test Composer ordering — under fault
    # injection the workload container's setup phase may be arbitrarily
    # delayed, but the hammer can begin producing as soon as Kafka is
    # reachable.
    while not _shutdown_requested:
        try:
            ensure_topic(TOPIC_UPSERT_STRESS, num_partitions=NUM_PARTITIONS)
            break
        except Exception as exc:  # noqa: BLE001
            LOG.info(
                "hammer[%s]: ensure_topic failed (%s); sleeping %.1fs and retrying",
                INSTANCE_ID,
                exc,
                TOPLEVEL_BACKOFF_S,
            )
            time.sleep(TOPLEVEL_BACKOFF_S)

    while not _shutdown_requested:
        try:
            _run_one_session(tombstone_prob)
        except Exception as exc:  # noqa: BLE001
            # The hammer's job is to keep load on the SUT. Any inner
            # crash (broker meltdown, librdkafka assert, lost producer
            # state) just triggers a fresh producer on the next loop —
            # we don't want to crash the load generator over a fault we
            # were already expecting.
            LOG.info(
                "hammer[%s]: session crashed (%s); restarting after %.1fs",
                INSTANCE_ID,
                exc,
                TOPLEVEL_BACKOFF_S,
            )
            time.sleep(TOPLEVEL_BACKOFF_S)

    LOG.info("hammer[%s] exiting cleanly", INSTANCE_ID)
    return 0


if __name__ == "__main__":
    sys.exit(main())
