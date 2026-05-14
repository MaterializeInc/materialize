#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for the inverse-pair NONE-envelope properties:
  - `kafka-source-no-data-loss` — every produced (partition, offset) is visible
  - `kafka-source-no-data-duplication` — no (partition, offset) appears twice

The two run on the same dataflow because they are the symmetric failure modes
of the same contract: one says "no row gone missing," the other says "no row
duplicated." Settling once and asserting both halves catches both bugs from
the same produce pass.

Each invocation:
  1. Ensures the NONE-envelope source exists.
  2. Picks a per-invocation prefix so concurrent driver instances scope to
     disjoint payloads. Every produced message has a `<prefix>:` prefix so the
     workload can filter the source down to its own rows when asserting.
  3. Produces N distinct payloads, recording the broker-assigned `(partition,
     offset)` for each via the delivery callback.
  4. Waits for `offset_committed` to reach the highest produced offset.
     The global fault-orchestrator service drives quiet/active windows
     on its own cadence; the catchup timeout is sized to span at least
     one quiet window so the source can advance during it.
  5. Runs two `assert_always` checks:
       - "kafka source: no duplicate (partition, offset)" — `GROUP BY 1, 2 HAVING COUNT(*) > 1` is empty
       - "kafka source: every produced payload is visible exactly once" —
         fires per produced payload; payload, presence, and observed count
         go into `details` so triage can localize which payloads went missing
         or duplicated
  6. Records one `assert_sometimes` liveness anchor confirming the safety
     checks ran against settled data.

This is a `parallel_driver_` — many concurrent instances exercise the source
without colliding because each invocation owns its prefix range.
"""

from __future__ import annotations

import logging
import sys

import helper_random
from helper_kafka import make_producer
from helper_none_source import (
    SOURCE_NONE_TEXT,
    TOPIC_NONE_TEXT,
    ensure_none_text_source,
)
from helper_pg import query_retry
from helper_source_stats import wait_for_catchup

from antithesis.assertions import always, sometimes

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
LOG = logging.getLogger("driver.kafka_none_envelope")

# Knobs. Tuned so each invocation is a small, self-contained unit of work
# — Antithesis launches the driver many times and accumulates coverage
# across invocations, not within one giant batch.
PRODUCES_PER_INVOCATION = 50
# Sized to span at least one MAX_OFF window from the global fault-
# orchestrator (default 40s) plus enough buffer for catchup itself.
CATCHUP_TIMEOUT_S = 90.0


def main() -> int:
    ensure_none_text_source()

    prefix = f"p{helper_random.random_u64():016x}"
    LOG.info("driver starting; prefix=%s", prefix)

    producer, tracker = make_producer(client_id=f"antithesis-none-{prefix}")

    # The set of payloads we attempted to produce. Each is unique to
    # (prefix, index) so we can filter the source on `text LIKE prefix:%`
    # and join payloads back to (partition, offset) without tracking them
    # at produce time.
    expected_payloads: set[str] = set()
    for i in range(PRODUCES_PER_INVOCATION):
        payload = f"{prefix}:{i:06d}"
        producer.produce(
            topic=TOPIC_NONE_TEXT,
            value=payload.encode("utf-8"),
            on_delivery=tracker.callback,
        )
        expected_payloads.add(payload)
        producer.poll(0)

    pending = producer.flush(timeout=30)
    if pending > 0 or tracker.last_error is not None:
        # Same fail-closed pattern as the upsert driver: under sustained
        # fault injection we cannot prove which messages Kafka accepted, so
        # the expected set may name payloads the source never saw. Bail
        # before running safety assertions.
        LOG.info(
            "skipping assertions: producer.flush pending=%d last_error=%s",
            pending,
            tracker.last_error,
        )
        return 0

    max_produced = tracker.topic_max_offset(TOPIC_NONE_TEXT)
    if max_produced < 0:
        LOG.info("no messages confirmed delivered this invocation; exiting cleanly")
        return 0

    # Each payload is unique to this invocation (prefix:NNNNNN), so the
    # source query below joins payloads back to (partition, offset)
    # assignments without us needing to track them at produce time.

    caught_up = wait_for_catchup(
        SOURCE_NONE_TEXT, max_produced, timeout_s=CATCHUP_TIMEOUT_S
    )

    sometimes(
        caught_up,
        "kafka source caught up to produced offsets within catchup budget (none envelope)",
        {"source": SOURCE_NONE_TEXT, "target_offset": max_produced},
    )

    if not caught_up:
        LOG.info("catchup did not complete in budget; skipping per-payload assertions")
        return 0

    # ----- no-data-duplication -----
    # `GROUP BY partition, "offset" HAVING COUNT(*) > 1` filtered to this
    # invocation's payloads. The catalog's `kafka-source-no-data-duplication`
    # property names this exact query shape. real_time_recency forces the
    # SELECT past the kafka broker's real-time frontier — see
    # helper_pg.query_retry for why this is required.
    dup_rows = query_retry(
        f"""
        SELECT partition, "offset", COUNT(*)::bigint
        FROM {SOURCE_NONE_TEXT}
        WHERE text LIKE %s
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
        """,
        (f"{prefix}:%",),
        real_time_recency=True,
    )
    always(
        len(dup_rows) == 0,
        "kafka source: no duplicate (partition, offset)",
        {
            "source": SOURCE_NONE_TEXT,
            "prefix": prefix,
            "dupe_count": len(dup_rows),
            # Carry up to a handful of offending rows for triage.
            "examples": [
                {"partition": int(p), "offset": int(o), "count": int(c)}
                for (p, o, c) in dup_rows[:5]
            ],
        },
    )

    # ----- no-data-loss -----
    # Confirm every payload we produced is visible *exactly once*. We do this
    # via a left-join: enumerate produced payloads, ask the source for each.
    # An always-pass requires every produced payload to map to exactly one
    # source row whose `text` matches.
    #
    # We batch all payloads into one query rather than one round-trip per
    # payload, so the assertion fires once per payload but the SQL cost
    # stays bounded.
    rows = query_retry(
        f"""
        SELECT text, partition, "offset", COUNT(*)::bigint
        FROM {SOURCE_NONE_TEXT}
        WHERE text LIKE %s
        GROUP BY 1, 2, 3
        """,
        (f"{prefix}:%",),
        real_time_recency=True,
    )
    by_payload: dict[str, tuple[int, int, int]] = {}
    for text, partition, offset, count in rows:
        by_payload[text] = (int(partition), int(offset), int(count))

    for payload in expected_payloads:
        info = by_payload.get(payload)
        present = info is not None
        count = info[2] if info else 0
        always(
            present and count == 1,
            "kafka source: every produced payload is visible exactly once",
            {
                "source": SOURCE_NONE_TEXT,
                "prefix": prefix,
                "payload": payload,
                "present": present,
                "observed_count": count,
            },
        )

    LOG.info(
        "driver done; asserted no-dupe + per-payload visibility on %d produced payloads",
        len(expected_payloads),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
