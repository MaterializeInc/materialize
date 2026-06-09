#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `kafka-source-frontier-monotonic`.

The `upper` of a Kafka source's persist data shard must never regress across
its lifetime, including across clusterd restarts and `compare_and_append`
retries. Approximated via the workload-visible `offset_committed` reported
in `mz_internal.mz_source_statistics`, which is the durably-ingested
upstream offset for the source.

This is an `anytime_` driver — it runs continuously throughout the timeline,
polling all of this workload's Kafka sources and asserting that each one's
`offset_committed` never decreases between successive observations. Faults
are active while it runs, which is the right shape for a continuous safety
invariant: Antithesis can crash clusterd between two of our polls and the
next poll must still report a value >= the previous one.

The driver exits after a bounded budget so Antithesis can re-launch it
freely without one instance pinning resources. Cross-invocation: each
instance reads the state from before-restart only via `offset_committed`
itself (no in-process memory carries across) — `last_seen` is reset on each
launch, but Antithesis runs many instances in parallel and the union of
their observations covers the regression window.

Errors during polling (network partitions, clusterd unavailable) are
*expected* under fault injection and must not produce false-positive
failures. We only assert when we have two successive successful reads for
the same source.
"""

from __future__ import annotations

import logging
import sys
import time

from antithesis.assertions import always
from helper_pg import query_retry
from helper_source_stats import offset_committed

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
LOG = logging.getLogger("driver.kafka_frontier_monotonic")

# Knobs.
POLL_INTERVAL_S = 0.5
RUN_BUDGET_S = 30.0

# The Antithesis cluster every driver in this workload provisions sources into.
# Discovering sources dynamically (rather than hardcoding names) means new
# drivers that introduce new Kafka sources get monotonicity coverage for free.
ANTITHESIS_CLUSTER = "antithesis_cluster"


def _sources_present() -> list[str]:
    """Return every Kafka source currently owned by `antithesis_cluster`."""
    rows = query_retry(
        """
        SELECT s.name
        FROM mz_sources s
        JOIN mz_clusters c ON c.id = s.cluster_id
        WHERE c.name = %s AND s.type = 'kafka'
        """,
        (ANTITHESIS_CLUSTER,),
    )
    return [r[0] for r in rows]


def main() -> int:
    deadline = time.monotonic() + RUN_BUDGET_S
    # Per-source highest committed offset observed across this invocation's
    # polls. Each successful new read for a source must be >= last_seen.
    last_seen: dict[str, int] = {}
    polled = 0

    while time.monotonic() < deadline:
        try:
            sources = _sources_present()
        except Exception as exc:  # noqa: BLE001
            LOG.info("source list query failed: %s; sleeping and retrying", exc)
            time.sleep(POLL_INTERVAL_S)
            continue

        for source in sources:
            try:
                observed = offset_committed(source)
            except Exception as exc:  # noqa: BLE001
                LOG.info("offset_committed query failed for %s: %s", source, exc)
                continue
            if observed is None:
                # Statistics row not initialized yet (very early in source
                # lifetime, or post-restart before stats first reported).
                # Not an assertion target.
                continue

            prev = last_seen.get(source)
            if prev is not None:
                always(
                    observed >= prev,
                    "kafka: source offset_committed non-monotonic",
                    {
                        "source": source,
                        "previous": prev,
                        "observed": observed,
                        "regression": prev - observed,
                    },
                )

            # Always update last_seen, even on regression — we want to keep
            # asserting against the most recent observation so a regression
            # surfaces once per discrete drop, not on every subsequent poll.
            last_seen[source] = observed
            polled += 1

        time.sleep(POLL_INTERVAL_S)

    LOG.info(
        "frontier monotonic check done; %d samples across %d sources",
        polled,
        len(last_seen),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
