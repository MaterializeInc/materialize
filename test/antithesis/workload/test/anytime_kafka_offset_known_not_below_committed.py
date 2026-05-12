#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `offset-known-not-below-committed`.

For every Kafka source, `mz_internal.mz_source_statistics_per_worker` must
always report `offset_known >= offset_committed`. `offset_known` reflects
what the broker has told us is available; `offset_committed` reflects what
Materialize has durably ingested. Causally, the broker's idea of "this
offset exists" cannot lag what we've already durably read past it. Direct
regression target for commit 3e32df1f69, which clamped the metric to
prevent this flip on the first sample after a clusterd restart.

This is an `anytime_` driver — it runs continuously throughout the timeline
under active fault injection. The interesting timing per the catalog is the
very first sample after a clusterd restart, where `offset_known` is
restored from the broker watermark while `offset_committed` is restored
from persist; we want Antithesis to drop a poll into that window.

Both fields are read in the same row of the same SELECT so the comparison
never crosses a metric-update boundary. The per-worker view is queried
(not the rolled-up `mz_source_statistics`) because the invariant must hold
per worker — averaging would mask a single worker that crossed the line.

Errors during polling (clusterd down, network partitioned) are *expected*
under fault injection and must not produce false-positive failures; we
just skip the sample.
"""

from __future__ import annotations

import logging
import sys
import time

from helper_pg import query_retry

from antithesis.assertions import always

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
LOG = logging.getLogger("driver.kafka_offset_known_not_below_committed")

POLL_INTERVAL_S = 0.5
RUN_BUDGET_S = 30.0

ANTITHESIS_CLUSTER = "antithesis_cluster"


def _samples() -> list[tuple[str, int, int, int]]:
    """Return (source_name, worker_id, offset_known, offset_committed) per worker.

    Joins `mz_source_statistics_per_worker` to `mz_sources` so the assertion
    `details` can name the source by name rather than by opaque id. Filters
    to Kafka sources owned by the antithesis cluster so the assertion does
    not fire against the introspection cluster's bookkeeping sources.

    Rows with NULL `offset_known` or `offset_committed` are dropped — those
    are early-lifetime samples that have not been populated yet.
    """
    rows = query_retry(
        """
        SELECT
            s.name,
            ss.worker_id::bigint,
            ss.offset_known::bigint,
            ss.offset_committed::bigint
        FROM mz_internal.mz_source_statistics_per_worker ss
        JOIN mz_sources s ON s.id = ss.id
        JOIN mz_clusters c ON c.id = s.cluster_id
        WHERE c.name = %s
          AND s.type = 'kafka'
          AND ss.offset_known IS NOT NULL
          AND ss.offset_committed IS NOT NULL
        """,
        (ANTITHESIS_CLUSTER,),
    )
    return [(str(n), int(w), int(k), int(o)) for (n, w, k, o) in rows]


def main() -> int:
    deadline = time.monotonic() + RUN_BUDGET_S
    polled = 0

    while time.monotonic() < deadline:
        try:
            samples = _samples()
        except Exception as exc:  # noqa: BLE001
            LOG.info("source stats query failed: %s; sleeping and retrying", exc)
            time.sleep(POLL_INTERVAL_S)
            continue

        for source, worker, known, committed in samples:
            always(
                known >= committed,
                "kafka: source offset_known < offset_committed",
                {
                    "source": source,
                    "worker_id": worker,
                    "offset_known": known,
                    "offset_committed": committed,
                    "deficit": committed - known,
                },
            )
            polled += 1

        time.sleep(POLL_INTERVAL_S)

    LOG.info("offset_known-not-below-committed check done; %d samples", polled)
    return 0


if __name__ == "__main__":
    sys.exit(main())
