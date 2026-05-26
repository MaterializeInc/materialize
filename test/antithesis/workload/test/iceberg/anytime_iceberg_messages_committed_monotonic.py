#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `iceberg-sink-messages-committed-monotonic`.

`mz_internal.mz_sink_statistics.messages_committed` is a never-reset
counter of messages the sink has durably committed to iceberg.  Across
a sink's lifetime — including clusterd restarts and catalog
commit-conflict retries — it must never decrease.

Sink-side mirror of `anytime_kafka_frontier_monotonic`: that driver
asserts the same shape on the source side (`offset_committed` never
regresses).  Pairing them gives us a continuous monotonicity safety
net across both ends of the pipeline.

Cross-invocation: each instance reads the state from before-restart
only via `messages_committed` itself (no in-process memory carries
across); `last_seen` is reset on each launch.  Antithesis runs many
instances in parallel and the union of their observations covers any
regression window the fault orchestrator opens.

Why this matters for SS-148: a dropped catalog commit response that
the sink retries and "succeeds" twice would (in iceberg-rust's current
behavior) still advance `messages_committed` by the messages in the
retry — the sink reports success based on its local view, not on
whether iceberg actually accepted the data files only once.  So this
property alone does NOT catch the SS-148 bug.  It's here as the
companion to the source-side monotonicity invariant, and because a
regression here would surface a different class of sink bug (a retry
that incorrectly *unwinds* the committed counter).
"""

from __future__ import annotations

import sys
import time

import helper_iceberg
import helper_logging
from antithesis.assertions import always

LOG = helper_logging.setup_logging("driver.iceberg_messages_committed_monotonic")

POLL_INTERVAL_S = 0.5
RUN_BUDGET_S = 30.0


def main() -> int:
    deadline = time.monotonic() + RUN_BUDGET_S
    last_seen: int | None = None
    polled = 0

    while time.monotonic() < deadline:
        try:
            observed = helper_iceberg.messages_committed()
        except Exception as exc:  # noqa: BLE001
            LOG.info("messages_committed query failed: %s; retrying", exc)
            time.sleep(POLL_INTERVAL_S)
            continue

        if observed is None:
            # Stats row not yet populated (very early in sink lifetime,
            # or post-restart before stats first reported).  Not an
            # assertion target.
            time.sleep(POLL_INTERVAL_S)
            continue

        if last_seen is not None:
            always(
                observed >= last_seen,
                "iceberg: sink messages_committed non-monotonic",
                {
                    "sink": helper_iceberg.SINK_NAME,
                    "previous": last_seen,
                    "observed": observed,
                    "regression": last_seen - observed,
                },
            )

        # Always update last_seen, even on regression — we want to keep
        # asserting against the most recent observation so a regression
        # surfaces once per discrete drop, not on every subsequent poll.
        last_seen = observed
        polled += 1
        time.sleep(POLL_INTERVAL_S)

    LOG.info("messages_committed monotonic check done; %d samples", polled)
    return 0


if __name__ == "__main__":
    sys.exit(main())
