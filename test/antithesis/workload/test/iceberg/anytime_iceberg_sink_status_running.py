#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `iceberg-sink-eventually-running`.

The iceberg sink must never enter a permanent failure state.  Faults
under Antithesis can briefly stall the sink (status flips to `stalled`
while polaris is unreachable, for example, or `dropped` while clusterd
is being torn down), but every fault window eventually closes and the
sink must recover.  This driver polls
`mz_internal.mz_sink_statuses` and asserts that whenever it has a
fresh enough read it does NOT see a hard-fail status that the global
fault-orchestrator's quiet window should have cleared.

Approach:

  * Every sample is judged independently — we do not carry status
    across polls.  This is sufficient for `sometimes(running)` (a
    liveness anchor that some poll observed a healthy sink) and for
    `always(not failed)` where `failed` is restricted to the
    irreversible status values.

  * Transient `stalled` / `starting` states are NOT treated as
    failures.  Faults legitimately produce them, and the source-side
    drivers (anytime_kafka_frontier_monotonic, etc.) follow the same
    convention — only assert against states from which Materialize
    does not recover.

The anytime cadence is what makes this useful: Antithesis can crash
clusterd, partition polaris, kill minio mid-commit, and the next
quiet window must restore the sink.  Per-invocation drivers are
typically not long-lived enough to catch a status flip; this one
keeps polling for `RUN_BUDGET_S` so an injected stall has time to
land inside its observation window.
"""

from __future__ import annotations

import sys
import time

import helper_iceberg
import helper_logging

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.iceberg_sink_status_running")

POLL_INTERVAL_S = 1.0
RUN_BUDGET_S = 30.0

# Statuses we consider irreversible.  `failed` is what materialized
# reports when the sink hits a FatalErr in `try_commit_batch` (e.g.
# the fencing-detected version mismatch).  Anything else — `stalled`,
# `starting`, `dropped` (transient during cluster reconfiguration) —
# can clear by itself and is not an assertion target.
TERMINAL_STATUSES = {"failed"}


def main() -> int:
    deadline = time.monotonic() + RUN_BUDGET_S
    samples = 0
    saw_running = False

    while time.monotonic() < deadline:
        try:
            status = helper_iceberg.sink_status()
        except Exception as exc:  # noqa: BLE001
            LOG.info("sink_status query failed: %s; retrying", exc)
            time.sleep(POLL_INTERVAL_S)
            continue

        if status is None:
            # The sink doesn't exist yet (very early in the timeline,
            # before first_iceberg_setup has finished, or in a
            # non-iceberg-group context where this driver shouldn't
            # have been launched).  Not an assertion target.
            time.sleep(POLL_INTERVAL_S)
            continue

        status_str, error_str = status
        always(
            status_str not in TERMINAL_STATUSES,
            "iceberg: sink reached a terminal failure state",
            {
                "sink": helper_iceberg.SINK_NAME,
                "status": status_str,
                "error": error_str,
            },
        )
        if status_str == "running":
            saw_running = True
        samples += 1
        time.sleep(POLL_INTERVAL_S)

    sometimes(
        saw_running,
        "iceberg: sink observed in `running` status at least once this invocation",
        {"sink": helper_iceberg.SINK_NAME, "samples": samples},
    )

    LOG.info("sink status check done; %d samples (saw_running=%s)", samples, saw_running)
    return 0


if __name__ == "__main__":
    sys.exit(main())
