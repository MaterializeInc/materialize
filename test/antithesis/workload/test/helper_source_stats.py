# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Catchup polling against `mz_internal.mz_source_statistics`.

Used by drivers to wait until a Kafka source has durably ingested at least
some target offset (typically the maximum produced offset). All durations are
budgeted; callers handle timeouts.
"""

from __future__ import annotations

import logging
import time

from helper_pg import query_one_retry

LOG = logging.getLogger("antithesis.helper_source_stats")


def offset_committed(source_name: str) -> int | None:
    """Return the maximum offset_committed for `source_name`, or None.

    `mz_source_statistics.offset_committed` is the durably-ingested upstream
    offset, aggregated across replicas in the view. Returns None if the
    statistics row does not exist yet (very early in source lifetime) so
    callers can distinguish "not initialized" from "still behind."
    """
    row = query_one_retry(
        """
        SELECT MAX(ss.offset_committed)::bigint
        FROM mz_internal.mz_source_statistics ss
        JOIN mz_sources s ON s.id = ss.id
        WHERE s.name = %s
        """,
        (source_name,),
    )
    if row is None or row[0] is None:
        return None
    return int(row[0])


def wait_for_catchup(
    source_name: str,
    target_offset: int,
    timeout_s: float = 60.0,
    poll_interval_s: float = 0.5,
) -> bool:
    """Wait until offset_committed for `source_name` reaches `target_offset`.

    Returns True if catchup completed within `timeout_s`, False on timeout.
    """
    deadline = time.monotonic() + timeout_s
    last_seen: int | None = None
    while time.monotonic() < deadline:
        observed = offset_committed(source_name)
        if observed is not None and observed >= target_offset:
            LOG.info(
                "source %s caught up: observed=%d target=%d",
                source_name,
                observed,
                target_offset,
            )
            return True
        if observed != last_seen:
            LOG.info(
                "source %s waiting for catchup: observed=%s target=%d",
                source_name,
                observed,
                target_offset,
            )
            last_seen = observed
        time.sleep(poll_interval_s)
    LOG.warning(
        "source %s catchup timeout: observed=%s target=%d",
        source_name,
        last_seen,
        target_offset,
    )
    return False
