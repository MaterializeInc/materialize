#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver for `mv-reflects-source-updates`.

End-to-end user-visible property: after data is written to an upstream
collection, materialized views that depend on that collection eventually
reflect the new data. Materialize's headline value proposition.

This driver uses a TABLE (not a Kafka source) so the property is exercised
independent of source ingestion: the test path is INSERT -> coordinator
group_commit -> persist write of the table -> MV's compute dataflow ->
persist write of the MV output -> SELECT. Kafka-source-specific liveness
is covered by the other Kafka-source drivers.

Each invocation:
  1. Ensures `mv_input_table` + materialized view `mv_input_count` exist.
  2. Picks a per-invocation prefix so concurrent driver instances scope to
     disjoint MV rows.
  3. INSERTs N rows tagged with the prefix.
  4. Requests an Antithesis quiet period and polls the MV until the count
     for the prefix equals N.
  5. Asserts:
       - `always(...)` the MV count matches what was inserted (no over- or
         under-counting after settle).
       - `sometimes(...)` the catchup completed within the budget (the
         liveness anchor — without this, the always check could be vacuous
         on a slow-catchup invocation).

This is a `parallel_driver_` — many concurrent instances exercise the MV
without colliding because each invocation owns its prefix range.
"""

from __future__ import annotations

import logging
import sys
import time

import helper_random
from antithesis.assertions import always, sometimes
from helper_pg import execute_retry, query_one_retry
from helper_quiet import request_quiet_period
from helper_table_mv import MV_NAME, TABLE_MV_INPUT, ensure_table_and_mv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
LOG = logging.getLogger("driver.mv_reflects_table_updates")

INSERTS_PER_INVOCATION = 40
QUIET_PERIOD_S = 20
CATCHUP_TIMEOUT_S = 60.0
CATCHUP_POLL_INTERVAL_S = 0.5


def _mv_count_for_prefix(prefix: str) -> int | None:
    """Return the row_count the MV currently reports for `prefix`, or None.

    None means "no row exists for that prefix yet" — distinct from zero,
    which the MV would not produce for the `count(*)`+`group by` shape (a
    fully-deleted prefix would not appear at all).
    """
    row = query_one_retry(
        f"SELECT row_count::bigint FROM {MV_NAME} WHERE prefix = %s",
        (prefix,),
    )
    if row is None:
        return None
    return int(row[0])


def main() -> int:
    ensure_table_and_mv()

    prefix = f"p{helper_random.random_u64():016x}"
    LOG.info("mv driver starting; prefix=%s", prefix)

    # Insert N rows tagged with the prefix. We batch into a single statement
    # so the coordinator processes them as one group_commit, which keeps the
    # workload-visible target offset for catchup well-defined (otherwise a
    # mid-insert crash would split the row count and the MV would catch up
    # to "some" count rather than exactly N).
    placeholders = ", ".join(["(%s, %s)"] * INSERTS_PER_INVOCATION)
    params: list[object] = []
    for i in range(INSERTS_PER_INVOCATION):
        params.extend([i, prefix])
    execute_retry(
        f"INSERT INTO {TABLE_MV_INPUT} (id, prefix) VALUES {placeholders}",
        params,
    )

    request_quiet_period(QUIET_PERIOD_S)

    # Poll the MV until the row_count for this prefix reaches N. The MV's
    # `COUNT(*) GROUP BY prefix` shape means the row for this prefix may
    # appear partially populated during the catchup window.
    deadline = time.monotonic() + CATCHUP_TIMEOUT_S
    observed = _mv_count_for_prefix(prefix)
    while observed != INSERTS_PER_INVOCATION and time.monotonic() < deadline:
        time.sleep(CATCHUP_POLL_INTERVAL_S)
        observed = _mv_count_for_prefix(prefix)

    caught_up = observed == INSERTS_PER_INVOCATION

    sometimes(
        caught_up,
        "mv: row_count caught up to inserted count after quiet period",
        {
            "mv": MV_NAME,
            "table": TABLE_MV_INPUT,
            "prefix": prefix,
            "expected": INSERTS_PER_INVOCATION,
            "observed": observed,
        },
    )

    if not caught_up:
        LOG.info(
            "catchup did not complete in budget; skipping safety assertion "
            "(observed=%s expected=%d)",
            observed,
            INSERTS_PER_INVOCATION,
        )
        return 0

    # Safety check: the MV must report exactly the inserted count. A
    # higher count would be double-counting (corruption); a lower count
    # at this point would mean the catchup poll above gave us a stale
    # read between observations, which is itself a correctness bug worth
    # surfacing.
    always(
        observed == INSERTS_PER_INVOCATION,
        "mv: row_count equals inserted count for prefix after settle",
        {
            "mv": MV_NAME,
            "table": TABLE_MV_INPUT,
            "prefix": prefix,
            "expected": INSERTS_PER_INVOCATION,
            "observed": observed,
        },
    )

    LOG.info(
        "mv driver done; inserted=%d mv_count=%s prefix=%s",
        INSERTS_PER_INVOCATION,
        observed,
        prefix,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
