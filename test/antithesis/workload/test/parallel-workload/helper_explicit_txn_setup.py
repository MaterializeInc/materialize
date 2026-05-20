#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis first_ command: scaffolding for the explicit-txn driver.

Creates one table and two materialized views over it, all in
`antithesis_cluster`. The objects are `antithesis_txn_*`-prefixed so
the testdrive-runner singleton's reset (which preserves
`antithesis_*`-prefixed names) won't clobber them.

Why a dedicated table+MVs: `parallel_driver_explicit_txn_no_since_violation`
issues `BEGIN -> SELECT ... -> SELECT ... -> COMMIT` sequences and asserts
`always(no SinceViolation)`. A non-empty table with MVs over it is the
smallest object set that:
  (a) gives the explicit-txn timestamp determination two collections to
      acquire read holds against (matching the bug's "stored read holds
      restricted to input_id_bundle" code path), and
  (b) gives the MV's dataflow background frontier advances that can
      race against the txn's stored as_of — the timing window the bug
      lives in.

Two MV variants — a plain MV and a `REFRESH EVERY` MV — because the
INC database-issues#11200 repro hit a `REFRESH EVERY` MV specifically.
The REFRESH EVERY variant advances its `since` in step ticks rather
than continuously, which interacts with stored-read-hold re-resolution
differently than a streaming MV.  The driver picks between them per
invocation so each timeline visits both code paths.
"""

from __future__ import annotations

import sys

import helper_logging
from helper_pg import execute_retry, query_retry

from antithesis.assertions import reachable

LOG = helper_logging.setup_logging("first.explicit_txn_setup")

CLUSTER = "antithesis_cluster"
TABLE_NAME = "antithesis_txn_table"
MV_NAME = "antithesis_txn_mv"
MV_REFRESH_NAME = "antithesis_txn_mv_refresh_every"

# Initial rows so the first SELECT inside an explicit txn has data to
# read. Small — the driver issues additional INSERTs to keep frontiers
# advancing.
INITIAL_ROWS = 1000

# Refresh interval for the REFRESH EVERY variant. Short enough that a
# refresh tick lands inside most explicit-txn windows (the txn issues
# SELECTS_PER_TXN=8 statements with no inter-statement sleep, so the
# txn lifetime is typically a few hundred ms to a few seconds under
# fault injection). Aligning the tick cadence with the txn lifetime
# maximises the probability that the bug's timing window — a refresh
# advancing `since` while the txn holds a now-stale as_of — actually
# opens.
MV_REFRESH_INTERVAL = "2 seconds"


def _exists(catalog: str, name: str) -> bool:
    rows = query_retry(f"SELECT 1 FROM {catalog} WHERE name = %s", (name,))
    return bool(rows)


def main() -> int:
    if not _exists("mz_tables", TABLE_NAME):
        LOG.info("creating %s in cluster %s", TABLE_NAME, CLUSTER)
        execute_retry(
            f"CREATE TABLE {TABLE_NAME} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"
        )
        execute_retry(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT i, i * 2 FROM generate_series(1, {INITIAL_ROWS}) AS i"
        )
    else:
        LOG.info("%s already exists; skipping", TABLE_NAME)

    if not _exists("mz_materialized_views", MV_NAME):
        LOG.info("creating %s in cluster %s", MV_NAME, CLUSTER)
        execute_retry(
            f"CREATE MATERIALIZED VIEW {MV_NAME} "
            f"IN CLUSTER {CLUSTER} AS "
            f"SELECT count(*) AS n, sum(v) AS s FROM {TABLE_NAME}"
        )
    else:
        LOG.info("%s already exists; skipping", MV_NAME)

    if not _exists("mz_materialized_views", MV_REFRESH_NAME):
        LOG.info("creating %s in cluster %s", MV_REFRESH_NAME, CLUSTER)
        execute_retry(
            f"CREATE MATERIALIZED VIEW {MV_REFRESH_NAME} "
            f"IN CLUSTER {CLUSTER} "
            f"WITH (REFRESH EVERY '{MV_REFRESH_INTERVAL}') AS "
            f"SELECT count(*) AS n, sum(v) AS s FROM {TABLE_NAME}"
        )
    else:
        LOG.info("%s already exists; skipping", MV_REFRESH_NAME)

    reachable(
        "explicit-txn: scaffolding ready (table + both MV variants in antithesis_cluster)",
        {
            "table": TABLE_NAME,
            "mv": MV_NAME,
            "mv_refresh": MV_REFRESH_NAME,
            "refresh_interval": MV_REFRESH_INTERVAL,
            "cluster": CLUSTER,
        },
    )
    LOG.info("explicit-txn scaffolding setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
