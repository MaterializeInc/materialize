#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis first_ command: scaffolding for the hot-objects driver.

Creates one shared table, two materialized views over it, and an index
on one of the MVs. Every invocation of
`parallel_driver_pw_hot_objects.py` reads from and writes to this fixed
object set — that's the whole point: concentrate per-object contention
in one place so #11200-shape bugs (continuous-frontier-advance +
stored-read-holds) actually have a window to fire.

Object set is described in `helper_pw_hot.py`. Why each piece:

  * `pw_hot_table` — TABLE. Accepts INSERTs and DELETEs from every
    invocation. The persist frontier on this table is the thing the
    bug needs to keep moving.
  * `pw_hot_mv_count` — MV computing count(*) on the table. Single-input,
    small footprint, very fast peeks. Indexed (below) to route through
    the `peek_target.id()` validation in `controller.rs:892` — one of
    the three SinceViolation sites.
  * `pw_hot_mv_sum` — MV joining count/sum/count-distinct. Larger
    input_id_bundle (when read alongside the table in explicit txns)
    so the stored-read-holds-subset code path gets exercised.
  * `pw_hot_idx_count` — INDEX on `pw_hot_mv_count`. Forces the MV to
    be arranged; peeks go through the indexed-collection path.

All four live in `antithesis_cluster` — the existing two-replica
unmanaged cluster — so multi-replica peek path is exercised.

Setup is idempotent: each step guards on the catalog. Safe to re-run
on every Antithesis timeline start; concurrent runs of this script
(unlikely, but possible if Antithesis spawns several `first_` commands
in parallel) tolerate each other through `IF NOT EXISTS` + catalog
re-checks. Objects are never dropped by any driver — they belong to
the timeline.
"""

from __future__ import annotations

import sys

import helper_logging
from antithesis.assertions import reachable
from helper_pg import execute_retry, query_retry
from helper_pw_hot import (
    CLUSTER,
    INDEX_NAME,
    MV_COUNT_NAME,
    MV_SUM_NAME,
    TABLE_NAME,
)

LOG = helper_logging.setup_logging("first.pw_hot_objects_setup")

# Initial seed rows — enough for the MVs to have something to compute
# without forcing a slow initial snapshot. The driver adds writes on
# every invocation.
INITIAL_ROWS = 1000


def _exists(catalog: str, name: str) -> bool:
    rows = query_retry(f"SELECT 1 FROM {catalog} WHERE name = %s", (name,))
    return bool(rows)


def main() -> int:
    if not _exists("mz_tables", TABLE_NAME):
        LOG.info("creating %s", TABLE_NAME)
        execute_retry(
            f"CREATE TABLE {TABLE_NAME} ("
            f"  id          BIGINT      PRIMARY KEY,"
            f"  v           BIGINT      NOT NULL,"
            f"  written_by  TEXT        NOT NULL,"
            f"  ts          TIMESTAMPTZ NOT NULL DEFAULT now()"
            f")"
        )
        execute_retry(
            f"INSERT INTO {TABLE_NAME} (id, v, written_by) "
            f"SELECT i, i * 2, 'initial-seed' "
            f"FROM generate_series(1, {INITIAL_ROWS}) AS i"
        )
    else:
        LOG.info("%s already exists; skipping", TABLE_NAME)

    if not _exists("mz_materialized_views", MV_COUNT_NAME):
        LOG.info("creating %s in cluster %s", MV_COUNT_NAME, CLUSTER)
        execute_retry(
            f"CREATE MATERIALIZED VIEW {MV_COUNT_NAME} "
            f"IN CLUSTER {CLUSTER} AS "
            f"SELECT count(*) AS n FROM {TABLE_NAME}"
        )
    else:
        LOG.info("%s already exists; skipping", MV_COUNT_NAME)

    if not _exists("mz_materialized_views", MV_SUM_NAME):
        LOG.info("creating %s in cluster %s", MV_SUM_NAME, CLUSTER)
        execute_retry(
            f"CREATE MATERIALIZED VIEW {MV_SUM_NAME} "
            f"IN CLUSTER {CLUSTER} AS "
            f"SELECT count(*) AS n, "
            f"       sum(v) AS s, "
            f"       count(DISTINCT written_by) AS writers "
            f"FROM {TABLE_NAME}"
        )
    else:
        LOG.info("%s already exists; skipping", MV_SUM_NAME)

    if not _exists("mz_indexes", INDEX_NAME):
        LOG.info(
            "creating index %s on %s in cluster %s",
            INDEX_NAME,
            MV_COUNT_NAME,
            CLUSTER,
        )
        execute_retry(
            f"CREATE INDEX {INDEX_NAME} "
            f"IN CLUSTER {CLUSTER} "
            f"ON {MV_COUNT_NAME} (n)"
        )
    else:
        LOG.info("%s already exists; skipping", INDEX_NAME)

    reachable(
        "pw-hot: shared table + MVs + index scaffolding ready",
        {
            "table": TABLE_NAME,
            "mvs": [MV_COUNT_NAME, MV_SUM_NAME],
            "index": INDEX_NAME,
            "cluster": CLUSTER,
        },
    )
    LOG.info("pw-hot scaffolding setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
