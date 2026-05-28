#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: cross-replica result consistency.

Two replicas of the same cluster independently maintain the same
indexed dataflow.  At any settled point they must compute identical
results — a divergence means non-deterministic execution or a
replica-local corruption that the controller's epoch isolation is
supposed to prevent.  Nothing in the existing suite compares replicas
against each other.

This driver:
  1. Checks the universal `antithesis_cluster` has >= 2 replicas (the
     multi-replica topology — clusterd1 + clusterd2).  Single-replica
     groups (workload-replay, upsert-stress) self-skip.
  2. Creates a prefix-scoped table + a DEFAULT INDEX on an aggregate
     view, built on `antithesis_cluster` so every replica maintains
     its own copy.
  3. Reads the indexed view once pinned to each replica via
     `SET cluster_replica`, and asserts the results are identical.

The table is static during the compare (this invocation owns it and
stops writing before reading), so the two per-replica reads return the
same logical result regardless of the small timestamp gap between them
— any difference is a real divergence.

Property shape:
  always(all replicas agree)              — safety
  sometimes(>= 2 replicas compared)       — liveness (multi-replica path hit)

Fault-shaped failures (a replica paused / killed mid-read) demote to
`sometimes(False)`; a paused replica is expected under Antithesis, not
a divergence.
"""

from __future__ import annotations

import sys

import helper_logging
import helper_random
import psycopg
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault
from helper_pg import (
    CONNECT_TIMEOUT_S,
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGUSER,
)

LOG = helper_logging.setup_logging("driver.cross_replica_consistency")

CLUSTER = "antithesis_cluster"
NUM_ROWS = 200
NUM_KEYS = 16
STATEMENT_TIMEOUT_MS = 10000


def _is_transient(msg: str) -> bool:
    """Fault/race shapes beyond helper_fault_tolerance that this driver's
    self-owned scratch objects can legitimately hit.

    `was dropped` covers the scratch view/index/table being concurrently
    torn down — during fault recovery the controller can drop in-flight
    dataflow state, and our own DROP ... CASCADE can race a retry.  The
    objects are prefix-scoped and owned solely by this invocation, so a
    drop mid-read is never a correctness signal here (unlike in drivers
    that read shared objects, which is why this stays local rather than
    in the shared FAULT_PATTERNS).
    """
    return "was dropped" in msg.lower()


def main() -> int:
    prefix = f"{helper_random.random_u64():016x}"
    rng = helper_random.AntithesisRandom()
    table = f"xrep_t_{prefix}"
    view = f"xrep_v_{prefix}"

    rows = [
        (rng.randrange(NUM_KEYS), rng.randrange(-1000, 1000)) for _ in range(NUM_ROWS)
    ]
    values_sql = ",".join(f"({k},{v})" for k, v in rows)
    agg = (
        f"SELECT k, count(*) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx "
        f"FROM {table} GROUP BY k"
    )

    try:
        with psycopg.connect(
            host=PGHOST,
            port=PGPORT,
            user=PGUSER,
            dbname=PGDATABASE,
            autocommit=True,
            connect_timeout=CONNECT_TIMEOUT_S,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}".encode())

                # Replica inventory for antithesis_cluster.
                cur.execute(
                    "SELECT cr.name FROM mz_cluster_replicas cr "
                    "JOIN mz_clusters c ON cr.cluster_id = c.id "
                    f"WHERE c.name = '{CLUSTER}' ORDER BY cr.name".encode()
                )
                replicas = [str(r[0]) for r in cur.fetchall()]
                if len(replicas) < 2:
                    # Single-replica group — nothing to compare.  Not a
                    # failure; just record that this timeline couldn't
                    # exercise the multi-replica path.
                    sometimes(
                        False,
                        "cross-replica: cluster had >= 2 replicas to compare",
                        {"prefix": prefix, "replicas": replicas},
                    )
                    return 0

                cur.execute(f"SET cluster = {CLUSTER}".encode())
                cur.execute(f"CREATE TABLE {table} (k int, v int)".encode())
                cur.execute(f"INSERT INTO {table} VALUES {values_sql}".encode())
                cur.execute(f"CREATE VIEW {view} AS {agg}".encode())
                cur.execute(f"CREATE DEFAULT INDEX ON {view}".encode())

                order = " ORDER BY k, c, s, mn, mx"
                select = f"SELECT k, c, s, mn, mx FROM {view}{order}"

                per_replica: dict[str, list[tuple]] = {}
                for replica in replicas:
                    cur.execute(f"SET cluster_replica = {replica}".encode())
                    cur.execute(select.encode())
                    per_replica[replica] = cur.fetchall()
                cur.execute(b"RESET cluster_replica")

                try:
                    cur.execute(f"DROP TABLE {table} CASCADE".encode())
                except Exception as exc:  # noqa: BLE001
                    LOG.debug("cleanup tolerated: %s", exc)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if looks_like_fault(msg) or _is_transient(msg):
            sometimes(
                False,
                "cross-replica: comparison completed without a fault",
                {"prefix": prefix, "fault": msg[:200]},
            )
            return 0
        LOG.warning("cross-replica non-fault error: %s", msg)
        always(
            False,
            "cross-replica: setup/read raised no unexpected (non-fault) error",
            {"prefix": prefix, "error": msg[:300]},
        )
        return 0

    # All replicas must produce identical results.
    baseline_name = replicas[0]
    baseline = per_replica[baseline_name]
    disagreeing = {
        name: len(rowset) for name, rowset in per_replica.items() if rowset != baseline
    }
    always(
        not disagreeing,
        "cross-replica: every replica of antithesis_cluster computed identical results",
        {
            "prefix": prefix,
            "replicas": replicas,
            "baseline_count": len(baseline),
            "disagreeing": disagreeing,
            "baseline_sample": [list(r) for r in baseline[:5]],
        },
    )
    sometimes(
        True,
        "cross-replica: compared >= 2 replicas",
        {"prefix": prefix, "replicas": len(replicas)},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
