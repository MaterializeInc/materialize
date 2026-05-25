# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Idempotent setup for the Antithesis table + materialized view scaffolding.

Used by the `mv-reflects-source-updates` driver. The table holds rows with a
per-invocation `prefix` so concurrent driver instances scope to disjoint
groups, and the materialized view rolls those rows up by prefix:

    CREATE TABLE mv_input_table (id BIGINT NOT NULL, prefix TEXT NOT NULL);
    CREATE MATERIALIZED VIEW mv_input_count AS
        SELECT prefix, COUNT(*)::BIGINT AS row_count
        FROM mv_input_table
        GROUP BY prefix;

Defining the MV on the local coordinator's table (rather than a Kafka
source) deliberately tests the end-to-end path independent of source
ingestion: dataflow rendering, persist write of the MV output, and
frontier advancement through compute. Source-side faults are still
exercised because the workload runs under the same fault-injection regime
as everything else.
"""

from __future__ import annotations

import logging
import os

from helper_pg import execute_retry

LOG = logging.getLogger("antithesis.helper_table_mv")

CLUSTER = os.environ.get("MZ_ANTITHESIS_CLUSTER", "antithesis_cluster")

TABLE_MV_INPUT = "mv_input_table"
MV_NAME = "mv_input_count"


def ensure_table_and_mv() -> None:
    """Create the input table and the materialized view.

    Table uses `IF NOT EXISTS` (the schema never changes; preserving
    data across re-runs is desirable).  The MV uses `CREATE OR
    REPLACE` so a definition change picked up by a redeploy
    automatically rebuilds the MV against the new SQL — without it,
    an `IF NOT EXISTS` no-ops against an MV with the *old* definition
    and the driver's assertions then compare against stale dataflow
    output.  This actually bit us recently: the MV definition flipped
    from `COUNT(*)` to `COUNT(DISTINCT id)` to absorb fault-window
    INSERT retries (Materialize doesn't enforce uniqueness, so a
    `COUNT(*)` MV would double-count a retried row); any pre-existing
    `COUNT(*)` MV from a prior build would silently keep firing the
    old shape under `IF NOT EXISTS`.

    Under Antithesis singleton-per-timeline semantics each timeline
    runs `ensure_table_and_mv` at most a handful of times, so the
    cost of `CREATE OR REPLACE` re-rendering the dataflow on every
    invocation (vs. a no-op `IF NOT EXISTS`) is negligible — the
    table backing the MV is small (per-prefix tens of rows) and MZ's
    dataflow setup completes in milliseconds at that scale.
    """
    LOG.info(
        "ensure_table_and_mv: starting (table=%s mv=%s cluster=%s)",
        TABLE_MV_INPUT,
        MV_NAME,
        CLUSTER,
    )
    execute_retry(
        f"CREATE TABLE IF NOT EXISTS {TABLE_MV_INPUT} "
        f"(id BIGINT NOT NULL, prefix TEXT NOT NULL)"
    )
    # `COUNT(DISTINCT id)` rather than `COUNT(*)` so a fault-window
    # INSERT retry that committed at the server but lost the client
    # ack lands a duplicate (id, prefix) row without skewing the MV's
    # count.  See the docstring above for the schema-drift recovery
    # rationale that motivates `CREATE OR REPLACE` over `IF NOT
    # EXISTS`.
    execute_retry(
        f"CREATE OR REPLACE MATERIALIZED VIEW {MV_NAME} "
        f"IN CLUSTER {CLUSTER} AS "
        f"SELECT prefix, COUNT(DISTINCT id)::BIGINT AS row_count "
        f"FROM {TABLE_MV_INPUT} "
        f"GROUP BY prefix"
    )
    LOG.info("ensure_table_and_mv: ready (table=%s mv=%s)", TABLE_MV_INPUT, MV_NAME)
