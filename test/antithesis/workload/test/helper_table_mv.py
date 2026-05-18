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
    """Create the input table and the materialized view if absent.

    Both DDLs use IF NOT EXISTS so concurrent driver instances racing
    through setup do not collide. The MV is created in the antithesis
    cluster so dataflow execution is colocated with the rest of the
    workload's compute.
    """
    execute_retry(
        f"CREATE TABLE IF NOT EXISTS {TABLE_MV_INPUT} "
        f"(id BIGINT NOT NULL, prefix TEXT NOT NULL)"
    )
    execute_retry(
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {MV_NAME} "
        f"IN CLUSTER {CLUSTER} AS "
        f"SELECT prefix, COUNT(*)::BIGINT AS row_count "
        f"FROM {TABLE_MV_INPUT} "
        f"GROUP BY prefix"
    )
    LOG.info("table %s and MV %s ready", TABLE_MV_INPUT, MV_NAME)
