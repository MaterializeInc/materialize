# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class PartitionBy(Check):
    """PARTITION BY on tables and materialized views, verified through
    EXPLAIN FILTER PUSHDOWN."""

    def _can_run(self, e: Executor) -> bool:
        # enable_collection_partition_by defaults to true from v0.125.
        return self.base_version >= MzVersion.parse_mz("v0.130.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE partition_by_table (ts TIMESTAMPTZ, id INT, val STRING)
              WITH (PARTITION BY (ts, id))
            > INSERT INTO partition_by_table VALUES
              ('2024-01-01 00:00:00+00', 1, 'jan'),
              ('2024-06-01 00:00:00+00', 2, 'jun')
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO partition_by_table VALUES ('2024-09-01 00:00:00+00', 3, 'sep')

                > CREATE MATERIALIZED VIEW partition_by_mv1
                  WITH (PARTITION BY (ts))
                  AS SELECT ts, count(*) AS cnt FROM partition_by_table GROUP BY ts
                """,
                """
                > INSERT INTO partition_by_table VALUES ('2024-12-01 00:00:00+00', 4, 'dec')
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT count(*) FROM partition_by_table
            4

            > SELECT cnt FROM partition_by_mv1 WHERE ts = '2024-09-01 00:00:00+00'
            1

            # Filter pushdown stats must show the source and never select more
            # than the total.
            $ set-regex match=\\d+ replacement=<N>

            > EXPLAIN FILTER PUSHDOWN FOR
              SELECT count(*) FROM partition_by_table WHERE ts >= '2024-11-01 00:00:00+00'
            materialize.public.partition_by_table <N> <N> <N> <N>

            > EXPLAIN FILTER PUSHDOWN FOR
              SELECT count(*) FROM partition_by_mv1 WHERE ts >= '2024-11-01 00:00:00+00'
            materialize.public.partition_by_mv<N> <N> <N> <N> <N>
            """))
