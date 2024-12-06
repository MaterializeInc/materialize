# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class ExplainCatalogItem(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE explain_item_t1(x int, y int);

                > CREATE TABLE explain_item_t2(x int, y int);

                > CREATE INDEX explain_item_t1_y ON explain_item_t1(y);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE OR REPLACE MATERIALIZED VIEW explain_mv1 AS
                  SELECT * FROM explain_item_t1 WHERE y = 7;

                > CREATE OR REPLACE MATERIALIZED VIEW explain_mv2 AS
                  SELECT * FROM explain_item_t2 WHERE y = 7;
                """,
                """
                > CREATE INDEX explain_item_t2_y ON explain_item_t2(y);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        # 1. Check the MV plans.
        # 2. Re-create explain_mv2 and check its plan again - it should be
        #    almost identical to the plan for explain_mv1 after picking up
        #    explain_item_t2_y as a used index.
        sql = dedent(
            """
            ? EXPLAIN MATERIALIZED VIEW explain_mv1;
            materialize.public.explain_mv1:
              Project (#0, #1)
                ReadIndex on=materialize.public.explain_item_t1 explain_item_t1_y=[lookup value=(7)]

            Used Indexes:
              - materialize.public.explain_item_t1_y (lookup)

            Target cluster: quickstart


            ? EXPLAIN MATERIALIZED VIEW explain_mv2;
            materialize.public.explain_mv2:
              Filter (#1 = 7)
                ReadStorage materialize.public.explain_item_t2

            Source materialize.public.explain_item_t2
              filter=((#1 = 7))

            Target cluster: quickstart


            > CREATE OR REPLACE MATERIALIZED VIEW explain_mv2_new AS
              SELECT * FROM explain_item_t2 WHERE y = 7;


            ? EXPLAIN MATERIALIZED VIEW explain_mv2_new;
            materialize.public.explain_mv2_new:
              Project (#0, #1)
                ReadIndex on=materialize.public.explain_item_t2 explain_item_t2_y=[lookup value=(7)]

            Used Indexes:
              - materialize.public.explain_item_t2_y (lookup)

            Target cluster: quickstart
            """
        )

        return Testdrive(sql)


def remove_target_cluster_from_explain(sql: str) -> str:
    return re.sub(r"\n\s*Target cluster: \w+\n", "", sql)
