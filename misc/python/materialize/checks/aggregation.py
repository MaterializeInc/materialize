# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class Aggregation(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE SCHEMA aggregation_schema

            > CREATE TABLE aggregation_schema.t1 (f1 INTEGER, f2 INTEGER, f3 INTEGER);

            > INSERT INTO aggregation_schema.t1 VALUES (1,1,1);
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                    > SET search_path=aggregation_schema;

                    > CREATE MATERIALIZED VIEW aggregation_schema.order_by1 AS SELECT * FROM t1 ORDER BY f3 DESC, f2 ASC , f1 DESC LIMIT 999999999;

                    > CREATE MATERIALIZED VIEW aggregation_schema.limit_one1 AS SELECT * FROM t1 LIMIT 1;

                    > CREATE MATERIALIZED VIEW aggregation_schema.limit_many1 AS SELECT * FROM t1 LIMIT 999999999;

                    > CREATE MATERIALIZED VIEW aggregation_schema.top_level_distinct1 AS SELECT DISTINCT f1, f2 FROM t1;

                    > CREATE MATERIALIZED VIEW aggregation_schema.global_count1 AS SELECT COUNT(*) FROM t1;

                    > CREATE MATERIALIZED VIEW aggregation_schema.global_aggregation1 AS SELECT COUNT(f1), MIN(f1), MAX(f1), SUM(f1) FROM t1;

                    > CREATE MATERIALIZED VIEW aggregation_schema.global_aggregation_distinct1 AS SELECT COUNT(DISTINCT f1), MIN(DISTINCT f1), MAX(DISTINCT f1), SUM(DISTINCT f1) FROM t1;

                    > INSERT INTO aggregation_schema.t1 VALUES (2,2,2), (3,3,3), (NULL, NULL, NULL);
                    """,
                """
                    > SET search_path=aggregation_schema;

                    > INSERT INTO aggregation_schema.t1 VALUES (3,3,3), (4,4,4), (NULL, NULL, NULL);

                    > CREATE MATERIALIZED VIEW aggregation_schema.order_by2 AS SELECT * FROM t1 ORDER BY f3 DESC, f2 ASC , f1 DESC LIMIT 999999999;

                    > CREATE MATERIALIZED VIEW aggregation_schema.limit_one2 AS SELECT * FROM t1 LIMIT 1;

                    > CREATE MATERIALIZED VIEW aggregation_schema.limit_many2 AS SELECT * FROM t1 LIMIT 999999999;

                    > CREATE MATERIALIZED VIEW aggregation_schema.top_level_distinct2 AS SELECT DISTINCT f1, f2 FROM t1;

                    > CREATE MATERIALIZED VIEW aggregation_schema.global_count2 AS SELECT COUNT(*) FROM t1;

                    > CREATE MATERIALIZED VIEW aggregation_schema.global_aggregation2 AS SELECT COUNT(f1), MIN(f1), MAX(f1), SUM(f1) FROM t1;

                    > CREATE MATERIALIZED VIEW aggregation_schema.global_aggregation_distinct2 AS SELECT COUNT(DISTINCT f1), MIN(DISTINCT f1), MAX(DISTINCT f1), SUM(DISTINCT f1) FROM t1;

                    > INSERT INTO aggregation_schema.t1 VALUES (5,5,5), (6,6,6), (NULL, NULL, NULL);
                    """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET search_path=aggregation_schema;

                > SELECT * FROM order_by1;
                1 1 1
                2 2 2
                3 3 3
                3 3 3
                4 4 4
                5 5 5
                6 6 6
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>

                > SELECT * FROM limit_one1;
                1 1 1

                > SELECT * FROM limit_many1;
                1 1 1
                2 2 2
                3 3 3
                3 3 3
                4 4 4
                5 5 5
                6 6 6
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>

                > SELECT * FROM top_level_distinct1;
                1 1
                2 2
                3 3
                4 4
                5 5
                6 6
                <null> <null>

                > SELECT * FROM global_count1;
                10

                > SELECT * FROM global_aggregation1;
                7 1 6 24

                > SELECT * FROM global_aggregation_distinct1;
                6 1 6 21

                > SELECT * FROM order_by2;
                1 1 1
                2 2 2
                3 3 3
                3 3 3
                4 4 4
                5 5 5
                6 6 6
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>

                > SELECT * FROM limit_one2;
                1 1 1

                > SELECT * FROM limit_many2;
                1 1 1
                2 2 2
                3 3 3
                3 3 3
                4 4 4
                5 5 5
                6 6 6
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>

                > SELECT * FROM top_level_distinct2;
                1 1
                2 2
                3 3
                4 4
                5 5
                6 6
                <null> <null>

                > SELECT * FROM global_count2;
                10

                > SELECT * FROM global_aggregation2;
                7 1 6 24

                > SELECT * FROM global_aggregation_distinct2;
                6 1 6 21
            """
            )
        )
