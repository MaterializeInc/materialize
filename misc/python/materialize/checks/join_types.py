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


class JoinTypes(Check):
    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                    > CREATE SCHEMA join_schema;
                    > SET search_path=join_schema;

                    > CREATE TABLE join_schema.t1 (f1 INTEGER);
                    > CREATE TABLE join_schema.t1a (f1 INTEGER);
                    > CREATE TABLE join_schema.t2 (f2 INTEGER);
                    > CREATE MATERIALIZED VIEW join_schema.comma_join AS SELECT * FROM t1 , t2;
                    > CREATE MATERIALIZED VIEW join_schema.cross_join AS SELECT * FROM t1 CROSS JOIN t2;
                    > CREATE MATERIALIZED VIEW join_schema.natural_join AS SELECT * FROM t1 NATURAL JOIN t1a;
                    > CREATE MATERIALIZED VIEW join_schema.full_outer_join AS SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.f1 = t2.f2;
                    > CREATE MATERIALIZED VIEW join_schema.left_join AS SELECT * FROM t1 LEFT JOIN t2 ON t1.f1 = t2.f2;
                    > CREATE MATERIALIZED VIEW join_schema.right_join AS SELECT * FROM t1 RIGHT JOIN t2 ON t1.f1 = t2.f2;

                    > CREATE MATERIALIZED VIEW join_schema.join_on AS SELECT * FROM t1 INNER JOIN t2 ON (t1.f1 < t2.f2);
                    > CREATE MATERIALIZED VIEW join_schema.join_using AS SELECT * FROM t1 INNER JOIN t1a USING (f1);

                    > CREATE MATERIALIZED VIEW join_schema.join_table_alias AS SELECT a1.f1, a2.f2 FROM t1 AS a1 JOIN t2 AS a2 ON a1.f1 = a2.f2;
                    > CREATE MATERIALIZED VIEW join_schema.join_column_alias AS SELECT t1.a1, t2.a2 FROM t1 AS t1 (a1), t2 AS t2 (a2);

                    > CREATE MATERIALIZED VIEW join_schema.lateral_join AS SELECT * FROM t1 LATERAL JOIN ( SELECT f2 + 1 AS f2 FROM t2 ) ON TRUE;

                    > INSERT INTO join_schema.t1 VALUES (NULL), (1), (2);
                    > INSERT INTO join_schema.t1a VALUES (NULL), (1), (2);
                """,
                """
                    > INSERT INTO join_schema.t2 VALUES (2), (3), (NULL);
                    """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET search_path=join_schema;

                > SELECT * FROM comma_join;
                1 2
                1 3
                1 <null>
                2 2
                2 3
                2 <null>
                <null> 2
                <null> 3
                <null> <null>

                > SELECT * FROM cross_join;
                1 2
                1 3
                1 <null>
                2 2
                2 3
                2 <null>
                <null> 2
                <null> 3
                <null> <null>

                > SELECT * FROM natural_join;
                1
                2

                > SELECT * FROM full_outer_join;
                1 <null>
                2 2
                <null> 3
                <null> <null>
                <null> <null>

                > SELECT * FROM left_join;
                1 <null>
                2 2
                <null> <null>

                > SELECT * FROM right_join;
                2 2
                <null> 3
                <null> <null>

                > SELECT * FROM join_on;
                1 2
                1 3
                2 3

                > SELECT * FROM join_using;
                1
                2

                > SELECT * FROM join_table_alias;
                2 2

                > SELECT * FROM join_column_alias;
                a1 a2
                ----
                1 2
                1 3
                1 <null>
                2 2
                2 3
                2 <null>
                <null> 2
                <null> 3
                <null> <null>

                > SELECT * FROM lateral_join;
                1 3
                1 4
                1 <null>
                2 3
                2 4
                2 <null>
                <null> 3
                <null> 4
                <null> <null>
                """
            )
        )
