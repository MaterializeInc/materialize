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
from materialize.util import MzVersion


class MaterializedViews(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE materialized_views_table (f1 STRING);
                > INSERT INTO materialized_views_table SELECT 'T1A' || generate_series FROM generate_series(1,10000);
                > INSERT INTO materialized_views_table SELECT 'T1B' || generate_series FROM generate_series(1,10000);
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO materialized_views_table SELECT 'T2A' || generate_series FROM generate_series(1, 10000);

                > CREATE MATERIALIZED VIEW materialized_view1 AS SELECT LEFT(f1, 3), COUNT(*) FROM materialized_views_table GROUP BY LEFT(f1, 3);

                > DELETE FROM materialized_views_table WHERE LEFT(f1, 3) = 'T1A';

                > INSERT INTO materialized_views_table SELECT 'T2B' || generate_series FROM generate_series(1, 10000);


                """,
                """
                > DELETE FROM materialized_views_table WHERE LEFT(f1, 3) = 'T2A';

                > CREATE MATERIALIZED VIEW materialized_view2 AS SELECT LEFT(f1, 3), COUNT(*) FROM materialized_views_table GROUP BY LEFT(f1, 3);

                > INSERT INTO materialized_views_table SELECT 'T3B' || generate_series FROM generate_series(1, 10000);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM materialized_view1
                T1B 10000
                T2B 10000
                T3B 10000

                > SELECT * FROM materialized_view2
                T1B 10000
                T2B 10000
                T3B 10000
           """
            )
        )


class MaterializedViewsAssertNotNull(Check):
    def _can_run(self, e: Executor) -> bool:
        # CREATE ROLE not compatible with older releases
        return self.base_version >= MzVersion.parse("0.73.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE not_null_table (x INT, y INT, z INT);
                > INSERT INTO not_null_table VALUES (NULL, 2, 3), (4, NULL, 6), (7, 8, NULL);
                > CREATE MATERIALIZED VIEW not_null_view1 WITH (ASSERT NOT NULL x) AS SELECT * FROM not_null_table;
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW not_null_view2 WITH (ASSERT NOT NULL y) AS SELECT * FROM not_null_table;
                > INSERT INTO not_null_table VALUES (NULL, 12, 13), (14, NULL, 16), (17, 18, NULL);
                """,
                """
                > CREATE MATERIALIZED VIEW not_null_view3 WITH (ASSERT NOT NULL z) AS SELECT * FROM not_null_table;
                > INSERT INTO not_null_table VALUES (NULL, 22, 23), (24, NULL, 26), (27, 28, NULL);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                ! SELECT * FROM not_null_view1
                contains: column 1 must not be null

                ! SELECT * FROM not_null_view2
                contains: column 2 must not be null

                ! SELECT * FROM not_null_view3
                contains: column 3 must not be null

                ! SELECT * FROM not_null_view1 WHERE x IS NOT NULL
                contains: column 1 must not be null

                ! SELECT * FROM not_null_view2 WHERE y IS NOT NULL
                contains: column 2 must not be null

                ! SELECT * FROM not_null_view3 WHERE z IS NOT NULL
                contains: column 3 must not be null

                ! SELECT y FROM not_null_view1
                contains: column 1 must not be null

                ! SELECT z FROM not_null_view2
                contains: column 2 must not be null

                ! SELECT x FROM not_null_view3
                contains: column 3 must not be null

                > DELETE FROM not_null_table WHERE x IS NULL;

                > SELECT * FROM not_null_view1
                4 <null> 6
                7 8 <null>
                14 <null> 16
                17 18 <null>
                24 <null> 26
                27 28 <null>

                > DELETE FROM not_null_table WHERE y IS NULL;

                > SELECT * FROM not_null_view2
                7 8 <null>
                17 18 <null>
                27 28 <null>

                > DELETE FROM not_null_table WHERE z IS NULL;

                ? EXPLAIN SELECT * FROM not_null_view1 WHERE x IS NOT NULL
                Explained Query:
                  ReadStorage materialize.public.not_null_view1

                ? EXPLAIN SELECT * FROM not_null_view2 WHERE y IS NOT NULL
                Explained Query:
                  ReadStorage materialize.public.not_null_view2

                ? EXPLAIN SELECT * FROM not_null_view3 WHERE z IS NOT NULL
                Explained Query:
                  ReadStorage materialize.public.not_null_view3

                > SELECT * FROM not_null_view3

                > INSERT INTO not_null_table VALUES (NULL, 2, 3), (4, NULL, 6), (7, 8, NULL);

                ! SELECT * FROM not_null_view1
                contains: column 1 must not be null

                ! SELECT * FROM not_null_view2
                contains: column 2 must not be null

                ! SELECT * FROM not_null_view3
                contains: column 3 must not be null
           """
            )
        )
