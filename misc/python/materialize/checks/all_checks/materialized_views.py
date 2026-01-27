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
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class MaterializedViews(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE materialized_views_table (f1 STRING);
                > INSERT INTO materialized_views_table SELECT 'T1A' || generate_series FROM generate_series(1,10000);
                > INSERT INTO materialized_views_table SELECT 'T1B' || generate_series FROM generate_series(1,10000);
                # Regression test for database-issues#8032.
                > CREATE MATERIALIZED VIEW zero_arity AS SELECT;
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
                > SELECT 1, * FROM zero_arity
                1
                """
            )
        )


class MaterializedViewsAssertNotNull(Check):
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
        sql = dedent(
            """
            ! SELECT * FROM not_null_view1
            contains: column "x" must not be null

            ! SELECT * FROM not_null_view2
            contains: column "y" must not be null

            ! SELECT * FROM not_null_view3
            contains: column "z" must not be null

            ! SELECT * FROM not_null_view1 WHERE x IS NOT NULL
            contains: column "x" must not be null

            ! SELECT * FROM not_null_view2 WHERE y IS NOT NULL
            contains: column "y" must not be null

            ! SELECT * FROM not_null_view3 WHERE z IS NOT NULL
            contains: column "z" must not be null

            ! SELECT y FROM not_null_view1
            contains: column "x" must not be null

            ! SELECT z FROM not_null_view2
            contains: column "y" must not be null

            ! SELECT x FROM not_null_view3
            contains: column "z" must not be null

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

            ?[version>=13500] EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM not_null_view1 WHERE x IS NOT NULL
            Explained Query:
              ReadStorage materialize.public.not_null_view1

            Source materialize.public.not_null_view1

            Target cluster: quickstart

            ?[version<13500] EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM not_null_view1 WHERE x IS NOT NULL
            Explained Query:
              ReadStorage materialize.public.not_null_view1

            Source materialize.public.not_null_view1

            Target cluster: quickstart

            ?[version>=13500] EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM not_null_view2 WHERE y IS NOT NULL
            Explained Query:
              ReadStorage materialize.public.not_null_view2

            Source materialize.public.not_null_view2

            Target cluster: quickstart

            ?[version<13500] EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM not_null_view2 WHERE y IS NOT NULL
            Explained Query:
              ReadStorage materialize.public.not_null_view2

            Source materialize.public.not_null_view2

            Target cluster: quickstart

            ?[version>=13500] EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM not_null_view3 WHERE z IS NOT NULL
            Explained Query:
              ReadStorage materialize.public.not_null_view3

            Source materialize.public.not_null_view3

            Target cluster: quickstart

            ?[version<13500] EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM not_null_view3 WHERE z IS NOT NULL
            Explained Query:
              ReadStorage materialize.public.not_null_view3

            Source materialize.public.not_null_view3

            Target cluster: quickstart

            > SELECT * FROM not_null_view3

            > INSERT INTO not_null_table VALUES (NULL, 2, 3), (4, NULL, 6), (7, 8, NULL);

            > INSERT INTO not_null_table VALUES (NULL, 12, 13), (14, NULL, 16), (17, 18, NULL);

            > INSERT INTO not_null_table VALUES (NULL, 22, 23), (24, NULL, 26), (27, 28, NULL);

            ! SELECT * FROM not_null_view1
            contains: column "x" must not be null

            ! SELECT * FROM not_null_view2
            contains: column "y" must not be null

            ! SELECT * FROM not_null_view3
            contains: column "z" must not be null
            """
        )

        return Testdrive(sql)


class MaterializedViewsRefresh(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE refresh_table (x INT);
                > INSERT INTO refresh_table VALUES (1);
                > CREATE MATERIALIZED VIEW refresh_view_2s_1 WITH (REFRESH EVERY '2 seconds') AS SELECT DISTINCT(x) FROM refresh_table;
                > CREATE MATERIALIZED VIEW refresh_view_at_1 WITH (REFRESH AT mz_now()::string::int8) AS SELECT DISTINCT(x) FROM refresh_table;
                > CREATE MATERIALIZED VIEW refresh_view_late_1 WITH (REFRESH AT mz_now()::string::int8 + 86400000) AS SELECT DISTINCT(x) FROM refresh_table;
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO refresh_table VALUES (2);
                > CREATE MATERIALIZED VIEW refresh_view_2s_2 WITH (REFRESH EVERY '2 seconds') AS SELECT DISTINCT(x) FROM refresh_table;
                > CREATE MATERIALIZED VIEW refresh_view_at_2 WITH (REFRESH AT mz_now()::string::int8) AS SELECT DISTINCT(x) FROM refresh_table;
                > CREATE MATERIALIZED VIEW refresh_view_late_2 WITH (REFRESH AT mz_now()::string::int8 + 86400000) AS SELECT DISTINCT(x) FROM refresh_table;
                """,
                """
                > INSERT INTO refresh_table VALUES (3);
                > CREATE MATERIALIZED VIEW refresh_view_2s_3 WITH (REFRESH EVERY '2 seconds') AS SELECT DISTINCT(x) FROM refresh_table;
                > CREATE MATERIALIZED VIEW refresh_view_at_3 WITH (REFRESH AT mz_now()::string::int8) AS SELECT DISTINCT(x) FROM refresh_table;
                > CREATE MATERIALIZED VIEW refresh_view_late_3 WITH (REFRESH AT mz_now()::string::int8 + 86400000) AS SELECT DISTINCT(x) FROM refresh_table;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > INSERT INTO refresh_table VALUES (4);

                > SELECT * FROM refresh_view_2s_1
                1
                2
                3
                4

                > SELECT * FROM refresh_view_2s_2
                1
                2
                3
                4

                > SELECT * FROM refresh_view_2s_3
                1
                2
                3
                4

                > SELECT * FROM refresh_view_at_1
                1

                > SELECT * FROM refresh_view_at_2
                1
                2

                > SELECT * FROM refresh_view_at_3
                1
                2
                3

                $ set-regex match=(s\\d+|\\d{13}|[ ]{12}0|u\\d{1,3}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)|\\(\\d+\\)) replacement=<>

                >[version<13900] EXPLAIN TIMESTAMP FOR SELECT * FROM refresh_view_late_1
                "                query timestamp: <> <>\\n          oracle read timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: false\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.refresh_view_late_1 (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n"
                >[version>=13900] EXPLAIN TIMESTAMP FOR SELECT * FROM refresh_view_late_1
                "                query timestamp: <> <>\\n          oracle read timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: false\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.refresh_view_late_1 (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (StorageInput([User<>])): [<> <>]\\n"

                >[version<13900] EXPLAIN TIMESTAMP FOR SELECT * FROM refresh_view_late_2
                "                query timestamp: <> <>\\n          oracle read timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: false\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.refresh_view_late_2 (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n"
                >[version>=13900] EXPLAIN TIMESTAMP FOR SELECT * FROM refresh_view_late_2
                "                query timestamp: <> <>\\n          oracle read timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: false\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.refresh_view_late_2 (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (StorageInput([User<>])): [<> <>]\\n"

                >[version<13900] EXPLAIN TIMESTAMP FOR SELECT * FROM refresh_view_late_3
                "                query timestamp: <> <>\\n          oracle read timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: false\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.refresh_view_late_3 (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n"
                >[version>=13900] EXPLAIN TIMESTAMP FOR SELECT * FROM refresh_view_late_3
                "                query timestamp: <> <>\\n          oracle read timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: false\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.refresh_view_late_3 (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (StorageInput([User<>])): [<> <>]\\n"
           """
            )
        )


class MaterializedViewReplacement(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.8.0")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE mv_replacement_table (a INT, b INT)
                > INSERT INTO mv_replacement_table VALUES (1, 2), (3, 4), (5, 6)
                > CREATE MATERIALIZED VIEW mv_replacement_target AS SELECT a, b FROM mv_replacement_table
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE REPLACEMENT MATERIALIZED VIEW mv_replacement_replacement FOR mv_replacement_target AS SELECT a + b AS a, b FROM mv_replacement_table

                > SELECT * FROM mv_replacement_target
                1 2
                3 4
                5 6

                > SELECT * FROM mv_replacement_replacement
                3  2
                7  4
                11 6
                """,
                """
                > ALTER MATERIALIZED VIEW mv_replacement_target APPLY REPLACEMENT mv_replacement_replacement

                > SELECT * FROM mv_replacement_target
                3  2
                7  4
                11 6

                > INSERT INTO mv_replacement_table VALUES (10, 20)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM mv_replacement_target
                3  2
                7  4
                11 6
                30 20

                > SELECT name FROM mz_materialized_views WHERE name LIKE 'mv_replacement_%'
                mv_replacement_target

                # Verify we can still create a new replacement.
                > CREATE REPLACEMENT MATERIALIZED VIEW mv_replacement_replacement FOR mv_replacement_target AS SELECT a * 2 AS a, b FROM mv_replacement_table

                > SELECT * FROM mv_replacement_replacement
                2  2
                6  4
                10 6
                20 20

                # Drop the replacement instead of applying it, to ensure
                # `validate` remains idempotent.
                > DROP MATERIALIZED VIEW mv_replacement_replacement
            """
            )
        )


def remove_target_cluster_from_explain(sql: str) -> str:
    return re.sub(r"\n\s*Target cluster: \w+\n", "", sql)
