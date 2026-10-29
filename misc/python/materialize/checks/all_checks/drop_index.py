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


class DropIndex(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                > CREATE TABLE drop_index_table (f1 INTEGER, f2 INTEGER, f3 INTEGER);
                > INSERT INTO drop_index_table VALUES (1,1,1);
                > CREATE DEFAULT INDEX ON drop_index_table;
                > INSERT INTO drop_index_table VALUES (2,2,2);
                > CREATE INDEX drop_index_index1 ON drop_index_table (f1, f2);
                > INSERT INTO drop_index_table VALUES (3,3,3);
                > CREATE MATERIALIZED VIEW drop_index_view AS SELECT f1, f2 FROM drop_index_table WHERE f1 > 0;
                """))

    # An index cannot be dropped while a materialized view or a newer index reads
    # from it, so each phase drops the view and then the indexes newest first,
    # and recreates the view afterwards.
    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO drop_index_table VALUES (4,4,4);
                > DROP MATERIALIZED VIEW drop_index_view;
                > DROP INDEX drop_index_index1;
                > INSERT INTO drop_index_table VALUES (5,5,5);
                > CREATE INDEX drop_index_index2 ON drop_index_table (f1, f2);
                > CREATE MATERIALIZED VIEW drop_index_view AS SELECT f1, f2 FROM drop_index_table WHERE f1 > 0;
                > INSERT INTO drop_index_table VALUES (6,6,6);
                """,
                """
                > INSERT INTO drop_index_table VALUES (7,7,7);
                > DROP MATERIALIZED VIEW drop_index_view;
                > DROP INDEX drop_index_index2;
                > INSERT INTO drop_index_table VALUES (8,8,8);
                > DROP INDEX drop_index_table_primary_idx;
                > CREATE MATERIALIZED VIEW drop_index_view AS SELECT f1, f2 FROM drop_index_table WHERE f1 > 0;
                > INSERT INTO drop_index_table VALUES (9,9,9);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                > SELECT * FROM drop_index_table;
                1 1 1
                2 2 2
                3 3 3
                4 4 4
                5 5 5
                6 6 6
                7 7 7
                8 8 8
                9 9 9

                > SELECT f1 FROM drop_index_table;
                1
                2
                3
                4
                5
                6
                7
                8
                9

                > SELECT f3 FROM drop_index_table;
                1
                2
                3
                4
                5
                6
                7
                8
                9

                > SELECT * FROM drop_index_view;
                1 1
                2 2
                3 3
                4 4
                5 5
                6 6
                7 7
                8 8
                9 9
           """))


class DropIndexInUse(Check):
    """`DROP INDEX` refuses an index that a materialized view reads from, also
    after a restart or upgrade replans the view at bootstrap."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                > CREATE TABLE drop_in_use_table1 (f1 INTEGER);
                > INSERT INTO drop_in_use_table1 VALUES (1);
                > CREATE INDEX drop_in_use_idx1 ON drop_in_use_table1 (f1);
                > CREATE MATERIALIZED VIEW drop_in_use_view1 AS SELECT f1 FROM drop_in_use_table1 WHERE f1 > 0;
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO drop_in_use_table1 VALUES (2);
                > CREATE TABLE drop_in_use_table2 (f1 INTEGER);
                > INSERT INTO drop_in_use_table2 VALUES (1);
                > CREATE INDEX drop_in_use_idx2 ON drop_in_use_table2 (f1);
                > CREATE MATERIALIZED VIEW drop_in_use_view2 AS SELECT f1 FROM drop_in_use_table2 WHERE f1 > 0;
                """,
                """
                > INSERT INTO drop_in_use_table1 VALUES (3);
                > INSERT INTO drop_in_use_table2 VALUES (2);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        # Older versions allow the drop and orphan the view, so the refusal is
        # only asserted on versions that have it.
        return Testdrive(dedent("""
                ![version>=2604900] DROP INDEX drop_in_use_idx1
                contains:still depended upon by

                ![version>=2604900] DROP INDEX drop_in_use_idx2
                contains:still depended upon by

                > SELECT * FROM drop_in_use_view1
                1
                2
                3

                > SELECT * FROM drop_in_use_view2
                1
                2
            """))
