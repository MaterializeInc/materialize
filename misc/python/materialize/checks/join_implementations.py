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


class DeltaJoin(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE delta_join_table1 (f1 INT, f2 INT);
            > CREATE INDEX delta_join_index1 ON delta_join_table1(f1);
            > INSERT INTO delta_join_table1 VALUES (1, 1);

            > CREATE TABLE delta_join_table2 (f3 INT, f4 INT);
            > CREATE INDEX delta_join_index2 ON delta_join_table2(f3);
            > INSERT INTO delta_join_table2 VALUES (1, 1);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO delta_join_table1 VALUES (2, 2);
                > CREATE MATERIALIZED VIEW delta_join_view1 AS SELECT * FROM delta_join_table1, delta_join_table2 WHERE delta_join_table1.f1 = delta_join_table2.f3;
                > INSERT INTO delta_join_table2 VALUES (2, 2);
                """,
                """
                > INSERT INTO delta_join_table1 VALUES (3, 3);
                > CREATE MATERIALIZED VIEW delta_join_view2 AS SELECT * FROM delta_join_table1, delta_join_table2 WHERE delta_join_table1.f1 = delta_join_table2.f3;
                > INSERT INTO delta_join_table2 VALUES (3, 3);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM delta_join_view1;
                1 1 1 1
                2 2 2 2
                3 3 3 3

                > SELECT * FROM delta_join_view2;
                1 1 1 1
                2 2 2 2
                3 3 3 3
            """
            )
        )


class LinearJoin(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE linear_join_table1 (f1 INT);
            > INSERT INTO linear_join_table1 VALUES (1);

            > CREATE TABLE linear_join_table2 (f2 INT);
            > INSERT INTO linear_join_table2 VALUES (1);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO linear_join_table1 VALUES (2);
                > CREATE MATERIALIZED VIEW linear_join_view1 AS SELECT * FROM linear_join_table1, linear_join_table2 WHERE linear_join_table1.f1 = linear_join_table2.f2;
                > INSERT INTO linear_join_table2 VALUES (2);
                """,
                """
                > INSERT INTO linear_join_table1 VALUES (3);
                > CREATE MATERIALIZED VIEW linear_join_view2 AS SELECT * FROM linear_join_table1, linear_join_table2 WHERE linear_join_table1.f1 = linear_join_table2.f2;
                > INSERT INTO linear_join_table2 VALUES (3);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM linear_join_view1;
                1 1
                2 2
                3 3

                > SELECT * FROM linear_join_view2;
                1 1
                2 2
                3 3
            """
            )
        )
