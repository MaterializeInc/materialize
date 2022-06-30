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


class DropIndex(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE drop_index_table (f1 INTEGER, f2 INTEGER, f3 INTEGER);
                > INSERT INTO drop_index_table VALUES (1,1,1);
                > CREATE DEFAULT INDEX ON drop_index_table;
                > INSERT INTO drop_index_table VALUES (2,2,2);
                > CREATE INDEX drop_index_index1 ON drop_index_table (f1, f2);
                > INSERT INTO drop_index_table VALUES (3,3,3);
                > CREATE MATERIALIZED VIEW drop_index_view AS SELECT f1, f2 FROM drop_index_table WHERE f1 > 0;
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO drop_index_table VALUES (4,4,4);
                > DROP INDEX drop_index_index1;
                > INSERT INTO drop_index_table VALUES (5,5,5);
                > CREATE INDEX drop_index_index2 ON drop_index_table (f1, f2);
                > INSERT INTO drop_index_table VALUES (6,6,6);
                """,
                """
                > INSERT INTO drop_index_table VALUES (7,7,7);
                > DROP INDEX drop_index_table_primary_idx;
                > INSERT INTO drop_index_table VALUES (8,8,8);
                > DROP INDEX drop_index_index2;
                > INSERT INTO drop_index_table VALUES (9,9,9);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
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
           """
            )
        )
