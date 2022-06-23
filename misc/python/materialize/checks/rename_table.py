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


class RenameTable(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE rename_table1 (f1 INTEGER);
                > INSERT INTO rename_table1 VALUES (1);
                > CREATE MATERIALIZED VIEW rename_table_view AS SELECT DISTINCT f1 FROM rename_table1;
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO rename_table1 VALUES (2);
                > ALTER TABLE rename_table1 RENAME TO rename_table2;
                > INSERT INTO rename_table2 VALUES (3);
                """,
                """
                > INSERT INTO rename_table2 VALUES (4);
                > ALTER TABLE rename_table2 RENAME TO rename_table3;
                > INSERT INTO rename_table3 VALUES (5);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM rename_table3;
                1
                2
                3
                4
                5

                > SELECT * FROM rename_table_view;
                1
                2
                3
                4
                5
           """
            )
        )
