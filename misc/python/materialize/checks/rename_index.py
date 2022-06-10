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


class RenameIndex(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE rename_index_table (f1 INTEGER,f2 INTEGER);
                > CREATE INDEX rename_index_index1 ON rename_index_table (f2);
                > INSERT INTO rename_index_table VALUES (1,1);
                > CREATE MATERIALIZED VIEW rename_index_view1 AS SELECT f2 FROM rename_index_table WHERE f2 > 0;
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO rename_index_table VALUES (2,2);
                > ALTER INDEX rename_index_index1 RENAME TO rename_index_index2;
                > CREATE INDEX rename_index_index1 ON rename_index_table (f2);
                > CREATE MATERIALIZED VIEW rename_index_view2 AS SELECT f2 FROM rename_index_table WHERE f2 > 0;
                > INSERT INTO rename_index_table VALUES (3,3);
                """,
                """
                > INSERT INTO rename_index_table VALUES (4,4);
                > CREATE MATERIALIZED VIEW rename_index_view3 AS SELECT f2 FROM rename_index_table WHERE f2 > 0;
                > ALTER INDEX rename_index_index2 RENAME TO rename_index_index3;
                > ALTER INDEX rename_index_index1 RENAME TO rename_index_index2;
                > INSERT INTO rename_index_table VALUES (5,5);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW INDEXES FROM rename_index_table;
                default rename_index_table rename_index_index2 1 f2 <null> true
                default rename_index_table rename_index_index3 1 f2 <null> true

                > SELECT * FROM rename_index_view1;
                1
                2
                3
                4
                5

                > SELECT * FROM rename_index_view2;
                1
                2
                3
                4
                5

                > SELECT * FROM rename_index_view3;
                1
                2
                3
                4
                5
           """
            )
        )
