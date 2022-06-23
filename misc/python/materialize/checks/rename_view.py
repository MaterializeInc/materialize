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


class RenameView(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE rename_view_table (f1 INTEGER, f2 INTEGER);
                > CREATE VIEW rename_view_viewA1 AS SELECT f2 FROM rename_view_table WHERE f2 > 0;
                > INSERT INTO rename_view_table VALUES (1,1);
                > CREATE VIEW rename_view_viewB1 AS SELECT f2 FROM rename_view_viewA1 WHERE f2 > 0;
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO rename_view_table VALUES (2,2);
                > ALTER VIEW rename_view_viewA1 RENAME TO rename_view_viewA2;
                > ALTER VIEW rename_view_viewB1 RENAME TO rename_view_viewB2;
                > INSERT INTO rename_view_table VALUES (3,3);
                """,
                """
                > INSERT INTO rename_view_table VALUES (4,4);
                > ALTER VIEW rename_view_viewB2 RENAME TO rename_view_viewB3;
                > ALTER VIEW rename_view_viewA2 RENAME TO rename_view_viewA3;
                > INSERT INTO rename_view_table VALUES (5,5);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW CREATE VIEW rename_view_viewB3;
                materialize.public.rename_view_viewb3 "CREATE VIEW \\"materialize\\".\\"public\\".\\"rename_view_viewb3\\" AS SELECT \\"f2\\" FROM \\"materialize\\".\\"public\\".\\"rename_view_viewa3\\" WHERE \\"f2\\" > 0"

                > SELECT * FROM rename_view_viewA3;
                1
                2
                3
                4
                5

                > SELECT * FROM rename_view_viewB3;
                1
                2
                3
                4
                5
           """
            )
        )
