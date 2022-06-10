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


class Having(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE having_table (f1 INTEGER, f2 INTEGER);
            > INSERT INTO having_table VALUES (1, 1);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW having_view1 AS
                  SELECT f1, SUM(f1) FROM having_table GROUP BY f1 HAVING SUM(f1) > 1 AND SUM(f1) < 3;
                > INSERT INTO having_table VALUES (2, 2);
                """,
                """
                > CREATE MATERIALIZED VIEW having_view2 AS
                  SELECT f1, SUM(f1) FROM having_table GROUP BY f1 HAVING SUM(f1) > 1 AND SUM(f1) < 3;
                > INSERT INTO having_table VALUES (3, 3);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM having_view1;
                2 2
                > SELECT * FROM having_view2;
                2 2
            """
            )
        )
