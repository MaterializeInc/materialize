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


class WindowFunctions(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE window_functions_table (f1 INTEGER, f2 INTEGER);
            > INSERT INTO window_functions_table VALUES (1,1), (2, 1), (3, 1);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW window_functions_view1 AS
                  SELECT
                  row_number() OVER (PARTITION BY f1 ORDER BY f2),
                  dense_rank() OVER (PARTITION BY f2 ORDER BY f1 DESC),
                  lag(f1, f2, f1) OVER (ORDER BY f2),
                  lead(f2, f1, f2) OVER (PARTITION BY f1),
                  row_number() OVER (RANGE UNBOUNDED PRECEDING) AS range1,
                  row_number() OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range2,
                  row_number() OVER (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS rows_between,
                  first_value(f1) OVER (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
                  FROM window_functions_table;
                > INSERT INTO window_functions_table VALUES (1, 2), (2, 2), (3, 2);
                """,
                """
                > CREATE MATERIALIZED VIEW window_functions_view2 AS
                  SELECT
                  row_number() OVER (PARTITION BY f1 ORDER BY f2),
                  dense_rank() OVER (PARTITION BY f2 ORDER BY f1 DESC),
                  lag(f1, f2, f1) OVER (ORDER BY f2),
                  lead(f2, f1, f2) OVER (PARTITION BY f1),
                  row_number() OVER (RANGE UNBOUNDED PRECEDING) AS range1,
                  row_number() OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range2,
                  row_number() OVER (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS rows_between,
                  first_value(f1) OVER (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
                  FROM window_functions_table;
                > INSERT INTO window_functions_table VALUES (1, 2), (2, 2), (3, 2);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM window_functions_view1;
                1 1 2 1 7 7 7 2
                1 2 1 2 4 4 4 1
                1 3 1 2 1 1 1 <null>
                2 1 2 2 8 8 8 2
                2 2 1 2 5 5 5 1
                2 3 2 2 2 2 2 1
                3 1 2 2 9 9 9 3
                3 2 1 2 6 6 6 2
                3 3 3 2 3 3 3 1
                > SELECT * FROM window_functions_view2;
                1 1 2 1 7 7 7 2
                1 2 1 2 4 4 4 1
                1 3 1 2 1 1 1 <null>
                2 1 2 2 8 8 8 2
                2 2 1 2 5 5 5 1
                2 3 2 2 2 2 2 1
                3 1 2 2 9 9 9 3
                3 2 1 2 6 6 6 2
                3 3 3 2 3 3 3 1
            """
            )
        )
