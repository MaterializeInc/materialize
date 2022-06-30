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


class Threshold(Check):
    """Exercise the 'threshold' portion of a MFP plan"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE threshold_table1 (f1 INT);
            > INSERT INTO threshold_table1 VALUES (0);

            > CREATE TABLE threshold_table2 (f1 INT);
            > INSERT INTO threshold_table2 VALUES (1);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO threshold_table1 VALUES (1);
                > CREATE MATERIALIZED VIEW threshold_view1 AS SELECT * FROM threshold_table1 EXCEPT SELECT * FROM threshold_table2;
                > INSERT INTO threshold_table2 VALUES (2);
                """,
                """
                > INSERT INTO threshold_table1 VALUES (2);
                > CREATE MATERIALIZED VIEW threshold_view2 AS SELECT * FROM threshold_table2 EXCEPT ALL SELECT * FROM threshold_table1;
                > INSERT INTO threshold_table2 VALUES (3);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM threshold_view1;
                0

                > SELECT * FROM threshold_view2;
                3
            """
            )
        )
