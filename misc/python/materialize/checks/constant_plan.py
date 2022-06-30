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


class ConstantPlan(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE constant_plan1 (f1 INT);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW constant_plan_view1 AS SELECT * FROM constant_plan1 WHERE FALSE
                """,
                """
                > CREATE MATERIALIZED VIEW constant_plan_view2 AS (VALUES (1), (2), (3))
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM constant_plan_view1;

                > SELECT * FROM constant_plan_view2;
                1
                2
                3
            """
            )
        )
