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


class MaterializeType(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE view v1 (a) AS SELECT 1::mz_timestamp;
        """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE VIEW v2 (a) AS SELECT 1::uint4;
                """,
                """
                > CREATE VIEW v3 (a) AS SELECT 1::uint8;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM v1;
                1

                > SELECT * FROM v2;
                1

                > SELECT * FROM v3;
                1
            """
            )
        )
