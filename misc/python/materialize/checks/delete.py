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


class Delete(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE delete_table (f1 INTEGER);
                > INSERT INTO delete_table SELECT * FROM generate_series(1,10000);
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > DELETE FROM delete_table WHERE f1 % 3 = 0;
                """,
                """
                > DELETE FROM delete_table WHERE f1 % 3 = 1;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*), MIN(f1), MAX(f1), COUNT(f1), COUNT(DISTINCT f1) FROM delete_table GROUP BY f1 % 3;
                3333 2 9998 3333 3333
                """
            )
        )
