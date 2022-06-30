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


class Update(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE update_table (f1 STRING, f2 BIGINT);
                > INSERT INTO update_table SELECT 'T1', generate_series FROM generate_series(1,10000);
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > UPDATE update_table SET f2 = f2 * 1000;
                """,
                """
                > UPDATE update_table SET f1 = 'T2', f2 = f2 * 1000;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT MIN(f1), MAX(f1), MIN(f2), MAX(f2), COUNT(*), COUNT(DISTINCT f2) FROM update_table;
                T2 T2 1000000 10000000000 10000 10000
           """
            )
        )
