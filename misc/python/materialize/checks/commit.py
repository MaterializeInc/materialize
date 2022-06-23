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


class Commit(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE commit_table (f1 INTEGER);
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > BEGIN
                > INSERT INTO commit_table VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

                # We want the transaction to take more than 1 second
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO commit_table VALUES (11), (12), (13), (14), (15), (16), (17), (18), (19), (20);
                > COMMIT
                """,
                """
                > BEGIN
                > INSERT INTO commit_table VALUES (21), (22), (23), (24), (25), (26), (27), (28), (29), (30);

                # We want the transaction to take more than 1 second
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO commit_table VALUES (31), (32), (33), (34), (35), (36), (37), (38), (39), (40);
                > COMMIT
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*), COUNT(f1), COUNT(DISTINCT f1), MIN(f1), MAX(f1) FROM commit_table;
                40 40 40 1 40
           """
            )
        )
