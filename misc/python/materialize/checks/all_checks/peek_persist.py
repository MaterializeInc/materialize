# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class PeekPersist(Check):
    """Make sure old data can still be read by the PeekPersist LIMIT optimization"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            """
            > CREATE TABLE peek_persist (f1 INTEGER);
            > INSERT INTO peek_persist VALUES (1), (2), (3), (NULL);
            """
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(s)
            for s in [
                """
                > INSERT INTO peek_persist VALUES (NULL), (1), (2), (3);
                > UPDATE peek_persist SET f1 = f1 + 1;
            """,
                """
                > INSERT INTO peek_persist VALUES (3), (NULL), (1), (2);
                > DELETE FROM peek_persist WHERE f1 = 4;
            """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            """
            > SELECT * FROM peek_persist LIMIT 100
            <null>
            <null>
            <null>
            1
            2
            2
            2
            3
            3
            3

            # TODO(bkirwi): revisit this when persist peeks have stabilized
            # ? EXPLAIN SELECT * FROM peek_persist LIMIT 100
            # Explained Query (fast path):
            #   Finish limit=100 output=[#0]
            #     PeekPersist materialize.public.peek_persist
            """
        )
