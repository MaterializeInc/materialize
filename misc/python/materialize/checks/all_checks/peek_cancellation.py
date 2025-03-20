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
from materialize.checks.checks import Check, disabled


@disabled("due to database-issues#6249")
class PeekCancellation(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE peek_cancellation (f1 INTEGER);
                > CREATE DEFAULT INDEX ON peek_cancellation;
                > INSERT INTO peek_cancellation SELECT * FROM generate_series(1, 10000);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > SET statement_timeout = '10ms';
                ! INSERT INTO peek_cancellation SELECT * FROM peek_cancellation;
                contains: timeout
                """,
                """
                > SET statement_timeout = '10ms';
                ! INSERT INTO peek_cancellation SELECT * FROM peek_cancellation;
                contains: timeout
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET statement_timeout = '10ms';
                ! INSERT INTO peek_cancellation SELECT * FROM peek_cancellation;
                contains: timeout
                """
            )
        )
