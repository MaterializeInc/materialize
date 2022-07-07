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


class WideRows(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE wide_rows (f1 TEXT);
                > CREATE DEFAULT INDEX ON wide_rows;
                > INSERT INTO wide_rows VALUES (REPEAT('a', 16 * 1024 * 1024));
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO wide_rows VALUES (REPEAT('b', 16 * 1024 * 1024));
                """,
                """
                > INSERT INTO wide_rows SELECT * FROM wide_rows;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT LEFT(f1, 1), SUM(LENGTH(f1)) FROM wide_rows GROUP BY LEFT(f1, 1);
                a 33554432
                b 33554432
                """
            )
        )


class ManyRows(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE many_rows (f1 TEXT);
                > CREATE DEFAULT INDEX ON many_rows;
                > INSERT INTO many_rows SELECT 'a' || generate_series FROM generate_series(1, 1000000);
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO many_rows SELECT 'b' || generate_series FROM generate_series(1, 1000000);
                """,
                """
                > INSERT INTO many_rows SELECT * FROM many_rows;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT LEFT(f1, 1), COUNT(f1) FROM many_rows GROUP BY LEFT(f1, 1);
                a 2000000
                b 2000000
                """
            )
        )
