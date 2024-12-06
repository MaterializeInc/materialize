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


class WithMutuallyRecursive(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE MATERIALIZED VIEW wmr1 AS WITH MUTUALLY RECURSIVE (RETURN AT RECURSION LIMIT 100)
                  foo (a int, b int) AS (SELECT 1, 2 UNION SELECT a, 7 FROM bar),
                  bar (a int) as (SELECT a FROM foo)
                  SELECT * FROM bar;
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE DEFAULT INDEX ON wmr1;

                > CREATE MATERIALIZED VIEW wmr1A AS SELECT a + 1 FROM wmr1;

                > CREATE MATERIALIZED VIEW wmr2 AS WITH MUTUALLY RECURSIVE (ERROR AT RECURSION LIMIT 1)
                  foo (a int, b int) AS (SELECT 1, 2 UNION SELECT a, 7 FROM bar),
                  bar (a int) as (SELECT a FROM foo)
                  SELECT * FROM bar;
                """,
                """
                > CREATE MATERIALIZED VIEW wmr2A AS SELECT a + 1 FROM wmr2;

                > CREATE MATERIALIZED VIEW wmr3 AS WITH MUTUALLY RECURSIVE
                  foo (a int, b int) AS (SELECT 1, 2 UNION SELECT a, 7 FROM bar),
                  bar (a int) as (SELECT a FROM foo)
                  SELECT * FROM bar;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM wmr1
                1
                1

                > SELECT * FROM wmr1A
                2
                2

                ! SELECT * FROM wmr2
                contains: Recursive query exceeded the recursion

                ! SELECT * FROM wmr2A
                contains: Recursive query exceeded the recursion

                > SELECT * FROM wmr3
                1
                1
           """
            )
        )
