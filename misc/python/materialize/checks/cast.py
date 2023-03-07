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


class Cast(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE cast_table (f1 INT);
            > INSERT INTO cast_table VALUES (0);
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW cast_view1 AS SELECT f1::bool AS c1, f1::int AS c2, f1::float AS c3, f1::numeric AS c4, f1::real AS c5, f1::text AS c6, f1::uint2 AS c7, f1::uint4 AS c8, f1::uint8 AS c9, f1::text AS c10, cast(f1 AS bool) AS c11 FROM cast_table WHERE f1 >= 0;
                > INSERT INTO cast_table VALUES (1);
            """,
                """
                > CREATE MATERIALIZED VIEW cast_view2 AS SELECT f1::bool AS c1, f1::int AS c2, f1::float AS c3, f1::numeric AS c4, f1::real AS c5, f1::text AS c6, f1::text AS c7, cast(f1 AS bool) AS c8 FROM cast_table;
                > INSERT INTO cast_table VALUES (-1);
            """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > SELECT * FROM cast_view1;
            false 0 0 0 0 0 0 0 0 0 false
            true 1 1 1 1 1 1 1 1 1 true

            > SELECT * FROM cast_view2;
            false 0 0 0 0 0 0 false
            true 1 1 1 1 1 1 true
            true -1 -1 -1 -1 -1 -1 true
            """
            )
        )
