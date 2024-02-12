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


class Like(Check):
    """LIKE, ILIKE, with and without a constant pattern are all compiled and evaluated differently"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE like_regex_table (f1 STRING, f2 STRING, f3 STRING, f4 STRING);
            > INSERT INTO like_regex_table VALUES ('abc', 'abc', 'a~%', '~');
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW like_regex_view1 AS SELECT f1 LIKE f2 AS c1, f1 ILIKE f2 AS c2, f1 LIKE 'x_z' AS c3, f1 ILIKE 'a_c' AS c4, f1 LIKE 'a~%' ESCAPE '~' AS c5, f1 LIKE f3 ESCAPE '~' AS c6, f1 LIKE f3 ESCAPE f4 AS c7 FROM like_regex_table;
                > INSERT INTO like_regex_table VALUES ('klm', 'klm', 'k_m', 'k');
            """,
                """
                > CREATE MATERIALIZED VIEW like_regex_view2 AS SELECT f1 LIKE f2 AS c1, f1 ILIKE f2 AS c2, f1 LIKE 'x_z' AS c3, f1 ILIKE 'a_c' AS c4, f1 LIKE 'a~%' ESCAPE '~' AS c5, f1 LIKE f3 ESCAPE '~' AS c6, f1 LIKE f3 ESCAPE f4 AS c7 FROM like_regex_table;
                > INSERT INTO like_regex_table VALUES ('xyz', 'xyz', 'x_%', '_');
            """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > SELECT * FROM like_regex_view1;
            true true false true false false false
            true true false false false true false
            true true true false false true false

            > SELECT * FROM like_regex_view2;
            true true false true false false false
            true true false false false true false
            true true true false false true false
            """
            )
        )
