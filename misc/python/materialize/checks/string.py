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


class String(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE string_table (f1 STRING, f2 STRING, f3 STRING, f4 INT, f5 INT, f6 INT[]);
            > INSERT INTO string_table VALUES (' foobar ', ' abc ', ' xyz ', 2, 3, '{1,NULL,3}');
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW string_view1 AS SELECT
                    f1 BETWEEN f2 AND f3 AS c1,
                    'foo' BETWEEN 'abc' AND 'xyz' AS c2,
                    f1 NOT BETWEEN f2 AND f3 AS c3,
                    substring(f1 FROM f4 FOR f5) AS c4,
                    substring(f1 FROM f4) AS c5,
                    substring(f1 FOR f5) AS c6,
                    trim(BOTH f1) AS c7,
                    trim(LEADING f1) AS c8,
                    trim(TRAILING f1) AS c9,
                    trim(LEADING ' f' FROM f1) AS c10,
                    trim(' fr' FROM f1) AS c11,
                    array_to_string(f6, ',', 'NULL') AS c12
                  FROM string_table;
                > INSERT INTO string_table VALUES (' foo ', ' abc ', ' xyz ', 2, 3, '{1,2,3,4,NULL,NULL}');
            """,
                """
                > CREATE MATERIALIZED VIEW string_view2 AS SELECT
                    f1 BETWEEN f2 AND f3 AS c1,
                    'foo' BETWEEN 'abc' AND 'xyz' AS c2,
                    f1 NOT BETWEEN f2 AND f3 AS c3,
                    substring(f1 FROM f4 FOR f5) AS c4,
                    substring(f1 FROM f4) AS c5,
                    substring(f1 FOR f5) AS c6,
                    trim(BOTH f1) AS c7,
                    trim(LEADING f1) AS c8,
                    trim(TRAILING f1) AS c9,
                    trim(LEADING ' f' FROM f1) AS c10,
                    trim(' fr' FROM f1) AS c11,
                    array_to_string(f6, ',', 'NULL') AS c12
                  FROM string_table;
                > INSERT INTO string_table VALUES (' bar ', 'abc', 'xyz', 2, 3, '{NULL}');
            """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > SELECT * FROM string_view1;
            true true false foo "foobar " " fo" "foobar" "foobar " " foobar" "oobar " "ooba" 1,NULL,3
            true true false foo "foo " " fo" foo "foo " " foo" "oo " oo 1,2,3,4,NULL,NULL
            false true true bar "bar " " ba" bar "bar " " bar" "bar " ba NULL

            > SELECT * FROM string_view2;
            true true false foo "foobar " " fo" "foobar" "foobar " " foobar" "oobar " "ooba" 1,NULL,3
            true true false foo "foo " " fo" foo "foo " " foo" "oo " oo 1,2,3,4,NULL,NULL
            false true true bar "bar " " ba" bar "bar " " bar" "bar " ba NULL
            """
            )
        )
