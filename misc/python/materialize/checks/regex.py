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


class RegexpExtract(Check):
    """The regex from regexp_extract has its own ProtoAnalyzedRegex"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE regexp_extract_table (f1 STRING);
            > INSERT INTO regexp_extract_table VALUES ('abc');
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW regexp_extract_view1 AS SELECT regexp_extract('((a)(.c))|((x)(.z))',f1) AS c1 FROM regexp_extract_table;
                > INSERT INTO regexp_extract_table VALUES ('klm');
            """,
                """
                > CREATE MATERIALIZED VIEW regexp_extract_view2 AS SELECT regexp_extract('((a)(.c))|((x)(.z))',f1) AS c1 FROM regexp_extract_table;
                > INSERT INTO regexp_extract_table VALUES ('xyz');
            """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > SELECT c1::string FROM regexp_extract_view1;
            (,,,xyz,x,yz)
            (abc,a,bc,,,)
            > SELECT c1::string FROM regexp_extract_view2;
            (,,,xyz,x,yz)
            (abc,a,bc,,,)
            """
            )
        )


class Regex(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE regex_table (f1 STRING, f2 STRING);
            > INSERT INTO regex_table VALUES ('abc', 'abc');
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW regex_view1 AS SELECT f1 ~ f2 AS c1, f1 ~* f2 AS c2, f1 ~ 'a.c|x.z' AS c3, f1 ~* 'a.c|x.z' AS c4 FROM regex_table;
                > INSERT INTO regex_table VALUES ('klm','klm');
            """,
                """
                > CREATE MATERIALIZED VIEW regex_view2 AS SELECT f1 ~ f2 AS c1, f1 ~* f2 AS c2, f1 ~ 'a.c|x.z' AS c3, f1 ~* 'a.c|x.z' AS c4 FROM regex_table;
                > INSERT INTO regex_table VALUES ('xyz','xyz');
            """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > SELECT * FROM regex_view1;
            true true false false
            true true true true
            true true true true

            > SELECT * FROM regex_view2;
            true true false false
            true true true true
            true true true true
            """
            )
        )
