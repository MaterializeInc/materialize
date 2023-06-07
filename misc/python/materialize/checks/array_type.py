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
from materialize.util import MzVersion


class ArrayType(Check):
    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.58.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE array_type_table(int_col int[], text_col text[], array_col int[][]);

            > INSERT INTO array_type_table VALUES (array_fill(2, ARRAY[2], ARRAY[2]), array_fill('foo'::text, ARRAY[2]), ARRAY[ARRAY[1,2], ARRAY[NULL, 4]]);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW array_type_view1 AS SELECT
                  int_col, array_fill(2, ARRAY[2], ARRAY[2]),
                  text_col, array_fill('foo'::text, ARRAY[2]) AS array_fill2,
                  array_col, ARRAY[ARRAY[1,2], ARRAY[NULL, 4]]
                  FROM array_type_table;

                > INSERT INTO array_type_table SELECT * FROM array_type_table LIMIT 1;
                """,
                """
                > CREATE MATERIALIZED VIEW array_type_view2 AS SELECT
                  int_col, array_fill(2, ARRAY[2], ARRAY[2]),
                  text_col, array_fill('foo'::text, ARRAY[2]) AS array_fill2,
                  array_col, ARRAY[ARRAY[1,2], ARRAY[NULL, 4]]
                  FROM array_type_table;

                > INSERT INTO array_type_table SELECT * FROM array_type_table LIMIT 1;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT int_col::text, array_fill::text, text_col::text, array_fill2::text, array_col::text, "array"::text FROM array_type_view1;
                [2:3]={2,2} [2:3]={2,2} {foo,foo} {foo,foo} {{1,2},{NULL,4}} {{1,2},{NULL,4}}
                [2:3]={2,2} [2:3]={2,2} {foo,foo} {foo,foo} {{1,2},{NULL,4}} {{1,2},{NULL,4}}
                [2:3]={2,2} [2:3]={2,2} {foo,foo} {foo,foo} {{1,2},{NULL,4}} {{1,2},{NULL,4}}

                > SELECT int_col::text, array_fill::text, text_col::text, array_fill2::text, array_col::text, "array"::text FROM array_type_view2;
                [2:3]={2,2} [2:3]={2,2} {foo,foo} {foo,foo} {{1,2},{NULL,4}} {{1,2},{NULL,4}}
                [2:3]={2,2} [2:3]={2,2} {foo,foo} {foo,foo} {{1,2},{NULL,4}} {{1,2},{NULL,4}}
                [2:3]={2,2} [2:3]={2,2} {foo,foo} {foo,foo} {{1,2},{NULL,4}} {{1,2},{NULL,4}}
            """
            )
        )
