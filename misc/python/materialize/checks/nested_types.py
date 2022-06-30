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


class NestedTypes(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TYPE record_type AS (f1 INT, f2 STRING);
            > CREATE TYPE map_type AS MAP (key_type=text, value_type=integer);

            > CREATE TYPE int4_list AS LIST (element_type = int4);
            > CREATE TYPE int4_list_list AS LIST (element_type = int4_list);

            > CREATE TABLE nested_types_table(map_col map_type, list_col int4_list_list, record_col record_type, array_col STRING);

            > INSERT INTO nested_types_table VALUES ('{a => 1, b => 2}'::map_type, '{{1,2},{3,4}}'::int4_list_list, ROW(1, 'abc'), ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW nested_types_view1 AS SELECT
                  map_col, '{a => 1, b => 2}'::map_type,
                  list_col, '{{1,2},{3,4}}'::int4_list_list,
                  record_col, ROW(1, 'abc'),
                  array_col, ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]
                  FROM nested_types_table;

                > INSERT INTO nested_types_table SELECT * FROM nested_types_table LIMIT 1;
                """,
                """
                > CREATE MATERIALIZED VIEW nested_types_view2 AS SELECT
                  map_col, '{a => 1, b => 2}'::map_type,
                  list_col, '{{1,2},{3,4}}'::int4_list_list,
                  record_col, ROW(1, 'abc'),
                  array_col, ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]
                  FROM nested_types_table;

                > INSERT INTO nested_types_table SELECT * FROM nested_types_table LIMIT 1;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT map_col::text, map_type::text, list_col::text, int4_list_list::text, record_col::text, "row"::text, array_col::text, "array"::text FROM nested_types_view1;
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}

                > SELECT map_col::text, map_type::text, list_col::text, int4_list_list::text, record_col::text, "row"::text, array_col::text, "array"::text FROM nested_types_view2;
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}

            """
            )
        )
