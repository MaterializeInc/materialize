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


class NestedTypes(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TYPE record_type AS (f1 INT, f2 STRING);
            > CREATE TYPE map_type AS MAP (KEY TYPE = text, VALUE TYPE = integer);

            > CREATE TYPE int4_list AS LIST (ELEMENT TYPE = int4);
            > CREATE TYPE int4_list_list AS LIST (ELEMENT TYPE = int4_list);

            > CREATE TYPE nested_types_numeric_list AS LIST (ELEMENT TYPE = numeric(38, 2));
            > CREATE TYPE nested_types_numeric_map AS MAP (KEY TYPE = text, VALUE TYPE = numeric(5, 2));

            > CREATE TABLE nested_types_table(map_col map_type, list_col int4_list_list, record_col record_type, array_col STRING);

            > INSERT INTO nested_types_table VALUES ('{a => 1, b => 2}'::map_type, '{{1,2},{3,4}}'::int4_list_list, ROW(1, 'abc'), ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]);
        """))

    def manipulate(self) -> list[Testdrive]:
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
        return Testdrive(dedent("""
                > SELECT map_col::text, map_type::text, list_col::text, int4_list_list::text, record_col::text, "row"::text, array_col::text, "array"::text FROM nested_types_view1;
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}

                > SELECT map_col::text, map_type::text, list_col::text, int4_list_list::text, record_col::text, "row"::text, array_col::text, "array"::text FROM nested_types_view2;
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}
                {a=>1,b=>2} {a=>1,b=>2} {{1,2},{3,4}} {{1,2},{3,4}} (1,abc) (1,abc) {{a,b},{c,d}} {{a,b},{c,d}}

                > SELECT t.name, e.name, l.element_modifiers::text
                  FROM mz_list_types l
                  JOIN mz_types t ON l.id = t.id
                  JOIN mz_types e ON l.element_id = e.id
                  WHERE t.name IN ('int4_list', 'int4_list_list', 'nested_types_numeric_list');
                int4_list int4 <null>
                int4_list_list int4_list <null>
                nested_types_numeric_list numeric {38,2}

                > SELECT t.name, k.name, v.name, m.key_modifiers::text, m.value_modifiers::text
                  FROM mz_map_types m
                  JOIN mz_types t ON m.id = t.id
                  JOIN mz_types k ON m.key_id = k.id
                  JOIN mz_types v ON m.value_id = v.id
                  WHERE t.name IN ('map_type', 'nested_types_numeric_map');
                map_type text int4 <null> <null>
                nested_types_numeric_map text numeric <null> {5,2}

                > SELECT t.name
                  FROM mz_base_types b
                  JOIN mz_types t ON b.id = t.id
                  WHERE t.name = 'record_type';
                record_type
            """))
