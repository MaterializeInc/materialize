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
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class TableFunctions(Check):
    """Table functions in maintained queries: csv_extract, unnest,
    generate_series over timestamps, and ROWS FROM."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE table_fns_table (id INT, csv_data STRING, arr INT[], lst INT LIST)
            > INSERT INTO table_fns_table VALUES
              (1, 'a,1', ARRAY[1, 2], LIST[10]),
              (2, 'b,2', ARRAY[3], LIST[20, 30])
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO table_fns_table VALUES (3, 'c,3', ARRAY[4], LIST[40])

                > CREATE MATERIALIZED VIEW table_fns_csv1 AS
                  SELECT id, csv.column1, csv.column2
                  FROM table_fns_table, csv_extract(2, csv_data) csv

                > CREATE MATERIALIZED VIEW table_fns_unnest1 AS
                  SELECT id, a.elem
                  FROM table_fns_table, unnest(arr) AS a (elem)

                > CREATE MATERIALIZED VIEW table_fns_unnest2 AS
                  SELECT id, l.elem
                  FROM table_fns_table, unnest(lst) AS l (elem)
                """,
                """
                > INSERT INTO table_fns_table VALUES (4, 'd,4', ARRAY[5], LIST[50])

                > CREATE MATERIALIZED VIEW table_fns_series1 AS
                  SELECT id, s.ts
                  FROM table_fns_table,
                       generate_series('2024-01-01 00:00:00'::timestamp,
                                       '2024-01-01 02:00:00'::timestamp,
                                       '1 hour') AS s (ts)
                  WHERE id = 1

                > CREATE MATERIALIZED VIEW table_fns_rows_from1 AS
                  SELECT *
                  FROM ROWS FROM (generate_series(1, 3), generate_series(11, 12)) AS r (a, b)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM table_fns_csv1
            1 a 1
            2 b 2
            3 c 3
            4 d 4

            > SELECT * FROM table_fns_unnest1
            1 1
            1 2
            2 3
            3 4
            4 5

            > SELECT * FROM table_fns_unnest2
            1 10
            2 20
            2 30
            3 40
            4 50

            > SELECT * FROM table_fns_series1
            1 "2024-01-01 00:00:00"
            1 "2024-01-01 01:00:00"
            1 "2024-01-01 02:00:00"

            > SELECT * FROM table_fns_rows_from1
            1 11
            2 12
            3 <null>
            """))


class TableFunctionsOrdinality(Check):
    """WITH ORDINALITY in maintained queries. Kept separate from
    TableFunctions: writing WITH ORDINALITY before the table alias only
    parses from v0.154."""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.154.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE table_fns_ord_table (id INT, arr INT[])
            > INSERT INTO table_fns_ord_table VALUES (1, ARRAY[10, 20])
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO table_fns_ord_table VALUES (2, ARRAY[30])

                > CREATE MATERIALIZED VIEW table_fns_ord_mv1 AS
                  SELECT id, a.elem, a.ord
                  FROM table_fns_ord_table, unnest(arr) WITH ORDINALITY AS a (elem, ord)
                """,
                """
                > INSERT INTO table_fns_ord_table VALUES (3, ARRAY[40, 50])

                > CREATE MATERIALIZED VIEW table_fns_ord_mv2 AS
                  SELECT *
                  FROM ROWS FROM (generate_series(1, 2), generate_series(11, 11)) WITH ORDINALITY AS r (a, b, ord)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM table_fns_ord_mv1
            1 10 1
            1 20 2
            2 30 1
            3 40 1
            3 50 2

            > SELECT * FROM table_fns_ord_mv2
            1 11 1
            2 <null> 2
            """))


class JsonbFunctions(Check):
    """jsonb functions from the jsonb type documentation: the table
    functions (jsonb_array_elements, jsonb_each, jsonb_object_keys) and the
    scalar constructors/inspectors."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE jsonb_fns_table (id INT, j JSONB)
            > INSERT INTO jsonb_fns_table VALUES
              (1, '{"arr": [1, 2], "obj": {"a": 1, "n": null}}')
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO jsonb_fns_table VALUES (2, '{"arr": ["x"], "obj": {"b": 2}}')

                > CREATE MATERIALIZED VIEW jsonb_fns_elements1 AS
                  SELECT id, e.value::text AS elem
                  FROM jsonb_fns_table, jsonb_array_elements(j -> 'arr') e

                > CREATE MATERIALIZED VIEW jsonb_fns_each1 AS
                  SELECT id, e.key, e.value::text AS value
                  FROM jsonb_fns_table, jsonb_each(j -> 'obj') e

                > CREATE MATERIALIZED VIEW jsonb_fns_keys1 AS
                  SELECT id, k AS key
                  FROM jsonb_fns_table, jsonb_object_keys(j -> 'obj') k
                """,
                """
                > INSERT INTO jsonb_fns_table VALUES (3, '{"arr": [true], "obj": {"c": [3]}}')

                > CREATE MATERIALIZED VIEW jsonb_fns_scalars1 AS
                  SELECT
                    id,
                    jsonb_array_length(j -> 'arr') AS alen,
                    jsonb_typeof(j -> 'arr') AS atype,
                    jsonb_strip_nulls(j -> 'obj')::text AS stripped,
                    jsonb_build_array(id, 'x')::text AS barr,
                    jsonb_build_object('id', id)::text AS bobj,
                    to_jsonb(id)::text AS tj,
                    jsonb_pretty(j -> 'arr') != '' AS pretty_nonempty
                  FROM jsonb_fns_table
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM jsonb_fns_elements1
            1 1
            1 2
            2 "\\"x\\""
            3 true

            > SELECT * FROM jsonb_fns_each1
            1 a 1
            1 n null
            2 b 2
            3 c [3]

            > SELECT * FROM jsonb_fns_keys1
            1 a
            1 n
            2 b
            3 c

            > SELECT * FROM jsonb_fns_scalars1
            1 2 array "{\\"a\\":1}" "[1,\\"x\\"]" "{\\"id\\":1}" 1 true
            2 1 array "{\\"b\\":2}" "[2,\\"x\\"]" "{\\"id\\":2}" 2 true
            3 1 array "{\\"c\\":[3]}" "[3,\\"x\\"]" "{\\"id\\":3}" 3 true
            """))
