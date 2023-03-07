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


class JsonbType(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE jsonb_type_table (jsonb_col JSONB);
            > INSERT INTO jsonb_type_table VALUES ('{"object_element":{"a":"b"},"array_element": [1,2], "string_element":"abc", "number_element":123.456, "boolean_element": true, "null_element":null}');
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW jsonb_type_view1 AS
                    WITH cte AS (SELECT '{"object_element":{"a":"b"},"array_element": [1,2], "string_element":"abc", "number_element":123.456, "boolean_element": true, "null_element":null}'::JSONB AS cte_jsonb_col)
                    SELECT
                      jsonb_col AS c1,
                      '{"object_element":{"a":"b"},"array_element": [1,2], "string_element":"abc", "number_element":123.456, "boolean_element": true, "null_element":null}'::JSONB AS c2,
                      jsonb_col -> 'number_element' AS c3,
                      cte_jsonb_col -> 'number_element' AS c4,
                      jsonb_col ->> 'number_element' AS c5,
                      jsonb_col #> '{array_element,1}' AS c6,
                      jsonb_col #>> '{array_element,1}' AS c7,
                      jsonb_col || '{"another_null": null}'::jsonb AS c8,
                      jsonb_col - 'object_element' AS c9,
                      jsonb_col @> '{"boolean_element": true}' AS c10,
                      jsonb_col <@ '{"boolean_element": true}' AS c11,
                      jsonb_col ? 'number_element' AS c12
                    FROM jsonb_type_table, cte;
                > INSERT INTO jsonb_type_table SELECT * FROM jsonb_type_table LIMIT 1;
                """,
                """
                > CREATE MATERIALIZED VIEW jsonb_type_view2 AS
                    WITH cte AS (SELECT '{"object_element":{"a":"b"},"array_element": [1,2], "string_element":"abc", "number_element":123.456, "boolean_element": true, "null_element":null}'::JSONB AS cte_jsonb_col)
                    SELECT
                      jsonb_col AS c1,
                      '{"object_element":{"a":"b"},"array_element": [1,2], "string_element":"abc", "number_element":123.456, "boolean_element": true, "null_element":null}'::JSONB AS c2,
                      jsonb_col -> 'number_element' AS c3,
                      cte_jsonb_col -> 'number_element' AS c4,
                      jsonb_col ->> 'number_element' AS c5,
                      jsonb_col #> '{array_element,1}' AS c6,
                      jsonb_col #>> '{array_element,1}' AS c7,
                      jsonb_col || '{"another_null": null}'::jsonb AS c8,
                      jsonb_col - 'object_element' AS c9,
                      jsonb_col @> '{"boolean_element": true}' AS c10,
                      jsonb_col <@ '{"boolean_element": true}' AS c11,
                      jsonb_col ? 'number_element' AS c12
                    FROM jsonb_type_table, cte;
                > INSERT INTO jsonb_type_table SELECT * FROM jsonb_type_table LIMIT 1;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM jsonb_type_view1;
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" 123.456 123.456 123.456 2  2  "{\\"another_null\\":null,\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"string_element\\":\\"abc\\"}" true false true
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" 123.456 123.456 123.456 2  2  "{\\"another_null\\":null,\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"string_element\\":\\"abc\\"}" true false true
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" 123.456 123.456 123.456 2  2  "{\\"another_null\\":null,\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"string_element\\":\\"abc\\"}" true false true

                > SELECT * FROM jsonb_type_view2;
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" 123.456 123.456 123.456 2  2  "{\\"another_null\\":null,\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"string_element\\":\\"abc\\"}" true false true
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" 123.456 123.456 123.456 2  2  "{\\"another_null\\":null,\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"string_element\\":\\"abc\\"}" true false true
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" 123.456 123.456 123.456 2  2  "{\\"another_null\\":null,\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"string_element\\":\\"abc\\"}" true false true
            """
            )
        )
