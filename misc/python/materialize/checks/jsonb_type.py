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
            > INSERT INTO jsonb_type_table VALUES ('{"object_element":{"a":"b"},"array_element": [1,2], "string_element":"abc", "number_element":123.456, "boolean_element": true, "null_element":null}')
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW jsonb_type_view1 AS
                  SELECT jsonb_col, '{"object_element":{"a":"b"},"array_element": [1,2], "string_element":"abc", "number_element":123.456, "boolean_element": true, "null_element":null}'::JSONB
                  FROM jsonb_type_table;

                > INSERT INTO jsonb_type_table SELECT * FROM jsonb_type_table LIMIT 1;
                """,
                """
                > CREATE MATERIALIZED VIEW jsonb_type_view2 AS
                  SELECT jsonb_col, '{"object_element":{"a":"b"},"array_element": [1,2], "string_element":"abc", "number_element":123.456, "boolean_element": true, "null_element":null}'::JSONB
                  FROM jsonb_type_table;

                > INSERT INTO jsonb_type_table SELECT * FROM jsonb_type_table LIMIT 1;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM jsonb_type_view1;
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}"
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}"
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}"

                > SELECT * FROM jsonb_type_view2;
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}"
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}"
                "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}" "{\\"array_element\\":[1,2],\\"boolean_element\\":true,\\"null_element\\":null,\\"number_element\\":123.456,\\"object_element\\":{\\"a\\":\\"b\\"},\\"string_element\\":\\"abc\\"}"
            """
            )
        )
