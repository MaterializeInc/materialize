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


class BooleanType(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE boolean_type_table (boolean_col BOOLEAN);
            > INSERT INTO boolean_type_table VALUES (TRUE), (FALSE), (NULL);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW boolean_type_view1 AS
                  SELECT boolean_col, 'TRUE'::boolean AS true_col, 'FALSE'::boolean AS false_col
                  FROM boolean_type_table
                  WHERE boolean_col IS TRUE OR boolean_col IS FALSE OR boolean_col is NULL;

                > INSERT INTO boolean_type_table SELECT * FROM boolean_type_table;
                """,
                """
                > CREATE MATERIALIZED VIEW boolean_type_view2 AS
                  SELECT boolean_col, 'TRUE'::boolean AS true_col, 'FALSE'::boolean AS false_col
                  FROM boolean_type_table
                  WHERE boolean_col IS TRUE OR boolean_col IS FALSE OR boolean_col is NULL;

                > INSERT INTO boolean_type_table SELECT * FROM boolean_type_table;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM boolean_type_view1;
                <null> true false
                <null> true false
                <null> true false
                <null> true false
                false true false
                false true false
                false true false
                false true false
                true true false
                true true false
                true true false
                true true false

                > SELECT * FROM boolean_type_view2;
                <null> true false
                <null> true false
                <null> true false
                <null> true false
                false true false
                false true false
                false true false
                false true false
                true true false
                true true false
                true true false
                true true false
            """
            )
        )
