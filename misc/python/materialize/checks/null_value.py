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


class NullValue(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE null_value_table (f1 INTEGER, f2 INTEGER DEFAULT NULL);
            > INSERT INTO null_value_table DEFAULT VALUES;
            > INSERT INTO null_value_table VALUES (NULL, NULL);
            > INSERT INTO null_value_table VALUES (NULL, NULL);
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE VIEW null_value_view1 AS
                  SELECT f1, f2, NULL
                  FROM null_value_table
                  WHERE f1 IS NULL OR f1 IS NOT NULL OR f1 = NULL;

                > INSERT INTO null_value_table SELECT * FROM null_value_table;
                """,
                """
                > CREATE MATERIALIZED VIEW null_value_view2 AS
                  SELECT f1, f2, NULL
                  FROM null_value_table
                  WHERE f1 IS NULL OR f1 IS NOT NULL OR f1 = NULL;

                > INSERT INTO null_value_table SELECT * FROM null_value_table;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW CREATE VIEW null_value_view1;
                materialize.public.null_value_view1 "CREATE VIEW \\"materialize\\".\\"public\\".\\"null_value_view1\\" AS SELECT \\"f1\\", \\"f2\\", NULL FROM \\"materialize\\".\\"public\\".\\"null_value_table\\" WHERE \\"f1\\" IS NULL OR \\"f1\\" IS NOT NULL OR \\"f1\\" = NULL"

                > SELECT * FROM null_value_view1;
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>

                > SHOW CREATE MATERIALIZED VIEW null_value_view2;
                materialize.public.null_value_view2 "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"null_value_view2\\" IN CLUSTER \\"default\\" AS SELECT \\"f1\\", \\"f2\\", NULL FROM \\"materialize\\".\\"public\\".\\"null_value_table\\" WHERE \\"f1\\" IS NULL OR \\"f1\\" IS NOT NULL OR \\"f1\\" = NULL"

                > SELECT * FROM null_value_view2;
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>
                <null> <null> <null>


            """
            )
        )
