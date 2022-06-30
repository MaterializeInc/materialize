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


class CreateTable(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE create_table1 (f1 INTEGER, f2 INTEGER NOT NULL DEFAULT 1234);
                > INSERT INTO create_table1 VALUES (1, 1);
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE create_table2 (f1 INTEGER, f2 INTEGER NOT NULL DEFAULT 1234);
                > INSERT INTO create_table2 VALUES (2,2);
                > CREATE MATERIALIZED VIEW create_table_view1 AS SELECT create_table1.f1 FROM create_table1, create_table2;
                """,
                """
                > CREATE TABLE create_table3 (f1 INTEGER, f2 INTEGER NOT NULL DEFAULT 1234);
                > INSERT INTO create_table3 VALUES (3,3);
                > CREATE MATERIALIZED VIEW create_table_view2 AS SELECT create_table2.f1 FROM create_table2, create_table3;

                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM create_table1;
                1 1

                > SELECT * FROM create_table2;
                2 2

                > SELECT * FROM create_table3;
                3 3

                > SELECT * FROM create_table_view1;
                1

                > SELECT * FROM create_table_view2;
                2

                > SHOW CREATE TABLE create_table1;
                materialize.public.create_table1 "CREATE TABLE \\"materialize\\".\\"public\\".\\"create_table1\\" (\\"f1\\" \\"pg_catalog\\".\\"int4\\", \\"f2\\" \\"pg_catalog\\".\\"int4\\" NOT NULL DEFAULT 1234)"

                ! INSERT INTO create_table1 (f2) VALUES (NULL);
                contains: null value in column

                > INSERT INTO create_table1 (f1) VALUES (999);
                > SELECT f2 FROM create_table1 WHERE f2 = 1234;
                1234

                > SHOW CREATE TABLE create_table2;
                materialize.public.create_table2 "CREATE TABLE \\"materialize\\".\\"public\\".\\"create_table2\\" (\\"f1\\" \\"pg_catalog\\".\\"int4\\", \\"f2\\" \\"pg_catalog\\".\\"int4\\" NOT NULL DEFAULT 1234)"

                ! INSERT INTO create_table2 (f2) VALUES (NULL);
                contains: null value in column

                > INSERT INTO create_table2 (f1) VALUES (999);
                > SELECT f2 FROM create_table2 WHERE f2 = 1234;
                1234

                > SHOW CREATE TABLE create_table3;
                materialize.public.create_table3 "CREATE TABLE \\"materialize\\".\\"public\\".\\"create_table3\\" (\\"f1\\" \\"pg_catalog\\".\\"int4\\", \\"f2\\" \\"pg_catalog\\".\\"int4\\" NOT NULL DEFAULT 1234)"

                ! INSERT INTO create_table3 (f2) VALUES (NULL);
                contains: null value in column

                > INSERT INTO create_table3 (f1) VALUES (999);
                > SELECT f2 FROM create_table3 WHERE f2 = 1234;
                1234
           """
            )
        )
