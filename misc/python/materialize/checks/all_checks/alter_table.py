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


class AlterTableAddColumn(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.131.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            # Note(parkmycar): We want to make sure to ALTER a table in the initialize to exercise
            # how ALTER-ed tables are handled on a restart of Materialize.
            dedent(
                """
                $postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true;

                > CREATE TABLE alter_table1 (f1 INTEGER, f2 INTEGER NOT NULL DEFAULT 1234);
                > INSERT INTO alter_table1 VALUES (100, 100);
                > CREATE VIEW alter_table_view_1 AS SELECT * FROM alter_table1;

                > CREATE TABLE alter_table2 (f1 text);
                > INSERT INTO alter_table2 VALUES ('hello');

                > ALTER TABLE alter_table2 ADD COLUMN f2 int;
                > INSERT INTO alter_table2 VALUES ('world', 1);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true;

                > ALTER TABLE alter_table1 ADD COLUMN f3 text;
                > INSERT INTO alter_table1 VALUES (200, 200, 'hello'), (NULL, 300, 'world'), (400, 400, NULL), (NULL, 500, NULL);
                > CREATE MATERIALIZED VIEW alter_table_mv1 AS SELECT alter_table2.f1 as f1_2, alter_table1.f1 as f1_1 FROM alter_table1, alter_table2;
                > CREATE INDEX alter_table1_idx ON alter_table1 (f1);
                """,
                """
                $postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_alter_table_add_column = true;

                > INSERT INTO alter_table2 VALUES ('foo', 900), ('bar', 800);
                > CREATE VIEW alter_table_view_2 AS SELECT * FROM alter_table2;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM alter_table1 ORDER BY f1, f2 ASC;
                100 100 <null>
                200 200 hello
                400 400 <null>
                <null> 300 world
                <null> 500 <null>

                > SELECT * FROM alter_table_view_1 ORDER BY f1, f2 ASC;
                100 100
                200 200
                400 400
                <null> 300
                <null> 500

                > SELECT * FROM alter_table_mv1;
                bar 100
                bar 200
                bar 400
                bar <null>
                bar <null>
                foo 100
                foo 200
                foo 400
                foo <null>
                foo <null>
                hello 100
                hello 200
                hello 400
                hello <null>
                hello <null>
                world 100
                world 200
                world 400
                world <null>
                world <null>

                > SELECT * FROM alter_table2 ORDER BY f1, f2 ASC;
                bar 800
                foo 900
                hello <null>
                world 1
           """
            )
        )
