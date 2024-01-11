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


class RenameIndex(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE rename_index_table (f1 INTEGER,f2 INTEGER);
                > CREATE INDEX rename_index_index1 ON rename_index_table (f2);
                > INSERT INTO rename_index_table VALUES (1,1);
                > CREATE MATERIALIZED VIEW rename_index_view1 WITH (RETAIN HISTORY FOR '30s') AS SELECT f2 FROM rename_index_table WHERE f2 > 0;
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO rename_index_table VALUES (2,2);
                > ALTER INDEX rename_index_index1 RENAME TO rename_index_index2;
                > CREATE INDEX rename_index_index1 ON rename_index_table (f2);
                > CREATE MATERIALIZED VIEW rename_index_view2 WITH (RETAIN HISTORY FOR '30s') AS SELECT f2 FROM rename_index_table WHERE f2 > 0;
                > INSERT INTO rename_index_table VALUES (3,3);
                """,
                """
                # When upgrading from old version without roles the indexes are
                # owned by default_role, thus we have to change the owner
                # before altering them:
                $[version>=4700] postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER INDEX rename_index_index1 OWNER TO materialize;
                ALTER INDEX rename_index_index2 OWNER TO materialize;

                > INSERT INTO rename_index_table VALUES (4,4);
                > CREATE MATERIALIZED VIEW rename_index_view3 WITH (RETAIN HISTORY FOR '30s') AS SELECT f2 FROM rename_index_table WHERE f2 > 0;
                > ALTER INDEX rename_index_index2 RENAME TO rename_index_index3;
                > ALTER INDEX rename_index_index1 RENAME TO rename_index_index2;
                > INSERT INTO rename_index_table VALUES (5,5);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                > SHOW INDEXES ON rename_index_table;
                rename_index_index2 rename_index_table {self._default_cluster()} {{f2}}
                rename_index_index3 rename_index_table {self._default_cluster()} {{f2}}

                > SELECT * FROM rename_index_view1;
                1
                2
                3
                4
                5

                > SELECT * FROM rename_index_view2;
                1
                2
                3
                4
                5

                > SELECT * FROM rename_index_view3;
                1
                2
                3
                4
                5
           """
            )
        )
