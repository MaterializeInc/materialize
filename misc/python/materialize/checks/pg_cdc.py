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


class PgCdc(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE SECRET pgpass1 AS 'postgres';

                > CREATE CONNECTION pg1 FOR POSTGRES
                  HOST 'postgres-source',
                  DATABASE postgres,
                  USER postgres,
                  PASSWORD SECRET pgpass1

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                ALTER USER postgres WITH replication;
                DROP PUBLICATION IF EXISTS postgres_source;

                DROP TABLE IF EXISTS postgres_source_table;

                CREATE TABLE postgres_source_table (f1 TEXT, f2 INTEGER, f3 TEXT);
                ALTER TABLE postgres_source_table REPLICA IDENTITY FULL;

                INSERT INTO postgres_source_table SELECT 'A', 1, REPEAT('X', 1024) FROM generate_series(1,100);

                CREATE PUBLICATION postgres_source FOR ALL TABLES;
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE SOURCE postgres_source1
                  FROM POSTGRES CONNECTION pg1
                  (PUBLICATION 'postgres_source')
                  FOR TABLES (postgres_source_table AS postgres_source_tableA);

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_source_table SELECT 'B', 1, REPEAT('X', 1024) FROM generate_series(1,100);
                UPDATE postgres_source_table SET f2 = f2 + 1;


                > CREATE SECRET pgpass2 AS 'postgres';

                > CREATE CONNECTION pg2 FOR POSTGRES
                  HOST 'postgres-source',
                  DATABASE postgres,
                  USER postgres,
                  PASSWORD SECRET pgpass1

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_source_table SELECT 'C', 1, REPEAT('X', 1024) FROM generate_series(1,100);
                UPDATE postgres_source_table SET f2 = f2 + 1;
                """,
                """

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_source_table SELECT 'D', 1, REPEAT('X', 1024) FROM generate_series(1,100);
                UPDATE postgres_source_table SET f2 = f2 + 1;

                > CREATE SOURCE postgres_source2
                  FROM POSTGRES CONNECTION pg2
                  (PUBLICATION 'postgres_source')
                  FOR TABLES (postgres_source_table AS postgres_source_tableB);

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_source_table SELECT 'E', 1, REPEAT('X', 1024) FROM generate_series(1,100);
                UPDATE postgres_source_table SET f2 = f2 + 1;

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_source_table SELECT 'F', 1, REPEAT('X', 1024) FROM generate_series(1,100);
                UPDATE postgres_source_table SET f2 = f2 + 1;

                > CREATE SECRET pgpass3 AS 'postgres';

                > CREATE CONNECTION pg3 FOR POSTGRES
                  HOST 'postgres-source',
                  DATABASE postgres,
                  USER postgres,
                  PASSWORD SECRET pgpass3

                > CREATE SOURCE postgres_source3
                  FROM POSTGRES CONNECTION pg3
                  (PUBLICATION 'postgres_source')
                  FOR TABLES (postgres_source_table AS postgres_source_tableC);

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_source_table SELECT 'G', 1, REPEAT('X', 1024) FROM generate_series(1,100);
                UPDATE postgres_source_table SET f2 = f2 + 1;


                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_source_table SELECT 'H', 1, REPEAT('X', 1024) FROM generate_series(1,100);
                UPDATE postgres_source_table SET f2 = f2 + 1;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT f1, f2, SUM(LENGTH(f3)) FROM postgres_source_tableA GROUP BY f1, f2;
                A 8 102400
                B 8 102400
                C 7 102400
                D 6 102400
                E 5 102400
                F 4 102400
                G 3 102400
                H 2 102400

                > SELECT f1, f2, SUM(LENGTH(f3)) FROM postgres_source_tableB GROUP BY f1, f2;
                A 8 102400
                B 8 102400
                C 7 102400
                D 6 102400
                E 5 102400
                F 4 102400
                G 3 102400
                H 2 102400

                > SELECT f1, f2, SUM(LENGTH(f3)) FROM postgres_source_tableC GROUP BY f1, f2;
                A 8 102400
                B 8 102400
                C 7 102400
                D 6 102400
                E 5 102400
                F 4 102400
                G 3 102400
                H 2 102400
           """
            )
        )
