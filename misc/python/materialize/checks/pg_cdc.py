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


class PgCdcMzNow(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE SECRET postgres_mz_now_pass AS 'postgres';

                > CREATE CONNECTION posgres_mz_now_conn FOR POSTGRES
                  HOST 'postgres-source',
                  DATABASE postgres,
                  USER postgres,
                  PASSWORD SECRET postgres_mz_now_pass

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                ALTER USER postgres WITH replication;
                DROP PUBLICATION IF EXISTS postgres_mz_now_publication;

                DROP TABLE IF EXISTS postgres_mz_now_table;

                CREATE TABLE postgres_mz_now_table (f1 TIMESTAMP, f2 CHAR(5), PRIMARY KEY (f1, f2));
                ALTER TABLE postgres_mz_now_table REPLICA IDENTITY FULL;

                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'A1');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'B1');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'C1');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'D1');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'E1');

                CREATE PUBLICATION postgres_mz_now_publication FOR ALL TABLES;

                > CREATE SOURCE postgres_mz_now_source
                  FROM POSTGRES CONNECTION posgres_mz_now_conn
                  (PUBLICATION 'postgres_mz_now_publication')
                  FOR ALL TABLES;

                # Return all rows fresher than 60 seconds
                > CREATE MATERIALIZED VIEW postgres_mz_now_view AS
                  SELECT * FROM postgres_mz_now_table
                  WHERE mz_now() <= ROUND(EXTRACT(epoch FROM f1 + INTERVAL '60' SECOND) * 1000)
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'A2');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'B2');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'C2');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'D2');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'E2');
                DELETE FROM postgres_mz_now_table WHERE f2 = 'B1';
                UPDATE postgres_mz_now_table SET f1 = NOW() WHERE f2 = 'C1';
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'A3');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'B3');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'C3');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'D3');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'E3');
                DELETE FROM postgres_mz_now_table WHERE f2 = 'B2';
                UPDATE postgres_mz_now_table SET f1 = NOW() WHERE f2 = 'D1';
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*) FROM postgres_mz_now_table;
                13

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'A4');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'B4');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'C4');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'D4');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'E4');
                DELETE FROM postgres_mz_now_table WHERE f2 = 'B3';
                UPDATE postgres_mz_now_table SET f1 = NOW() WHERE f2 = 'E1'

                # Expect some rows newer than 60 seconds in view
                > SELECT COUNT(*) >= 6 FROM postgres_mz_now_view
                  WHERE f1 > NOW() - INTERVAL '60' SECOND;
                true

                # Expect no rows older than 60 seconds in view
                > SELECT COUNT(*) FROM postgres_mz_now_view
                  WHERE f1 < NOW() - INTERVAL '60' SECOND;
                0

                # Rollback the last INSERTs so that validate() can be called multiple times
                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'B3');
                DELETE FROM postgres_mz_now_table WHERE f2 LIKE '%4%';
                """
            )
        )
