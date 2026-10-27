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
from materialize.checks.checks import Check, externally_idempotent


@externally_idempotent(False)
class AlterSourceAddSubsource(Check):
    """ALTER SOURCE ... ADD SUBSOURCE on a legacy-syntax Postgres source."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            CREATE USER alter_source_user WITH SUPERUSER PASSWORD 'postgres';
            ALTER USER alter_source_user WITH replication;
            DROP PUBLICATION IF EXISTS alter_source_pub;
            DROP TABLE IF EXISTS alter_source_t1;
            DROP TABLE IF EXISTS alter_source_t2;
            DROP TABLE IF EXISTS alter_source_t3;
            DROP TYPE IF EXISTS alter_source_enum;
            CREATE TYPE alter_source_enum AS ENUM ('a', 'b');
            CREATE TABLE alter_source_t1 (f1 INT);
            CREATE TABLE alter_source_t2 (f1 INT);
            CREATE TABLE alter_source_t3 (f1 alter_source_enum);
            ALTER TABLE alter_source_t1 REPLICA IDENTITY FULL;
            ALTER TABLE alter_source_t2 REPLICA IDENTITY FULL;
            ALTER TABLE alter_source_t3 REPLICA IDENTITY FULL;
            INSERT INTO alter_source_t1 VALUES (1);
            INSERT INTO alter_source_t2 VALUES (10);
            INSERT INTO alter_source_t3 VALUES ('a');
            CREATE PUBLICATION alter_source_pub FOR TABLE alter_source_t1, alter_source_t2, alter_source_t3;

            > CREATE SECRET alter_source_pgpass AS 'postgres'
            > CREATE CONNECTION alter_source_pg FOR POSTGRES
              HOST 'postgres',
              DATABASE postgres,
              USER alter_source_user,
              PASSWORD SECRET alter_source_pgpass

            > CREATE SOURCE alter_source_src
              FROM POSTGRES CONNECTION alter_source_pg (PUBLICATION 'alter_source_pub')
              FOR TABLES (alter_source_t1)
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > ALTER SOURCE alter_source_src ADD SUBSOURCE alter_source_t2

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO alter_source_t1 VALUES (2);
                INSERT INTO alter_source_t2 VALUES (20);
                """,
                """
                > ALTER SOURCE alter_source_src ADD SUBSOURCE alter_source_t3 WITH (TEXT COLUMNS [alter_source_t3.f1])

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO alter_source_t1 VALUES (3);
                INSERT INTO alter_source_t2 VALUES (30);
                INSERT INTO alter_source_t3 VALUES ('b');
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SHOW SUBSOURCES ON alter_source_src
            alter_source_src_progress progress
            alter_source_t1 subsource
            alter_source_t2 subsource
            alter_source_t3 subsource

            > SELECT * FROM alter_source_t1
            1
            2
            3

            > SELECT * FROM alter_source_t2
            10
            20
            30

            > SELECT * FROM alter_source_t3
            a
            b
            """))
