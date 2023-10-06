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


class DropSubsource(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE SECRET pgpass_ss AS 'postgres'
                > CREATE CONNECTION pg_ss TO POSTGRES (
                    HOST 'postgres',
                    DATABASE postgres,
                    USER postgres,
                    PASSWORD SECRET pgpass_ss
                  );

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                ALTER USER postgres WITH replication;
                DROP SCHEMA IF EXISTS public_ss CASCADE;
                DROP PUBLICATION IF EXISTS mz_source_ss;
                CREATE SCHEMA public_ss;

                CREATE TABLE t1 (f1 INTEGER);
                ALTER TABLE t1 REPLICA IDENTITY FULL;
                INSERT INTO t1 VALUES (1);

                CREATE PUBLICATION mz_source_ss FOR ALL TABLES;

                > CREATE SOURCE mz_source_ss
                  FROM POSTGRES CONNECTION pg_ss
                  (PUBLICATION 'mz_source_ss')
                  FOR TABLES (t1);

                > SELECT COUNT(*) = 1 FROM t1;
                true
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                CREATE TABLE t2 (f1 INTEGER);
                ALTER TABLE t2 REPLICA IDENTITY FULL;
                INSERT INTO t2 VALUES (1);

                CREATE TABLE t3 (f1 INTEGER);
                ALTER TABLE t3 REPLICA IDENTITY FULL;
                INSERT INTO t3 VALUES (1);

                > ALTER SOURCE mz_source_ss
                  ADD SUBSOURCE t2;

                > ALTER SOURCE mz_source_ss
                  ADD SUBSOURCE t3;
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                > ALTER SOURCE mz_source_ss
                  DROP SUBSOURCE t3;

                > ALTER SOURCE mz_source_ss
                  ADD SUBSOURCE t3;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            ! DROP SOURCE t2;
            contains: SOURCE \"t2\" is a subsource and must be dropped with ALTER SOURCE...DROP SUBSOURCE

            ! DROP SOURCE t3;
            contains: SOURCE \"t3\" is a subsource and must be dropped with ALTER SOURCE...DROP SUBSOURCE
            """
            )
        )
