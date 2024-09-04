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
from materialize.mzcompose.services.mysql import MySql


class TableFromSourceBase(Check):

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.116.0-dev")

    def generic_setup(self) -> str:
        return dedent(
            """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_create_table_from_source = true
                """
        )


class TableFromPgSource(TableFromSourceBase):
    suffix = "tbl_from_pg_source"

    def initialize(self) -> Testdrive:

        return Testdrive(
            self.generic_setup()
            + dedent(
                f"""
                > CREATE SECRET pgpass_{self.suffix} AS 'postgres'

                > CREATE CONNECTION pg_conn_{self.suffix} TO POSTGRES (
                    HOST postgres,
                    DATABASE postgres,
                    USER postgres,
                    PASSWORD SECRET pgpass_{self.suffix}
                  )

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                ALTER USER postgres WITH replication;
                DROP SCHEMA IF EXISTS public_{self.suffix} CASCADE;
                CREATE SCHEMA public_{self.suffix};

                DROP PUBLICATION IF EXISTS mz_source_{self.suffix};
                CREATE PUBLICATION mz_source_{self.suffix} FOR ALL TABLES;

                CREATE TABLE pg_table_1 (a INTEGER, b INTEGER);
                INSERT INTO pg_table_1 VALUES (1, 1234), (2, 0);
                ALTER TABLE pg_table_1 REPLICA IDENTITY FULL;

                CREATE TABLE pg_table_2 (a INTEGER);
                INSERT INTO pg_table_2 VALUES (1000), (2000);
                ALTER TABLE pg_table_2 REPLICA IDENTITY FULL;

                > CREATE SOURCE pg_source_{self.suffix} FROM POSTGRES CONNECTION pg_conn_{self.suffix} (PUBLICATION 'mz_source_{self.suffix}');

                > CREATE SOURCE old_pg_source_{self.suffix} FROM POSTGRES CONNECTION pg_conn_{self.suffix} (PUBLICATION 'mz_source_{self.suffix}') FOR TABLES (pg_table_1 AS pg_table_1_old_syntax);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE TABLE pg_table_1 FROM SOURCE pg_source_{self.suffix} (REFERENCE "pg_table_1") WITH (TEXT COLUMNS = (a));

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO pg_table_1 VALUES (3, 2345);
                """,
                f"""
                > CREATE TABLE pg_table_1b FROM SOURCE pg_source_{self.suffix} (REFERENCE "pg_table_1");
                > CREATE TABLE pg_table_2 FROM SOURCE pg_source_{self.suffix} (REFERENCE "pg_table_2");

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO pg_table_1 VALUES (4, 3456);
                INSERT INTO pg_table_2 VALUES (3000);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM pg_table_1;
                1 1234
                2 0
                3 2345
                4 3456

                > SELECT * FROM pg_table_1b;
                1 1234
                2 0
                3 2345
                4 3456

                > SELECT * FROM pg_table_2;
                1000
                2000
                3000

                > SELECT * FROM pg_table_1_old_syntax;
                1 1234
                2 0
                3 2345
                4 3456
           """
            )
        )


class TableFromMySqlSource(TableFromSourceBase):
    suffix = "tbl_from_mysql_source"

    def initialize(self) -> Testdrive:
        return Testdrive(
            self.generic_setup()
            + dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                > CREATE SECRET mysqlpass_{self.suffix} AS 'p@ssw0rd';

                > CREATE CONNECTION mysql_conn_{self.suffix} TO MYSQL (
                    HOST mysql,
                    USER root,
                    PASSWORD SECRET mysqlpass_{self.suffix}
                  )

                $ mysql-execute name=mysql
                DROP DATABASE IF EXISTS public_{self.suffix};
                CREATE DATABASE public_{self.suffix};
                USE public_{self.suffix};

                CREATE TABLE mysql_source_table_1 (a INTEGER, b INTEGER);
                INSERT INTO mysql_source_table_1 VALUES (1, 1234), (2, 0);

                CREATE TABLE mysql_source_table_2 (a INTEGER);
                INSERT INTO mysql_source_table_2 VALUES (1000), (2000);

                > CREATE SOURCE mysql_source_{self.suffix} FROM MYSQL CONNECTION mysql_conn_{self.suffix};

                > CREATE SOURCE old_mysql_source_{self.suffix} FROM MYSQL CONNECTION mysql_conn_{self.suffix} FOR TABLES (public_{self.suffix}.mysql_source_table_1 AS mysql_table_1_old_syntax);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE TABLE mysql_table_1 FROM SOURCE mysql_source_{self.suffix} (REFERENCE "public_{self.suffix}"."mysql_source_table_1");

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public_{self.suffix};
                INSERT INTO mysql_source_table_1 VALUES (3, 2345);
                """,
                f"""
                > CREATE TABLE mysql_table_1b FROM SOURCE mysql_source_{self.suffix} (REFERENCE "public_{self.suffix}"."mysql_source_table_1");
                > CREATE TABLE mysql_table_2 FROM SOURCE mysql_source_{self.suffix} (REFERENCE "public_{self.suffix}"."mysql_source_table_2");

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public_{self.suffix};
                INSERT INTO mysql_source_table_1 VALUES (4, 3456);
                INSERT INTO mysql_source_table_2 VALUES (3000);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM mysql_table_1;
                1 1234
                2 0
                3 2345
                4 3456

                > SELECT * FROM mysql_table_1b;
                1 1234
                2 0
                3 2345
                4 3456

                > SELECT * FROM mysql_table_2;
                1000
                2000
                3000

                # old source syntax still working
                > SELECT * FROM mysql_table_1_old_syntax;
                1 1234
                2 0
                3 2345
                4 3456
           """
            )
        )


class TableFromLoadGenSource(TableFromSourceBase):
    suffix = "tbl_from_lg_source"

    def initialize(self) -> Testdrive:
        return Testdrive(
            self.generic_setup()
            + dedent(
                f"""
                > CREATE SOURCE auction_house_{self.suffix} FROM LOAD GENERATOR AUCTION (AS OF 300, UP TO 301);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE TABLE bids_1 FROM SOURCE auction_house_{self.suffix} (REFERENCE "auction"."bids");
                """,
                f"""
                > CREATE TABLE bids_2 FROM SOURCE auction_house_{self.suffix} (REFERENCE "bids");
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT count(*) FROM bids_1;
                255

                > SELECT count(*) FROM bids_2;
                255
                """
            )
        )
