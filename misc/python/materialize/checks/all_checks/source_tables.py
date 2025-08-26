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
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.sql_server import SqlServer


class TableFromSourceBase(Check):
    def generic_setup(self) -> str:
        return dedent(
            """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_create_table_from_source = true
                """
        )


@externally_idempotent(False)
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

                CREATE TYPE an_enum AS ENUM ('x1', 'x2');
                CREATE TABLE pg_table_1 (a INTEGER, b INTEGER, c an_enum);
                INSERT INTO pg_table_1 VALUES (1, 1234, NULL), (2, 0, 'x1');
                ALTER TABLE pg_table_1 REPLICA IDENTITY FULL;

                CREATE TABLE pg_table_2 (a INTEGER);
                INSERT INTO pg_table_2 VALUES (1000), (2000);
                ALTER TABLE pg_table_2 REPLICA IDENTITY FULL;

                > CREATE SOURCE pg_source_{self.suffix} FROM POSTGRES CONNECTION pg_conn_{self.suffix} (PUBLICATION 'mz_source_{self.suffix}');

                > CREATE SOURCE old_pg_source_{self.suffix} FROM POSTGRES CONNECTION pg_conn_{self.suffix}
                  (PUBLICATION 'mz_source_{self.suffix}', TEXT COLUMNS = (pg_table_1.c))
                  FOR TABLES (pg_table_1 AS pg_table_1_old_syntax);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE TABLE pg_table_1 FROM SOURCE pg_source_{self.suffix}
                  (REFERENCE "pg_table_1")
                  WITH (TEXT COLUMNS = (c));

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO pg_table_1 VALUES (3, 2345, 'x2');
                """,
                f"""
                > CREATE TABLE pg_table_1b FROM SOURCE pg_source_{self.suffix}
                  (REFERENCE "pg_table_1")
                  WITH (TEXT COLUMNS = (c));
                > CREATE TABLE pg_table_2 FROM SOURCE pg_source_{self.suffix} (REFERENCE "pg_table_2");

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO pg_table_1 VALUES (4, 3456, 'x2');
                INSERT INTO pg_table_2 VALUES (3000);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM pg_table_1;
                1 1234 <null>
                2 0 x1
                3 2345 x2
                4 3456 x2

                > SELECT * FROM pg_table_1b;
                1 1234 <null>
                2 0 x1
                3 2345 x2
                4 3456 x2

                > SELECT * FROM pg_table_2;
                1000
                2000
                3000

                > SELECT * FROM pg_table_1_old_syntax;
                1 1234 <null>
                2 0 x1
                3 2345 x2
                4 3456 x2
           """
            )
        )


@externally_idempotent(False)
class TableFromMySqlSource(TableFromSourceBase):
    suffix = "tbl_from_mysql_source"

    def _can_run(self, e: Executor) -> bool:
        # TODO: Reenable when database-issues#9288 is fixed
        return False

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

                CREATE TABLE mysql_source_table_1 (a INTEGER, b INTEGER, y YEAR);
                INSERT INTO mysql_source_table_1 VALUES (1, 1234, 2024), (2, 0, 2001);

                CREATE TABLE mysql_source_table_2 (a INTEGER);
                INSERT INTO mysql_source_table_2 VALUES (1000), (2000);

                > CREATE SOURCE mysql_source_{self.suffix} FROM MYSQL CONNECTION mysql_conn_{self.suffix};

                > CREATE SOURCE old_mysql_source_{self.suffix} FROM MYSQL CONNECTION mysql_conn_{self.suffix}
                  (TEXT COLUMNS = (public_{self.suffix}.mysql_source_table_1.y))
                  FOR TABLES (public_{self.suffix}.mysql_source_table_1 AS mysql_table_1_old_syntax);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE TABLE mysql_table_1 FROM SOURCE mysql_source_{self.suffix}
                  (REFERENCE "public_{self.suffix}"."mysql_source_table_1")
                  WITH (TEXT COLUMNS = (y));

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public_{self.suffix};
                INSERT INTO mysql_source_table_1 VALUES (3, 2345, 2000);
                """,
                f"""
                > CREATE TABLE mysql_table_1b FROM SOURCE mysql_source_{self.suffix}
                  (REFERENCE "public_{self.suffix}"."mysql_source_table_1")
                  WITH (IGNORE COLUMNS = (y));
                > CREATE TABLE mysql_table_2 FROM SOURCE mysql_source_{self.suffix} (REFERENCE "public_{self.suffix}"."mysql_source_table_2");

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public_{self.suffix};
                INSERT INTO mysql_source_table_1 VALUES (4, 3456, NULL);
                INSERT INTO mysql_source_table_2 VALUES (3000);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM mysql_table_1;
                1 1234 2024
                2 0 2001
                3 2345 2000
                4 3456 <null>

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
                1 1234 2024
                2 0 2001
                3 2345 2000
                4 3456 <null>
           """
            )
        )


@externally_idempotent(False)
class TableFromSqlServerSource(TableFromSourceBase):
    suffix = "tbl_from_sql_server_source"

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.154.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            self.generic_setup()
            + dedent(
                f"""
                > CREATE SECRET sql_serverpass_{self.suffix} AS '{SqlServer.DEFAULT_SA_PASSWORD}';

                > CREATE CONNECTION sql_server_conn_{self.suffix} TO SQL SERVER (
                    HOST 'sql-server',
                    DATABASE test,
                    USER {SqlServer.DEFAULT_USER},
                    PASSWORD SECRET sql_serverpass_{self.suffix}
                  )

                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                CREATE TABLE sql_server_source_table_1 (a INTEGER, b INTEGER, y YEAR);
                EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sql_server_source_table_1', @role_name = 'SA', @supports_net_changes = 0;
                INSERT INTO sql_server_source_table_1 VALUES (1, 1234, 2024), (2, 0, 2001);

                CREATE TABLE sql_server_source_table_2 (a INTEGER);
                EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sql_server_source_table_2', @role_name = 'SA', @supports_net_changes = 0;
                INSERT INTO sql_server_source_table_2 VALUES (1000), (2000);

                > CREATE SOURCE sql_server_source_{self.suffix} FROM SQL SERVER CONNECTION sql_server_conn_{self.suffix};

                > CREATE SOURCE old_sql_server_source_{self.suffix} FROM SQL SERVER CONNECTION sql_server_conn_{self.suffix}
                  (TEXT COLUMNS = (sql_server_source_table_1.y))
                  FOR TABLES (sql_server_source_table_1 AS sql_server_table_1_old_syntax);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE TABLE sql_server_table_1 FROM SOURCE sql_server_source_{self.suffix}
                  (REFERENCE "sql_server_source_table_1")
                  WITH (TEXT COLUMNS = (y));

                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                INSERT INTO sql_server_source_table_1 VALUES (3, 2345, 2000);
                """,
                f"""
                > CREATE TABLE sql_server_table_1b FROM SOURCE sql_server_source_{self.suffix}
                  (REFERENCE "public_{self.suffix}"."sql_server_source_table_1")
                  WITH (IGNORE COLUMNS = (y));
                > CREATE TABLE sql_server_table_2 FROM SOURCE sql_server_source_{self.suffix} (REFERENCE "public_{self.suffix}"."sql_server_source_table_2");

                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                INSERT INTO sql_server_source_table_1 VALUES (4, 3456, NULL);
                INSERT INTO sql_server_source_table_2 VALUES (3000);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM sql_server_table_1;
                1 1234 2024
                2 0 2001
                3 2345 2000
                4 3456 <null>

                > SELECT * FROM sql_server_table_1b;
                1 1234
                2 0
                3 2345
                4 3456

                > SELECT * FROM sql_server_table_2;
                1000
                2000
                3000

                # old source syntax still working
                > SELECT * FROM sql_server_table_1_old_syntax;
                1 1234 2024
                2 0 2001
                3 2345 2000
                4 3456 <null>
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
