# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from random import Random
from textwrap import dedent
from typing import Any

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, externally_idempotent
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion
from materialize.mzcompose.services.sql_server import SqlServer


class SqlServerCdcBase:
    base_version: MzVersion
    current_version: MzVersion
    wait: bool
    suffix: str
    repeats: int
    expects: int

    def __init__(self, wait: bool, **kwargs: Any) -> None:
        self.wait = wait
        self.repeats = 1024 if wait else 1700
        self.expects = 97350 if wait else 164950
        self.suffix = f"_{str(wait).lower()}"
        super().__init__(**kwargs)  # forward unused args to Check

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                DROP TABLE IF EXISTS sql_server_source_table{self.suffix};
                CREATE TABLE sql_server_source_table{self.suffix} (f1 VARCHAR(1700), f2 INTEGER, f3 VARCHAR(1700) UNIQUE NOT NULL, f4 VARCHAR(1700), PRIMARY KEY(f1, f2));
                EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sql_server_source_table{self.suffix}', @role_name = 'SA', @supports_net_changes = 0;
                WITH nums AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM nums WHERE i < 100) INSERT INTO sql_server_source_table{self.suffix} (f1, f2, f3, f4) SELECT 'A', i, REPLICATE('A', {self.repeats} - i), NULL FROM nums OPTION (MAXRECURSION 100);

                > CREATE SECRET sql_server_password1{self.suffix} AS '{SqlServer.DEFAULT_SA_PASSWORD}';

                > CREATE CONNECTION sql_server_connection1{self.suffix} TO SQL SERVER (
                    HOST 'sql-server',
                    DATABASE test,
                    USER {SqlServer.DEFAULT_USER},
                    PASSWORD SECRET sql_server_password1{self.suffix}
                  )
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE SOURCE sql_server_source1{self.suffix}
                  FROM SQL SERVER CONNECTION sql_server_connection1{self.suffix};
                > CREATE TABLE sql_server_source_tableA{self.suffix} FROM SOURCE sql_server_source1{self.suffix} (REFERENCE sql_server_source_table{self.suffix});

                > CREATE DEFAULT INDEX ON sql_server_source_tableA{self.suffix};

                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                WITH nums AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM nums WHERE i < 100) INSERT INTO sql_server_source_table{self.suffix} (f1, f2, f3, f4) SELECT 'B', i, REPLICATE('B', {self.repeats} - i), NULL FROM nums OPTION (MAXRECURSION 100);
                UPDATE sql_server_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SECRET sql_server_password2{self.suffix} AS '{SqlServer.DEFAULT_SA_PASSWORD}';

                > CREATE CONNECTION sql_server_connection2{self.suffix} TO SQL SERVER (
                    HOST 'sql-server',
                    DATABASE test,
                    USER {SqlServer.DEFAULT_USER},
                    PASSWORD SECRET sql_server_password2{self.suffix}
                  )

                $ sql-server-execute name=sql-server
                USE test;
                WITH nums AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM nums WHERE i < 100) INSERT INTO sql_server_source_table{self.suffix} (f1, f2, f3, f4) SELECT 'C', i, REPLICATE('C', {self.repeats} - i), NULL FROM nums OPTION (MAXRECURSION 100);
                UPDATE sql_server_source_table{self.suffix} SET f2 = f2 + 100;

                $ sql-server-execute name=sql-server
                USE test;
                WITH nums AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM nums WHERE i < 100) INSERT INTO sql_server_source_table{self.suffix} (f1, f2, f3, f4) SELECT 'D', i, REPLICATE('D', {self.repeats} - i), NULL FROM nums OPTION (MAXRECURSION 100);
                UPDATE sql_server_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SOURCE sql_server_source2{self.suffix}
                  FROM SQL SERVER CONNECTION sql_server_connection2{self.suffix};
                > CREATE TABLE sql_server_source_tableB{self.suffix} FROM SOURCE sql_server_source2{self.suffix} (REFERENCE sql_server_source_table{self.suffix});

                # Create a view with a complex dependency structure
                > CREATE VIEW IF NOT EXISTS table_a_b_count_sum AS SELECT SUM(total_count) AS total_rows FROM (
                        SELECT COUNT(*) AS total_count FROM sql_server_source_tableA{self.suffix}
                        UNION ALL
                        SELECT COUNT(*) AS total_count FROM sql_server_source_tableB{self.suffix}
                    );
                """
                + (
                    f"""
                # Wait until Pg snapshot is complete in order to avoid database-issues#5601
                > SELECT COUNT(*) > 0 FROM sql_server_source_tableA{self.suffix}
                true

                # Wait until Pg snapshot is complete in order to avoid database-issues#5601
                > SELECT COUNT(*) > 0 FROM sql_server_source_tableB{self.suffix}
                true
                """
                    if self.wait
                    else ""
                ),
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                WITH nums AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM nums WHERE i < 100) INSERT INTO sql_server_source_table{self.suffix} (f1, f2, f3, f4) SELECT 'E', i, REPLICATE('E', {self.repeats} - i), NULL FROM nums OPTION (MAXRECURSION 100);
                UPDATE sql_server_source_table{self.suffix} SET f2 = f2 + 100;

                $ sql-server-execute name=sql-server
                USE test;
                WITH nums AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM nums WHERE i < 100) INSERT INTO sql_server_source_table{self.suffix} (f1, f2, f3, f4) SELECT 'F', i, REPLICATE('F', {self.repeats} - i), NULL FROM nums OPTION (MAXRECURSION 100);
                UPDATE sql_server_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SECRET sql_server_password3{self.suffix} AS '{SqlServer.DEFAULT_SA_PASSWORD}';

                > CREATE CONNECTION sql_server_connection3{self.suffix} TO SQL SERVER (
                    HOST 'sql-server',
                    DATABASE test,
                    USER {SqlServer.DEFAULT_USER},
                    PASSWORD SECRET sql_server_password3{self.suffix}
                  )

                > CREATE SOURCE sql_server_source3{self.suffix}
                  FROM SQL SERVER CONNECTION sql_server_connection3{self.suffix};
                > CREATE TABLE sql_server_source_tableC{self.suffix} FROM SOURCE sql_server_source3{self.suffix} (REFERENCE sql_server_source_table{self.suffix});

                $ sql-server-execute name=sql-server
                USE test;
                WITH nums AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM nums WHERE i < 100) INSERT INTO sql_server_source_table{self.suffix} (f1, f2, f3, f4) SELECT 'G', i, REPLICATE('G', {self.repeats} - i), NULL FROM nums OPTION (MAXRECURSION 100);
                UPDATE sql_server_source_table{self.suffix} SET f2 = f2 + 100;


                $ sql-server-execute name=sql-server
                USE test;
                WITH nums AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM nums WHERE i < 100) INSERT INTO sql_server_source_table{self.suffix} (f1, f2, f3, f4) SELECT 'H', i, REPLICATE('H', {self.repeats} - i), NULL FROM nums OPTION (MAXRECURSION 100);
                UPDATE sql_server_source_table{self.suffix} SET f2 = f2 + 100;
                """
                + (
                    f"""
                # Wait until Pg snapshot is complete in order to avoid database-issues#5601
                > SELECT COUNT(*) > 0 FROM sql_server_source_tableB{self.suffix}
                true
                > SELECT COUNT(*) > 0 FROM sql_server_source_tableC{self.suffix}
                true
                """
                    if self.wait
                    else ""
                ),
            ]
        ]

    def validate(self) -> Testdrive:
        sql = dedent(
            f"""
            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            # Can take longer after a restart
            $ set-sql-timeout duration=600s

            # Trying to narrow down whether
            # https://github.com/MaterializeInc/database-issues/issues/8102
            # is a problem with the source or elsewhere.
            > WITH t AS (SELECT * FROM sql_server_source_tableA{self.suffix})
              SELECT COUNT(*)
              FROM t
              GROUP BY row(t.*)
              HAVING COUNT(*) < 0;

            > WITH t AS (SELECT * FROM sql_server_source_tableB{self.suffix})
              SELECT COUNT(*)
              FROM t
              GROUP BY row(t.*)
              HAVING COUNT(*) < 0;

            > WITH t AS (SELECT * FROM sql_server_source_tableC{self.suffix})
              SELECT COUNT(*)
              FROM t
              GROUP BY row(t.*)
              HAVING COUNT(*) < 0;

            > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM sql_server_source_tableA{self.suffix} GROUP BY f1;
            A 800 {self.expects}
            B 800 {self.expects}
            C 700 {self.expects}
            D 600 {self.expects}
            E 500 {self.expects}
            F 400 {self.expects}
            G 300 {self.expects}
            H 200 {self.expects}

            > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM sql_server_source_tableB{self.suffix} GROUP BY f1;
            A 800 {self.expects}
            B 800 {self.expects}
            C 700 {self.expects}
            D 600 {self.expects}
            E 500 {self.expects}
            F 400 {self.expects}
            G 300 {self.expects}
            H 200 {self.expects}

            > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM sql_server_source_tableC{self.suffix} GROUP BY f1;
            A 800 {self.expects}
            B 800 {self.expects}
            C 700 {self.expects}
            D 600 {self.expects}
            E 500 {self.expects}
            F 400 {self.expects}
            G 300 {self.expects}
            H 200 {self.expects}

            > SELECT total_rows FROM table_a_b_count_sum;
            1600

            # TODO: Figure out the quoting here -- it returns "f4" when done using the SQL shell
            # > SELECT regexp_match(create_sql, 'TEXT COLUMNS = \\((.*?)\\)')[1] FROM (SHOW CREATE SOURCE sql_server_source_tableA{self.suffix});
            # "\"f4\""

            # Confirm that the primary key information has been propagated from Pg
            > SELECT key FROM (SHOW INDEXES ON sql_server_source_tableA{self.suffix});
            {{f1,f2}}

            ?[version>=13500] EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM sql_server_source_tableA{self.suffix};
            Explained Query (fast path):
              Project (#0, #1)
                ReadIndex on=materialize.public.sql_server_source_tablea{self.suffix} sql_server_source_tablea{self.suffix}_primary_idx=[*** full scan ***]

            Used Indexes:
              - materialize.public.sql_server_source_tablea{self.suffix}_primary_idx (*** full scan ***)

            Target cluster: quickstart

            ?[version<13500] EXPLAIN OPTIMIZED PLAN FOR SELECT DISTINCT f1, f2 FROM sql_server_source_tableA{self.suffix};
            Explained Query (fast path):
              Project (#0, #1)
                ReadIndex on=materialize.public.sql_server_source_tablea{self.suffix} sql_server_source_tablea{self.suffix}_primary_idx=[*** full scan ***]

            Used Indexes:
              - materialize.public.sql_server_source_tablea{self.suffix}_primary_idx (*** full scan ***)

            Target cluster: quickstart
            """
        )

        return Testdrive(sql)


@externally_idempotent(False)
class SqlServerCdc(SqlServerCdcBase, Check):
    def __init__(
        self,
        base_version: MzVersion,
        rng: Random | None,
    ) -> None:
        super().__init__(wait=True, base_version=base_version, rng=rng)

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.154.0-dev")


@externally_idempotent(False)
class SqlServerCdcNoWait(SqlServerCdcBase, Check):
    def __init__(self, base_version: MzVersion, rng: Random | None) -> None:
        super().__init__(wait=False, base_version=base_version, rng=rng)

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.154.0-dev")


@externally_idempotent(False)
class SqlServerCdcMzNow(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.154.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                CREATE TABLE sql_server_mz_now_table (f1 DATETIME2, f2 CHAR(5), PRIMARY KEY (f1, f2));
                EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sql_server_mz_now_table', @role_name = 'SA', @supports_net_changes = 0;

                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'A1');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'B1');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'C1');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'D1');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'E1');

                > CREATE SECRET sql_server_mz_now_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}';

                > CREATE CONNECTION sql_server_mz_now_conn TO SQL SERVER (
                    HOST 'sql-server',
                    DATABASE test,
                    USER {SqlServer.DEFAULT_USER},
                    PASSWORD SECRET sql_server_mz_now_pass
                  );

                > CREATE SOURCE sql_server_mz_now_source
                  FROM SQL SERVER CONNECTION sql_server_mz_now_conn;
                > CREATE TABLE sql_server_mz_now_table FROM SOURCE sql_server_mz_now_source (REFERENCE sql_server_mz_now_table);

                # Return all rows fresher than 60 seconds
                > CREATE MATERIALIZED VIEW sql_server_mz_now_view AS
                  SELECT * FROM sql_server_mz_now_table
                  WHERE mz_now() <= ROUND(EXTRACT(epoch FROM f1 + INTERVAL '60' SECOND) * 1000)
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'A2');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'B2');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'C2');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'D2');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'E2');
                DELETE FROM sql_server_mz_now_table WHERE f2 = 'B1';
                UPDATE sql_server_mz_now_table SET f1 = SYSDATETIME() WHERE f2 = 'C1';
                """,
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'A3');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'B3');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'C3');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'D3');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'E3');
                DELETE FROM sql_server_mz_now_table WHERE f2 = 'B2';
                UPDATE sql_server_mz_now_table SET f1 = SYSDATETIME() WHERE f2 = 'D1';
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                > SELECT COUNT(*) FROM sql_server_mz_now_table;
                13

                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'A4');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'B4');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'C4');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'D4');
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'E4');
                DELETE FROM sql_server_mz_now_table WHERE f2 = 'B3';
                UPDATE sql_server_mz_now_table SET f1 = SYSDATETIME() WHERE f2 = 'E1'

                # Expect some rows newer than 180 seconds in view
                > SELECT COUNT(*) >= 6 FROM sql_server_mz_now_view
                  WHERE f1 > NOW() - INTERVAL '180' SECOND;
                true

                # Expect no rows older than 180 seconds in view
                > SELECT COUNT(*) FROM sql_server_mz_now_view
                  WHERE f1 < NOW() - INTERVAL '180' SECOND;
                0

                # Rollback the last INSERTs so that validate() can be called multiple times
                $ sql-server-execute name=sql-server
                USE test;
                INSERT INTO sql_server_mz_now_table VALUES (SYSDATETIME(), 'B3');
                DELETE FROM sql_server_mz_now_table WHERE f2 LIKE '%4%';
                """
            )
        )
