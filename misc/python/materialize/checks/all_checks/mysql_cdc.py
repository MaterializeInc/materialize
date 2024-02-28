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
from materialize.mz_version import MzVersion
from materialize.mzcompose.services.mysql import MySql


class MySqlCdcBase:
    base_version: MzVersion
    wait: bool
    suffix: str
    repeats: int
    expects: int

    def __init__(self, wait: bool, **kwargs: Any) -> None:
        self.wait = wait
        self.repeats = 1024 if wait else 16384
        self.expects = 97350 if wait else 1633350
        self.suffix = f"_{str(wait).lower()}"
        super().__init__(**kwargs)  # forward unused args to Check

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                # create the database if it does not exist yet but do not drop it
                CREATE DATABASE IF NOT EXISTS public;
                USE public;

                CREATE USER mysql1{self.suffix} IDENTIFIED BY 'mysql';
                GRANT SELECT ON performance_schema.replication_connection_configuration TO mysql1{self.suffix};
                GRANT REPLICATION SLAVE ON *.* TO mysql1{self.suffix};
                GRANT ALL ON public.* TO mysql1{self.suffix};

                DROP TABLE IF EXISTS mysql_source_table{self.suffix};

                # uniqueness constraint not possible for length of 1024 characters upwards (max key length is 3072 bytes)
                CREATE TABLE mysql_source_table{self.suffix} (f1 VARCHAR(32), f2 INTEGER, f3 TEXT NOT NULL, PRIMARY KEY(f1, f2));

                SET @i:=0;
                CREATE TABLE sequence{self.suffix} (i INT);
                INSERT INTO sequence{self.suffix} SELECT (@i:=@i+1) FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 100;

                INSERT INTO mysql_source_table{self.suffix} SELECT 'A', i, REPEAT('A', {self.repeats} - i) FROM sequence{self.suffix} WHERE i <= 100;

                > CREATE SECRET mysqlpass1{self.suffix} AS 'mysql';

                > CREATE CONNECTION mysql1{self.suffix} TO MYSQL (
                    HOST 'mysql',
                    USER mysql1{self.suffix},
                    PASSWORD SECRET mysqlpass1{self.suffix}
                  )
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE SOURCE mysql_source1{self.suffix}
                  FROM MYSQL CONNECTION mysql1{self.suffix}
                  FOR TABLES (public.mysql_source_table{self.suffix} AS mysql_source_tableA{self.suffix});

                > CREATE DEFAULT INDEX ON mysql_source_tableA{self.suffix};

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'B', i, REPEAT('B', {self.repeats} - i) FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SECRET mysqlpass2{self.suffix} AS 'mysql';

                > CREATE CONNECTION mysql2{self.suffix} TO MYSQL (
                    HOST 'mysql',
                    USER mysql1{self.suffix},
                    PASSWORD SECRET mysqlpass1{self.suffix}
                  )

                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'C', i, REPEAT('C', {self.repeats} - i) FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;
                """
                + (
                    f"""
                # Wait until MySQL snapshot is complete
                > SELECT COUNT(*) > 0 FROM mysql_source_tableA{self.suffix}
                true
                """
                    if self.wait
                    else ""
                ),
                f"""
                $[version>=5200] postgres-execute connection=postgres://mz_system@${{testdrive.materialize-internal-sql-addr}}
                GRANT USAGE ON CONNECTION mysql2{self.suffix} TO materialize

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'D', i, REPEAT('D', {self.repeats} - i) FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SOURCE mysql_source2{self.suffix}
                  FROM MYSQL CONNECTION mysql2{self.suffix}
                  FOR TABLES (public.mysql_source_table{self.suffix} AS mysql_source_tableB{self.suffix});

                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'E', i, REPEAT('E', {self.repeats} - i) FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;

                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'F', i, REPEAT('F', {self.repeats} - i) FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SECRET mysqlpass3{self.suffix} AS 'mysql';

                > CREATE CONNECTION mysql3{self.suffix} TO MYSQL (
                    HOST 'mysql',
                    USER mysql1{self.suffix},
                    PASSWORD SECRET mysqlpass3{self.suffix}
                  )

                > CREATE SOURCE mysql_source3{self.suffix}
                  FROM MYSQL CONNECTION mysql3{self.suffix}
                  FOR TABLES (public.mysql_source_table{self.suffix} AS mysql_source_tableC{self.suffix});

                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'G', i, REPEAT('G', {self.repeats} - i) FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;


                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'H', i, REPEAT('X', {self.repeats} - i) FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;
                """
                + (
                    f"""
                # Wait until MySQL snapshot is complete
                > SELECT COUNT(*) > 0 FROM mysql_source_tableB{self.suffix}
                true
                > SELECT COUNT(*) > 0 FROM mysql_source_tableC{self.suffix}
                true
                """
                    if self.wait
                    else ""
                ),
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://mz_system@${{testdrive.materialize-internal-sql-addr}}
                GRANT SELECT ON mysql_source_tableA{self.suffix} TO materialize
                GRANT SELECT ON mysql_source_tableB{self.suffix} TO materialize
                GRANT SELECT ON mysql_source_tableC{self.suffix} TO materialize

                > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM mysql_source_tableA{self.suffix} GROUP BY f1;
                A 800 {self.expects}
                B 800 {self.expects}
                C 700 {self.expects}
                D 600 {self.expects}
                E 500 {self.expects}
                F 400 {self.expects}
                G 300 {self.expects}
                H 200 {self.expects}

                > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM mysql_source_tableB{self.suffix} GROUP BY f1;
                A 800 {self.expects}
                B 800 {self.expects}
                C 700 {self.expects}
                D 600 {self.expects}
                E 500 {self.expects}
                F 400 {self.expects}
                G 300 {self.expects}
                H 200 {self.expects}

                > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM mysql_source_tableC{self.suffix} GROUP BY f1;
                A 800 {self.expects}
                B 800 {self.expects}
                C 700 {self.expects}
                D 600 {self.expects}
                E 500 {self.expects}
                F 400 {self.expects}
                G 300 {self.expects}
                H 200 {self.expects}
                """
            )
            + (
                dedent(
                    f"""
                    # Confirm that the primary key information has been propagated from MySQL
                    > SELECT key FROM (SHOW INDEXES ON mysql_source_tableA{self.suffix});
                    {{f1,f2}}

                    ? EXPLAIN SELECT DISTINCT f1, f2 FROM mysql_source_tableA{self.suffix};
                    Explained Query (fast path):
                      Project (#0, #1)
                        ReadIndex on=materialize.public.mysql_source_tablea{self.suffix} mysql_source_tablea{self.suffix}_primary_idx=[*** full scan ***]

                    Used Indexes:
                      - materialize.public.mysql_source_tablea{self.suffix}_primary_idx (*** full scan ***)
                    """
                )
                if self.base_version >= MzVersion.parse_mz("v0.50.0-dev")
                else ""
            )
        )


@externally_idempotent(False)
class MySqlCdc(MySqlCdcBase, Check):
    def __init__(self, base_version: MzVersion, rng: Random | None) -> None:
        super().__init__(wait=True, base_version=base_version, rng=rng)


@externally_idempotent(False)
class MySqlCdcNoWait(MySqlCdcBase, Check):
    def __init__(self, base_version: MzVersion, rng: Random | None) -> None:
        super().__init__(wait=False, base_version=base_version, rng=rng)


@externally_idempotent(False)
class MySqlCdcMzNow(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                # create the database if it does not exist yet but do not drop it
                CREATE DATABASE IF NOT EXISTS public;
                USE public;

                CREATE USER mysql2 IDENTIFIED BY 'mysql';
                GRANT SELECT ON performance_schema.replication_connection_configuration TO mysql2;
                GRANT REPLICATION SLAVE ON *.* TO mysql2;
                GRANT ALL ON public.* TO mysql2;

                DROP TABLE IF EXISTS mysql_mz_now_table;

                CREATE TABLE mysql_mz_now_table (f1 TIMESTAMP, f2 CHAR(5), PRIMARY KEY (f1, f2));

                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'A1');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'B1');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'C1');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'D1');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'E1');

                > CREATE SECRET mysql_mz_now_pass AS 'mysql';
                > CREATE CONNECTION mysql_mz_now_conn TO MYSQL (
                    HOST 'mysql',
                    USER mysql2,
                    PASSWORD SECRET mysql_mz_now_pass
                  )

                > CREATE SOURCE mysql_mz_now_source
                  FROM MYSQL CONNECTION mysql_mz_now_conn
                  FOR TABLES (public.mysql_mz_now_table);

                # Return all rows fresher than 60 seconds
                > CREATE MATERIALIZED VIEW mysql_mz_now_view AS
                  SELECT * FROM mysql_mz_now_table
                  WHERE mz_now() <= ROUND(EXTRACT(epoch FROM f1 + INTERVAL '60' SECOND) * 1000)
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'A2');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'B2');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'C2');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'D2');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'E2');
                DELETE FROM mysql_mz_now_table WHERE f2 = 'B1';
                UPDATE mysql_mz_now_table SET f1 = NOW() WHERE f2 = 'C1';
                """,
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'A3');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'B3');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'C3');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'D3');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'E3');
                DELETE FROM mysql_mz_now_table WHERE f2 = 'B2';
                UPDATE mysql_mz_now_table SET f1 = NOW() WHERE f2 = 'D1';
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                > SELECT COUNT(*) FROM mysql_mz_now_table;
                13

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'A4');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'B4');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'C4');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'D4');
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'E4');
                DELETE FROM mysql_mz_now_table WHERE f2 = 'B3';
                UPDATE mysql_mz_now_table SET f1 = NOW() WHERE f2 = 'E1'

                # Expect some rows newer than 60 seconds in view
                > SELECT COUNT(*) >= 6 FROM mysql_mz_now_view
                  WHERE f1 > NOW() - INTERVAL '60' SECOND;
                true

                # Expect no rows older than 60 seconds in view
                > SELECT COUNT(*) FROM mysql_mz_now_view
                  WHERE f1 < NOW() - INTERVAL '60' SECOND;
                0

                # Rollback the last INSERTs so that validate() can be called multiple times
                $ mysql-execute name=mysql
                INSERT INTO mysql_mz_now_table VALUES (NOW(), 'B3');
                DELETE FROM mysql_mz_now_table WHERE f2 LIKE '%4%';
                """
            )
        )
