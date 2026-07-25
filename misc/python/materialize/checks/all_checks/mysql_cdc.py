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
from materialize.mzcompose.services.mysql import MySql


class MySqlCdcBase:
    base_version: MzVersion
    current_version: MzVersion
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
        return Testdrive(dedent(f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                # create the database if it does not exist yet but do not drop it
                CREATE DATABASE IF NOT EXISTS public;
                USE public;

                CREATE USER mysql1{self.suffix} IDENTIFIED BY 'mysql';
                GRANT REPLICATION SLAVE ON *.* TO mysql1{self.suffix};
                GRANT ALL ON public.* TO mysql1{self.suffix};

                DROP TABLE IF EXISTS mysql_source_table{self.suffix};

                # uniqueness constraint not possible for length of 1024 characters upwards (max key length is 3072 bytes)
                CREATE TABLE mysql_source_table{self.suffix} (f1 VARCHAR(32), f2 INTEGER, f3 TEXT NOT NULL, f4 JSON, PRIMARY KEY(f1, f2));

                SET @i:=0;
                CREATE TABLE sequence{self.suffix} (i INT);
                INSERT INTO sequence{self.suffix} SELECT (@i:=@i+1) FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 100;

                INSERT INTO mysql_source_table{self.suffix} SELECT 'A', i, REPEAT('A', {self.repeats} - i), NULL FROM sequence{self.suffix} WHERE i <= 100;

                > CREATE SECRET mysqlpass1{self.suffix} AS 'mysql';

                > CREATE CONNECTION mysql1{self.suffix} TO MYSQL (
                    HOST 'mysql',
                    USER mysql1{self.suffix},
                    PASSWORD SECRET mysqlpass1{self.suffix}
                  )
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE SOURCE mysql_source1{self.suffix}
                  FROM MYSQL CONNECTION mysql1{self.suffix};
                > CREATE TABLE mysql_source_tableA{self.suffix} FROM SOURCE mysql_source1{self.suffix} (REFERENCE public.mysql_source_table{self.suffix});

                > CREATE DEFAULT INDEX ON mysql_source_tableA{self.suffix};

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'B', i, REPEAT('B', {self.repeats} - i), NULL FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SECRET mysqlpass2{self.suffix} AS 'mysql';

                > CREATE CONNECTION mysql2{self.suffix} TO MYSQL (
                    HOST 'mysql',
                    USER mysql1{self.suffix},
                    PASSWORD SECRET mysqlpass2{self.suffix}
                  )

                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'C', i, REPEAT('C', {self.repeats} - i), NULL FROM sequence{self.suffix} WHERE i <= 100;
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
                $ postgres-execute connection=postgres://mz_system@${{testdrive.materialize-internal-sql-addr}}
                GRANT USAGE ON CONNECTION mysql2{self.suffix} TO materialize

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'D', i, REPEAT('D', {self.repeats} - i), NULL FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SOURCE mysql_source2{self.suffix}
                  FROM MYSQL CONNECTION mysql2{self.suffix};
                > CREATE TABLE mysql_source_tableB{self.suffix} FROM SOURCE mysql_source2{self.suffix} (REFERENCE public.mysql_source_table{self.suffix});

                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'E', i, REPEAT('E', {self.repeats} - i), NULL FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;

                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'F', i, REPEAT('F', {self.repeats} - i), NULL FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SECRET mysqlpass3{self.suffix} AS 'mysql';

                > CREATE CONNECTION mysql3{self.suffix} TO MYSQL (
                    HOST 'mysql',
                    USER mysql1{self.suffix},
                    PASSWORD SECRET mysqlpass3{self.suffix}
                  )

                > CREATE SOURCE mysql_source3{self.suffix}
                  FROM MYSQL CONNECTION mysql3{self.suffix};
                > CREATE TABLE mysql_source_tableC{self.suffix} FROM SOURCE mysql_source3{self.suffix} (REFERENCE public.mysql_source_table{self.suffix});

                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'G', i, REPEAT('G', {self.repeats} - i), NULL FROM sequence{self.suffix} WHERE i <= 100;
                UPDATE mysql_source_table{self.suffix} SET f2 = f2 + 100;


                $ mysql-execute name=mysql
                SET @i:=0;
                INSERT INTO mysql_source_table{self.suffix} SELECT 'H', i, REPEAT('X', {self.repeats} - i), NULL FROM sequence{self.suffix} WHERE i <= 100;
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
        sql = dedent(f"""
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

            # TODO: Figure out the quoting here -- it returns "f4" when done using the SQL shell
            # (Might have changed again with https://github.com/MaterializeInc/materialize/pull/31933)
            # > SELECT regexp_match(create_sql, 'TEXT COLUMNS = \\((.*?)\\)')[1] FROM (SHOW CREATE SOURCE mysql_source_tableA{self.suffix});
            # "\"f4\""

            # Confirm that the primary key information has been propagated from MySQL
            > SELECT key FROM (SHOW INDEXES ON mysql_source_tableA{self.suffix});
            {{f1,f2}}

            ?[version>=13500] EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM mysql_source_tableA{self.suffix};
            Explained Query (fast path):
              Project (#0, #1)
                ReadIndex on=materialize.public.mysql_source_tablea{self.suffix} mysql_source_tablea{self.suffix}_primary_idx=[*** full scan ***]

            Used Indexes:
              - materialize.public.mysql_source_tablea{self.suffix}_primary_idx (*** full scan ***)

            Target cluster: quickstart

            ?[version<13500] EXPLAIN OPTIMIZED PLAN FOR SELECT DISTINCT f1, f2 FROM mysql_source_tableA{self.suffix};
            Explained Query (fast path):
              Project (#0, #1)
                ReadIndex on=materialize.public.mysql_source_tablea{self.suffix} mysql_source_tablea{self.suffix}_primary_idx=[*** full scan ***]

            Used Indexes:
              - materialize.public.mysql_source_tablea{self.suffix}_primary_idx (*** full scan ***)

            Target cluster: quickstart
            """)

        return Testdrive(sql)


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
        return Testdrive(dedent(f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                # create the database if it does not exist yet but do not drop it
                CREATE DATABASE IF NOT EXISTS public;
                USE public;

                CREATE USER mysql2 IDENTIFIED BY 'mysql';
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
                  FROM MYSQL CONNECTION mysql_mz_now_conn;
                > CREATE TABLE mysql_mz_now_table FROM SOURCE mysql_mz_now_source (REFERENCE public.mysql_mz_now_table);

                # Return all rows fresher than 60 seconds
                > CREATE MATERIALIZED VIEW mysql_mz_now_view AS
                  SELECT * FROM mysql_mz_now_table
                  WHERE mz_now() <= ROUND(EXTRACT(epoch FROM f1 + INTERVAL '60' SECOND) * 1000)
                """))

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
        return Testdrive(dedent(f"""
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
                """))


@externally_idempotent(False)
class MySqlBitType(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.131.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent(f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                # create the database if it does not exist yet but do not drop it
                CREATE DATABASE IF NOT EXISTS public;
                USE public;

                CREATE USER mysql3 IDENTIFIED BY 'mysql';
                GRANT REPLICATION SLAVE ON *.* TO mysql3;
                GRANT ALL ON public.* TO mysql3;

                DROP TABLE IF EXISTS mysql_bit_table;

                CREATE TABLE mysql_bit_table (f1 BIT(11), f2 BIT(1));

                INSERT INTO mysql_bit_table VALUES (8, 0);
                INSERT INTO mysql_bit_table VALUES (13, 1)
                INSERT INTO mysql_bit_table VALUES (b'11100000100', b'1');
                INSERT INTO mysql_bit_table VALUES (b'0000', b'0');
                INSERT INTO mysql_bit_table VALUES (b'11111111111', b'0');

                > CREATE SECRET mysql_bit_pass AS 'mysql';
                > CREATE CONNECTION mysql_bit_conn TO MYSQL (
                    HOST 'mysql',
                    USER mysql3,
                    PASSWORD SECRET mysql_bit_pass
                  )

                > CREATE SOURCE mysql_bit_source
                  FROM MYSQL CONNECTION mysql_bit_conn;
                > CREATE TABLE mysql_bit_table FROM SOURCE mysql_bit_source (REFERENCE public.mysql_bit_table);

                # Return all rows
                > CREATE MATERIALIZED VIEW mysql_bit_view AS
                  SELECT * FROM mysql_bit_table
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                INSERT INTO mysql_bit_table VALUES (20, 1);
                """,
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                INSERT INTO mysql_bit_table VALUES (30, 1);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent(f"""
                > SELECT * FROM mysql_bit_table;
                0 0
                8 0
                13 1
                20 1
                30 1
                1796 1
                2047 0

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                INSERT INTO mysql_bit_table VALUES (40, 1);

                > SELECT * FROM mysql_bit_table;
                0 0
                8 0
                13 1
                20 1
                30 1
                40 1
                1796 1
                2047 0

                # Rollback the last INSERTs so that validate() can be called multiple times
                $ mysql-execute name=mysql
                DELETE FROM mysql_bit_table WHERE f1 = 40;

                > SELECT * FROM mysql_bit_table;
                0 0
                8 0
                13 1
                20 1
                30 1
                1796 1
                2047 0
                """))


@externally_idempotent(False)
class MySqlInvisibleColumn(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.133.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent(f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                # create the database if it does not exist yet but do not drop it
                CREATE DATABASE IF NOT EXISTS public;
                USE public;

                CREATE USER mysql4 IDENTIFIED BY 'mysql';
                GRANT REPLICATION SLAVE ON *.* TO mysql4;
                GRANT ALL ON public.* TO mysql4;

                DROP TABLE IF EXISTS mysql_invisible_table;

                CREATE TABLE mysql_invisible_table (f1 INT, f2 FLOAT INVISIBLE, f3 DATE INVISIBLE, f4 TEXT INVISIBLE);

                INSERT INTO mysql_invisible_table (f1, f2, f3, f4) VALUES (1, 0.1, '2025-01-01', 'one');

                > CREATE SECRET mysql_invisible_pass AS 'mysql';
                > CREATE CONNECTION mysql_invisible_conn TO MYSQL (
                    HOST 'mysql',
                    USER mysql4,
                    PASSWORD SECRET mysql_invisible_pass
                  )

                > CREATE SOURCE mysql_invisible_source
                  FROM MYSQL CONNECTION mysql_invisible_conn;
                > CREATE TABLE mysql_invisible_table FROM SOURCE mysql_invisible_source (REFERENCE public.mysql_invisible_table);

                # Return all rows
                > CREATE MATERIALIZED VIEW mysql_invisible_view AS
                  SELECT * FROM mysql_invisible_table
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                INSERT INTO mysql_invisible_table (f1, f2, f3, f4) VALUES (2, 0.2, '2025-02-02', 'two');
                """,
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                INSERT INTO mysql_invisible_table (f1, f2, f3, f4) VALUES (3, 0.3, '2025-03-03', 'three');
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent(f"""
                > SELECT * FROM mysql_invisible_table;
                1 0.1 2025-01-01 one
                2 0.2 2025-02-02 two
                3 0.3 2025-03-03 three

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                ALTER TABLE mysql_invisible_table ALTER COLUMN f2 SET VISIBLE;
                INSERT INTO mysql_invisible_table (f1, f2, f3, f4) VALUES (4, 0.4, '2025-04-04', 'four');

                > SELECT * FROM mysql_invisible_table;
                1 0.1 2025-01-01 one
                2 0.2 2025-02-02 two
                3 0.3 2025-03-03 three
                4 0.4 2025-04-04 four

                # Rollback the last INSERTs so that validate() can be called multiple times
                $ mysql-execute name=mysql
                DELETE FROM mysql_invisible_table WHERE f1 = 4;
                ALTER TABLE mysql_invisible_table ALTER COLUMN f2 SET INVISIBLE;

                > SELECT * FROM mysql_invisible_table;
                1 0.1 2025-01-01 one
                2 0.2 2025-02-02 two
                3 0.3 2025-03-03 three
                """))


def _sum_range(lo: int, hi: int) -> int:
    """Sum of the integers in [lo, hi]."""
    return (lo + hi) * (hi - lo + 1) // 2


@externally_idempotent(False)
class MySqlCdcPkSnapshot(Check):
    """Parallel PK-range snapshots across restarts and upgrades.

    Tables with a single-column BIGINT and a single-column VARCHAR primary key
    are snapshot on a multi-worker cluster, so the snapshot is split into
    per-worker PK ranges. A source is created in each phase so the snapshot
    runs on every version the scenario passes through. Validation asserts
    exact counts, distinct PK counts and checksums, catching duplicated or
    dropped ranges that a plain count can mask.
    """

    # The int table holds pk = 1..100000 with f2 = 3 * pk. Phase 1 deletes
    # pks 1..10000, bumps f2 by 7 for pks 50001..60000 and inserts pks
    # 100001..110000. Phase 2 deletes pks 20001..30000, bumps f2 by 11 for
    # pks 70001..80000 and inserts pks 110001..120000.
    INT_FINAL_COUNT = 100_000
    INT_FINAL_SUM = (
        3 * (_sum_range(10_001, 120_000) - _sum_range(20_001, 30_000))
        + 7 * 10_000
        + 11 * 10_000
    )
    # The text table holds pk = base-36 rendering of 1..50000 with f2 as the
    # sequence value. Phase 1 deletes f2 1..5000 and moves f2 10001..15000 up
    # by 1000000, phase 2 deletes f2 5001..10000 and moves f2 15001..20000 up
    # by 1000000.
    TEXT_FINAL_COUNT = 40_000
    TEXT_FINAL_SUM = _sum_range(10_001, 50_000) + 1_000_000 * 10_000

    def initialize(self) -> Testdrive:
        return Testdrive(dedent(f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                # create the database if it does not exist yet but do not drop it
                CREATE DATABASE IF NOT EXISTS public;
                USE public;

                CREATE USER mysql_pk_user IDENTIFIED BY 'mysql';
                GRANT REPLICATION SLAVE ON *.* TO mysql_pk_user;
                GRANT ALL ON public.* TO mysql_pk_user;

                DROP TABLE IF EXISTS mysql_pk_ten;
                CREATE TABLE mysql_pk_ten (f1 INTEGER);
                INSERT INTO mysql_pk_ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

                DROP TABLE IF EXISTS mysql_pk_int;
                CREATE TABLE mysql_pk_int (pk BIGINT PRIMARY KEY, f2 BIGINT);
                SET @i := 0;
                INSERT INTO mysql_pk_int SELECT @i := @i + 1, @i * 3 FROM mysql_pk_ten a1, mysql_pk_ten a2, mysql_pk_ten a3, mysql_pk_ten a4, mysql_pk_ten a5, mysql_pk_ten a6 LIMIT 100000;

                DROP TABLE IF EXISTS mysql_pk_text;
                CREATE TABLE mysql_pk_text (pk VARCHAR(32) PRIMARY KEY, f2 BIGINT);
                SET @j := 0;
                INSERT INTO mysql_pk_text SELECT CONCAT('key-', LPAD(CONV(@j := @j + 1, 10, 36), 8, '0')), @j FROM mysql_pk_ten a1, mysql_pk_ten a2, mysql_pk_ten a3, mysql_pk_ten a4, mysql_pk_ten a5 LIMIT 50000;

                > CREATE SECRET mysql_pk_pass AS 'mysql';
                > CREATE CONNECTION mysql_pk_conn TO MYSQL (
                    HOST 'mysql',
                    USER mysql_pk_user,
                    PASSWORD SECRET mysql_pk_pass
                  )

                # A multi-worker cluster so the snapshots split into per-worker
                # PK ranges instead of the single-worker whole-table read.
                > CREATE CLUSTER mysql_pk_cluster SIZE 'scale=1,workers=4', REPLICATION FACTOR 1

                > CREATE SOURCE mysql_pk_source1
                  IN CLUSTER mysql_pk_cluster
                  FROM MYSQL CONNECTION mysql_pk_conn;
                > CREATE TABLE mysql_pk_intA FROM SOURCE mysql_pk_source1 (REFERENCE public.mysql_pk_int);
                > CREATE TABLE mysql_pk_textA FROM SOURCE mysql_pk_source1 (REFERENCE public.mysql_pk_text);

                > CREATE DEFAULT INDEX ON mysql_pk_intA;

                > SELECT COUNT(*) FROM mysql_pk_intA
                100000
                > SELECT COUNT(*) FROM mysql_pk_textA
                50000
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                DELETE FROM mysql_pk_int WHERE pk BETWEEN 1 AND 10000;
                UPDATE mysql_pk_int SET f2 = f2 + 7 WHERE pk BETWEEN 50001 AND 60000;
                SET @i := 100000;
                INSERT INTO mysql_pk_int SELECT @i := @i + 1, @i * 3 FROM mysql_pk_ten a1, mysql_pk_ten a2, mysql_pk_ten a3, mysql_pk_ten a4 LIMIT 10000;
                DELETE FROM mysql_pk_text WHERE f2 BETWEEN 1 AND 5000;
                UPDATE mysql_pk_text SET f2 = f2 + 1000000 WHERE f2 BETWEEN 10001 AND 15000;

                > CREATE SOURCE mysql_pk_source2
                  IN CLUSTER mysql_pk_cluster
                  FROM MYSQL CONNECTION mysql_pk_conn;
                > CREATE TABLE mysql_pk_intB FROM SOURCE mysql_pk_source2 (REFERENCE public.mysql_pk_int);

                > SELECT COUNT(*) FROM mysql_pk_intB
                100000
                """,
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                DELETE FROM mysql_pk_int WHERE pk BETWEEN 20001 AND 30000;
                UPDATE mysql_pk_int SET f2 = f2 + 11 WHERE pk BETWEEN 70001 AND 80000;
                SET @i := 110000;
                INSERT INTO mysql_pk_int SELECT @i := @i + 1, @i * 3 FROM mysql_pk_ten a1, mysql_pk_ten a2, mysql_pk_ten a3, mysql_pk_ten a4 LIMIT 10000;
                DELETE FROM mysql_pk_text WHERE f2 BETWEEN 5001 AND 10000;
                UPDATE mysql_pk_text SET f2 = f2 + 1000000 WHERE f2 BETWEEN 15001 AND 20000;

                > CREATE SOURCE mysql_pk_source3
                  IN CLUSTER mysql_pk_cluster
                  FROM MYSQL CONNECTION mysql_pk_conn;
                > CREATE TABLE mysql_pk_intC FROM SOURCE mysql_pk_source3 (REFERENCE public.mysql_pk_int);

                > SELECT COUNT(*) FROM mysql_pk_intC
                100000
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        checks = "\n".join(dedent(f"""
                > SELECT COUNT(*), COUNT(DISTINCT pk), SUM(f2) FROM {table};
                {self.INT_FINAL_COUNT} {self.INT_FINAL_COUNT} {self.INT_FINAL_SUM}
                """) for table in ["mysql_pk_intA", "mysql_pk_intB", "mysql_pk_intC"])
        return Testdrive(
            dedent("""
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON mysql_pk_intA TO materialize
                GRANT SELECT ON mysql_pk_intB TO materialize
                GRANT SELECT ON mysql_pk_intC TO materialize
                GRANT SELECT ON mysql_pk_textA TO materialize
                """)
            + checks
            + dedent(f"""
                > SELECT COUNT(*), COUNT(DISTINCT pk), SUM(f2) FROM mysql_pk_textA;
                {self.TEXT_FINAL_COUNT} {self.TEXT_FINAL_COUNT} {self.TEXT_FINAL_SUM}

                # The primary key information has been propagated from MySQL
                > SELECT key FROM (SHOW INDEXES ON mysql_pk_intA);
                {{pk}}
                """)
        )


@externally_idempotent(False)
class MySqlCdcPkSnapshotNoWait(Check):
    """A large parallel PK-range snapshot that is never awaited, so restart,
    upgrade and 0dt scenarios can interrupt it mid-flight. The re-run snapshot
    plus replication must still converge to exact, duplicate-free contents.
    """

    # pk = 1..1000000 with f2 = 3 * pk. Phase 1 deletes pks 1..100000 and
    # inserts pks 1000001..1050000. Phase 2 deletes pks 100001..200000 and
    # bumps f2 by 5 for pks 500001..600000.
    FINAL_COUNT = 850_000
    FINAL_SUM = 3 * _sum_range(200_001, 1_050_000) + 5 * 100_000

    def initialize(self) -> Testdrive:
        return Testdrive(dedent(f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                # create the database if it does not exist yet but do not drop it
                CREATE DATABASE IF NOT EXISTS public;
                USE public;

                CREATE USER mysql_pk_nw_user IDENTIFIED BY 'mysql';
                GRANT REPLICATION SLAVE ON *.* TO mysql_pk_nw_user;
                GRANT ALL ON public.* TO mysql_pk_nw_user;

                DROP TABLE IF EXISTS mysql_pk_nw_ten;
                CREATE TABLE mysql_pk_nw_ten (f1 INTEGER);
                INSERT INTO mysql_pk_nw_ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

                DROP TABLE IF EXISTS mysql_pk_nowait;
                CREATE TABLE mysql_pk_nowait (pk BIGINT PRIMARY KEY, f2 BIGINT);
                SET @i := 0;
                INSERT INTO mysql_pk_nowait SELECT @i := @i + 1, @i * 3 FROM mysql_pk_nw_ten a1, mysql_pk_nw_ten a2, mysql_pk_nw_ten a3, mysql_pk_nw_ten a4, mysql_pk_nw_ten a5, mysql_pk_nw_ten a6 LIMIT 1000000;

                > CREATE SECRET mysql_pk_nw_pass AS 'mysql';
                > CREATE CONNECTION mysql_pk_nw_conn TO MYSQL (
                    HOST 'mysql',
                    USER mysql_pk_nw_user,
                    PASSWORD SECRET mysql_pk_nw_pass
                  )

                > CREATE CLUSTER mysql_pk_nw_cluster SIZE 'scale=1,workers=4', REPLICATION FACTOR 1

                # No wait for the snapshot anywhere before validate(), so that
                # the scenario's restarts can land while it is still running.
                > CREATE SOURCE mysql_pk_nw_source
                  IN CLUSTER mysql_pk_nw_cluster
                  FROM MYSQL CONNECTION mysql_pk_nw_conn;
                > CREATE TABLE mysql_pk_nowait FROM SOURCE mysql_pk_nw_source (REFERENCE public.mysql_pk_nowait);
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                DELETE FROM mysql_pk_nowait WHERE pk BETWEEN 1 AND 100000;
                SET @i := 1000000;
                INSERT INTO mysql_pk_nowait SELECT @i := @i + 1, @i * 3 FROM mysql_pk_nw_ten a1, mysql_pk_nw_ten a2, mysql_pk_nw_ten a3, mysql_pk_nw_ten a4, mysql_pk_nw_ten a5 LIMIT 50000;
                """,
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                DELETE FROM mysql_pk_nowait WHERE pk BETWEEN 100001 AND 200000;
                UPDATE mysql_pk_nowait SET f2 = f2 + 5 WHERE pk BETWEEN 500001 AND 600000;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent(f"""
                $ postgres-execute connection=postgres://mz_system@${{testdrive.materialize-internal-sql-addr}}
                GRANT SELECT ON mysql_pk_nowait TO materialize

                > SELECT COUNT(*), COUNT(DISTINCT pk), SUM(f2) FROM mysql_pk_nowait;
                {self.FINAL_COUNT} {self.FINAL_COUNT} {self.FINAL_SUM}
                """))
