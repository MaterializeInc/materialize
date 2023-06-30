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
from typing import Any, List, Optional

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, CheckDisabled
from materialize.util import MzVersion


class PgCdcBase:
    base_version: MzVersion
    wait: bool
    repeats: int
    expects: int

    def __init__(self, wait: bool, **kwargs: Any) -> None:
        self.wait = wait
        self.repeats = 1024 if wait else 16384
        self.expects = 97350 if wait else 1633350
        super().__init__(**kwargs)  # foward unused args to Check/CheckDisabled

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                CREATE USER postgres1 WITH SUPERUSER PASSWORD 'postgres';
                ALTER USER postgres1 WITH replication;
                DROP PUBLICATION IF EXISTS postgres_source;

                DROP TABLE IF EXISTS postgres_source_table;

                CREATE TABLE postgres_source_table (f1 TEXT, f2 INTEGER, f3 TEXT UNIQUE NOT NULL, PRIMARY KEY(f1, f2));
                ALTER TABLE postgres_source_table REPLICA IDENTITY FULL;

                INSERT INTO postgres_source_table SELECT 'A', i, REPEAT('A', {self.repeats} - i) FROM generate_series(1,100) AS i;

                CREATE PUBLICATION postgres_source FOR ALL TABLES;

                > CREATE SECRET pgpass1 AS 'postgres';

                > CREATE CONNECTION pg1 FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER postgres1,
                  PASSWORD SECRET pgpass1
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE SOURCE postgres_source1
                  FROM POSTGRES CONNECTION pg1
                  (PUBLICATION 'postgres_source')
                  FOR TABLES (postgres_source_table AS postgres_source_tableA);

                > CREATE DEFAULT INDEX ON postgres_source_tableA;

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table SELECT 'B', i, REPEAT('B', {self.repeats} - i) FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table SET f2 = f2 + 100;

                > CREATE SECRET pgpass2 AS 'postgres';

                > CREATE CONNECTION pg2 FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER postgres1,
                  PASSWORD SECRET pgpass1

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table SELECT 'C', i, REPEAT('C', {self.repeats} - i) FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table SET f2 = f2 + 100;
                """
                + (
                    """
                # Wait until Pg snapshot is complete in order to avoid #18940
                > SELECT COUNT(*) > 0 FROM postgres_source_tableA
                true
                """
                    if self.wait
                    else ""
                ),
                f"""
                $[version>=5200] postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                GRANT USAGE ON CONNECTION pg2 TO materialize

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table SELECT 'D', i, REPEAT('D', {self.repeats} - i) FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table SET f2 = f2 + 100;

                > CREATE SOURCE postgres_source2
                  FROM POSTGRES CONNECTION pg2
                  (PUBLICATION 'postgres_source')
                  FOR TABLES (postgres_source_table AS postgres_source_tableB);

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table SELECT 'E', i, REPEAT('E', {self.repeats} - i) FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table SET f2 = f2 + 100;

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table SELECT 'F', i, REPEAT('F', {self.repeats} - i) FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table SET f2 = f2 + 100;

                > CREATE SECRET pgpass3 AS 'postgres';

                > CREATE CONNECTION pg3 FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER postgres1,
                  PASSWORD SECRET pgpass3

                > CREATE SOURCE postgres_source3
                  FROM POSTGRES CONNECTION pg3
                  (PUBLICATION 'postgres_source')
                  FOR TABLES (postgres_source_table AS postgres_source_tableC);

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table SELECT 'G', i, REPEAT('G', {self.repeats} - i) FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table SET f2 = f2 + 100;


                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table SELECT 'H', i, REPEAT('X', {self.repeats} - i) FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table SET f2 = f2 + 100;
                """
                + (
                    """
                # Wait until Pg snapshot is complete in order to avoid #18940
                > SELECT COUNT(*) > 0 FROM postgres_source_tableB
                true
                > SELECT COUNT(*) > 0 FROM postgres_source_tableC
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
                $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                GRANT SELECT ON postgres_source_tableA TO materialize
                GRANT SELECT ON postgres_source_tableB TO materialize
                GRANT SELECT ON postgres_source_tableC TO materialize

                > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM postgres_source_tableA GROUP BY f1;
                A 800 {self.expects}
                B 800 {self.expects}
                C 700 {self.expects}
                D 600 {self.expects}
                E 500 {self.expects}
                F 400 {self.expects}
                G 300 {self.expects}
                H 200 {self.expects}

                > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM postgres_source_tableB GROUP BY f1;
                A 800 {self.expects}
                B 800 {self.expects}
                C 700 {self.expects}
                D 600 {self.expects}
                E 500 {self.expects}
                F 400 {self.expects}
                G 300 {self.expects}
                H 200 {self.expects}

                > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM postgres_source_tableC GROUP BY f1;
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
                    """
                    # Confirm that the primary key information has been propagated from Pg
                    > SELECT key FROM (SHOW INDEXES ON postgres_source_tableA);
                    {f1,f2}

                    ? EXPLAIN SELECT DISTINCT f1, f2 FROM postgres_source_tableA;
                    Explained Query (fast path):
                      Project (#0, #1)
                        ReadExistingIndex materialize.public.postgres_source_tablea_primary_idx

                    Used Indexes:
                      - materialize.public.postgres_source_tablea_primary_idx
                    """
                )
                if self.base_version >= MzVersion.parse("0.50.0-dev")
                else ""
            )
        )


class PgCdc(PgCdcBase, Check):
    def __init__(self, base_version: MzVersion, rng: Optional[Random]) -> None:
        super().__init__(wait=True, base_version=base_version, rng=rng)


# TODO(def-) Enable this check (with an adequate version limitation) when #18940 is fixed
class PgCdcNoWait(PgCdcBase, CheckDisabled):
    def __init__(self, base_version: MzVersion, rng: Optional[Random]) -> None:
        super().__init__(wait=False, base_version=base_version, rng=rng)


class PgCdcMzNow(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                CREATE USER postgres2 WITH SUPERUSER PASSWORD 'postgres';
                ALTER USER postgres2 WITH replication;
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

                > CREATE SECRET postgres_mz_now_pass AS 'postgres';

                > CREATE CONNECTION postgres_mz_now_conn FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER postgres2,
                  PASSWORD SECRET postgres_mz_now_pass

                > CREATE SOURCE postgres_mz_now_source
                  FROM POSTGRES CONNECTION postgres_mz_now_conn
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
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'A2');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'B2');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'C2');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'D2');
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'E2');
                DELETE FROM postgres_mz_now_table WHERE f2 = 'B1';
                UPDATE postgres_mz_now_table SET f1 = NOW() WHERE f2 = 'C1';
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
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

                $ postgres-execute connection=postgres://postgres:postgres@postgres
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
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'B3');
                DELETE FROM postgres_mz_now_table WHERE f2 LIKE '%4%';
                """
            )
        )
