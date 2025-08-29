# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
from random import Random
from textwrap import dedent
from typing import Any

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, externally_idempotent
from materialize.mz_version import MzVersion


class PgCdcBase:
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
        return Testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                CREATE USER postgres1{self.suffix} WITH SUPERUSER PASSWORD 'postgres';
                ALTER USER postgres1{self.suffix} WITH replication;
                DROP PUBLICATION IF EXISTS postgres_source{self.suffix};

                DROP TABLE IF EXISTS postgres_source_table{self.suffix};

                CREATE TABLE postgres_source_table{self.suffix} (f1 TEXT, f2 INTEGER, f3 TEXT UNIQUE NOT NULL, f4 JSONB, PRIMARY KEY(f1, f2));
                ALTER TABLE postgres_source_table{self.suffix} REPLICA IDENTITY FULL;

                INSERT INTO postgres_source_table{self.suffix} SELECT 'A', i, REPEAT('A', {self.repeats} - i), NULL FROM generate_series(1,100) AS i;

                CREATE PUBLICATION postgres_source{self.suffix} FOR ALL TABLES;

                > CREATE SECRET pgpass1{self.suffix} AS 'postgres';

                > CREATE CONNECTION pg1{self.suffix} FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER postgres1{self.suffix},
                  PASSWORD SECRET pgpass1{self.suffix}
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > CREATE SOURCE postgres_source1{self.suffix}
                  FROM POSTGRES CONNECTION pg1{self.suffix}
                  (PUBLICATION 'postgres_source{self.suffix}');
                > CREATE TABLE postgres_source_tableA{self.suffix} FROM SOURCE postgres_source1{self.suffix} (REFERENCE postgres_source_table{self.suffix});

                > CREATE DEFAULT INDEX ON postgres_source_tableA{self.suffix};

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table{self.suffix} SELECT 'B', i, REPEAT('B', {self.repeats} - i), NULL FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SECRET pgpass2{self.suffix} AS 'postgres';

                > CREATE CONNECTION pg2{self.suffix} FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER postgres1{self.suffix},
                  PASSWORD SECRET pgpass1{self.suffix}

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table{self.suffix} SELECT 'C', i, REPEAT('C', {self.repeats} - i), NULL FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table{self.suffix} SET f2 = f2 + 100;

                $ postgres-execute connection=postgres://mz_system@${{testdrive.materialize-internal-sql-addr}}
                GRANT USAGE ON CONNECTION pg2{self.suffix} TO materialize

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table{self.suffix} SELECT 'D', i, REPEAT('D', {self.repeats} - i), NULL FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SOURCE postgres_source2{self.suffix}
                  FROM POSTGRES CONNECTION pg2{self.suffix}
                  (PUBLICATION 'postgres_source{self.suffix}');
                > CREATE TABLE postgres_source_tableB{self.suffix} FROM SOURCE postgres_source2{self.suffix} (REFERENCE postgres_source_table{self.suffix});

                # Create a view with a complex dependency structure
                > CREATE VIEW IF NOT EXISTS table_a_b_count_sum AS SELECT SUM(total_count) AS total_rows FROM (
                        SELECT COUNT(*) AS total_count FROM postgres_source_tableA{self.suffix}
                        UNION ALL
                        SELECT COUNT(*) AS total_count FROM postgres_source_tableB{self.suffix}
                    );
                """
                + (
                    f"""
                # Wait until Pg snapshot is complete in order to avoid database-issues#5601
                > SELECT COUNT(*) > 0 FROM postgres_source_tableA{self.suffix}
                true

                # Wait until Pg snapshot is complete in order to avoid database-issues#5601
                > SELECT COUNT(*) > 0 FROM postgres_source_tableB{self.suffix}
                true
                """
                    if self.wait
                    else ""
                ),
                f"""

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table{self.suffix} SELECT 'E', i, REPEAT('E', {self.repeats} - i), NULL FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table{self.suffix} SET f2 = f2 + 100;

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table{self.suffix} SELECT 'F', i, REPEAT('F', {self.repeats} - i), NULL FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table{self.suffix} SET f2 = f2 + 100;

                > CREATE SECRET pgpass3{self.suffix} AS 'postgres';

                > CREATE CONNECTION pg3{self.suffix} FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER postgres1{self.suffix},
                  PASSWORD SECRET pgpass3{self.suffix}

                > CREATE SOURCE postgres_source3{self.suffix}
                  FROM POSTGRES CONNECTION pg3{self.suffix}
                  (PUBLICATION 'postgres_source{self.suffix}');
                > CREATE TABLE postgres_source_tableC{self.suffix} FROM SOURCE postgres_source3{self.suffix} (REFERENCE postgres_source_table{self.suffix});

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table{self.suffix} SELECT 'G', i, REPEAT('G', {self.repeats} - i), NULL FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table{self.suffix} SET f2 = f2 + 100;


                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_source_table{self.suffix} SELECT 'H', i, REPEAT('X', {self.repeats} - i), NULL FROM generate_series(1,100) AS i;
                UPDATE postgres_source_table{self.suffix} SET f2 = f2 + 100;
                """
                + (
                    f"""
                # Wait until Pg snapshot is complete in order to avoid database-issues#5601
                > SELECT COUNT(*) > 0 FROM postgres_source_tableB{self.suffix}
                true
                > SELECT COUNT(*) > 0 FROM postgres_source_tableC{self.suffix}
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
            $ postgres-execute connection=postgres://mz_system@${{testdrive.materialize-internal-sql-addr}}
            GRANT SELECT ON postgres_source_tableA{self.suffix} TO materialize
            GRANT SELECT ON postgres_source_tableB{self.suffix} TO materialize
            GRANT SELECT ON postgres_source_tableC{self.suffix} TO materialize

            # Can take longer after a restart
            $ set-sql-timeout duration=600s

            # Trying to narrow down whether
            # https://github.com/MaterializeInc/database-issues/issues/8102
            # is a problem with the source or elsewhere.
            > WITH t AS (SELECT * FROM postgres_source_tableA{self.suffix})
              SELECT COUNT(*)
              FROM t
              GROUP BY row(t.*)
              HAVING COUNT(*) < 0;

            > WITH t AS (SELECT * FROM postgres_source_tableB{self.suffix})
              SELECT COUNT(*)
              FROM t
              GROUP BY row(t.*)
              HAVING COUNT(*) < 0;

            > WITH t AS (SELECT * FROM postgres_source_tableC{self.suffix})
              SELECT COUNT(*)
              FROM t
              GROUP BY row(t.*)
              HAVING COUNT(*) < 0;

            > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM postgres_source_tableA{self.suffix} GROUP BY f1;
            A 800 {self.expects}
            B 800 {self.expects}
            C 700 {self.expects}
            D 600 {self.expects}
            E 500 {self.expects}
            F 400 {self.expects}
            G 300 {self.expects}
            H 200 {self.expects}

            > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM postgres_source_tableB{self.suffix} GROUP BY f1;
            A 800 {self.expects}
            B 800 {self.expects}
            C 700 {self.expects}
            D 600 {self.expects}
            E 500 {self.expects}
            F 400 {self.expects}
            G 300 {self.expects}
            H 200 {self.expects}

            > SELECT f1, max(f2), SUM(LENGTH(f3)) FROM postgres_source_tableC{self.suffix} GROUP BY f1;
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
            # > SELECT regexp_match(create_sql, 'TEXT COLUMNS = \\((.*?)\\)')[1] FROM (SHOW CREATE SOURCE postgres_source_tableA{self.suffix});
            # "\"f4\""

            # Confirm that the primary key information has been propagated from Pg
            > SELECT key FROM (SHOW INDEXES ON postgres_source_tableA{self.suffix});
            {{f1,f2}}

            ?[version>=13500] EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM postgres_source_tableA{self.suffix};
            Explained Query (fast path):
              Project (#0, #1)
                ReadIndex on=materialize.public.postgres_source_tablea{self.suffix} postgres_source_tablea{self.suffix}_primary_idx=[*** full scan ***]

            Used Indexes:
              - materialize.public.postgres_source_tablea{self.suffix}_primary_idx (*** full scan ***)

            Target cluster: quickstart

            ?[version<13500] EXPLAIN OPTIMIZED PLAN FOR SELECT DISTINCT f1, f2 FROM postgres_source_tableA{self.suffix};
            Explained Query (fast path):
              Project (#0, #1)
                ReadIndex on=materialize.public.postgres_source_tablea{self.suffix} postgres_source_tablea{self.suffix}_primary_idx=[*** full scan ***]

            Used Indexes:
              - materialize.public.postgres_source_tablea{self.suffix}_primary_idx (*** full scan ***)

            Target cluster: quickstart
            """
        )

        return Testdrive(sql)


@externally_idempotent(False)
class PgCdc(PgCdcBase, Check):
    def __init__(self, base_version: MzVersion, rng: Random | None) -> None:
        super().__init__(wait=True, base_version=base_version, rng=rng)


@externally_idempotent(False)
class PgCdcNoWait(PgCdcBase, Check):
    def __init__(self, base_version: MzVersion, rng: Random | None) -> None:
        super().__init__(wait=False, base_version=base_version, rng=rng)


@externally_idempotent(False)
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
                  (PUBLICATION 'postgres_mz_now_publication');
                > CREATE TABLE postgres_mz_now_table FROM SOURCE postgres_mz_now_source (REFERENCE postgres_mz_now_table);

                # Return all rows fresher than 60 seconds
                > CREATE MATERIALIZED VIEW postgres_mz_now_view AS
                  SELECT * FROM postgres_mz_now_table
                  WHERE mz_now() <= ROUND(EXTRACT(epoch FROM f1 + INTERVAL '60' SECOND) * 1000)
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
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

                # Expect some rows newer than 180 seconds in view
                > SELECT COUNT(*) >= 6 FROM postgres_mz_now_view
                  WHERE f1 > NOW() - INTERVAL '180' SECOND;
                true

                # Expect no rows older than 180 seconds in view
                > SELECT COUNT(*) FROM postgres_mz_now_view
                  WHERE f1 < NOW() - INTERVAL '180' SECOND;
                0

                # Rollback the last INSERTs so that validate() can be called multiple times
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO postgres_mz_now_table VALUES (NOW(), 'B3');
                DELETE FROM postgres_mz_now_table WHERE f2 LIKE '%4%';
                """
            )
        )


def remove_target_cluster_from_explain(sql: str) -> str:
    return re.sub(r"\n\s*Target cluster: \w+\n", "", sql)
