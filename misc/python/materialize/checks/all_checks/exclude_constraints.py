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


@externally_idempotent(False)
class ExcludeConstraints(Check):
    """EXCLUDE CONSTRAINTS / EXCLUDE ALL CONSTRAINTS on CREATE TABLE .. FROM
    SOURCE: the option must survive restarts and upgrades (it is part of the
    persisted create_sql and is re-planned at boot), and dropping the excluded
    upstream constraints must remain a non-event across them."""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.39.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_exclude_constraints_option = true

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            CREATE USER postgres_exc_con WITH SUPERUSER PASSWORD 'postgres';
            ALTER USER postgres_exc_con WITH replication;
            DROP PUBLICATION IF EXISTS exc_con_pub;
            DROP TABLE IF EXISTS exc_con_t1;
            DROP TABLE IF EXISTS exc_con_t2;

            CREATE TABLE exc_con_t1 (id int PRIMARY KEY, wallet text NOT NULL, CONSTRAINT exc_con_uq UNIQUE (wallet));
            ALTER TABLE exc_con_t1 REPLICA IDENTITY FULL;
            INSERT INTO exc_con_t1 SELECT i, 'w' || i FROM generate_series(1, 10) AS i;

            CREATE TABLE exc_con_t2 (id int PRIMARY KEY, val int NOT NULL);
            ALTER TABLE exc_con_t2 REPLICA IDENTITY FULL;
            INSERT INTO exc_con_t2 SELECT i, i FROM generate_series(1, 10) AS i;

            CREATE PUBLICATION exc_con_pub FOR TABLE exc_con_t1, exc_con_t2;

            > CREATE SECRET exc_con_pgpass AS 'postgres';

            > CREATE CONNECTION exc_con_pg FOR POSTGRES
              HOST 'postgres',
              DATABASE postgres,
              USER postgres_exc_con,
              PASSWORD SECRET exc_con_pgpass

            > CREATE SOURCE exc_con_source
              FROM POSTGRES CONNECTION exc_con_pg
              (PUBLICATION 'exc_con_pub');

            > CREATE TABLE exclude_constraints_t1
              FROM SOURCE exc_con_source (REFERENCE exc_con_t1)
              WITH (EXCLUDE CONSTRAINTS ('exc_con_uq'));

            > CREATE TABLE exclude_constraints_t2
              FROM SOURCE exc_con_source (REFERENCE exc_con_t2)
              WITH (EXCLUDE ALL CONSTRAINTS);
        """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                ALTER TABLE exc_con_t1 DROP CONSTRAINT exc_con_uq;
                ALTER TABLE exc_con_t2 DROP CONSTRAINT exc_con_t2_pkey;
                ALTER TABLE exc_con_t2 ALTER COLUMN val DROP NOT NULL;
                INSERT INTO exc_con_t1 SELECT i, 'w' || i FROM generate_series(11, 20) AS i;
                INSERT INTO exc_con_t2 SELECT i, NULL FROM generate_series(11, 20) AS i;

                > CREATE DEFAULT INDEX ON exclude_constraints_t1;
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO exc_con_t1 SELECT i, 'w' || i FROM generate_series(21, 30) AS i;
                INSERT INTO exc_con_t2 SELECT i, NULL FROM generate_series(21, 30) AS i;

                > CREATE MATERIALIZED VIEW exclude_constraints_mv AS
                  SELECT count(*) AS c1, (SELECT count(val) FROM exclude_constraints_t2) AS c2
                  FROM exclude_constraints_t1;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT count(*) FROM exclude_constraints_t1;
            30

            > SELECT count(*), count(val) FROM exclude_constraints_t2;
            30 10

            > SELECT * FROM exclude_constraints_mv;
            30 10

            # The excluded UNIQUE constraint was not recorded as a key, so only
            # the PRIMARY KEY remains on t1; t2 recorded no constraints at all,
            # so every column is nullable.
            > SELECT nullable FROM mz_columns WHERE id = (SELECT id FROM mz_tables WHERE name = 'exclude_constraints_t2');
            true
            true

            > SELECT create_sql LIKE '%EXCLUDE CONSTRAINTS%exc_con_uq%' FROM (SHOW CREATE TABLE exclude_constraints_t1);
            true

            > SELECT create_sql LIKE '%EXCLUDE ALL CONSTRAINTS%' FROM (SHOW CREATE TABLE exclude_constraints_t2);
            true
        """))
