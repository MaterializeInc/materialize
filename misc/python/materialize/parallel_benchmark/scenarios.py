# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import queue
import time
from copy import deepcopy
from dataclasses import replace

import psycopg

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.mysql import MySql
from materialize.parallel_benchmark.framework import (
    Action,
    ClosedLoop,
    LoadPhase,
    Measurement,
    OpenLoop,
    Periodic,
    PooledQuery,
    ReuseConnQuery,
    Scenario,
    StandaloneQuery,
    State,
    TdAction,
    TdPhase,
    disabled,
    execute_query,
)
from materialize.util import PgConnInfo


class Kafka(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    $ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }
                    $ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

                    $ kafka-create-topic topic=kafka

                    $ kafka-ingest format=avro topic=kafka key-format=avro key-schema=${keyschema} schema=${schema} repeat=10
                    {"f1": 1} {"f2": ${kafka-ingest.iteration} }

                    > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

                    > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                      URL '${testdrive.schema-registry-url}');

                    > CREATE SOURCE kafka
                      FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-${testdrive.seed}');

                    > CREATE TABLE kafka_tbl FROM SOURCE kafka (REFERENCE "testdrive-kafka-${testdrive.seed}")
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE UPSERT;

                    > CREATE MATERIALIZED VIEW kafka_mv AS SELECT * FROM kafka_tbl;

                    > CREATE DEFAULT INDEX ON kafka_mv;
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=TdAction(
                                """
                                $ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }
                                $ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }
                                $ kafka-ingest format=avro topic=kafka key-format=avro key-schema=${keyschema} schema=${schema} repeat=10
                                {"f1": 1} {"f2": ${kafka-ingest.iteration} }
                                """,
                                c,
                            ),
                            dist=Periodic(per_second=1),
                        )
                    ]
                    + [
                        ClosedLoop(
                            action=StandaloneQuery(
                                "SELECT * FROM kafka_mv",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                # PR#35328 (compute: move MV sink persist I/O off Timely thread) reduced qps / increased p99
                "SELECT * FROM kafka_mv (standalone)": {"qps": 3, "p99": 2000},
            },
        )


class CreateKafkaSink(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    ALTER SYSTEM SET max_objects_per_schema = 1000000;
                    ALTER SYSTEM SET max_sinks = 1000000;

                    > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

                    > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                      URL '${testdrive.schema-registry-url}');

                    > CREATE TABLE IF NOT EXISTS t (c INT)

                    > INSERT INTO t VALUES (1)
                    """),
                LoadPhase(
                    duration=300,
                    actions=[
                        ClosedLoop(
                            action=TdAction(
                                """
                                $ set-from-sql var=id
                                SELECT mz_now()::text

                                > CREATE SINK sink${id} FROM t INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-${id}') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE DEBEZIUM
                                """,
                                c,
                            ),
                            report_regressions=False,  # TODO: Currently not stable enough, add guarantees when we improve
                        )
                    ],
                ),
            ],
        )


class PgReadReplica(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    > DROP SECRET IF EXISTS pgpass CASCADE
                    > CREATE SECRET pgpass AS 'postgres'
                    > CREATE CONNECTION pg TO POSTGRES (
                        HOST postgres,
                        DATABASE postgres,
                        USER postgres,
                        PASSWORD SECRET pgpass
                      )

                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    DROP PUBLICATION IF EXISTS mz_source;
                    DROP TABLE IF EXISTS t1 CASCADE;
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t1 (f1 INTEGER);
                    ALTER TABLE t1 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source FOR ALL TABLES;

                    > CREATE SOURCE mz_source
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

                    > CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);

                    > CREATE MATERIALIZED VIEW mv_sum AS
                      SELECT COUNT(*) FROM t1;

                    > CREATE DEFAULT INDEX ON mv_sum;
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "INSERT INTO t1 VALUES (1)",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=100),
                        )
                    ]
                    + [
                        ClosedLoop(
                            action=StandaloneQuery(
                                "SELECT * FROM mv_sum",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                "SELECT * FROM mv_sum (standalone)": {"qps": 15, "p99": 400},
            },
        )


class PgReadReplicaRTR(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    > DROP SECRET IF EXISTS pgpass CASCADE
                    > CREATE SECRET pgpass AS 'postgres'
                    > CREATE CONNECTION pg TO POSTGRES (
                        HOST postgres,
                        DATABASE postgres,
                        USER postgres,
                        PASSWORD SECRET pgpass
                      )

                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    DROP PUBLICATION IF EXISTS mz_source2;
                    DROP TABLE IF EXISTS t2 CASCADE;
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t2 (f1 INTEGER);
                    ALTER TABLE t2 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source2 FOR ALL TABLES;

                    > CREATE SOURCE mz_source2
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source2');

                    > CREATE TABLE t2 FROM SOURCE mz_source2 (REFERENCE t2);

                    > CREATE MATERIALIZED VIEW mv_sum AS
                      SELECT COUNT(*) FROM t2;

                    > CREATE DEFAULT INDEX ON mv_sum;
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "INSERT INTO t2 VALUES (1)",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=100),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "SET REAL_TIME_RECENCY TO TRUE; SELECT * FROM mv_sum",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=125),
                            report_regressions=False,  # TODO: Currently not stable enough, reenable when RTR becomes more consistent
                        ),
                    ],
                ),
            ],
            guarantees={
                # TODO(def-): Lower max when RTR becomes more performant
                "SET REAL_TIME_RECENCY TO TRUE; SELECT * FROM mv_sum (standalone)": {
                    "qps": 50,
                    "p99": 5000,
                },
            },
        )


class MySQLReadReplica(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(f"""
                    > DROP SECRET IF EXISTS mysqlpass CASCADE
                    > CREATE SECRET mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'
                    > CREATE CONNECTION IF NOT EXISTS mysql_conn TO MYSQL (HOST mysql, USER root, PASSWORD SECRET mysqlpass)

                    $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                    $ mysql-execute name=mysql
                    DROP DATABASE IF EXISTS public;
                    CREATE DATABASE public;
                    USE public;
                    CREATE TABLE t3 (f1 INTEGER);

                    > CREATE SOURCE mysql_source
                      FROM MYSQL CONNECTION mysql_conn
                      FOR TABLES (public.t3);

                    > CREATE MATERIALIZED VIEW mv_sum_mysql AS
                      SELECT COUNT(*) FROM t3;

                    > CREATE DEFAULT INDEX ON mv_sum_mysql;
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=TdAction(
                                f"""
                                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                                $ mysql-execute name=mysql
                                USE public;
                                {"INSERT INTO t3 VALUES (1); " * 100}
                                """,
                                c,
                            ),
                            dist=Periodic(per_second=1),
                        )
                    ]
                    + [
                        ClosedLoop(
                            action=StandaloneQuery(
                                "SELECT * FROM mv_sum_mysql",
                                conn_info=conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                "SELECT * FROM mv_sum_mysql (standalone)": {"qps": 15, "p99": 400},
            },
        )


class OpenIndexedSelects(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    > CREATE TABLE t4 (f1 TEXT, f2 INTEGER);
                    > CREATE DEFAULT INDEX ON t4;
                    > INSERT INTO t4 VALUES ('A', 1);
                    > INSERT INTO t4 VALUES ('B', 2);
                    > INSERT INTO t4 VALUES ('C', 3);
                    > INSERT INTO t4 VALUES ('D', 4);
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=PooledQuery("SELECT * FROM t4"),
                            dist=Periodic(per_second=400),
                        ),
                    ],
                ),
            ],
            conn_pool_size=100,
            guarantees={
                "SELECT * FROM t4 (pooled)": {"qps": 390, "p99": 100},
            },
        )


class ConnectRead(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                LoadPhase(
                    duration=120,
                    actions=[
                        ClosedLoop(
                            action=StandaloneQuery(
                                "SELECT 1",
                                conn_info=conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                "SELECT 1 (standalone)": {"qps": 35, "max": 700},
            },
        )


class FlagUpdate(Scenario):
    """Reproduces database-issues#8480"""

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=ReuseConnQuery(
                                # The particular flag and value used here
                                # doesn't matter. It just needs to be a flag
                                # that exists in both versions to be
                                # benchmarked.
                                "ALTER SYSTEM SET compute_hydration_concurrency = 1",
                                conn_info=conn_infos["mz_system"],
                            ),
                            dist=Periodic(per_second=1),
                            report_regressions=False,  # We don't care about this query getting slower
                        ),
                    ]
                    + [
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT 1",
                                conn_info=conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                # TODO(def-): Lower when database-issues#8480 is fixed to prevent regressions
                "SELECT 1 (reuse connection)": {"avg": 8, "max": 500, "slope": 0.1},
            },
        )


class Read(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                LoadPhase(
                    duration=120,
                    actions=[
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT 1",
                                conn_info=conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                "SELECT 1 (reuse connection)": {"qps": 1400, "max": 100, "slope": 0.1},
            },
        )


class PoolRead(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=PooledQuery("SELECT 1"),
                            dist=Periodic(per_second=100),
                            # dist=Gaussian(mean=0.01, stddev=0.05),
                        ),
                    ],
                ),
            ],
            conn_pool_size=100,
            guarantees={
                "SELECT 1 (pooled)": {"avg": 5, "max": 200, "slope": 0.1},
            },
        )


class GrantRevokeAllTables(Action):
    """Grants then revokes `SELECT` on every table in a schema to many roles, over a
    reused connection. Each invocation drives one bulk privilege transaction in each
    direction, so the action keeps doing real work when looped (a repeated grant alone
    would be a no-op after the first)."""

    def __init__(self, schema: str, roles: list[str], conn_info: PgConnInfo):
        grantees = ", ".join(roles)
        self.grant = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {grantees}"
        self.revoke = f"REVOKE SELECT ON ALL TABLES IN SCHEMA {schema} FROM {grantees}"
        self.conn_info = conn_info
        self.conn = conn_info.connect()
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def _run(self, conns: queue.Queue):
        execute_query(self.cur, self.grant)
        execute_query(self.cur, self.revoke)

    def __str__(self) -> str:
        return "GRANT/REVOKE SELECT ON ALL TABLES (reuse connection)"


class BulkPrivilegeGrant(Scenario):
    """Guards coordinator responsiveness during a bulk
    `GRANT ... ON ALL TABLES IN SCHEMA ... TO <many roles>`.

    The statement fans out to one catalog change per (table, grantee). Before the
    durable-layer and op-batching fixes, every change ran an O(n) uniqueness scan over
    the whole catalog, so the single big transaction wedged the single-threaded
    coordinator for minutes and stalled unrelated queries. The operation is still not
    cheap, but it no longer halts the coordinator, so we assert that a concurrent
    `SELECT 1` stays responsive while grants and revokes churn in the background."""

    SCALE = 150

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        schema = "bulk_privilege_grant"
        tables = [f"t{i}" for i in range(self.SCALE)]
        roles = [f"bulk_privilege_grant_r{i}" for i in range(self.SCALE)]
        # Create the schema, tables, and roles as mz_system so it owns them and can grant on them,
        # and raise the object/role limits the bulk fan-out needs.
        setup = "\n".join(
            [
                "$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}",
                "ALTER SYSTEM SET max_objects_per_schema = 1000000;",
                "ALTER SYSTEM SET max_roles = 1000000;",
                f"DROP SCHEMA IF EXISTS {schema} CASCADE;",
                f"CREATE SCHEMA {schema};",
                *[f"CREATE TABLE {schema}.{table} (a int);" for table in tables],
                *[f"CREATE ROLE {role};" for role in roles],
            ]
        )
        self.init(
            [
                TdPhase(setup),
                LoadPhase(
                    duration=60,
                    actions=[
                        # A single background thread issues the bulk grant/revoke back to back, so
                        # at most one large catalog transaction runs at a time.
                        ClosedLoop(
                            action=GrantRevokeAllTables(
                                schema=schema,
                                roles=roles,
                                conn_info=conn_infos["mz_system"],
                            ),
                            # We don't care whether the grant/revoke itself gets slower.
                            report_regressions=False,
                        ),
                    ]
                    + [
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT 1",
                                conn_info=conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for _ in range(10)
                    ],
                ),
            ],
            guarantees={
                # Before the fix a single bulk grant blocked the coordinator for
                # minutes. This bound catches that regression while leaving headroom
                # for the background churn, which can briefly delay a `SELECT 1`.
                "SELECT 1 (reuse connection)": {"max": 30000},
            },
        )


class StatementLogging(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    ALTER SYSTEM SET statement_logging_max_sample_rate = 1.0;
                    ALTER SYSTEM SET statement_logging_default_sample_rate = 1.0;
                    ALTER SYSTEM SET enable_statement_lifecycle_logging = true;
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=PooledQuery("SELECT 1"),
                            dist=Periodic(per_second=100),
                            # dist=Gaussian(mean=0.01, stddev=0.05),
                        ),
                    ],
                ),
                TdPhase("""
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    ALTER SYSTEM SET statement_logging_default_sample_rate = 0;
                    ALTER SYSTEM SET statement_logging_max_sample_rate = 0;
                    ALTER SYSTEM SET enable_statement_lifecycle_logging = false;
                    """),
            ],
            conn_pool_size=100,
            guarantees={
                "SELECT 1 (pooled)": {"avg": 5, "max": 200, "slope": 0.1},
            },
        )


class InsertWhereNotExists(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    > CREATE TABLE insert_table (a int, b text);
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=ReuseConnQuery(
                                "INSERT INTO insert_table SELECT 1, '1' WHERE NOT EXISTS (SELECT 1 FROM insert_table WHERE a = 100)",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=5),
                        )
                    ],
                ),
            ],
            conn_pool_size=100,
            # TODO(def-): Bump per_second and add guarantees when https://linear.app/materializeinc/issue/DB-135 is fixed
        )


class InsertsSelects(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    > CREATE TABLE insert_select_table (a int, b text);
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=ReuseConnQuery(
                                "INSERT INTO insert_select_table VALUES (1, '1')",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=1),
                            report_regressions=False,
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT min(a) FROM insert_select_table",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        ),
                    ],
                ),
            ],
            conn_pool_size=100,
            guarantees={
                "SELECT min(a) FROM insert_select_table (reuse connection)": {
                    "qps": 10,
                    "p99": 350,
                },
            },
        )


# TODO Try these scenarios' scaling behavior against cc sizes (locally and remote)


class CommandQueryResponsibilitySegregation(Scenario):
    # TODO: Have one Postgres source with many inserts/updates/deletes and multiple complex materialized view on top of it, read from Mz
    # This should be blocked by materialized view performance
    # We probably need strict serializable to make sure results stay up to date
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    > DROP SECRET IF EXISTS pgpass CASCADE
                    > CREATE SECRET pgpass AS 'postgres'
                    > CREATE CONNECTION pg TO POSTGRES (
                        HOST postgres,
                        DATABASE postgres,
                        USER postgres,
                        PASSWORD SECRET pgpass
                      )

                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    DROP PUBLICATION IF EXISTS mz_cqrs_source;
                    DROP TABLE IF EXISTS t1 CASCADE;
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t1 (id INTEGER, name TEXT, date TIMESTAMPTZ);
                    ALTER TABLE t1 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_cqrs_source FOR ALL TABLES;

                    > CREATE SOURCE mz_cqrs_source
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_cqrs_source')

                    > CREATE TABLE t1 FROM SOURCE mz_cqrs_source (REFERENCE t1);

                    > CREATE MATERIALIZED VIEW mv_cqrs AS
                      SELECT t1.date, SUM(t1.id) FROM t1 JOIN t1 AS t2 ON true JOIN t1 AS t3 ON true JOIN t1 AS t4 ON true GROUP BY t1.date;
                    > CREATE DEFAULT INDEX ON mv_cqrs;
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "INSERT INTO t1 VALUES (1, '1', now())",
                                # "INSERT INTO t1 (id, name, date) SELECT i, i::text, now() FROM generate_series(1, 1000) AS s(i);",
                                conn_infos["postgres"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=100),
                            report_regressions=False,
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE t1 SET id = id + 1",
                                conn_infos["postgres"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=10),
                            report_regressions=False,
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "DELETE FROM t1 WHERE date < now() - INTERVAL '10 seconds'",
                                conn_infos["postgres"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=1),
                            report_regressions=False,
                        ),
                    ]
                    + [
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM mv_cqrs",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                            report_regressions=False,  # TODO: Currently not stable enough
                        )
                    ],
                ),
            ],
        )


class OperationalDataStore(Scenario):
    # TODO: Get data from multiple sources with high volume (webhook source, Kafka, Postgres, MySQL), export to Kafka Sink and Subscribes
    # This should be blocked by read/write performance
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    > DROP SECRET IF EXISTS pgpass CASCADE
                    > CREATE SECRET pgpass AS 'postgres'
                    > CREATE CONNECTION pg TO POSTGRES (
                        HOST postgres,
                        DATABASE postgres,
                        USER postgres,
                        PASSWORD SECRET pgpass
                      )

                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    DROP PUBLICATION IF EXISTS mz_source;
                    DROP TABLE IF EXISTS t1 CASCADE;
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t1 (f1 INTEGER);
                    ALTER TABLE t1 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source FOR ALL TABLES;

                    > CREATE SOURCE mz_source
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

                    > CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);

                    > CREATE MATERIALIZED VIEW mv_sum AS
                      SELECT COUNT(*) FROM t1;

                    > CREATE DEFAULT INDEX ON mv_sum;

                    # TODO: Other sources
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "INSERT INTO t1 (f1) SELECT i FROM generate_series(1, 50000) AS s(i);",
                                conn_infos["postgres"],
                                strict_serializable=False,
                            ),
                            report_regressions=False,
                            dist=Periodic(per_second=10),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SET REAL_TIME_RECENCY TO TRUE; SELECT * FROM mv_sum",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                            report_regressions=False,  # TODO: Currently not stable enough, reenable when RTR becomes more consistent
                        ),
                    ],
                ),
            ],
        )


class OperationalDataMesh(Scenario):
    # TODO: One Kafka source/sink, one data source, many materialized views, all exported to Kafka
    # This should be blocked by the number of source/sink combinations
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    $ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }
                    $ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

                    $ kafka-create-topic topic=kafka-mesh

                    $ kafka-ingest format=avro topic=kafka-mesh key-format=avro key-schema=${keyschema} schema=${schema} repeat=10
                    {"f1": 1} {"f2": ${kafka-ingest.iteration} }

                    > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

                    > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                      URL '${testdrive.schema-registry-url}');

                    > CREATE SOURCE kafka_mesh
                      FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-mesh-${testdrive.seed}');

                    > CREATE TABLE kafka_mesh_tbl FROM SOURCE kafka_mesh (REFERENCE "testdrive-kafka-mesh-${testdrive.seed}")
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE UPSERT;

                    > CREATE MATERIALIZED VIEW kafka_mesh_mv AS SELECT * FROM kafka_mesh_tbl;

                    > CREATE DEFAULT INDEX ON kafka_mesh_mv;

                    > CREATE SINK sink FROM kafka_mesh_mv
                      INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink')
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE DEBEZIUM;

                    $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="10s"

                    #$ kafka-verify-topic sink=sink

                    > CREATE SOURCE sink_source
                      FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink');

                    > CREATE TABLE sink_source_tbl FROM SOURCE sink_source (REFERENCE "sink")
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE NONE;
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=TdAction(
                                """
                                $ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }
                                $ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }
                                $ kafka-ingest format=avro topic=kafka-mesh key-format=avro key-schema=${keyschema} schema=${schema} repeat=100000
                                {"f1": 1} {"f2": ${kafka-ingest.iteration} }
                                """,
                                c,
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        ClosedLoop(
                            action=StandaloneQuery(
                                # TODO: This doesn't actually measure rtr all the way
                                "SET REAL_TIME_RECENCY TO TRUE; SELECT * FROM sink_source",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                            report_regressions=False,  # TODO: Currently not stable enough, reenable when RTR becomes more consistent
                        ),
                    ],
                ),
            ],
        )


@disabled(
    "Not well suited to measure regressions since too many queries are running at once"
)
class ReadReplicaBenchmark(Scenario):
    # We might want to run a full version of rr-bench instead, this is not a
    # very realistic representation of it but might already help us catch some
    # regressions: https://github.com/MaterializeIncLabs/rr-bench
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    DROP TABLE IF EXISTS customers CASCADE;
                    DROP TABLE IF EXISTS accounts CASCADE;
                    DROP TABLE IF EXISTS securities CASCADE;
                    DROP TABLE IF EXISTS trades CASCADE;
                    DROP TABLE IF EXISTS orders CASCADE;
                    DROP TABLE IF EXISTS market_data CASCADE;
                    CREATE TABLE customers (customer_id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, address VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE accounts (account_id SERIAL PRIMARY KEY, customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE, account_type VARCHAR(50) NOT NULL, balance DECIMAL(18, 2) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE securities (security_id SERIAL PRIMARY KEY, ticker VARCHAR(10) NOT NULL, name VARCHAR(255), sector VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE trades (trade_id SERIAL PRIMARY KEY, account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE, security_id INT REFERENCES securities(security_id) ON DELETE CASCADE, trade_type VARCHAR(10) NOT NULL CHECK (trade_type IN ('buy', 'sell')), quantity INT NOT NULL, price DECIMAL(18, 4) NOT NULL, trade_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE orders (order_id SERIAL PRIMARY KEY, account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE, security_id INT REFERENCES securities(security_id) ON DELETE CASCADE, order_type VARCHAR(10) NOT NULL CHECK (order_type IN ('buy', 'sell')), quantity INT NOT NULL, limit_price DECIMAL(18, 4), status VARCHAR(10) NOT NULL CHECK (status IN ('pending', 'completed', 'canceled')), order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE market_data (market_data_id SERIAL PRIMARY KEY, security_id INT REFERENCES securities(security_id) ON DELETE CASCADE, price DECIMAL(18, 4) NOT NULL, volume INT NOT NULL, market_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    DROP PUBLICATION IF EXISTS mz_source3;
                    ALTER USER postgres WITH replication;
                    ALTER TABLE customers REPLICA IDENTITY FULL;
                    ALTER TABLE accounts REPLICA IDENTITY FULL;
                    ALTER TABLE securities REPLICA IDENTITY FULL;
                    ALTER TABLE trades REPLICA IDENTITY FULL;
                    ALTER TABLE orders REPLICA IDENTITY FULL;
                    ALTER TABLE market_data REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source3 FOR ALL TABLES;
                    INSERT INTO customers (customer_id, name, address, created_at) VALUES (1, 'Elizabeth Ebert', 'Raleigh Motorway', '2024-09-11 15:27:44'), (2, 'Kelley Kuhlman', 'Marvin Circle', '2024-09-11 15:27:44'), (3, 'Frieda Waters', 'Jessy Roads', '2024-09-11 15:27:44'), (4, 'Ian Thiel', 'Rodriguez Squares', '2024-09-11 15:27:44'), (5, 'Clementine Hauck', 'Allen Junction', '2024-09-11 15:27:44'), (6, 'Caesar White', 'Cheyenne Green', '2024-09-11 15:27:44'), (7, 'Hudson Wintheiser', 'Wiza Plain', '2024-09-11 15:27:44'), (8, 'Kendall Marks', 'Kuhn Ports', '2024-09-11 15:27:44'), (9, 'Haley Schneider', 'Erwin Cliffs', '2024-09-11 15:27:44');
                    INSERT INTO accounts (account_id, customer_id, account_type, balance, created_at) VALUES (1, 1, 'Brokerage', 796.9554824679382, '2024-09-11 15:27:44'), (2, 2, 'Checking', 7808.991622105239, '2024-09-11 15:27:44'), (3, 3, 'Checking', 4540.988288421537, '2024-09-11 15:27:44'), (4, 4, 'Brokerage', 4607.257663873947, '2024-09-11 15:27:44'), (5, 5, 'Savings', 9105.123905180497, '2024-09-11 15:27:44'), (6, 6, 'Brokerage', 6072.871742690154, '2024-09-11 15:27:44'), (7, 7, 'Savings', 7374.831288928072, '2024-09-11 15:27:44'), (8, 8, 'Brokerage', 6554.8717824477, '2024-09-11 15:27:44'), (9, 9, 'Checking', 2629.393130856843, '2024-09-11 15:27:44');
                    INSERT INTO securities (security_id, ticker, name, sector, created_at) VALUES (1, 'Y1Fu', 'Goldner and Bechtelar LLC', 'Printing', '2024-09-11 15:27:44'), (2, 'MOF5', 'Adams and Homenick Inc', 'Market Research', '2024-09-11 15:27:44'), (3, 'Oo09', 'Tillman and Wilkinson Inc', 'Apparel & Fashion', '2024-09-11 15:27:44'), (4, 'zmAy', 'Toy and Williamson LLC', 'International Affairs', '2024-09-11 15:27:44'), (5, 'ORyo', 'Olson and Prohaska and Sons', 'Textiles', '2024-09-11 15:27:44'), (6, 'Fpn2', 'Gusikowski and Schinner Inc', 'Think Tanks', '2024-09-11 15:27:44'), (7, 'gTv2', 'Davis and Sons', 'Package / Freight Delivery', '2024-09-11 15:27:44'), (8, '38RH', 'Johns and Braun Group', 'Public Safety', '2024-09-11 15:27:44'), (9, 'Ym5u', 'Goyette Group', 'Cosmetics', '2024-09-11 15:27:44');
                    INSERT INTO trades (trade_id, account_id, security_id, trade_type, quantity, price, trade_date) VALUES (1, 1, 1, 'buy', 337, 464.45448203724607, '2024-09-11 15:27:44'), (2, 2, 2, 'buy', 312, 299.91031464748926, '2024-09-11 15:27:44'), (3, 3, 3, 'buy', 874, 338.5711431239059, '2024-09-11 15:27:44'), (4, 4, 4, 'buy', 523, 356.4236193709552, '2024-09-11 15:27:44'), (5, 5, 5, 'sell', 251, 354.6345239481285, '2024-09-11 15:27:44'), (6, 6, 6, 'buy', 810, 437.6742610108604, '2024-09-11 15:27:44'), (7, 7, 7, 'sell', 271, 116.70199857394587, '2024-09-11 15:27:44'), (8, 8, 8, 'buy', 84, 415.0658279744514, '2024-09-11 15:27:44'), (9, 9, 9, 'sell', 763, 312.3375311232852, '2024-09-11 15:27:44');
                    INSERT INTO orders (order_id, account_id, security_id, order_type, quantity, limit_price, status, order_date) VALUES (1, 1, 1, 'buy', 207, 456.0, 'completed', '2024-09-11 15:27:44'), (2, 2, 2, 'buy', 697, 515.0, 'canceled', '2024-09-11 15:27:44'), (3, 3, 3, 'buy', 789, 198.0, 'completed', '2024-09-11 15:27:44'), (4, 4, 4, 'sell', 280, 505.0, 'completed', '2024-09-11 15:27:44'), (5, 5, 5, 'buy', 368, 966.0, 'pending', '2024-09-11 15:27:44'), (6, 6, 6, 'buy', 439, 7.0, 'completed', '2024-09-11 15:27:44'), (7, 7, 7, 'sell', 345, 972.0, 'completed', '2024-09-11 15:27:44'), (8, 8, 8, 'sell', 867, 968.0, 'completed', '2024-09-11 15:27:44'), (9, 9, 9, 'sell', 472, 534.0, 'completed', '2024-09-11 15:27:44');
                    INSERT INTO market_data (market_data_id, security_id, price, volume, market_date) VALUES (1, 1, 134.07573356469547, 17326, '2024-09-11 15:27:44'), (2, 2, 107.2440801092168, 63229, '2024-09-11 15:27:44'), (3, 3, 498.13544872323644, 69305, '2024-09-11 15:27:44'), (4, 4, 194.24235075387645, 45224, '2024-09-11 15:27:44'), (5, 5, 352.2334739296001, 79796, '2024-09-11 15:27:44'), (6, 6, 241.83322476711587, 44295, '2024-09-11 15:27:44'), (7, 7, 226.93537920792713, 23212, '2024-09-11 15:27:44'), (8, 8, 169.2983285300141, 96883, '2024-09-11 15:27:44'), (9, 9, 331.36982054471935, 5651, '2024-09-11 15:27:44');

                    > DROP SECRET IF EXISTS pgpass CASCADE
                    > CREATE SECRET pgpass AS 'postgres'
                    > CREATE CONNECTION pg TO POSTGRES (
                        HOST postgres,
                        DATABASE postgres,
                        USER postgres,
                        PASSWORD SECRET pgpass
                      )
                    > CREATE SOURCE mz_source3
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source3');

                    > CREATE TABLE customers FROM SOURCE mz_source3 (REFERENCE customers);
                    > CREATE TABLE accounts FROM SOURCE mz_source3 (REFERENCE accounts);
                    > CREATE TABLE securities FROM SOURCE mz_source3 (REFERENCE securities);
                    > CREATE TABLE trades FROM SOURCE mz_source3 (REFERENCE trades);
                    > CREATE TABLE orders FROM SOURCE mz_source3 (REFERENCE orders);
                    > CREATE TABLE market_data FROM SOURCE mz_source3 (REFERENCE market_data);

                    > CREATE VIEW customer_portfolio AS
                      SELECT c.customer_id, c.name, a.account_id, s.ticker, s.name AS security_name,
                           SUM(t.quantity * t.price) AS total_value
                      FROM customers c
                      JOIN accounts a ON c.customer_id = a.customer_id
                      JOIN trades t ON a.account_id = t.account_id
                      JOIN securities s ON t.security_id = s.security_id
                      GROUP BY c.customer_id, c.name, a.account_id, s.ticker, s.name;

                    > CREATE VIEW top_performers AS
                      WITH trade_volume AS (
                          SELECT security_id, SUM(quantity) AS total_traded_volume
                          FROM trades
                          GROUP BY security_id
                          ORDER BY SUM(quantity) DESC
                          LIMIT 10
                      )
                      SELECT s.ticker, s.name, t.total_traded_volume
                      FROM trade_volume t
                      JOIN securities s USING (security_id);

                    > CREATE VIEW market_overview AS
                      SELECT s.sector, AVG(md.price) AS avg_price, SUM(md.volume) AS total_volume,
                             MAX(md.market_date) AS last_update
                      FROM securities s
                      LEFT JOIN market_data md ON s.security_id = md.security_id
                      GROUP BY s.sector
                      HAVING MAX(md.market_date) + INTERVAL '5 minutes' > mz_now() ;

                    > CREATE VIEW recent_large_trades AS
                      SELECT t.trade_id, a.account_id, s.ticker, t.quantity, t.price, t.trade_date
                      FROM trades t
                      JOIN accounts a ON t.account_id = a.account_id
                      JOIN securities s ON t.security_id = s.security_id
                      WHERE t.quantity > (SELECT AVG(quantity) FROM trades) * 5
                        AND t.trade_date + INTERVAL '1 hour' > mz_now();


                    > CREATE VIEW customer_order_book AS
                      SELECT c.customer_id, c.name, COUNT(o.order_id) AS open_orders,
                             SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS completed_orders
                      FROM customers c
                      JOIN accounts a ON c.customer_id = a.customer_id
                      JOIN orders o ON a.account_id = o.account_id
                      GROUP BY c.customer_id, c.name;

                    > CREATE VIEW sector_performance AS
                      SELECT s.sector, AVG(t.price) AS avg_trade_price, COUNT(t.trade_id) AS trade_count,
                             SUM(t.quantity) AS total_volume
                      FROM trades t
                      JOIN securities s ON t.security_id = s.security_id
                      GROUP BY s.sector;

                    > CREATE VIEW account_activity_summary AS
                      SELECT a.account_id, COUNT(t.trade_id) AS trade_count,
                             SUM(t.quantity * t.price) AS total_trade_value,
                             MAX(t.trade_date) AS last_trade_date
                      FROM accounts a
                      LEFT JOIN trades t ON a.account_id = t.account_id
                      GROUP BY a.account_id;

                    > CREATE VIEW daily_market_movements AS
                      WITH last_two_days AS (
                          SELECT grp.security_id, price, market_date
                          FROM (SELECT DISTINCT security_id FROM market_data) grp,
                          LATERAL (
                              SELECT md.security_id, md.price, md.market_date
                              FROM market_data md
                              WHERE md.security_id = grp.security_id AND md.market_date + INTERVAL '1 day' > mz_now()
                              ORDER BY md.market_date DESC
                              LIMIT 2
                          )
                      ),
                      stg AS (
                          SELECT security_id, today.price AS current_price, yesterday.price AS previous_price, today.market_date
                          FROM last_two_days today
                          LEFT JOIN last_two_days yesterday USING (security_id)
                          WHERE today.market_date > yesterday.market_date
                      )
                      SELECT
                          security_id,
                          ticker,
                          name,
                          current_price,
                          previous_price,
                          current_price - previous_price AS price_change,
                          market_date
                      FROM stg
                      JOIN securities USING (security_id);

                    > CREATE VIEW high_value_customers AS
                      SELECT c.customer_id, c.name, SUM(a.balance) AS total_balance
                      FROM customers c
                      JOIN accounts a ON c.customer_id = a.customer_id
                      GROUP BY c.customer_id, c.name
                      HAVING SUM(a.balance) > 1000000;

                    > CREATE VIEW pending_orders_summary AS
                      SELECT s.ticker, s.name, COUNT(o.order_id) AS pending_order_count,
                             SUM(o.quantity) AS pending_volume,
                             AVG(o.limit_price) AS avg_limit_price
                      FROM orders o
                      JOIN securities s ON o.security_id = s.security_id
                      WHERE o.status = 'pending'
                      GROUP BY s.ticker, s.name;

                    > CREATE VIEW trade_volume_by_hour AS
                      SELECT EXTRACT(HOUR FROM t.trade_date) AS trade_hour,
                             COUNT(t.trade_id) AS trade_count,
                             SUM(t.quantity) AS total_quantity
                      FROM trades t
                      GROUP BY EXTRACT(HOUR FROM t.trade_date);

                    > CREATE VIEW top_securities_by_sector AS
                      SELECT grp.sector, ticker, name, total_volume
                      FROM (SELECT DISTINCT sector FROM securities) grp,
                      LATERAL (
                          SELECT s.sector, s.ticker, s.name, SUM(t.quantity) AS total_volume
                          FROM trades t
                          JOIN securities s ON t.security_id = s.security_id
                          WHERE s.sector = grp.sector
                          GROUP BY s.sector, s.ticker, s.name
                          ORDER BY total_volume DESC
                          LIMIT 5
                      );


                    > CREATE VIEW recent_trades_by_account AS
                      SELECT a.account_id, s.ticker, t.quantity, t.price, t.trade_date
                      FROM trades t
                      JOIN accounts a ON t.account_id = a.account_id
                      JOIN securities s ON t.security_id = s.security_id
                      WHERE t.trade_date + INTERVAL '1 day'> mz_now();

                    > CREATE VIEW order_fulfillment_rates AS
                      SELECT c.customer_id, c.name,
                             COUNT(o.order_id) AS total_orders,
                             SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS fulfilled_orders,
                             (SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id)) AS fulfillment_rate
                      FROM customers c
                      JOIN accounts a ON c.customer_id = a.customer_id
                      JOIN orders o ON a.account_id = o.account_id
                      GROUP BY c.customer_id, c.name;

                    > CREATE VIEW sector_order_activity AS
                      SELECT s.sector, COUNT(o.order_id) AS order_count,
                             SUM(o.quantity) AS total_quantity,
                             AVG(o.limit_price) AS avg_limit_price
                      FROM orders o
                      JOIN securities s ON o.security_id = s.security_id
                      GROUP BY s.sector;

                    > CREATE INDEX ON securities (security_id);
                    > CREATE INDEX ON accounts (account_id);
                    > CREATE INDEX ON customers (customer_id);
                    > CREATE INDEX ON customer_portfolio (customer_id);
                    > CREATE INDEX ON top_performers (ticker);
                    > CREATE INDEX ON market_overview (sector);
                    > CREATE INDEX ON recent_large_trades (trade_id);
                    > CREATE INDEX ON customer_order_book (customer_id);
                    > CREATE INDEX ON account_activity_summary (account_id);
                    > CREATE INDEX ON daily_market_movements (security_id);
                    > CREATE INDEX ON high_value_customers (customer_id);
                    > CREATE INDEX ON pending_orders_summary (ticker);
                    > CREATE INDEX ON trade_volume_by_hour (trade_hour);
                    > CREATE INDEX ON top_securities_by_sector (sector);
                    > CREATE INDEX ON recent_trades_by_account (account_id);
                    > CREATE INDEX ON order_fulfillment_rates (customer_id);
                    > CREATE INDEX ON sector_order_activity (sector);
                    > CREATE INDEX ON sector_performance (sector);
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE customers SET address = 'foo' WHERE customer_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE accounts SET balance = balance + 1 WHERE customer_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE trades SET price = price + 1 WHERE trade_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE orders SET status = 'pending', limit_price = limit_price + 1 WHERE order_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE market_data SET price = price + 1, volume = volume + 1, market_date = CURRENT_TIMESTAMP WHERE market_data_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        # TODO deletes
                        # DELETE FROM accounts WHERE account_id = $1
                        # DELETE FROM securities WHERE security_id = $1
                        # DELETE FROM trades WHERE trade_id = $1
                        # DELETE FROM orders WHERE order_id = $1
                        # DELETE FROM market_data WHERE market_data_id = $1
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM customer_portfolio WHERE customer_id = 1",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM top_performers",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM market_overview WHERE sector = 'Printing'",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM recent_large_trades WHERE account_id = 1",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM customer_order_book WHERE customer_id = 1",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM sector_performance WHERE sector = 'Printing'",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        # TODO: More selects
                        # SELECT * FROM account_activity_summary WHERE account_id = $1
                        # SELECT * FROM daily_market_movements WHERE security_id = $1
                        # SELECT * FROM high_value_customers
                        # SELECT * FROM pending_orders_summary WHERE ticker = $1
                        # SELECT * FROM trade_volume_by_hour
                        # SELECT * FROM top_securities_by_sector WHERE sector = $1
                        # SELECT * FROM recent_trades_by_account WHERE account_id = $1
                        # SELECT * FROM order_fulfillment_rates WHERE customer_id = $1
                        # SELECT * FROM sector_order_activity WHERE sector = $1
                        # SELECT * FROM cascading_order_cancellation_alert
                    ],
                ),
            ]
        )


@disabled("Only run separately in QA Canary pipeline")
class StagingBench(Scenario):
    # TODO: Kafka source + sink
    # TODO: Webhook source
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        conn_infos = deepcopy(conn_infos)
        conn_infos["materialized"].cluster = "quickstart"
        self.init(
            [
                LoadPhase(
                    duration=82800,
                    actions=[
                        OpenLoop(
                            action=PooledQuery("SELECT 1"),
                            dist=Periodic(per_second=500),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT COUNT(DISTINCT l_returnflag) FROM qa_canary_environment.public_tpch.tpch_q01 WHERE sum_charge > 0",
                                conn_info=conn_infos["materialized"],
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT COUNT(DISTINCT c_name) FROM qa_canary_environment.public_tpch.tpch_q18 WHERE o_orderdate <= '2023-01-01'",
                                conn_info=conn_infos["materialized"],
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_pg_cdc.pg_wmr WHERE degree > 1",
                                conn_info=conn_infos["materialized"],
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_mysql_cdc.mysql_wmr WHERE degree > 1",
                                conn_info=conn_infos["materialized"],
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT COUNT(DISTINCT count_star) FROM qa_canary_environment.public_loadgen.sales_product_product_category WHERE count_distinct_product_id > 0",
                                conn_info=conn_infos["materialized"],
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM qa_canary_environment.public_table.table_mv",
                                conn_info=conn_infos["materialized"],
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT min(c), max(c), count(*) FROM qa_canary_environment.public_table.table",
                                conn_info=conn_infos["materialized"],
                            ),
                        ),
                    ],
                ),
            ],
            conn_pool_size=100,
        )


# Regression thresholds for a query measured while something else contends for
# its replica. Contended tails vary more between runs than a quiet query's.
CONTENDED_THRESHOLDS = {
    "qps": 1.5,
    "avg": 1.5,
    "p50": 1.5,
    "p95": 1.5,
    "p99": 2.0,
}


class HydrationChurn(Action):
    """Continuously builds a heavy maintained materialized view, forces it to
    hydrate, then drops it, keeping the replica's maintenance workers busy.

    `SELECT count(*)` blocks until the view has hydrated, so each iteration does
    real hydration work before the drop. Runs in a `ClosedLoop`, which is
    single-threaded, so a fixed view name is safe.

    An iteration that fails is logged and skipped rather than allowed to
    propagate. `ClosedLoop.run` is the thread's whole body and has no handler,
    so an escaping exception would remove the contention for the rest of the
    load phase while the measured loops kept reporting against a replica with
    nothing else to do, which reads as the change under test having fixed read
    isolation.

    A closed loop calls back to back with no pause of its own, so a failure that
    repeats is backed off and the connection reopened. Without both, an unusable
    cursor raises without a round trip and the loop spins, taking the
    measurement store's process-wide lock against the threads that record the
    measured reads.

    Each iteration drops the view before it creates it, rather than trusting the
    previous one to have cleaned up. An iteration whose own drop fails would
    otherwise leave the view behind on a working session, and the next create
    would find it already there, hydrate nothing, and record the milliseconds
    that took as the fastest hydration of the run.

    A failed iteration records no measurement, so the count of this action in
    the report is the number of hydrations that actually happened. Recording one
    would report the backoff as though it were a fast hydration, and the count
    would rise as the contention fell.
    """

    # Long enough that a persistently failing loop cannot crowd out the threads
    # recording the measured reads, short enough to lose little contention.
    RETRY_BACKOFF_SECONDS = 1.0

    def __init__(self, conn_info: PgConnInfo, name: str, view_sql: str):
        self.conn_info = conn_info
        self.name = name
        self.view_sql = view_sql
        self.conn: psycopg.Connection | None = None
        self._connect()

    def _connect(self) -> None:
        conn = self.conn_info.connect()
        conn.autocommit = True
        cur = conn.cursor()
        old = self.conn
        self.conn = conn
        self.cur = cur
        if old is not None:
            try:
                old.close()
            except Exception as e:
                print(
                    f"Hydration churn {self.name} could not close the old connection: {e}"
                )

    def run(self, start_time: float, conns: queue.Queue, state: State):
        # `Action.run` records a measurement whenever `_run` returns. A failed
        # iteration did no hydration and spent its time in the backoff, so it
        # records nothing rather than entering the series as a fast one.
        if not self._churn():
            return
        duration = time.time() - start_time
        state.measurements.add(str(self), Measurement(duration, start_time))

    def _run(self, conns: queue.Queue):
        raise NotImplementedError("run() drives the churn directly")

    def _churn(self) -> bool:
        """Builds, hydrates and drops the view once. False if the iteration failed."""
        try:
            # Leading, so an iteration cleans up after whatever the last one
            # left rather than depending on it. The create is deliberately not
            # `IF NOT EXISTS`: a view that survived both this drop and the last
            # one has to fail the iteration, not turn it into a no-op that
            # records a hydration it never performed.
            execute_query(self.cur, f"DROP MATERIALIZED VIEW IF EXISTS {self.name}")
            execute_query(
                self.cur,
                f"CREATE MATERIALIZED VIEW {self.name} AS {self.view_sql}",
            )
            # Force hydration to complete before we drop, so the replica actually
            # does the build work rather than cancelling it immediately.
            execute_query(self.cur, f"SELECT count(*) FROM {self.name}")
            self.cur.fetchall()
            execute_query(self.cur, f"DROP MATERIALIZED VIEW IF EXISTS {self.name}")
        except Exception as e:
            print(f"Hydration churn {self.name} failed, retrying: {e}")
            time.sleep(self.RETRY_BACKOFF_SECONDS)
            try:
                self._connect()
            except Exception as e:
                print(f"Hydration churn {self.name} could not reconnect: {e}")
            return False
        return True

    def __str__(self) -> str:
        return f"hydration churn {self.name}"


class ReadIsolationUnderHydration(Scenario):
    r"""Measures whether peeks stay fast while the replica's maintenance workers
    are saturated by continuous dataflow hydration.

    Reads and dataflow maintenance share a replica's workers, so a build that
    monopolizes them delays the peeks issued against already hydrated
    collections. This scenario puts both on one replica and reports what the
    reads experience.

    The peeks run open-loop at a fixed rate, so a replica that cannot keep up
    under contention accumulates queue-wait latency the reported p50/p99
    capture. That backlog is the signal: it is what a user issuing reads at a
    steady rate experiences when maintenance steals the serving capacity.

    The measured loops are pooled so that the backlog is the replica's, and the
    pool is sized well above what they hold. By Little's law a local run held
    about 73 connections in flight across the two loops, so the 100 they
    started with left no room: a slower replica blocks in `conns.get()`, the
    offered rate stops reaching the replica, and the percentiles collapse back
    into the arrival-schedule artifact at concurrency 100 instead of 1. The
    slower side of an A/B is the one that would hit it, and nothing in the
    output distinguishes a pool-capped run from a replica-limited one.

    `ReuseConnQuery` serializes an open loop on one connection behind one lock,
    so read concurrency is one however large the thread pool is, and any query
    slower than the inter-arrival time backs up in the client instead. The
    percentiles that come out of that scale with the length of the load phase
    rather than with peek latency, which is the same on both sides of an A/B.

    The pool is set to SERIALIZABLE. Under STRICT SERIALIZABLE a peek is pinned
    to the timestamp oracle and cannot answer until the index's frontier on the
    replica reaches it, and advancing that frontier is compute work on the very
    workers the churn saturates. The measurement would then blend peek service
    time with frontier lag, and a change that lets peeks overtake maintenance
    does not advance the frontier any sooner, so it would report no improvement
    where there was one.

    Compare p50 and p99, not qps. An open loop offers a fixed number of
    queries, every one of them is drained before the run ends, and each is
    timestamped when it was scheduled rather than when it completed, so the
    reported qps is the offered rate whatever the replica does.

    Check `queries` on both sides before comparing percentiles. A query that
    raises is logged and dropped rather than recorded, so a run that lost
    samples reports percentiles over the ones that survived, and the ones that
    fail are the ones taken when the replica was worst.

    To A/B a change that claims to improve read isolation:

        bin/mzcompose --find parallel-benchmark run default \
            --scenario ReadIsolationUnderHydration \
            --this-params <flag>=true
        bin/mzcompose --find parallel-benchmark run default \
            --scenario ReadIsolationUnderHydration \
            --this-params <flag>=false
    """

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        mz = conn_infos["materialized"]
        # Heavy enough that one build takes real CPU, small enough that two
        # concurrent churn loops do not exhaust memory.
        churn_view_sql = "SELECT a, count(*) AS c FROM big GROUP BY a"
        self.init(
            [
                TdPhase("""
                    > DROP TABLE IF EXISTS hot CASCADE
                    > DROP TABLE IF EXISTS big CASCADE

                    # The peek target: a small indexed table, pre-hydrated.
                    > CREATE TABLE hot (k int, v int)
                    > INSERT INTO hot SELECT n, n * 2 FROM generate_series(1, 100000) AS n
                    > CREATE INDEX hot_k ON hot (k)

                    # The contention source: a large table churned into heavy MVs.
                    > CREATE TABLE big (a int, b int)
                    > INSERT INTO big SELECT n, n % 1000 FROM generate_series(1, 1000000) AS n

                    # Wait for the hot index to hydrate before measuring.
                    > SELECT v FROM hot WHERE k = 42
                    84
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        # Measured: fast-path index point lookup.
                        OpenLoop(
                            action=PooledQuery("SELECT v FROM hot WHERE k = 42"),
                            dist=Periodic(per_second=50),
                            report_regressions=True,
                        ),
                        # Measured: slow-path range scan + reduce peek (more
                        # sensitive to contention on the replica).
                        OpenLoop(
                            action=PooledQuery(
                                "SELECT count(*) FROM hot WHERE k < 50000"
                            ),
                            dist=Periodic(per_second=12),
                            report_regressions=True,
                        ),
                    ]
                    + [
                        # Contention: continuous hydration churn on the same replica.
                        ClosedLoop(
                            action=HydrationChurn(mz, f"churn_{i}", churn_view_sql),
                            report_regressions=False,
                        )
                        for i in range(2)
                    ],
                ),
                # Nothing resets the services between scenarios when the
                # benchmark runs against an existing environment, so the
                # scenario that created these drops them. `big` takes the churn
                # views with it if a load phase ended mid-iteration.
                TdPhase("""
                    > DROP TABLE IF EXISTS hot CASCADE
                    > DROP TABLE IF EXISTS big CASCADE
                    """),
            ],
            conn_pool_size=1000,
            conn_pool_setup=["SET TRANSACTION_ISOLATION TO 'SERIALIZABLE'"],
            regression_thresholds={
                "SELECT v FROM hot WHERE k = 42 (pooled)": CONTENDED_THRESHOLDS,
                "SELECT count(*) FROM hot WHERE k < 50000 (pooled)": CONTENDED_THRESHOLDS,
            },
        )


class PeekIsolationUnderExpensivePeeks(Scenario):
    r"""Measures whether a cheap peek stays fast while expensive peeks occupy the
    same workers.

    Every query is a fast-path index peek, so nothing renders a dataflow during
    the load phase. That is what separates this from
    `ReadIsolationUnderHydration`, whose contention is dataflow maintenance and
    one of whose measured queries is an aggregate.

    The expensive query filters on a non-key column, so it walks every position
    of the index and returns nothing: its cost is the walk, which keeps result
    size and the peek response stash out of the measurement. The cheap query is
    a literal lookup on the key, and its p99 is what this reports.

    Both queries are peeks, so they share a runtime wherever peeks are placed.
    This guards how the runtime that serves peeks interleaves a cheap one with
    an expensive one, not the split between peeks and maintenance.

    Three things the numbers depend on, none of which the output would reveal if
    they stopped holding:

    * The expensive loop has to leave the worker idle between walks, or the
      lookups queue without bound and the percentiles report the length of the
      load phase. The measured cluster is one worker, since `Materialized` boots
      at the `bootstrap` replica size and `--size` does not reach it, so the rate
      is set against one walk: 100,000 positions measured about 40ms, so 500,000
      is about 200ms, and 3/s of those is about 60% of that worker. At 100,000
      positions and 2/s the lookups did not notice the walks at all. Raising the
      rate or the row count means redoing this.
    * The loops are pooled and the pool is far larger than the ~3 connections
      they hold. Waiting for a connection is timed like waiting for the replica,
      and `ReuseConnQuery` would cap concurrency at one and report a client-side
      backlog instead.
    * The pool is SERIALIZABLE. Under STRICT SERIALIZABLE a peek waits for the
      index's frontier, which is work on the same worker the walks occupy, so
      the tail would include frontier lag that no change to peek placement
      shortens.

    Check `queries` on both sides before comparing percentiles: a query that
    raises is dropped from the sample rather than recorded as slow.

    To A/B a change that claims to improve peek isolation, run the scenario
    twice with the flag that gates it and compare the lookup p50/p99:

        bin/mzcompose --find parallel-benchmark run default \
            --scenario PeekIsolationUnderExpensivePeeks \
            --this-params <flag>=true
        bin/mzcompose --find parallel-benchmark run default \
            --scenario PeekIsolationUnderExpensivePeeks \
            --this-params <flag>=false
    """

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase("""
                    > DROP TABLE IF EXISTS hot CASCADE

                    # Sized so one full walk is long enough to delay what is
                    # queued behind it and short enough to leave the worker idle
                    # between walks. The class docstring has the arithmetic.
                    > CREATE TABLE hot (k int, v int)
                    > INSERT INTO hot SELECT n, n * 2 FROM generate_series(1, 500000) AS n
                    > CREATE INDEX hot_k ON hot (k)

                    # Wait for the index to hydrate before measuring.
                    > SELECT v FROM hot WHERE k = 42
                    84
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        # Measured: a literal lookup on the key, which is the
                        # peek that has to stay fast.
                        OpenLoop(
                            action=PooledQuery("SELECT v FROM hot WHERE k = 42"),
                            dist=Periodic(per_second=50),
                            report_regressions=True,
                        ),
                        # Contention: a full walk of the same index. `v` is
                        # always even and positive, so the filter matches
                        # nothing and every position is examined for no rows.
                        OpenLoop(
                            action=PooledQuery("SELECT v FROM hot WHERE v = -1"),
                            dist=Periodic(per_second=3),
                            report_regressions=False,
                        ),
                    ],
                ),
            ],
            conn_pool_size=100,
            conn_pool_setup=["SET TRANSACTION_ISOLATION TO 'SERIALIZABLE'"],
            regression_thresholds={
                "SELECT v FROM hot WHERE k = 42 (pooled)": CONTENDED_THRESHOLDS,
            },
        )


class TemporaryDataflowFloor(Scenario):
    """Measures the cost of a peek that has to build a dataflow, on a quiet
    replica.

    A join of two indexed tables cannot take the fast path, so every query
    renders a dataflow that imports both indexes, runs it to its single time,
    and tears it down. That fixed cost is the floor under every non-fast-path
    read, and it is what placement on another runtime adds to or removes from.
    The tables are small so the join itself is a negligible part of the
    measurement.

    The loop is closed on one connection, so the reported latency is service
    time with no queueing in it.
    """

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        mz = conn_infos["materialized"]
        self.init(
            [
                TdPhase("""
                    > DROP TABLE IF EXISTS tdf_a CASCADE
                    > DROP TABLE IF EXISTS tdf_b CASCADE

                    > CREATE TABLE tdf_a (k int, v int)
                    > CREATE TABLE tdf_b (k int, v int)
                    > INSERT INTO tdf_a SELECT n, n FROM generate_series(1, 1000) AS n
                    > INSERT INTO tdf_b SELECT n, n FROM generate_series(1, 1000) AS n
                    > CREATE INDEX tdf_a_k ON tdf_a (k)
                    > CREATE INDEX tdf_b_k ON tdf_b (k)

                    # Wait for both indexes to hydrate before measuring.
                    > SELECT count(*) FROM tdf_a JOIN tdf_b USING (k)
                    1000
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT count(*) FROM tdf_a JOIN tdf_b USING (k)",
                                mz,
                                strict_serializable=False,
                            ),
                        ),
                    ],
                ),
                TdPhase("""
                    > DROP TABLE IF EXISTS tdf_a CASCADE
                    > DROP TABLE IF EXISTS tdf_b CASCADE
                    """),
            ],
        )


class IntrospectionUnderHydration(Scenario):
    """Measures whether per-replica introspection stays answerable while the
    replica's maintenance workers are saturated by hydration.

    Introspection is what an operator reaches for when a replica is busy, and
    a replica that answers it only once the hydration yields is unobservable
    exactly when it matters. The measured query reads an introspection
    arrangement of the replica doing the hydrating. The contention is the same
    churn `ReadIsolationUnderHydration` uses.

    Open loop at a fixed rate, so a replica that cannot keep up accumulates
    queue-wait latency the reported p50/p99 capture. See that scenario for why
    the loop is pooled and SERIALIZABLE, and why qps is not a signal.
    """

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        mz = conn_infos["materialized"]
        churn_view_sql = "SELECT a, count(*) AS c FROM big GROUP BY a"
        self.init(
            [
                TdPhase("""
                    > DROP TABLE IF EXISTS big CASCADE

                    > CREATE TABLE big (a int, b int)
                    > INSERT INTO big SELECT n, n % 1000 FROM generate_series(1, 1000000) AS n

                    # Something for introspection to report on.
                    > CREATE INDEX big_a ON big (a)
                    > SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_arrangement_sizes
                    true
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        # Measured: a per-replica introspection read.
                        OpenLoop(
                            action=PooledQuery(
                                "SELECT count(*) FROM mz_introspection.mz_dataflow_arrangement_sizes"
                            ),
                            dist=Periodic(per_second=10),
                            report_regressions=True,
                        ),
                    ]
                    + [
                        ClosedLoop(
                            action=HydrationChurn(mz, f"ichurn_{i}", churn_view_sql),
                            report_regressions=False,
                        )
                        for i in range(2)
                    ],
                ),
                TdPhase("""
                    > DROP TABLE IF EXISTS big CASCADE
                    """),
            ],
            conn_pool_size=1000,
            conn_pool_setup=[
                "SET TRANSACTION_ISOLATION TO 'SERIALIZABLE'",
                # The read must run on the replica being hydrated, not be routed
                # to the catalog server.
                "SET auto_route_introspection_queries TO false",
            ],
            regression_thresholds={
                "SELECT count(*) FROM mz_introspection.mz_dataflow_arrangement_sizes (pooled)": CONTENDED_THRESHOLDS,
            },
        )


class FreshnessUnderPeekWalks(Scenario):
    """Measures how far expensive peeks hold back a maintained index's frontier.

    Serving a peek on the worker that maintains an index is time that worker
    does not spend applying writes to it, so the index's frontier lags while
    the walk runs. A STRICT SERIALIZABLE read after a write cannot answer until
    the frontier passes the write's timestamp, so its latency is that lag made
    visible. The measured loop alternates a write and such a read on one
    connection; the contention is a fixed rate of full index walks on the same
    replica.

    The walk table is 100,000 rows at 2/s, about 40ms per walk on one worker,
    so each walk holds the frontier back measurably while leaving the worker
    idle between walks and the read does not queue without bound.
    """

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        mz = conn_infos["materialized"]
        self.init(
            [
                TdPhase("""
                    > DROP TABLE IF EXISTS fresh_w CASCADE
                    > DROP TABLE IF EXISTS fresh_hot CASCADE

                    # The written index, whose frontier the read waits for.
                    > CREATE TABLE fresh_w (k int, v int)
                    > INSERT INTO fresh_w SELECT n, n FROM generate_series(1, 1000) AS n
                    > CREATE INDEX fresh_w_k ON fresh_w (k)

                    # The walked index. `v` is always positive, so a filter on
                    # -1 examines every position and returns nothing.
                    > CREATE TABLE fresh_hot (k int, v int)
                    > INSERT INTO fresh_hot SELECT n, n * 2 FROM generate_series(1, 100000) AS n
                    > CREATE INDEX fresh_hot_k ON fresh_hot (k)

                    > SELECT count(*) FROM fresh_w
                    1000
                    > SELECT v FROM fresh_hot WHERE k = 42
                    84
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        # Contention: writes the read has to wait for.
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "INSERT INTO fresh_w VALUES (0, 0)",
                                mz,
                                strict_serializable=False,
                            ),
                            report_regressions=False,
                        ),
                        # Measured: a read that waits for the index frontier to
                        # pass the latest write.
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT count(*) FROM fresh_w WHERE k = 0",
                                mz,
                                strict_serializable=True,
                            ),
                        ),
                        # Contention: full walks of the other index.
                        OpenLoop(
                            action=PooledQuery("SELECT v FROM fresh_hot WHERE v = -1"),
                            dist=Periodic(per_second=2),
                            report_regressions=False,
                        ),
                    ],
                ),
                TdPhase("""
                    > DROP TABLE IF EXISTS fresh_w CASCADE
                    > DROP TABLE IF EXISTS fresh_hot CASCADE
                    """),
            ],
            conn_pool_size=100,
            conn_pool_setup=["SET TRANSACTION_ISOLATION TO 'SERIALIZABLE'"],
            regression_thresholds={
                "SELECT count(*) FROM fresh_w WHERE k = 0 (reuse connection)": CONTENDED_THRESHOLDS,
            },
        )


class MaintenanceUnderPeekSaturation(Scenario):
    """Measures how far a saturating peek load holds back maintenance on the same
    replica.

    The other isolation scenarios saturate maintenance and measure peeks. This
    is the converse. Cluster `sat` has eight workers, half the cores of the
    agents the nightly runs on, and carries three loads: eight closed loops of a
    join peek, one in flight per worker, so every worker that serves peeks is
    busy; two hydration churn loops, so the maintenance workers are busy too;
    and a small materialized view over a table a writer keeps advancing. With a
    second runtime that is sixteen worker threads on sixteen cores, which is the
    oversubscription this prices. With one runtime the eight workers do all of
    it in turn.

    The measured query is a strict serializable read of the materialized view
    from `sat_idle`, a separate one-worker cluster with nothing else to do. It
    cannot answer until the view's write frontier passes the write, so its
    latency is the view's maintenance lag on `sat` plus one idle lookup. Read on
    `sat` itself it would instead measure the lookup queueing behind the joins
    on the runtime that serves peeks, which the join loops' own latency already
    reports. That latency is reported too, as the serving runtime's throughput.
    """

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        # `connect()` issues `SET cluster` itself, which has to run in autocommit:
        # `ReuseConnQuery` and `HydrationChurn` turn autocommit on afterwards and
        # cannot while that statement's transaction is open. The clusters do not
        # exist yet when these connections open, which is a notice, not an error.
        sat = replace(conn_infos["materialized"], cluster="sat", autocommit=True)
        idle = replace(conn_infos["materialized"], cluster="sat_idle", autocommit=True)
        join = "SELECT count(*) FROM sat_big a JOIN sat_big b USING (k)"
        churn_view_sql = "SELECT a, count(*) AS c FROM sat_churn GROUP BY a"
        self.init(
            [
                TdPhase(f"""
                    > DROP TABLE IF EXISTS sat_w CASCADE
                    > DROP TABLE IF EXISTS sat_big CASCADE
                    > DROP TABLE IF EXISTS sat_churn CASCADE
                    > DROP CLUSTER IF EXISTS sat CASCADE
                    > DROP CLUSTER IF EXISTS sat_idle CASCADE

                    > CREATE CLUSTER sat SIZE 'scale=1,workers=8', REPLICATION FACTOR 1
                    > CREATE CLUSTER sat_idle SIZE 'scale=1,workers=1', REPLICATION FACTOR 1

                    # The written table and the view whose freshness is measured.
                    > CREATE TABLE sat_w (k int, v int)
                    > INSERT INTO sat_w SELECT n, n FROM generate_series(1, 1000) AS n
                    > CREATE MATERIALIZED VIEW sat_mv IN CLUSTER sat AS SELECT count(*) AS c FROM sat_w

                    # The join input.
                    > CREATE TABLE sat_big (k int, v int)
                    > INSERT INTO sat_big SELECT n, n * 2 FROM generate_series(1, 200000) AS n
                    > CREATE INDEX sat_big_k IN CLUSTER sat ON sat_big (k)

                    # The churn input.
                    > CREATE TABLE sat_churn (a int, b int)
                    > INSERT INTO sat_churn SELECT n, n % 1000 FROM generate_series(1, 1000000) AS n

                    > SET cluster = sat_idle
                    > SELECT c FROM sat_mv
                    1000

                    > SET cluster = sat
                    > {join}
                    200000
                    """),
                LoadPhase(
                    duration=120,
                    actions=[
                        # Contention: writes the read has to wait for.
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "INSERT INTO sat_w VALUES (0, 0)",
                                sat,
                                strict_serializable=False,
                            ),
                            report_regressions=False,
                        ),
                        # Measured: a read that waits for the view's write
                        # frontier to pass the latest write, served off `sat`.
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT c FROM sat_mv",
                                idle,
                                strict_serializable=True,
                            ),
                        ),
                    ]
                    + [
                        # Contention and measured: one peek dataflow in flight
                        # per worker.
                        ClosedLoop(action=PooledQuery(join))
                        for _ in range(8)
                    ]
                    + [
                        # Contention: continuous hydration on the maintenance
                        # workers.
                        ClosedLoop(
                            action=HydrationChurn(
                                sat, f"sat_churn_{i}", churn_view_sql
                            ),
                            report_regressions=False,
                        )
                        for i in range(2)
                    ],
                ),
                TdPhase("""
                    > DROP TABLE IF EXISTS sat_w CASCADE
                    > DROP TABLE IF EXISTS sat_big CASCADE
                    > DROP TABLE IF EXISTS sat_churn CASCADE
                    > DROP CLUSTER IF EXISTS sat CASCADE
                    > DROP CLUSTER IF EXISTS sat_idle CASCADE
                    """),
            ],
            conn_pool_size=100,
            conn_pool_setup=[
                "SET TRANSACTION_ISOLATION TO 'SERIALIZABLE'",
                "SET cluster = sat",
            ],
            regression_thresholds={
                "SELECT c FROM sat_mv (reuse connection)": CONTENDED_THRESHOLDS,
                f"{join} (pooled)": CONTENDED_THRESHOLDS,
            },
        )
