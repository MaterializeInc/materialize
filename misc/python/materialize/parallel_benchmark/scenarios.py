# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from copy import deepcopy

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.mysql import MySql
from materialize.parallel_benchmark.framework import (
    ClosedLoop,
    LoadPhase,
    OpenLoop,
    Periodic,
    PooledQuery,
    ReuseConnQuery,
    Scenario,
    StandaloneQuery,
    TdAction,
    TdPhase,
    disabled,
)
from materialize.util import PgConnInfo


class Kafka(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
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
                    """
                ),
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
                "SELECT * FROM kafka_mv (standalone)": {"qps": 15, "p99": 400},
            },
        )


class PgReadReplica(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
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
                    """
                ),
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
                TdPhase(
                    """
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
                    """
                ),
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
                TdPhase(
                    f"""
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
                    """
                ),
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
                TdPhase(
                    """
                    > CREATE TABLE t4 (f1 TEXT, f2 INTEGER);
                    > CREATE DEFAULT INDEX ON t4;
                    > INSERT INTO t4 VALUES ('A', 1);
                    > INSERT INTO t4 VALUES ('B', 2);
                    > INSERT INTO t4 VALUES ('C', 3);
                    > INSERT INTO t4 VALUES ('D', 4);
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=PooledQuery(
                                "SELECT * FROM t4", conn_info=conn_infos["materialized"]
                            ),
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
                "SELECT * FROM t4 (pooled)": {"qps": 35, "max": 700},
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
                                "ALTER SYSTEM SET enable_disk_cluster_replicas = true",
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
                "SELECT 1 (reuse connection)": {"avg": 5, "max": 500, "slope": 0.1},
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
                "SELECT 1 (reuse connection)": {"qps": 2000, "max": 100, "slope": 0.1},
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
                            action=PooledQuery(
                                "SELECT 1", conn_info=conn_infos["materialized"]
                            ),
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


class StatementLogging(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    ALTER SYSTEM SET statement_logging_max_sample_rate = 1.0;
                    ALTER SYSTEM SET statement_logging_default_sample_rate = 1.0;
                    ALTER SYSTEM SET enable_statement_lifecycle_logging = true;
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=PooledQuery(
                                "SELECT 1", conn_info=conn_infos["materialized"]
                            ),
                            dist=Periodic(per_second=100),
                            # dist=Gaussian(mean=0.01, stddev=0.05),
                        ),
                    ],
                ),
                TdPhase(
                    """
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    ALTER SYSTEM SET statement_logging_default_sample_rate = 0;
                    ALTER SYSTEM SET statement_logging_max_sample_rate = 0;
                    ALTER SYSTEM SET enable_statement_lifecycle_logging = false;
                    """
                ),
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
                TdPhase(
                    """
                    > CREATE TABLE insert_table (a int, b text);
                    """
                ),
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
            # TODO(def-): Bump per_second and add guarantees when database-issues#8510 is fixed
        )


class InsertsSelects(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    > CREATE TABLE insert_select_table (a int, b text);
                    """
                ),
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
                TdPhase(
                    """
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
                    """
                ),
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
                TdPhase(
                    """
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
                    """
                ),
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
                TdPhase(
                    """
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
                    """
                ),
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
                TdPhase(
                    """
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
                    """
                ),
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
    # TODO: Reenable queries other than SELECT 1
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
                            action=PooledQuery(
                                "SELECT 1", conn_info=conn_infos["materialized"]
                            ),
                            dist=Periodic(per_second=500),
                        ),
                        # TODO: Reenable when database-issues#5511 is fixed
                        # ClosedLoop(
                        #     action=ReuseConnQuery(
                        #         "SELECT COUNT(DISTINCT l_returnflag) FROM qa_canary_environment.public_tpch.tpch_q01 WHERE sum_charge > 0",
                        #         conn_info=conn_infos["materialized"],
                        #     ),
                        # ),
                        # ClosedLoop(
                        #     action=ReuseConnQuery(
                        #         "SELECT COUNT(DISTINCT c_name) FROM qa_canary_environment.public_tpch.tpch_q18 WHERE o_orderdate <= '2023-01-01'",
                        #         conn_info=conn_infos["materialized"],
                        #     ),
                        # ),
                        # ClosedLoop(
                        #    action=ReuseConnQuery(
                        #        "SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_pg_cdc.pg_wmr WHERE degree > 1",
                        #        conn_info=conn_infos["materialized"],
                        #    ),
                        # ),
                        # ClosedLoop(
                        #    action=ReuseConnQuery(
                        #        "SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_mysql_cdc.mysql_wmr WHERE degree > 1",
                        #        conn_info=conn_infos["materialized"],
                        #    ),
                        # ),
                        # ClosedLoop(
                        #    action=ReuseConnQuery(
                        #        "SELECT COUNT(DISTINCT count_star) FROM qa_canary_environment.public_loadgen.sales_product_product_category WHERE count_distinct_product_id > 0",
                        #        conn_info=conn_infos["materialized"],
                        #    ),
                        # ),
                        # ClosedLoop(
                        #    action=ReuseConnQuery(
                        #        "SELECT * FROM qa_canary_environment.public_table.table_mv",
                        #        conn_info=conn_infos["materialized"],
                        #    ),
                        # ),
                        # ClosedLoop(
                        #    action=ReuseConnQuery(
                        #        "SELECT min(c), max(c), count(*) FROM qa_canary_environment.public_table.table",
                        #        conn_info=conn_infos["materialized"],
                        #    ),
                        # ),
                    ],
                ),
            ],
            conn_pool_size=100,
        )
