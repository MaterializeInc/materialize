# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Explicit deterministic tests for read-only mode and zero downtime deploys (same
version, no upgrade).
"""

import time
from datetime import datetime, timedelta
from textwrap import dedent
from threading import Thread

from psycopg.errors import OperationalError

from materialize import buildkite
from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import (
    LEADER_STATUS_HEALTHCHECK,
    DeploymentStatus,
    Materialized,
)
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import (
    CockroachOrPostgresMetadata,
    Postgres,
)
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import (
    SqlServer,
    setup_sql_server_testing,
)
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.ui import CommandFailureCausedUIError

DEFAULT_TIMEOUT = "300s"

SYSTEM_PARAMETER_DEFAULTS = get_default_system_parameters(zero_downtime=True)

SERVICES = [
    MySql(),
    Postgres(),
    SqlServer(),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    CockroachOrPostgresMetadata(),
    Mz(app_password=""),
    Materialized(
        name="mz_old",
        sanity_restart=False,
        deploy_generation=0,
        system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
        external_metadata_store=True,
        default_replication_factor=2,
    ),
    Materialized(
        name="mz_new",
        sanity_restart=False,
        deploy_generation=1,
        system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
        restart="on-failure",
        external_metadata_store=True,
        default_replication_factor=2,
    ),
    Testdrive(
        materialize_url="postgres://materialize@mz_old:6875",
        materialize_url_internal="postgres://materialize@mz_old:6877",
        mz_service="mz_old",
        materialize_params={"cluster": "cluster"},
        no_reset=True,
        seed=1,
        default_timeout=DEFAULT_TIMEOUT,
    ),
]


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return
        with c.test_case(name):
            c.workflow(name)

    workflows = buildkite.shard_list(
        list(c.workflows.keys()), lambda workflow: workflow
    )
    c.test_parts(workflows, process)


def workflow_read_only(c: Composition) -> None:
    """Verify read-only mode."""
    c.down(destroy_volumes=True)
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "postgres",
        "mysql",
        "sql-server",
        "mz_old",
        Service("testdrive", idle=True),
    )
    setup(c)
    setup_sql_server_testing(c)

    # Inserts should be reflected when writes are allowed.
    c.testdrive(
        dedent(
            f"""
        > SET CLUSTER = cluster;
        > CREATE TABLE t (a int, b int);

        > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL = 'PLAINTEXT';
        > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';
        > CREATE SINK kafka_sink
          IN CLUSTER cluster
          FROM t
          INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${{testdrive.seed}}')
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE DEBEZIUM;

        > INSERT INTO t VALUES (1, 2);
        > CREATE INDEX t_idx ON t (a, b);
        > CREATE MATERIALIZED VIEW mv AS SELECT sum(a) FROM t;
        > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
        > SELECT * FROM mv;
        1
        > SELECT max(b) FROM t;
        2

        $ kafka-create-topic topic=kafka
        $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
        key1A,key1B:value1A,value1B
        > CREATE SOURCE kafka_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-${{testdrive.seed}}');

        > CREATE TABLE kafka_source_tbl (key1, key2, value1, value2)
          FROM SOURCE kafka_source (REFERENCE "testdrive-kafka-${{testdrive.seed}}")
          KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          ENVELOPE UPSERT;
        > SELECT * FROM kafka_source_tbl
        key1A key1B value1A value1B

        $ postgres-execute connection=postgres://postgres:postgres@postgres
        CREATE USER postgres1 WITH SUPERUSER PASSWORD 'postgres';
        ALTER USER postgres1 WITH replication;
        DROP PUBLICATION IF EXISTS postgres_source;
        DROP TABLE IF EXISTS postgres_source_table;
        CREATE TABLE postgres_source_table (f1 TEXT, f2 INTEGER);
        ALTER TABLE postgres_source_table REPLICA IDENTITY FULL;
        INSERT INTO postgres_source_table SELECT 'A', 0;
        CREATE PUBLICATION postgres_source FOR ALL TABLES;

        > CREATE SECRET pgpass AS 'postgres';
        > CREATE CONNECTION pg FOR POSTGRES
          HOST 'postgres',
          DATABASE postgres,
          USER postgres1,
          PASSWORD SECRET pgpass;
        > CREATE SOURCE postgres_source
          IN CLUSTER cluster
          FROM POSTGRES CONNECTION pg
          (PUBLICATION 'postgres_source');
        > CREATE TABLE postgres_source_table FROM SOURCE postgres_source (REFERENCE postgres_source_table)
        > SELECT * FROM postgres_source_table;
        A 0

        $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
        $ mysql-execute name=mysql
        # create the database if it does not exist yet but do not drop it
        CREATE DATABASE IF NOT EXISTS public;
        USE public;
        CREATE USER mysql1 IDENTIFIED BY 'mysql';
        GRANT REPLICATION SLAVE ON *.* TO mysql1;
        GRANT ALL ON public.* TO mysql1;
        CREATE TABLE mysql_source_table (f1 VARCHAR(32), f2 INTEGER);
        INSERT INTO mysql_source_table VALUES ('A', 0);

        > CREATE SECRET mysqlpass AS 'mysql';
        > CREATE CONNECTION mysql TO MYSQL (
          HOST 'mysql',
          USER mysql1,
          PASSWORD SECRET mysqlpass);
        > CREATE SOURCE mysql_source
          IN CLUSTER cluster
          FROM MYSQL CONNECTION mysql;
        > CREATE TABLE mysql_source_table FROM SOURCE mysql_source (REFERENCE public.mysql_source_table);
        > SELECT * FROM mysql_source_table;
        A 0

        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

        $ sql-server-execute name=sql-server
        USE test;
        CREATE TABLE sql_server_source_table (f1 VARCHAR(32), f2 INTEGER);
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sql_server_source_table', @role_name = 'SA', @supports_net_changes = 0;
        INSERT INTO sql_server_source_table VALUES ('A', 0);

        > CREATE SECRET sqlserverpass AS '{SqlServer.DEFAULT_SA_PASSWORD}';
        > CREATE CONNECTION sqlserver TO SQL SERVER (
          HOST 'sql-server',
          DATABASE test,
          USER {SqlServer.DEFAULT_USER},
          PASSWORD SECRET sqlserverpass);
        > CREATE SOURCE sql_server_source
          IN CLUSTER cluster
          FROM SQL SERVER CONNECTION sqlserver;
        > CREATE TABLE sql_server_source_table FROM SOURCE sql_server_source (REFERENCE sql_server_source_table);
        > SELECT * FROM sql_server_source_table;
        A 0

        $ kafka-verify-topic sink=materialize.public.kafka_sink

        > CREATE SOURCE kafka_sink_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${{testdrive.seed}}')

        > CREATE TABLE kafka_sink_source_tbl FROM SOURCE kafka_sink_source (REFERENCE "testdrive-kafka-sink-${{testdrive.seed}}")
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE NONE

        > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
        <null> <null> 1 2

        > CREATE SOURCE webhook_source
          IN CLUSTER cluster_singlereplica
          FROM WEBHOOK BODY FORMAT TEXT

        $ webhook-append database=materialize schema=public name=webhook_source
        AAA
        > SELECT * FROM webhook_source
        AAA
        """
        )
    )

    # Restart in a new deploy generation, which will cause Materialize to
    # boot in read-only mode.
    with c.override(
        Materialized(
            name="mz_old",
            deploy_generation=1,
            external_metadata_store=True,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            default_replication_factor=2,
        )
    ):
        c.up("mz_old")

        c.testdrive(
            dedent(
                f"""
            $ webhook-append database=materialize schema=public name=webhook_source status=500
            BBB

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
            key2A,key2B:value2A,value2B

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO postgres_source_table VALUES ('B', 1);

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            USE public;
            INSERT INTO mysql_source_table VALUES ('B', 1);

            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            $ sql-server-execute name=sql-server
            USE test;
            INSERT INTO sql_server_source_table VALUES ('B', 1);

            > SET CLUSTER = cluster;
            > SELECT 1
            1
            ! INSERT INTO t VALUES (3, 4);
            contains: cannot write in read-only mode
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM mv;
            1
            # TODO: Currently hangs
            # > SELECT max(b) FROM t;
            # 2
            > SELECT mz_unsafe.mz_sleep(5)
            <null>
            ! INSERT INTO t VALUES (5, 6);
            contains: cannot write in read-only mode
            > SELECT * FROM mv;
            1
            ! DROP INDEX t_idx
            contains: cannot write in read-only mode
            ! CREATE INDEX t_idx2 ON t (a, b)
            contains: cannot write in read-only mode
            ! CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;
            contains: cannot write in read-only mode

            $ set-regex match=(s\\d+|\\d{{13}}|[ ]{{12}}0|u\\d{{1,3}}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

            > EXPLAIN TIMESTAMP FOR SELECT * FROM mv;
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (StorageInput([User(6)])): [<> <>]\\n"

            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            > SELECT * FROM postgres_source_table
            A 0
            > SELECT * FROM mysql_source_table;
            A 0
            > SELECT * FROM sql_server_source_table;
            A 0
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
            <null> <null> 1 2
            > SELECT * FROM webhook_source
            AAA
            """
            )
        )

        c.up("mz_old")
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_old")
        c.promote_mz("mz_old")

    # After promotion, the deployment should boot with writes allowed.
    with c.override(
        Materialized(
            name="mz_old",
            healthcheck=[
                "CMD-SHELL",
                """[ "$(curl -f localhost:6878/api/leader/status)" = '{"status":"IsLeader"}' ]""",
            ],
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            external_metadata_store=True,
            default_replication_factor=2,
        )
    ):
        c.up("mz_old")

        c.testdrive(
            dedent(
                f"""
            $ webhook-append database=materialize schema=public name=webhook_source
            CCC
            > SET CLUSTER = cluster;
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;
            > SELECT * FROM mv;
            1
            > SELECT * FROM mv2;
            1
            > SELECT max(b) FROM t;
            2
            > INSERT INTO t VALUES (7, 8);
            > SELECT * FROM mv;
            8
            > SELECT * FROM mv2;
            8
            > SELECT max(b) FROM t;
            8
            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
            <null> <null> 1 2
            <null> <null> 7 8

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
            key3A,key3B:value3A,value3B

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO postgres_source_table VALUES ('C', 2);

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            USE public;
            INSERT INTO mysql_source_table VALUES ('C', 2);

            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            $ sql-server-execute name=sql-server
            USE test;
            INSERT INTO sql_server_source_table VALUES ('C', 2);

            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            key3A key3B value3A value3B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            C 2
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            C 2
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            C 2
            > SELECT * FROM webhook_source
            AAA
            CCC
            """
            )
        )


def workflow_basic(c: Composition) -> None:
    """Verify basic 0dt deployment flow."""
    c.down(destroy_volumes=True)
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "postgres",
        "mysql",
        "sql-server",
        "mz_old",
        Service("testdrive", idle=True),
    )
    setup(c)
    setup_sql_server_testing(c)

    # Inserts should be reflected when writes are allowed.
    c.testdrive(
        dedent(
            f"""
        > SET CLUSTER = cluster;
        > CREATE TABLE t (a int, b int);

        > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL = 'PLAINTEXT';
        > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';
        > CREATE SINK kafka_sink
          IN CLUSTER cluster
          FROM t
          INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${{testdrive.seed}}')
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE DEBEZIUM;

        > INSERT INTO t VALUES (1, 2);
        > CREATE INDEX t_idx ON t (a, b);
        > CREATE MATERIALIZED VIEW mv AS SELECT sum(a) FROM t;
        > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
        > SELECT * FROM mv;
        1
        > SELECT max(b) FROM t;
        2

        $ kafka-create-topic topic=kafka
        $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
        key1A,key1B:value1A,value1B
        > CREATE SOURCE kafka_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-${{testdrive.seed}}');

        > CREATE TABLE kafka_source_tbl (key1, key2, value1, value2)
          FROM SOURCE kafka_source (REFERENCE "testdrive-kafka-${{testdrive.seed}}")
          KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          ENVELOPE UPSERT;
        > SELECT * FROM kafka_source_tbl
        key1A key1B value1A value1B

        $ postgres-execute connection=postgres://postgres:postgres@postgres
        CREATE USER postgres1 WITH SUPERUSER PASSWORD 'postgres';
        ALTER USER postgres1 WITH replication;
        DROP PUBLICATION IF EXISTS postgres_source;
        DROP TABLE IF EXISTS postgres_source_table;
        CREATE TABLE postgres_source_table (f1 TEXT, f2 INTEGER);
        ALTER TABLE postgres_source_table REPLICA IDENTITY FULL;
        INSERT INTO postgres_source_table SELECT 'A', 0;
        CREATE PUBLICATION postgres_source FOR ALL TABLES;

        > CREATE SECRET pgpass AS 'postgres';
        > CREATE CONNECTION pg FOR POSTGRES
          HOST 'postgres',
          DATABASE postgres,
          USER postgres1,
          PASSWORD SECRET pgpass;
        > CREATE SOURCE postgres_source
          IN CLUSTER cluster
          FROM POSTGRES CONNECTION pg
          (PUBLICATION 'postgres_source');
        > CREATE TABLE postgres_source_table FROM SOURCE postgres_source (REFERENCE postgres_source_table)
        > SELECT * FROM postgres_source_table;
        A 0

        $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
        $ mysql-execute name=mysql
        # create the database if it does not exist yet but do not drop it
        CREATE DATABASE IF NOT EXISTS public;
        USE public;
        CREATE USER mysql1 IDENTIFIED BY 'mysql';
        GRANT REPLICATION SLAVE ON *.* TO mysql1;
        GRANT ALL ON public.* TO mysql1;
        CREATE TABLE mysql_source_table (f1 VARCHAR(32), f2 INTEGER);
        INSERT INTO mysql_source_table VALUES ('A', 0);

        > CREATE SECRET mysqlpass AS 'mysql';
        > CREATE CONNECTION mysql TO MYSQL (
          HOST 'mysql',
          USER mysql1,
          PASSWORD SECRET mysqlpass);
        > CREATE SOURCE mysql_source1
          IN CLUSTER cluster
          FROM MYSQL CONNECTION mysql;
        > CREATE TABLE mysql_source_table FROM SOURCE mysql_source1 (REFERENCE public.mysql_source_table);
        > SELECT * FROM mysql_source_table;
        A 0

        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

        $ sql-server-execute name=sql-server
        USE test;
        CREATE TABLE sql_server_source_table (f1 VARCHAR(32), f2 INTEGER);
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sql_server_source_table', @role_name = 'SA', @supports_net_changes = 0;
        INSERT INTO sql_server_source_table VALUES ('A', 0);

        > CREATE SECRET sqlserverpass AS '{SqlServer.DEFAULT_SA_PASSWORD}';
        > CREATE CONNECTION sqlserver TO SQL SERVER (
          HOST 'sql-server',
          DATABASE test,
          USER {SqlServer.DEFAULT_USER},
          PASSWORD SECRET sqlserverpass);
        > CREATE SOURCE sql_server_source
          IN CLUSTER cluster
          FROM SQL SERVER CONNECTION sqlserver;
        > CREATE TABLE sql_server_source_table FROM SOURCE sql_server_source (REFERENCE sql_server_source_table);
        > SELECT * FROM sql_server_source_table;
        A 0

        $ kafka-verify-topic sink=materialize.public.kafka_sink

        > CREATE SOURCE kafka_sink_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${{testdrive.seed}}')

        > CREATE TABLE kafka_sink_source_tbl FROM SOURCE kafka_sink_source (REFERENCE "testdrive-kafka-sink-${{testdrive.seed}}")
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE NONE

        > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
        <null> <null> 1 2

        > CREATE SOURCE webhook_source
          IN CLUSTER cluster_singlereplica
          FROM WEBHOOK BODY FORMAT TEXT

        $ webhook-append database=materialize schema=public name=webhook_source
        AAA
        > SELECT * FROM webhook_source
        AAA

        $ set-max-tries max-tries=1
        $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
        > BEGIN
        > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
        > FETCH ALL c WITH (timeout='5s');
        <TIMESTAMP> 1 1
        > COMMIT
        """
        )
    )

    # Start new Materialize in a new deploy generation, which will cause
    # Materialize to boot in read-only mode.
    c.up("mz_new")

    # Verify against new Materialize that it is in read-only mode
    with c.override(
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        )
    ):
        c.up(Service("testdrive", idle=True))
        c.testdrive(
            dedent(
                f"""
            $ webhook-append database=materialize schema=public name=webhook_source status=500
            BBB

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
            key2A,key2B:value2A,value2B

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO postgres_source_table VALUES ('B', 1);

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            USE public;
            INSERT INTO mysql_source_table VALUES ('B', 1);

            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            $ sql-server-execute name=sql-server
            USE test;
            INSERT INTO sql_server_source_table VALUES ('B', 1);

            > SET CLUSTER = cluster;
            > SELECT 1
            1
            ! INSERT INTO t VALUES (3, 4);
            contains: cannot write in read-only mode
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM mv;
            1
            # TODO: Currently hangs
            # > SELECT max(b) FROM t;
            # 2
            > SELECT mz_unsafe.mz_sleep(5)
            <null>
            ! INSERT INTO t VALUES (5, 6);
            contains: cannot write in read-only mode
            > SELECT * FROM mv;
            1
            ! DROP INDEX t_idx
            contains: cannot write in read-only mode
            ! CREATE INDEX t_idx2 ON t (a, b)
            contains: cannot write in read-only mode
            ! CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;
            contains: cannot write in read-only mode

            $ set-regex match=(s\\d+|\\d{{13}}|[ ]{{12}}0|u\\d{{1,3}}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

            > EXPLAIN TIMESTAMP FOR SELECT * FROM mv;
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (StorageInput([User(6)])): [<> <>]\\n"

            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
            <null> <null> 1 2

            > SELECT * FROM webhook_source
            AAA

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
            > BEGIN
            ! DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            contains: cannot write in read-only mode
            > ROLLBACK
            # Actual subscribes without a declare still work though
            > SUBSCRIBE (WITH a(x) AS (SELECT 'a') SELECT generate_series(1, 2), x FROM a)
            <TIMESTAMP> 1 1 a
            <TIMESTAMP> 1 2 a
            """
            )
        )

    # But the old Materialize can still run writes
    c.up(Service("testdrive", idle=True))
    c.testdrive(
        dedent(
            f"""
        $ webhook-append database=materialize schema=public name=webhook_source
        CCC

        $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
        key3A,key3B:value3A,value3B

        $ postgres-execute connection=postgres://postgres:postgres@postgres
        INSERT INTO postgres_source_table VALUES ('C', 2);

        $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
        $ mysql-execute name=mysql
        USE public;
        INSERT INTO mysql_source_table VALUES ('C', 2);

        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

        $ sql-server-execute name=sql-server
        USE test;
        INSERT INTO sql_server_source_table VALUES ('C', 2);

        > SET CLUSTER = cluster;
        > SELECT 1
        1
        > INSERT INTO t VALUES (3, 4);
        > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
        > SELECT * FROM mv;
        4
        > SELECT max(b) FROM t;
        4
        > SELECT mz_unsafe.mz_sleep(5)
        <null>
        > INSERT INTO t VALUES (5, 6);
        > SELECT * FROM mv;
        9
        > DROP INDEX t_idx
        > CREATE INDEX t_idx2 ON t (a, b)
        > CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;

        $ set-regex match=(s\\d+|\\d{{13}}|[ ]{{12}}0|u\\d{{1,3}}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

        > EXPLAIN TIMESTAMP FOR SELECT * FROM mv;
        "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (StorageInput([User(6)])): [<> <>]\\n"

        > SELECT * FROM kafka_source_tbl
        key1A key1B value1A value1B
        key2A key2B value2A value2B
        key3A key3B value3A value3B
        > SELECT * FROM postgres_source_table
        A 0
        B 1
        C 2
        > SELECT * FROM mysql_source_table;
        A 0
        B 1
        C 2
        > SELECT * FROM sql_server_source_table;
        A 0
        B 1
        C 2
        > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
        <null> <null> 1 2
        <null> <null> 3 4
        <null> <null> 5 6
        > SELECT * FROM webhook_source
        AAA
        CCC

        $ set-max-tries max-tries=1
        $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
        > BEGIN
        > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
        > FETCH ALL c WITH (timeout='5s');
        <TIMESTAMP> 1 1
        <TIMESTAMP> 1 3
        <TIMESTAMP> 1 5
        > COMMIT
        """
        )
    )

    with c.override(
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        )
    ):
        c.up(Service("testdrive", idle=True))
        c.testdrive(
            dedent(
                """
            $ webhook-append database=materialize schema=public name=webhook_source status=500
            DDD

            > SET CLUSTER = cluster;
            > SELECT 1
            1
            ! INSERT INTO t VALUES (3, 4);
            contains: cannot write in read-only mode
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM mv;
            9
            > SELECT max(b) FROM t;
            6
            > SELECT * FROM mv;
            9
            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            key3A key3B value3A value3B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            C 2
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            C 2
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            C 2
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
            <null> <null> 1 2
            <null> <null> 3 4
            <null> <null> 5 6
            > SELECT * FROM webhook_source
            AAA
            CCC

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{13,20} replacement=<TIMESTAMP>
            > BEGIN
            ! DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            contains: cannot write in read-only mode
            > ROLLBACK
            # Actual subscribes without a declare still work though
            > SUBSCRIBE (WITH a(x) AS (SELECT 'a') SELECT generate_series(1, 2), x FROM a)
            <TIMESTAMP> 1 1 a
            <TIMESTAMP> 1 2 a
            """
            )
        )

        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        c.promote_mz("mz_new")

        # Give some time for Mz to restart after promotion
        for i in range(10):
            try:
                c.sql("SELECT 1", service="mz_old")
            except OperationalError as e:
                assert (
                    "server closed the connection unexpectedly" in str(e)
                    or "Can't create a connection to host" in str(e)
                    or "Connection refused" in str(e)
                    or "the connection is closed" in str(e)
                ), f"Unexpected error: {e}"
            except CommandFailureCausedUIError as e:
                # service "mz_old" is not running
                assert "running docker compose failed" in str(
                    e
                ), f"Unexpected error: {e}"
                break
            time.sleep(1)
        else:
            raise RuntimeError("mz_old didn't stop running within 10 seconds")

        for i in range(10):
            try:
                c.sql("SELECT 1", service="mz_new")
                break
            except CommandFailureCausedUIError:
                pass
            except OperationalError:
                pass
            time.sleep(1)
        else:
            raise RuntimeError("mz_new didn't come up within 10 seconds")

        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")

        c.testdrive(
            dedent(
                f"""
            $ webhook-append database=materialize schema=public name=webhook_source
            EEE
            > SET CLUSTER = cluster;
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > CREATE MATERIALIZED VIEW mv3 AS SELECT sum(a) FROM t;
            > SELECT * FROM mv;
            9
            > SELECT * FROM mv2;
            9
            > SELECT * FROM mv3;
            9
            > SELECT max(b) FROM t;
            6
            > INSERT INTO t VALUES (7, 8);
            > SELECT * FROM mv;
            16
            > SELECT * FROM mv2;
            16
            > SELECT max(b) FROM t;
            8
            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            key3A key3B value3A value3B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            C 2
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            C 2
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            C 2

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
            key4A,key4B:value4A,value4B

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO postgres_source_table VALUES ('D', 3);

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            USE public;
            INSERT INTO mysql_source_table VALUES ('D', 3);

            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            $ sql-server-execute name=sql-server
            USE test;
            INSERT INTO sql_server_source_table VALUES ('D', 3);

            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            key3A key3B value3A value3B
            key4A key4B value4A value4B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            C 2
            D 3
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            C 2
            D 3
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            C 2
            D 3
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
            <null> <null> 1 2
            <null> <null> 3 4
            <null> <null> 5 6
            <null> <null> 7 8
            > SELECT * FROM webhook_source
            AAA
            CCC
            EEE

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
            > BEGIN
            > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            > FETCH ALL c WITH (timeout='5s');
            <TIMESTAMP> 1 1
            <TIMESTAMP> 1 3
            <TIMESTAMP> 1 5
            <TIMESTAMP> 1 7
            > COMMIT
            """
            )
        )


def workflow_kafka_source_rehydration(c: Composition) -> None:
    """Verify Kafka source rehydration in 0dt deployment"""
    c.down(destroy_volumes=True)
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "mz_old",
        Service("testdrive", idle=True),
    )
    setup(c)

    count = 1000000
    repeats = 20

    start_time = time.time()
    c.testdrive(
        dedent(
            f"""
        > SET CLUSTER = cluster;

        > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL = 'PLAINTEXT';
        > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';

        $ kafka-create-topic topic=kafka-large
        $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-large repeat={count}
        key0A,key${{kafka-ingest.iteration}}:value0A,${{kafka-ingest.iteration}}
        > CREATE SOURCE kafka_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-large-${{testdrive.seed}}');

        > CREATE TABLE kafka_source_tbl (key1, key2, value1, value2)
          FROM SOURCE kafka_source (REFERENCE "testdrive-kafka-large-${{testdrive.seed}}")
          KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          ENVELOPE UPSERT;
        > CREATE VIEW kafka_source_cnt AS SELECT count(*) FROM kafka_source_tbl
        > CREATE DEFAULT INDEX on kafka_source_cnt
        > SELECT * FROM kafka_source_cnt
        {count}
        """
        )
    )
    for i in range(1, repeats):
        c.testdrive(
            dedent(
                f"""
        $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-large repeat={count}
        key{i}A,key{i}${{kafka-ingest.iteration}}:value{i}A,${{kafka-ingest.iteration}}
        > SELECT * FROM kafka_source_cnt
        {count*(i+1)}
            """
            )
        )

    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    with c.override(
        Materialized(
            name="mz_new",
            sanity_restart=False,
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            restart="on-failure",
            external_metadata_store=True,
            default_replication_factor=2,
        ),
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        ),
    ):
        c.up("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        elapsed = time.time() - start_time
        print(f"re-hydration took {elapsed} seconds")
        c.promote_mz("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )
        elapsed = time.time() - start_time
        print(f"promotion took {elapsed} seconds")

        start_time = time.time()
        result = c.sql_query("SELECT * FROM kafka_source_cnt", service="mz_new")
        elapsed = time.time() - start_time
        print(f"final check took {elapsed} seconds")
        assert result[0][0] == count * repeats, f"Wrong result: {result}"
        result = c.sql_query("SELECT count(*) FROM kafka_source_tbl", service="mz_new")
        assert result[0][0] == count * repeats, f"Wrong result: {result}"
        assert (
            elapsed < 3
        ), f"Took {elapsed}s to SELECT on Kafka source after 0dt upgrade, is it hydrated?"

        start_time = time.time()
        result = c.sql_query("SELECT 1", service="mz_new")
        elapsed = time.time() - start_time
        print(f"bootstrapping (checked via SELECT 1) took {elapsed} seconds")
        assert result[0][0] == 1, f"Wrong result: {result}"

        print("Ingesting again")
        for i in range(repeats, repeats * 2):
            c.testdrive(
                dedent(
                    f"""
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-large repeat={count}
                key{i}A,key{i}${{kafka-ingest.iteration}}:value{i}A,${{kafka-ingest.iteration}}
                """
                )
            )

        c.testdrive(
            dedent(
                f"""
            > SET CLUSTER = cluster;
            > SELECT * FROM kafka_source_cnt
            {2*count*repeats}
            > SELECT count(*) FROM kafka_source_tbl
            {2*count*repeats}
            """
            )
        )


def workflow_kafka_source_rehydration_large_initial(c: Composition) -> None:
    """Verify Kafka source rehydration in 0dt deployment"""
    c.down(destroy_volumes=True)
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "mz_old",
        Service("testdrive", idle=True),
    )
    setup(c)

    count = 1000000
    repeats = 20

    start_time = time.time()
    c.testdrive(
        dedent(
            """
        $ kafka-create-topic topic=kafka-large
            """
        )
    )
    for i in range(repeats):
        c.testdrive(
            dedent(
                f"""
            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-large repeat={count}
            key{i}A,key{i}${{kafka-ingest.iteration}}:value{i}A,${{kafka-ingest.iteration}}
            """
            )
        )

    c.testdrive(
        dedent(
            f"""
        > SET CLUSTER = cluster;

        > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL = 'PLAINTEXT';
        > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';

        > CREATE SOURCE kafka_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-large-${{testdrive.seed}}');

        > CREATE TABLE kafka_source_tbl (key1, key2, value1, value2)
          FROM SOURCE kafka_source (REFERENCE "testdrive-kafka-large-${{testdrive.seed}}")
          KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          ENVELOPE UPSERT;
        > CREATE VIEW kafka_source_cnt AS SELECT count(*) FROM kafka_source_tbl
        > CREATE DEFAULT INDEX on kafka_source_cnt
        > SELECT * FROM kafka_source_cnt
        {count*repeats}
        """
        )
    )

    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    with c.override(
        Materialized(
            name="mz_new",
            sanity_restart=False,
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            restart="on-failure",
            external_metadata_store=True,
            default_replication_factor=2,
        ),
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        ),
    ):
        c.up("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        elapsed = time.time() - start_time
        print(f"re-hydration took {elapsed} seconds")
        c.promote_mz("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )
        elapsed = time.time() - start_time
        print(f"promotion took {elapsed} seconds")

        start_time = time.time()
        result = c.sql_query("SELECT * FROM kafka_source_cnt", service="mz_new")
        elapsed = time.time() - start_time
        print(f"final check took {elapsed} seconds")
        assert result[0][0] == count * repeats, f"Wrong result: {result}"
        result = c.sql_query("SELECT count(*) FROM kafka_source_tbl", service="mz_new")
        assert result[0][0] == count * repeats, f"Wrong result: {result}"
        assert (
            elapsed < 3
        ), f"Took {elapsed}s to SELECT on Kafka source after 0dt upgrade, is it hydrated?"

        start_time = time.time()
        result = c.sql_query("SELECT 1", service="mz_new")
        elapsed = time.time() - start_time
        print(f"bootstrapping (checked via SELECT 1) took {elapsed} seconds")
        assert result[0][0] == 1, f"Wrong result: {result}"

        print("Ingesting again")
        for i in range(repeats, repeats * 2):
            c.testdrive(
                dedent(
                    f"""
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-large repeat={count}
                key{i}A,key{i}${{kafka-ingest.iteration}}:value{i}A,${{kafka-ingest.iteration}}
                """
                )
            )

        c.testdrive(
            dedent(
                f"""
            > SET CLUSTER = cluster;
            > SELECT * FROM kafka_source_cnt
            {2*count*repeats}
            > SELECT count(*) FROM kafka_source_tbl
            {2*count*repeats}
            """
            )
        )


def workflow_pg_source_rehydration(c: Composition) -> None:
    """Verify Postgres source rehydration in 0dt deployment"""
    c.down(destroy_volumes=True)
    c.up("postgres", "mz_old", Service("testdrive", idle=True))
    setup(c)

    count = 1000000
    repeats = 100

    inserts = (
        "INSERT INTO postgres_source_table VALUES "
        + ", ".join([f"({i})" for i in range(count)])
        + ";"
    )

    start_time = time.time()
    c.testdrive(
        dedent(
            f"""
        > SET CLUSTER = cluster;

        $ postgres-execute connection=postgres://postgres:postgres@postgres
        CREATE USER postgres1 WITH SUPERUSER PASSWORD 'postgres';
        ALTER USER postgres1 WITH replication;
        DROP PUBLICATION IF EXISTS postgres_source;
        DROP TABLE IF EXISTS postgres_source_table;
        CREATE TABLE postgres_source_table (f1 INTEGER);
        ALTER TABLE postgres_source_table REPLICA IDENTITY FULL;
        {inserts}
        CREATE PUBLICATION postgres_source FOR ALL TABLES;

        > CREATE SECRET pgpass AS 'postgres';
        > CREATE CONNECTION pg FOR POSTGRES
          HOST 'postgres',
          DATABASE postgres,
          USER postgres1,
          PASSWORD SECRET pgpass;
        > CREATE SOURCE postgres_source
          IN CLUSTER cluster
          FROM POSTGRES CONNECTION pg
          (PUBLICATION 'postgres_source');
        > CREATE TABLE postgres_source_table FROM SOURCE postgres_source (REFERENCE postgres_source_table)
        > CREATE VIEW postgres_source_cnt AS SELECT count(*) FROM postgres_source_table
        > CREATE DEFAULT INDEX ON postgres_source_cnt
        > SELECT * FROM postgres_source_cnt;
        {count}
        """
        ),
        quiet=True,
    )

    for i in range(1, repeats):
        c.testdrive(
            dedent(
                f"""
        $ postgres-execute connection=postgres://postgres:postgres@postgres
        {inserts}
        > SELECT * FROM postgres_source_cnt
        {count*(i+1)}
        """
            ),
            quiet=True,
        )

    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    with c.override(
        Materialized(
            name="mz_new",
            sanity_restart=False,
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            restart="on-failure",
            external_metadata_store=True,
            default_replication_factor=2,
        ),
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        ),
    ):
        c.up("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        elapsed = time.time() - start_time
        print(f"re-hydration took {elapsed} seconds")
        c.promote_mz("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )
        elapsed = time.time() - start_time
        print(f"promotion took {elapsed} seconds")
        start_time = time.time()
        result = c.sql_query("SELECT * FROM postgres_source_cnt", service="mz_new")
        elapsed = time.time() - start_time
        print(f"final check took {elapsed} seconds")
        assert result[0][0] == count * repeats, f"Wrong result: {result}"
        assert (
            elapsed < 4
        ), f"Took {elapsed}s to SELECT on Postgres source after 0dt upgrade, is it hydrated?"

        result = c.sql_query(
            "SELECT count(*) FROM postgres_source_table", service="mz_new"
        )
        assert result[0][0] == count * repeats, f"Wrong result: {result}"

        print("Ingesting again")
        for i in range(repeats, repeats * 2):
            c.testdrive(
                dedent(
                    f"""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            {inserts}
            > SELECT * FROM postgres_source_cnt
            {count*(i+1)}
            """
                ),
                quiet=True,
            )

        result = c.sql_query("SELECT * FROM postgres_source_cnt", service="mz_new")
        assert result[0][0] == 2 * count * repeats, f"Wrong result: {result}"

        result = c.sql_query(
            "SELECT count(*) FROM postgres_source_table", service="mz_new"
        )
        assert result[0][0] == 2 * count * repeats, f"Wrong result: {result}"


def workflow_mysql_source_rehydration(c: Composition) -> None:
    """Verify Postgres source rehydration in 0dt deployment"""
    c.down(destroy_volumes=True)
    c.up("mysql", "mz_old", Service("testdrive", idle=True))
    setup(c)

    count = 1000000
    repeats = 100

    inserts = (
        "INSERT INTO mysql_source_table VALUES "
        + ", ".join([f"({i})" for i in range(count)])
        + ";"
    )

    start_time = time.time()
    c.testdrive(
        dedent(
            f"""
        > SET CLUSTER = cluster;

        $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
        $ mysql-execute name=mysql
        # create the database if it does not exist yet but do not drop it
        CREATE DATABASE IF NOT EXISTS public;
        USE public;
        CREATE USER mysql1 IDENTIFIED BY 'mysql';
        GRANT REPLICATION SLAVE ON *.* TO mysql1;
        GRANT ALL ON public.* TO mysql1;
        CREATE TABLE mysql_source_table (f1 INTEGER);
        {inserts}

        > CREATE SECRET mysqlpass AS 'mysql';
        > CREATE CONNECTION mysql TO MYSQL (
          HOST 'mysql',
          USER mysql1,
          PASSWORD SECRET mysqlpass);
        > CREATE SOURCE mysql_source
          IN CLUSTER cluster
          FROM MYSQL CONNECTION mysql;
        > CREATE TABLE mysql_source_table FROM SOURCE mysql_source (REFERENCE public.mysql_source_table);
        > CREATE VIEW mysql_source_cnt AS SELECT count(*) FROM mysql_source_table
        > CREATE DEFAULT INDEX ON mysql_source_cnt
        > SELECT * FROM mysql_source_cnt;
        {count}
        """
        ),
        quiet=True,
    )

    for i in range(1, repeats):
        c.testdrive(
            dedent(
                f"""
        $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
        $ mysql-execute name=mysql
        USE public;
        {inserts}
        > SELECT * FROM mysql_source_cnt;
        {count*(i+1)}
        """
            ),
            quiet=True,
        )

    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    with c.override(
        Materialized(
            name="mz_new",
            sanity_restart=False,
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            restart="on-failure",
            external_metadata_store=True,
            default_replication_factor=2,
        ),
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        ),
    ):
        c.up("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        elapsed = time.time() - start_time
        print(f"re-hydration took {elapsed} seconds")
        c.promote_mz("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )
        elapsed = time.time() - start_time
        print(f"promotion took {elapsed} seconds")
        start_time = time.time()
        result = c.sql_query("SELECT * FROM mysql_source_cnt", service="mz_new")
        elapsed = time.time() - start_time
        print(f"final check took {elapsed} seconds")
        assert result[0][0] == count * repeats, f"Wrong result: {result}"
        assert (
            elapsed < 4
        ), f"Took {elapsed}s to SELECT on MySQL source after 0dt upgrade, is it hydrated?"

        result = c.sql_query(
            "SELECT count(*) FROM mysql_source_table", service="mz_new"
        )
        assert result[0][0] == count * repeats, f"Wrong result: {result}"

        print("Ingesting again")
        for i in range(repeats, repeats * 2):
            c.testdrive(
                dedent(
                    f"""
            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            USE public;
            {inserts}
            > SELECT * FROM mysql_source_cnt;
            {count*(i+1)}
            """
                ),
                quiet=True,
            )

        result = c.sql_query("SELECT * FROM mysql_source_cnt", service="mz_new")
        assert result[0][0] == 2 * count * repeats, f"Wrong result: {result}"

        result = c.sql_query(
            "SELECT count(*) FROM mysql_source_table", service="mz_new"
        )
        assert result[0][0] == 2 * count * repeats, f"Wrong result: {result}"


def workflow_sql_server_source_rehydration(c: Composition) -> None:
    """Verify SQL Server source rehydration in 0dt deployment"""
    c.down(destroy_volumes=True)
    c.up("sql-server", "mz_old", Service("testdrive", idle=True))
    setup(c)
    setup_sql_server_testing(c)

    # The number of row value expressions in the INSERT statement exceeds the maximum allowed number of 1000 row values.
    count = 1000
    repeats = 100

    inserts = (
        "INSERT INTO sql_server_source_table VALUES "
        + ", ".join([f"({i})" for i in range(count)])
        + ";"
    )

    start_time = time.time()
    c.testdrive(
        dedent(
            f"""
        > SET CLUSTER = cluster;

        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

        $ sql-server-execute name=sql-server
        USE test;
        CREATE TABLE sql_server_source_table (f1 INTEGER);
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sql_server_source_table', @role_name = 'SA', @supports_net_changes = 0;
        {inserts}

        > CREATE SECRET sqlserverpass AS '{SqlServer.DEFAULT_SA_PASSWORD}';
        > CREATE CONNECTION sqlserver TO SQL SERVER (
          HOST 'sql-server',
          DATABASE test,
          USER {SqlServer.DEFAULT_USER},
          PASSWORD SECRET sqlserverpass);
        > CREATE SOURCE sql_server_source
          IN CLUSTER cluster
          FROM SQL SERVER CONNECTION sqlserver;
        > CREATE TABLE sql_server_source_table FROM SOURCE sql_server_source (REFERENCE sql_server_source_table);
        > CREATE VIEW sql_server_source_cnt AS SELECT count(*) FROM sql_server_source_table
        > CREATE DEFAULT INDEX ON sql_server_source_cnt
        > SELECT * FROM sql_server_source_cnt;
        {count}
        """
        ),
        quiet=True,
    )

    for i in range(1, repeats):
        c.testdrive(
            dedent(
                f"""
        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

        $ sql-server-execute name=sql-server
        USE test;
        {inserts}
        > SELECT * FROM sql_server_source_cnt;
        {count*(i+1)}
        """
            ),
            quiet=True,
        )

    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    with c.override(
        Materialized(
            name="mz_new",
            sanity_restart=False,
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            restart="on-failure",
            external_metadata_store=True,
            default_replication_factor=2,
        ),
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        ),
    ):
        c.up("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        elapsed = time.time() - start_time
        print(f"re-hydration took {elapsed} seconds")
        c.promote_mz("mz_new")
        start_time = time.time()
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )
        elapsed = time.time() - start_time
        print(f"promotion took {elapsed} seconds")
        start_time = time.time()
        result = c.sql_query("SELECT * FROM sql_serveR_source_cnt", service="mz_new")
        elapsed = time.time() - start_time
        print(f"final check took {elapsed} seconds")
        assert result[0][0] == count * repeats, f"Wrong result: {result}"
        assert (
            elapsed < 4
        ), f"Took {elapsed}s to SELECT on SQL Server source after 0dt upgrade, is it hydrated?"

        result = c.sql_query(
            "SELECT count(*) FROM sql_server_source_table", service="mz_new"
        )
        assert result[0][0] == count * repeats, f"Wrong result: {result}"

        print("Ingesting again")
        for i in range(repeats, repeats * 2):
            c.testdrive(
                dedent(
                    f"""
            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            $ sql-server-execute name=sql-server
            USE test;
            {inserts}
            > SELECT * FROM sql_server_source_cnt;
            {count*(i+1)}
            """
                ),
                quiet=True,
            )

        result = c.sql_query("SELECT * FROM sql_serveR_source_cnt", service="mz_new")
        assert result[0][0] == 2 * count * repeats, f"Wrong result: {result}"

        result = c.sql_query(
            "SELECT count(*) FROM sql_server_source_table", service="mz_new"
        )
        assert result[0][0] == 2 * count * repeats, f"Wrong result: {result}"


def workflow_kafka_source_failpoint(c: Composition) -> None:
    """Verify that source status updates of the newly deployed environment take
    precedent over older source status updates when promoted.

    The original Materialized instance (mz_old) is started with a failpoint
    that simulates a failure during state multi-put. After creating a Kafka
    source, we promote a new deployment (mz_new) and verify that the source
    status in mz_source_statuses is marked as 'running', indicating that the
    source has rehydrated correctly despite the injected failure."""
    c.down(destroy_volumes=True)
    # Start the required services.
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        Service("testdrive", idle=True),
    )

    # Start the original Materialized instance with the failpoint enabled.
    with c.override(
        Materialized(
            name="mz_old",
            sanity_restart=False,
            deploy_generation=0,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            external_metadata_store=True,
            environment_extra=["FAILPOINTS=fail_state_multi_put=return"],
            default_replication_factor=2,
        )
    ):
        c.up("mz_old")
        setup(c)

        c.testdrive(
            dedent(
                """
                > SET CLUSTER = cluster;
                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL = 'PLAINTEXT';

                $ kafka-create-topic topic=kafka-fp
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-fp
                keyA,keyA:valA,valA

                > CREATE SOURCE kafka_source_fp
                  IN CLUSTER cluster
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-fp-${testdrive.seed}');

                > CREATE TABLE kafka_source_tbl (key1, key2, value1, value2)
                  FROM SOURCE kafka_source_fp (REFERENCE "testdrive-kafka-fp-${testdrive.seed}")
                  KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
                  VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
                  ENVELOPE UPSERT;

                > SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'kafka_source_fp';
                stalled
                """
            )
        )

    with c.override(
        Materialized(
            name="mz_new",
            sanity_restart=False,
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            restart="on-failure",
            external_metadata_store=True,
            default_replication_factor=2,
        ),
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        ),
    ):
        c.up("mz_new")
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        c.promote_mz("mz_new")

        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )

        # Verify that the Kafka source's status is marked as "running" in mz_source_statuses.
        c.testdrive(
            dedent(
                """
                > SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'kafka_source_fp';
                running
                """
            )
        )


def fetch_reconciliation_metrics(c: Composition, process: str) -> tuple[int, int]:
    # TODO: Replace me with mz_internal.mz_cluster_replica_ports when it exists
    internal_http = c.exec(
        process,
        "bash",
        "-c",
        'ps aux | grep -v grep | grep "cluster_id=s2" | sed -e "s#.* --internal-http-listen-addr=\\([^ ]*\\) .*#\\1#"',
        capture=True,
    ).stdout.strip()
    metrics = c.exec(
        process,
        "curl",
        "--silent",
        "--unix-socket",
        internal_http,
        "localhost/metrics",
        capture=True,
    ).stdout

    reused = 0
    replaced = 0
    for metric in metrics.splitlines():
        if metric.startswith("mz_compute_reconciliation_reused_dataflows_count_total"):
            reused += int(metric.split()[1])
        elif metric.startswith(
            "mz_compute_reconciliation_replaced_dataflows_count_total"
        ):
            replaced += int(metric.split()[1])

    return reused, replaced


def workflow_builtin_item_migrations(c: Composition) -> None:
    """Verify builtin item migrations"""
    c.down(destroy_volumes=True)
    c.up("mz_old")
    c.sql(
        "CREATE MATERIALIZED VIEW mv AS SELECT name FROM mz_tables;",
        service="mz_old",
        port=6877,
        user="mz_system",
    )

    mz_tables_gid = c.sql_query(
        "SELECT id FROM mz_tables WHERE name = 'mz_tables'",
        service="mz_old",
    )[0][0]
    mv_gid = c.sql_query(
        "SELECT id FROM mz_materialized_views WHERE name = 'mv'",
        service="mz_old",
    )[0][0]
    mz_tables_shard_id = c.sql_query(
        f"SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id = '{mz_tables_gid}'",
        service="mz_old",
    )[0][0]
    mv_shard_id = c.sql_query(
        f"SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id = '{mv_gid}'",
        service="mz_old",
    )[0][0]

    with c.override(
        Materialized(
            name="mz_new",
            sanity_restart=False,
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            restart="on-failure",
            external_metadata_store=True,
            force_migrations="all",
            healthcheck=LEADER_STATUS_HEALTHCHECK,
            default_replication_factor=2,
        ),
    ):
        c.up("mz_new")

        new_mz_tables_gid = c.sql_query(
            "SELECT id FROM mz_tables WHERE name = 'mz_tables'",
            service="mz_new",
        )[0][0]
        new_mv_gid = c.sql_query(
            "SELECT id FROM mz_materialized_views WHERE name = 'mv'",
            service="mz_new",
        )[0][0]
        assert new_mz_tables_gid == mz_tables_gid
        assert new_mv_gid == mv_gid
        # mz_internal.mz_storage_shards won't update until this instance becomes the leader

        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        c.promote_mz("mz_new")
        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")

        new_mz_tables_gid = c.sql_query(
            "SELECT id FROM mz_tables WHERE name = 'mz_tables'",
            service="mz_new",
            reuse_connection=False,
        )[0][0]
        new_mv_gid = c.sql_query(
            "SELECT id FROM mz_materialized_views WHERE name = 'mv'",
            service="mz_new",
            reuse_connection=False,
        )[0][0]
        assert new_mz_tables_gid == mz_tables_gid
        assert new_mv_gid == mv_gid
        new_mz_tables_shard_id = c.sql_query(
            f"SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id = '{mz_tables_gid}'",
            service="mz_new",
            reuse_connection=False,
        )[0][0]
        new_mv_shard_id = c.sql_query(
            f"SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id = '{mv_gid}'",
            service="mz_new",
            reuse_connection=False,
        )[0][0]
        assert new_mz_tables_shard_id != mz_tables_shard_id
        assert new_mv_shard_id == mv_shard_id

        reused, replaced = fetch_reconciliation_metrics(c, "mz_new")
        assert reused > 0
        assert (
            replaced == 0
        ), f"{replaced} dataflows have been replaced, expected all to be reused"


def workflow_materialized_view_correction_pruning(c: Composition) -> None:
    """
    Verify that the MV sink consolidates away the snapshot updates in read-only
    mode.
    """

    c.down(destroy_volumes=True)
    c.up("mz_old")

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        service="mz_old",
        port=6877,
        user="mz_system",
    )
    c.sql(
        """
         CREATE TABLE t (a int);
         INSERT INTO t SELECT generate_series(1, 1000);
         CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;
         SELECT * FROM mv LIMIT 1;
         """,
        service="mz_old",
    )

    c.up("mz_new")
    c.sql("SELECT * FROM mv LIMIT 1", service="mz_new")

    def get_clusterd_internal_http_address():
        logs = c.invoke("logs", "mz_new", capture=True).stdout
        for line in logs.splitlines():
            # quickstart must be u1 since it's the only non-system cluster
            if (
                "cluster-u1-replica-u1-gen-1" in line
                and "mz_clusterd: serving internal HTTP server on" in line
            ):
                return line.split(" ")[-1]

        raise RuntimeError("No HTTP endpoint for quickstart clusterd found in logs")

    def get_correction_metrics():
        address = get_clusterd_internal_http_address()
        resp = c.exec(
            "mz_new",
            "curl",
            "--unix-socket",
            address,
            "http:/prof/metrics",
            capture=True,
        ).stdout

        metrics = {}
        for line in resp.splitlines():
            key, value = line.split(maxsplit=1)
            metrics[key] = value

        insertions = int(metrics["mz_persist_sink_correction_insertions_total"])
        deletions = int(metrics["mz_persist_sink_correction_deletions_total"])
        return (insertions, deletions)

    insertions = None
    deletions = None

    # The correction buffer should stabilize in a state where it has seen 2000
    # insertions (positive + negative updates), and as many deletions. The
    # absolute amount of records in the correction buffer should be zero.
    for _ in range(10):
        time.sleep(1)
        insertions, deletions = get_correction_metrics()
        if insertions > 1000 and insertions - deletions == 0:
            break
    else:
        raise AssertionError(
            f"unexpected correction metrics: {insertions=}, {deletions=}"
        )


def setup(c: Composition) -> None:
    # Make sure cluster is owned by the system so it doesn't get dropped
    # between testdrive runs.
    c.sql(
        """
        DROP CLUSTER IF EXISTS cluster CASCADE;
        CREATE CLUSTER cluster SIZE 'scale=2,workers=4';
        GRANT ALL ON CLUSTER cluster TO materialize;
        ALTER SYSTEM SET cluster = cluster;
        CREATE CLUSTER cluster_singlereplica SIZE 'scale=1,workers=4', REPLICATION FACTOR 1;
        GRANT ALL ON CLUSTER cluster_singlereplica TO materialize;
        ALTER SYSTEM SET max_sources = 100;
        ALTER SYSTEM SET max_materialized_views = 100;
    """,
        service="mz_old",
        port=6877,
        user="mz_system",
    )


def workflow_upsert_sources(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "mz_old",
        Service("testdrive", idle=True),
    )
    num_threads = 50

    setup(c)

    c.testdrive(
        dedent(
            """
        > SET CLUSTER = cluster;

        > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL = 'PLAINTEXT';
        > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';
            """
        )
    )

    end_time = datetime.now() + timedelta(seconds=200)
    mz1 = "mz_old"
    mz2 = "mz_new"

    def worker(i: int) -> None:
        c.testdrive(
            dedent(
                f"""
            $ kafka-create-topic topic=kafka{i}
            > CREATE SOURCE kafka_source{i}
              IN CLUSTER cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka{i}-${{testdrive.seed}}');
            > CREATE TABLE kafka_source_tbl{i} (key1, key2, value1, value2)
              FROM SOURCE kafka_source{i} (REFERENCE "testdrive-kafka{i}-${{testdrive.seed}}")
              KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
              VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
              ENVELOPE UPSERT;

            > CREATE DEFAULT INDEX ON kafka_source_tbl{i}
            > CREATE MATERIALIZED VIEW mv{i} AS SELECT * FROM kafka_source_tbl{i}
                """
            )
        )

        while datetime.now() < end_time:
            try:
                c.testdrive(
                    dedent(
                        f"""
                    $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka{i} repeat=10000
                    key1A,key1B:value1A,value1B
                    """
                    )
                )
            except:
                pass

    threads = []
    for i in range(num_threads):
        thread = Thread(name=f"worker_{i}", target=worker, args=(i,))
        threads.append(thread)

    for thread in threads:
        thread.start()

    i = 1
    while datetime.now() < end_time:
        with c.override(
            Materialized(
                name=mz2,
                sanity_restart=False,
                deploy_generation=i,
                system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
                restart="on-failure",
                external_metadata_store=True,
                default_replication_factor=2,
            ),
            Testdrive(
                materialize_url=f"postgres://materialize@{mz1}:6875",
                materialize_url_internal=f"postgres://materialize@{mz1}:6877",
                mz_service=mz1,
                materialize_params={"cluster": "cluster"},
                no_consistency_checks=True,
                no_reset=True,
                seed=1,
                default_timeout=DEFAULT_TIMEOUT,
            ),
        ):
            c.up(mz2)
            c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, mz2)
            c.promote_mz(mz2)
            c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, mz2)

        i += 1
        mz1, mz2 = mz2, mz1

    for thread in threads:
        thread.join()


def workflow_ddl(c: Composition) -> None:
    """Verify basic 0dt deployment flow with DDLs running during the 0dt deployment."""
    c.down(destroy_volumes=True)
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "postgres",
        "mysql",
        "sql-server",
        "mz_old",
        Service("testdrive", idle=True),
    )

    setup(c)
    setup_sql_server_testing(c)

    # Inserts should be reflected when writes are allowed.
    c.testdrive(
        dedent(
            f"""
        > SET CLUSTER = cluster;
        > CREATE TABLE t (a int, b int);

        > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL = 'PLAINTEXT';
        > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';
        > CREATE SINK kafka_sink
          IN CLUSTER cluster
          FROM t
          INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${{testdrive.seed}}')
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE DEBEZIUM;

        > INSERT INTO t VALUES (1, 2);
        > CREATE INDEX t_idx ON t (a, b);
        > CREATE MATERIALIZED VIEW mv AS SELECT sum(a) FROM t;
        > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
        > SELECT * FROM mv;
        1
        > SELECT max(b) FROM t;
        2

        $ kafka-create-topic topic=kafka
        $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
        key1A,key1B:value1A,value1B
        > CREATE SOURCE kafka_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-${{testdrive.seed}}');

        > CREATE TABLE kafka_source_tbl (key1, key2, value1, value2)
          FROM SOURCE kafka_source (REFERENCE "testdrive-kafka-${{testdrive.seed}}")
          KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          ENVELOPE UPSERT;
        > SELECT * FROM kafka_source_tbl
        key1A key1B value1A value1B

        $ postgres-execute connection=postgres://postgres:postgres@postgres
        CREATE USER postgres1 WITH SUPERUSER PASSWORD 'postgres';
        ALTER USER postgres1 WITH replication;
        DROP PUBLICATION IF EXISTS postgres_source;
        DROP TABLE IF EXISTS postgres_source_table;
        CREATE TABLE postgres_source_table (f1 TEXT, f2 INTEGER);
        ALTER TABLE postgres_source_table REPLICA IDENTITY FULL;
        INSERT INTO postgres_source_table SELECT 'A', 0;
        CREATE PUBLICATION postgres_source FOR ALL TABLES;

        > CREATE SECRET pgpass AS 'postgres';
        > CREATE CONNECTION pg FOR POSTGRES
          HOST 'postgres',
          DATABASE postgres,
          USER postgres1,
          PASSWORD SECRET pgpass;
        > CREATE SOURCE postgres_source
          IN CLUSTER cluster
          FROM POSTGRES CONNECTION pg
          (PUBLICATION 'postgres_source');
        > CREATE TABLE postgres_source_table FROM SOURCE postgres_source (REFERENCE postgres_source_table)
        > SELECT * FROM postgres_source_table;
        A 0

        $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
        $ mysql-execute name=mysql
        # create the database if it does not exist yet but do not drop it
        CREATE DATABASE IF NOT EXISTS public;
        USE public;
        CREATE USER mysql1 IDENTIFIED BY 'mysql';
        GRANT REPLICATION SLAVE ON *.* TO mysql1;
        GRANT ALL ON public.* TO mysql1;
        CREATE TABLE mysql_source_table (f1 VARCHAR(32), f2 INTEGER);
        INSERT INTO mysql_source_table VALUES ('A', 0);

        > CREATE SECRET mysqlpass AS 'mysql';
        > CREATE CONNECTION mysql TO MYSQL (
          HOST 'mysql',
          USER mysql1,
          PASSWORD SECRET mysqlpass);
        > CREATE SOURCE mysql_source1
          IN CLUSTER cluster
          FROM MYSQL CONNECTION mysql;
        > CREATE TABLE mysql_source_table FROM SOURCE mysql_source1 (REFERENCE public.mysql_source_table);
        > SELECT * FROM mysql_source_table;
        A 0

        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

        $ sql-server-execute name=sql-server
        USE test;
        CREATE TABLE sql_server_source_table (f1 VARCHAR(32), f2 INTEGER);
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sql_server_source_table', @role_name = 'SA', @supports_net_changes = 0;
        INSERT INTO sql_server_source_table VALUES ('A', 0);

        > CREATE SECRET sqlserverpass AS '{SqlServer.DEFAULT_SA_PASSWORD}';
        > CREATE CONNECTION sqlserver TO SQL SERVER (
          HOST 'sql-server',
          DATABASE test,
          USER {SqlServer.DEFAULT_USER},
          PASSWORD SECRET sqlserverpass);
        > CREATE SOURCE sql_server_source
          IN CLUSTER cluster
          FROM SQL SERVER CONNECTION sqlserver;
        > CREATE TABLE sql_server_source_table FROM SOURCE sql_server_source (REFERENCE sql_server_source_table);
        > SELECT * FROM sql_server_source_table;
        A 0

        $ kafka-verify-topic sink=materialize.public.kafka_sink

        > CREATE SOURCE kafka_sink_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${{testdrive.seed}}')

        > CREATE TABLE kafka_sink_source_tbl FROM SOURCE kafka_sink_source (REFERENCE "testdrive-kafka-sink-${{testdrive.seed}}")
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE NONE

        > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
        <null> <null> 1 2

        > CREATE SOURCE webhook_source
          IN CLUSTER cluster_singlereplica
          FROM WEBHOOK BODY FORMAT TEXT

        $ webhook-append database=materialize schema=public name=webhook_source
        AAA
        > SELECT * FROM webhook_source
        AAA

        $ set-max-tries max-tries=1
        $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
        > BEGIN
        > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
        > FETCH ALL c WITH (timeout='5s');
        <TIMESTAMP> 1 1
        > COMMIT
        """
        )
    )

    # Start new Materialize in a new deploy generation, which will cause
    # Materialize to boot in read-only mode.
    c.up("mz_new")

    # Verify against new Materialize that it is in read-only mode
    with c.override(
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        )
    ):
        c.up(Service("testdrive", idle=True))
        c.testdrive(
            dedent(
                f"""
            $ webhook-append database=materialize schema=public name=webhook_source status=500
            BBB

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
            key2A,key2B:value2A,value2B

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO postgres_source_table VALUES ('B', 1);

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            USE public;
            INSERT INTO mysql_source_table VALUES ('B', 1);

            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            $ sql-server-execute name=sql-server
            USE test;
            INSERT INTO sql_server_source_table VALUES ('B', 1);

            > SET CLUSTER = cluster;
            > SELECT 1
            1
            ! INSERT INTO t VALUES (3, 4);
            contains: cannot write in read-only mode
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM mv;
            1
            # TODO: Currently hangs
            # > SELECT max(b) FROM t;
            # 2
            > SELECT mz_unsafe.mz_sleep(5)
            <null>
            ! INSERT INTO t VALUES (5, 6);
            contains: cannot write in read-only mode
            > SELECT * FROM mv;
            1
            ! DROP INDEX t_idx
            contains: cannot write in read-only mode
            ! CREATE INDEX t_idx2 ON t (a, b)
            contains: cannot write in read-only mode
            ! CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;
            contains: cannot write in read-only mode

            $ set-regex match=(s\\d+|\\d{{13}}|[ ]{{12}}0|u\\d{{1,3}}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

            > EXPLAIN TIMESTAMP FOR SELECT * FROM mv;
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (StorageInput([User(6)])): [<> <>]\\n"

            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
            <null> <null> 1 2

            > SELECT * FROM webhook_source
            AAA

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
            > BEGIN
            ! DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            contains: cannot write in read-only mode
            > ROLLBACK
            # Actual subscribes without a declare still work though
            > SUBSCRIBE (WITH a(x) AS (SELECT 'a') SELECT generate_series(1, 2), x FROM a)
            <TIMESTAMP> 1 1 a
            <TIMESTAMP> 1 2 a
            """
            )
        )

    # Run DDLs against the old Materialize, which should restart the new one
    c.up(Service("testdrive", idle=True))
    c.testdrive(
        dedent(
            f"""
        > CREATE TABLE t1 (a INT);

        $ webhook-append database=materialize schema=public name=webhook_source
        CCC

        $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
        key3A,key3B:value3A,value3B

        $ postgres-execute connection=postgres://postgres:postgres@postgres
        INSERT INTO postgres_source_table VALUES ('C', 2);

        $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
        $ mysql-execute name=mysql
        USE public;
        INSERT INTO mysql_source_table VALUES ('C', 2);

        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

        $ sql-server-execute name=sql-server
        USE test;
        INSERT INTO sql_server_source_table VALUES ('C', 2);

        > CREATE TABLE t2 (a INT);

        > SET CLUSTER = cluster;
        > SELECT 1
        1
        > INSERT INTO t VALUES (3, 4);
        > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
        > SELECT * FROM mv;
        4
        > SELECT max(b) FROM t;
        4
        > SELECT mz_unsafe.mz_sleep(5)
        <null>
        > INSERT INTO t VALUES (5, 6);
        > SELECT * FROM mv;
        9
        > DROP INDEX t_idx
        > CREATE INDEX t_idx2 ON t (a, b)
        > CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;

        $ set-regex match=(s\\d+|\\d{{13}}|[ ]{{12}}0|u\\d{{1,3}}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

        > EXPLAIN TIMESTAMP FOR SELECT * FROM mv;
        "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (StorageInput([User(6)])): [<> <>]\\n"

        > SELECT * FROM kafka_source_tbl
        key1A key1B value1A value1B
        key2A key2B value2A value2B
        key3A key3B value3A value3B
        > SELECT * FROM postgres_source_table
        A 0
        B 1
        C 2
        > SELECT * FROM mysql_source_table;
        A 0
        B 1
        C 2
        > SELECT * FROM sql_server_source_table;
        A 0
        B 1
        C 2
        > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
        <null> <null> 1 2
        <null> <null> 3 4
        <null> <null> 5 6
        > SELECT * FROM webhook_source
        AAA
        CCC

        > CREATE TABLE t3 (a INT);

        $ set-max-tries max-tries=1
        $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
        > BEGIN
        > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
        > FETCH ALL c WITH (timeout='5s');
        <TIMESTAMP> 1 1
        <TIMESTAMP> 1 3
        <TIMESTAMP> 1 5
        > COMMIT

        > CREATE TABLE t4 (a INT);
        """
        )
    )

    with c.override(
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "cluster"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        )
    ):
        c.up(Service("testdrive", idle=True))
        c.testdrive(
            dedent(
                """
            $ webhook-append database=materialize schema=public name=webhook_source status=500
            DDD

            > SET CLUSTER = cluster;
            > SELECT 1
            1
            ! INSERT INTO t VALUES (3, 4);
            contains: cannot write in read-only mode
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM mv;
            9
            > SELECT max(b) FROM t;
            6
            > SELECT * FROM mv;
            9
            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            key3A key3B value3A value3B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            C 2
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            C 2
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            C 2
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
            <null> <null> 1 2
            <null> <null> 3 4
            <null> <null> 5 6
            > SELECT * FROM webhook_source
            AAA
            CCC

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{13,20} replacement=<TIMESTAMP>
            > BEGIN
            ! DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            contains: cannot write in read-only mode
            > ROLLBACK
            # Actual subscribes without a declare still work though
            > SUBSCRIBE (WITH a(x) AS (SELECT 'a') SELECT generate_series(1, 2), x FROM a)
            <TIMESTAMP> 1 1 a
            <TIMESTAMP> 1 2 a
            """
            )
        )

        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        c.promote_mz("mz_new")

        # Give some time for Mz to restart after promotion
        for i in range(10):
            try:
                c.sql("SELECT 1", service="mz_old")
            except OperationalError as e:
                assert (
                    "server closed the connection unexpectedly" in str(e)
                    or "Can't create a connection to host" in str(e)
                    or "Connection refused" in str(e)
                    or "the connection is closed" in str(e)
                ), f"Unexpected error: {e}"
            except CommandFailureCausedUIError as e:
                # service "mz_old" is not running
                assert "running docker compose failed" in str(
                    e
                ), f"Unexpected error: {e}"
                break
            time.sleep(1)
        else:
            raise RuntimeError("mz_old didn't stop running within 10 seconds")

        for i in range(10):
            try:
                c.sql("SELECT 1", service="mz_new")
                break
            except CommandFailureCausedUIError:
                pass
            except OperationalError:
                pass
            time.sleep(1)
        else:
            raise RuntimeError("mz_new didn't come up within 10 seconds")

        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")

        c.testdrive(
            dedent(
                f"""
            $ webhook-append database=materialize schema=public name=webhook_source
            EEE
            > SET CLUSTER = cluster;
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > CREATE MATERIALIZED VIEW mv3 AS SELECT sum(a) FROM t;
            > SELECT * FROM mv;
            9
            > SELECT * FROM mv2;
            9
            > SELECT * FROM mv3;
            9
            > SELECT max(b) FROM t;
            6
            > INSERT INTO t VALUES (7, 8);
            > SELECT * FROM mv;
            16
            > SELECT * FROM mv2;
            16
            > SELECT max(b) FROM t;
            8
            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            key3A key3B value3A value3B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            C 2
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            C 2
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            C 2

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
            key4A,key4B:value4A,value4B

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO postgres_source_table VALUES ('D', 3);

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            USE public;
            INSERT INTO mysql_source_table VALUES ('D', 3);

            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            $ sql-server-execute name=sql-server
            USE test;
            INSERT INTO sql_server_source_table VALUES ('D', 3);

            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            key3A key3B value3A value3B
            key4A key4B value4A value4B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            C 2
            D 3
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            C 2
            D 3
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            C 2
            D 3
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
            <null> <null> 1 2
            <null> <null> 3 4
            <null> <null> 5 6
            <null> <null> 7 8
            > SELECT * FROM webhook_source
            AAA
            CCC
            EEE

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
            > BEGIN
            > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            > FETCH ALL c WITH (timeout='5s');
            <TIMESTAMP> 1 1
            <TIMESTAMP> 1 3
            <TIMESTAMP> 1 5
            <TIMESTAMP> 1 7
            > COMMIT
            """
            )
        )
