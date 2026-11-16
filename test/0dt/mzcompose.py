# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Explicit deterministic tests for read-only mode and zero downtime deploys.
Deployments use the same version unless a workflow selects a compatible image.
"""

import json
import re
import subprocess
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from queue import SimpleQueue
from tempfile import TemporaryDirectory
from textwrap import dedent
from threading import Event, Thread
from typing import Any
from unittest import SkipTest

import psycopg
import pymysql
from psycopg.errors import OperationalError
from psycopg.sql import SQL, Identifier, Literal

from materialize import buildkite
from materialize.mzcompose import (
    DEFAULT_MZ_ENVIRONMENT_ID,
    get_default_system_parameters,
    sanitizer_enabled,
)
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import (
    LEADER_STATUS_HEALTHCHECK,
    DeploymentStatus,
    Materialized,
)
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.pubsub import Pubsub
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import (
    SqlServer,
    setup_sql_server_testing,
)
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import CommandFailureCausedUIError, env_is_truthy

DEFAULT_TIMEOUT = "300s"

# Sanitized builds run the whole read path several times slower, so the bounds
# on "this SELECT is fast, hence the source is hydrated" have to move with them.
# NOTE: that weakens those assertions under a sanitizer, where a read that is
# slow because the source is only partly hydrated can still come in under the
# scaled bound. The unscaled bound guards the non-sanitizer runs.
HYDRATED_SELECT_FACTOR = 10 if sanitizer_enabled() else 1

SYSTEM_PARAMETER_DEFAULTS = get_default_system_parameters()


def native_mz(
    *,
    options: list[str] | None = None,
    depends_on: list[str] | None = None,
    **kwargs: Any,
) -> Materialized:
    # Every override replaces the whole service definition. Keep all generations
    # on the same relay, including compatible-version image overrides.
    return Materialized(
        options=[
            "--persist-pubsub-external",
            "--persist-pubsub-url=http://pubsub:6879",
            *(options or []),
        ],
        depends_on=["pubsub", *(depends_on or [])],
        **kwargs,
    )


SERVICES = [
    Pubsub(),
    MySql(),
    Postgres(),
    SqlServer(),
    Kafka(),
    SchemaRegistry(),
    Mz(app_password=""),
    native_mz(
        name="mz_old",
        sanity_restart=False,
        deploy_generation=0,
        system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
        external_metadata_store=True,
        default_replication_factor=2,
    ),
    native_mz(
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


def _wait_for_mz_count(
    c: Composition,
    query: str,
    expected: int,
    service: str,
    timeout: int = 300,
) -> None:
    """Poll MZ until *query* returns *expected* count."""
    deadline = time.time() + timeout
    while True:
        result = c.sql_query(query, service=service)
        if result[0][0] == expected:
            return
        if time.time() > deadline:
            raise RuntimeError(
                f"Timed out waiting for {query} to return {expected}, got {result[0][0]}"
            )
        time.sleep(1)


def _wait_for_cdc_reader(
    observe: Callable[[], Sequence[str | None]],
    *,
    expected_host: str | None = None,
    excluded_host: str | None = None,
) -> str:
    """Wait for one stable upstream reader host, allowing transient handover overlap."""
    deadline = time.monotonic() + 300
    stable_since = None
    stable_host = None
    while True:
        hosts = observe()
        now = time.monotonic()
        if (
            len(hosts) == 1
            and hosts[0] is not None
            and hosts[0] != excluded_host
            and (expected_host is None or hosts[0] == expected_host)
        ):
            if hosts[0] != stable_host:
                stable_host = hosts[0]
                stable_since = now
            assert stable_since is not None
            if now - stable_since >= 10:
                print(
                    f"Stable CDC reader: {stable_host} (expected={expected_host}, excluded={excluded_host})"
                )
                return hosts[0]
        else:
            stable_host = None
            stable_since = None
        assert now < deadline, (expected_host, excluded_host, hosts)
        time.sleep(0.5)


def _pg_direct_ingest(c: Composition, table: str, count: int, repeats: int) -> None:
    """Bulk-load *count* × *repeats* rows into a Postgres table via COPY."""
    conn = psycopg.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        port=c.default_port("postgres"),
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        for _ in range(repeats):
            with cur.copy(
                SQL("COPY {} (f1) FROM STDIN").format(Identifier(table))
            ) as copy:
                for i in range(count):
                    copy.write_row([i])
    conn.close()


def _mysql_direct_ingest(c: Composition, table: str, count: int, repeats: int) -> None:
    """Bulk-load *count* × *repeats* rows into a MySQL table via batched INSERT."""
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password=MySql.DEFAULT_ROOT_PASSWORD,
        database="public",
        port=c.default_port("mysql"),
    )
    batch_size = 10_000
    with conn.cursor() as cur:
        for _ in range(repeats):
            for start in range(0, count, batch_size):
                n = min(batch_size, count - start)
                values = ", ".join(f"({i})" for i in range(start, start + n))
                cur.execute(f"INSERT INTO {table} VALUES {values}")
            conn.commit()
    conn.close()


def _sql_server_direct_ingest(c: Composition, insert_stmt: str, repeats: int) -> None:
    """Execute *repeats* copies of *insert_stmt* in SQL Server via a single testdrive call."""
    all_inserts = "\n".join([insert_stmt] * repeats)
    c.testdrive(
        f"""\
$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

$ sql-server-execute name=sql-server
USE test;
{all_inserts}
""",
        quiet=True,
    )


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


def workflow_candidate_without_active_processes(c: Composition) -> None:
    """Keep candidate writes available while the active deployment is offline."""
    c.down(destroy_volumes=True)
    c.up(
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
    c.testdrive(dedent(f"""
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
        """))

    sink_topic = c.sql_query(
        "SELECT k.topic FROM mz_catalog.mz_kafka_sinks k "
        "JOIN mz_catalog.mz_sinks s ON s.id = k.id WHERE s.name = 'kafka_sink'",
        service="mz_old",
        reuse_connection=False,
    )[0][0]

    def sink_end_offsets() -> dict[int, int]:
        output = c.exec(
            "kafka",
            "env",
            "KAFKA_HEAP_OPTS=-Xms16m -Xmx64m",
            "kafka-get-offsets",
            "--bootstrap-server=kafka:9092",
            f"--topic={sink_topic}",
            "--time=-1",
            capture=True,
        ).stdout
        offsets = {}
        for line in output.splitlines():
            topic, partition, offset = line.rsplit(":", 2)
            assert topic == sink_topic, line
            assert int(partition) not in offsets, output
            offsets[int(partition)] = int(offset)
        assert offsets, output
        return offsets

    # The initial row is already committed and read back above. No further
    # writes to this sink's data topic are authorized until promotion.
    sink_before = sink_end_offsets()

    # Replace the only running deployment with a candidate. The catalog still
    # assigns singleton CDC readers and sink output to generation zero. The
    # candidate can write tables, webhooks and DDL before taking that authority.
    # Maintained Persist output is still gated, including MVs and Kafka sources.
    with c.override(
        native_mz(
            name="mz_old",
            deploy_generation=1,
            external_metadata_store=True,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            default_replication_factor=2,
        )
    ):
        c.up("mz_old")
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_old")

        c.testdrive(dedent(f"""
            $ webhook-append database=materialize schema=public name=webhook_source
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
            > INSERT INTO t VALUES (3, 4);
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            # Admission permits table writes, not maintained Persist output.
            # The index serves the new rows while the MV output remains frozen.
            > SELECT * FROM mv;
            1
            > SELECT max(b) FROM t;
            4
            > SELECT mz_unsafe.mz_sleep(5)
            <null>
            > INSERT INTO t VALUES (5, 6);
            > SELECT * FROM mv;
            1
            > DROP INDEX t_idx
            > CREATE INDEX t_idx2 ON t (a, b)
            > CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;
            # Its output is checked after promotion, not before it can write.
            > SELECT max(b) FROM t;
            6

            $ set-regex match=(s\\d+|\\d{{13}}|[ ]{{12}}0|u\\d{{1,3}}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

            > EXPLAIN TIMESTAMP FOR SELECT * FROM mv;
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (Storage inputs: [<>]): [<> <>]\\n"

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
            BBB

            """))

        # Rows alone cannot distinguish an unauthorized reader from one that
        # has not caught up. Observe the upstream reader sessions as well.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert c.sql_query(
                "SELECT count(*) FROM pg_stat_replication WHERE usename = 'postgres1'",
                service="postgres",
                user="postgres",
                password="postgres",
                database="postgres",
            ) == [(0,)]
            with pymysql.connect(
                host="localhost",
                user="root",
                password=MySql.DEFAULT_ROOT_PASSWORD,
                port=c.default_port("mysql"),
                autocommit=True,
                connect_timeout=5,
                read_timeout=5,
                write_timeout=5,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) FROM information_schema.PROCESSLIST "
                        "WHERE USER = 'mysql1' AND COMMAND LIKE 'Binlog Dump%'"
                    )
                    assert cursor.fetchone() == (0,)
            sessions = c.exec(
                "sql-server",
                "bash",
                "-c",
                "SQLCMD=/opt/mssql-tools18/bin/sqlcmd; "
                '[ -x "$SQLCMD" ] || SQLCMD=/opt/mssql-tools/bin/sqlcmd; '
                'exec "$SQLCMD" -b -C -S localhost -U sa -P "$1" -h -1 -W -l 5 -t 5 -Q "$2"',
                "sqlcmd",
                SqlServer.DEFAULT_SA_PASSWORD,
                "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.dm_exec_sessions s "
                "JOIN sys.dm_exec_connections c ON c.session_id = s.session_id "
                "LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id "
                "CROSS APPLY sys.dm_exec_sql_text(COALESCE(r.sql_handle, c.most_recent_sql_handle)) q "
                "WHERE s.program_name = 'materialize' AND c.client_net_address IS NOT NULL "
                "AND (r.session_id IS NOT NULL OR s.last_request_start_time >= DATEADD(second, -5, SYSDATETIME())) "
                "AND (q.text LIKE '%fn_cdc_get_max_lsn%' OR q.text LIKE '%fn_cdc_get_all_changes_%')",
                capture=True,
                silent=True,
            ).stdout.strip()
            assert sessions == "0", sessions
            time.sleep(1)

        # Broker end offsets, rather than a possibly lagging MZ source, prove
        # the candidate did not append either buffered table write during the
        # observation window. Transaction markers also count as appends.
        sink_after = sink_end_offsets()
        assert sink_after == sink_before, (sink_before, sink_after)

        c.promote_mz("mz_old", retire_mz_service=None)
        # Promotion acknowledgement precedes its durable commit. Do not recreate
        # the container below until the generation is actually active.
        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_old")

    # Restart the promoted generation and verify buffered output and CDC catch up.
    with c.override(
        native_mz(
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

        c.testdrive(dedent(f"""
            $ webhook-append database=materialize schema=public name=webhook_source
            CCC
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
            > SELECT * FROM mv3;
            16
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
            <null> <null> 3 4
            <null> <null> 5 6
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
            BBB
            CCC
            """))

        c.retire_mz_deployment("mz_old", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)


def workflow_basic(c: Composition) -> None:
    """Verify native writer coexistence, ingestion and explicit retirement."""
    c.down(destroy_volumes=True)
    c.up(
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
    c.testdrive(dedent(f"""
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
        """))

    # Both adapters can commit while the active deployment maintains shared
    # output. Wait for deployment-local replicas before querying the candidate.
    c.up("mz_new")
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")

    # Exercise candidate writes, DDL and cursor subscriptions before promotion.
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
        c.testdrive(dedent(f"""
            $ webhook-append database=materialize schema=public name=webhook_source
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
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (Storage inputs: [<>]): [<> <>]\\n"

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
            <null> <null> 3 4
            <null> <null> 5 6

            > SELECT * FROM webhook_source
            AAA
            BBB

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
            > BEGIN
            > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            > FETCH ALL c WITH (timeout='5s');
            <TIMESTAMP> 1 1
            <TIMESTAMP> 1 3
            <TIMESTAMP> 1 5
            > COMMIT
            # Also exercise a direct finite subscription.
            > SUBSCRIBE (WITH a(x) AS (SELECT 'a') SELECT generate_series(1, 2), x FROM a)
            <TIMESTAMP> 1 1 a
            <TIMESTAMP> 1 2 a
            """))

    # The old deployment follows candidate DDL and commits its own changes.
    c.up(Service("testdrive", idle=True))
    c.testdrive(dedent(f"""
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
        > INSERT INTO t VALUES (7, 8);
        > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
        > SELECT * FROM mv;
        16
        > SELECT max(b) FROM t;
        8
        > SELECT mz_unsafe.mz_sleep(5)
        <null>
        > INSERT INTO t VALUES (9, 10);
        > SELECT * FROM mv;
        25
        > DROP INDEX t_idx2
        > CREATE INDEX t_idx_peer ON t (a, b)
        > CREATE MATERIALIZED VIEW mv_peer AS SELECT sum(a) FROM t;
        > SELECT * FROM mv2;
        25

        $ set-regex match=(s\\d+|\\d{{13}}|[ ]{{12}}0|u\\d{{1,3}}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

        > EXPLAIN TIMESTAMP FOR SELECT * FROM mv;
        "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (Storage inputs: [<>]): [<> <>]\\n"

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
        <null> <null> 7 8
        <null> <null> 9 10
        > SELECT * FROM webhook_source
        AAA
        BBB
        CCC

        $ set-max-tries max-tries=1
        $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
        > BEGIN
        > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
        > FETCH ALL c WITH (timeout='5s');
        <TIMESTAMP> 1 1
        <TIMESTAMP> 1 3
        <TIMESTAMP> 1 5
        <TIMESTAMP> 1 7
        <TIMESTAMP> 1 9
        > COMMIT
        """))

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
        c.testdrive(dedent("""
            $ webhook-append database=materialize schema=public name=webhook_source
            DDD

            > SET CLUSTER = cluster;
            > SELECT 1
            1
            > INSERT INTO t VALUES (11, 12);
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM mv;
            36
            > SELECT max(b) FROM t;
            12
            > SELECT * FROM mv;
            36
            > SELECT * FROM mv2;
            36
            > SELECT * FROM mv_peer;
            36
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
            <null> <null> 7 8
            <null> <null> 9 10
            <null> <null> 11 12
            > SELECT * FROM webhook_source
            AAA
            BBB
            CCC
            DDD

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{13,20} replacement=<TIMESTAMP>
            > BEGIN
            > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            > FETCH ALL c WITH (timeout='5s');
            <TIMESTAMP> 1 1
            <TIMESTAMP> 1 3
            <TIMESTAMP> 1 5
            <TIMESTAMP> 1 7
            <TIMESTAMP> 1 9
            <TIMESTAMP> 1 11
            > COMMIT
            # Also exercise a direct finite subscription.
            > SUBSCRIBE (WITH a(x) AS (SELECT 'a') SELECT generate_series(1, 2), x FROM a)
            <TIMESTAMP> 1 1 a
            <TIMESTAMP> 1 2 a
            """))

        with c.sql_connection(service="mz_old") as old_conn:
            old_conn.execute("SET cluster = cluster")
            assert old_conn.execute("SELECT 1").fetchall() == [(1,)]
            c.promote_mz("mz_new", retire_mz_service=None)
            c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")

            for i in range(10):
                try:
                    c.sql("SELECT 1", service="mz_new", reuse_connection=False)
                    break
                except CommandFailureCausedUIError:
                    pass
                except OperationalError:
                    pass
                time.sleep(1)
            else:
                raise RuntimeError("mz_new didn't come up within 10 seconds")

            old_conn.execute("INSERT INTO t VALUES (13, 14)")
            old_conn.execute("CREATE TABLE basic_peer (a int)")
            old_conn.execute("CREATE INDEX basic_peer_idx ON basic_peer (a)")

            c.testdrive(dedent(f"""
                $ webhook-append database=materialize schema=public name=webhook_source
                EEE
                > SET CLUSTER = cluster;
                > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
                > INSERT INTO basic_peer VALUES (42);
                > SELECT * FROM basic_peer;
                42
                > CREATE MATERIALIZED VIEW mv3 AS SELECT sum(a) FROM t;
                > SELECT * FROM mv;
                49
                > SELECT * FROM mv2;
                49
                > SELECT * FROM mv3;
                49
                > SELECT * FROM mv_peer;
                49
                > SELECT max(b) FROM t;
                14
                > INSERT INTO t VALUES (15, 16);
                > SELECT * FROM mv;
                64
                > SELECT * FROM mv2;
                64
                > SELECT * FROM mv3;
                64
                > SELECT * FROM mv_peer;
                64
                > SELECT max(b) FROM t;
                16
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
                <null> <null> 9 10
                <null> <null> 11 12
                <null> <null> 13 14
                <null> <null> 15 16
                > SELECT * FROM webhook_source
                AAA
                BBB
                CCC
                DDD
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
                <TIMESTAMP> 1 9
                <TIMESTAMP> 1 11
                <TIMESTAMP> 1 13
                <TIMESTAMP> 1 15
                > COMMIT
                """))

            assert old_conn.execute("SELECT * FROM basic_peer").fetchall() == [(42,)]
            c.retire_mz_deployment(
                "mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID
            )
            try:
                old_conn.execute("INSERT INTO t VALUES (99, 100)")
            except psycopg.Error as error:
                assert isinstance(error, OperationalError) or "has been retired" in str(
                    error
                ), error
            else:
                raise AssertionError("retired deployment accepted a table write")

        c.kill_fenced_mz("mz_old")
        c.testdrive(dedent(f"""
            > SET CLUSTER = cluster;
            > SELECT count(*), sum(a), max(b) FROM t;
            8 64 16
            > CREATE TABLE basic_survivor (a int);
            > INSERT INTO basic_survivor SELECT a FROM basic_peer;
            > SELECT * FROM basic_survivor;
            42
            > INSERT INTO t VALUES (17, 18);
            > SELECT count(*), sum(a), max(b) FROM t;
            9 81 18
            > SELECT * FROM mv;
            81
            > SELECT * FROM mv2;
            81
            > SELECT * FROM mv_peer;
            81
            > SELECT * FROM mv3;
            81

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
            key5A,key5B:value5A,value5B

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO postgres_source_table VALUES ('E', 4);

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            USE public;
            INSERT INTO mysql_source_table VALUES ('E', 4);

            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            $ sql-server-execute name=sql-server
            USE test;
            INSERT INTO sql_server_source_table VALUES ('E', 4);

            > SELECT * FROM kafka_source_tbl
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            key3A key3B value3A value3B
            key4A key4B value4A value4B
            key5A key5B value5A value5B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            C 2
            D 3
            E 4
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            C 2
            D 3
            E 4
            > SELECT * FROM sql_server_source_table;
            A 0
            B 1
            C 2
            D 3
            E 4
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
            <null> <null> 1 2
            <null> <null> 3 4
            <null> <null> 5 6
            <null> <null> 7 8
            <null> <null> 9 10
            <null> <null> 11 12
            <null> <null> 13 14
            <null> <null> 15 16
            <null> <null> 17 18
            $ webhook-append database=materialize schema=public name=webhook_source
            FFF
            > SELECT * FROM webhook_source
            AAA
            BBB
            CCC
            DDD
            EEE
            FFF

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
            > BEGIN
            > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            > FETCH ALL c WITH (timeout='5s');
            <TIMESTAMP> 1 1
            <TIMESTAMP> 1 3
            <TIMESTAMP> 1 5
            <TIMESTAMP> 1 7
            <TIMESTAMP> 1 9
            <TIMESTAMP> 1 11
            <TIMESTAMP> 1 13
            <TIMESTAMP> 1 15
            <TIMESTAMP> 1 17
            > COMMIT
        """))


def workflow_subscribe_cursors(c: Composition) -> None:
    """Start and resume subscription cursors through both pgwire protocols."""
    c.down(destroy_volumes=True)
    with c.override(
        Testdrive(
            materialize_url="postgres://materialize@mz_old:6875",
            materialize_url_internal="postgres://materialize@mz_old:6877",
            mz_service="mz_old",
            no_reset=True,
            default_timeout=DEFAULT_TIMEOUT,
        )
    ):
        c.up("mz_old", Service("testdrive", idle=True))
        c.testdrive("> SELECT 1\n1\n")

    # Exercise the production binary's stacks, not the larger debug stacks in
    # the Rust TestHarness. Parameter-free prepare=False uses simple Query,
    # while prepare=True forces the extended protocol.
    for extended in (False, True):
        with (
            c.sql_connection(service="mz_old") as writer,
            c.sql_connection(
                service="mz_old", startup_params={"statement_timeout": "30s"}
            ) as subscriber,
        ):
            writer.execute("CREATE TABLE cursor_input (v int)")
            writer.execute("CREATE DEFAULT INDEX ON cursor_input")
            writer.execute("INSERT INTO cursor_input VALUES (1)")
            subscriber.execute("BEGIN", prepare=extended)
            subscriber.execute(
                "DECLARE probe CURSOR FOR SUBSCRIBE cursor_input", prepare=extended
            )

            def fetch(expected: int) -> None:
                deadline = time.monotonic() + 30
                while True:
                    rows = subscriber.execute(
                        "FETCH ALL probe WITH (timeout='1s')", prepare=extended
                    ).fetchall()
                    if rows:
                        assert [(diff, value) for _, diff, value in rows] == [
                            (1, expected)
                        ], rows
                        return
                    assert time.monotonic() < deadline, (extended, expected)

            fetch(1)
            writer.execute("INSERT INTO cursor_input VALUES (2)")
            fetch(2)
            subscriber.execute("CLOSE probe", prepare=extended)
            subscriber.execute("COMMIT", prepare=extended)
            writer.execute("DROP TABLE cursor_input")


def workflow_subscribe_execution_identity(c: Composition) -> None:
    """Colliding adapter-local subscriptions retain independent public progress."""
    c.down(destroy_volumes=True)
    c.up("mz_old")
    connections = []
    active: dict[str, dict[str, Any]] = {}

    def connect(service: str) -> Any:
        conn = c.sql_connection(
            service=service, startup_params={"statement_timeout": "30s"}
        )
        connections.append(conn)
        return conn

    def pump() -> None:
        # Normal FETCH keeps both transactions alive during allocator alignment
        # and asynchronous publication. Retain data consumed by these keepalives.
        for sub in active.values():
            rows = (
                sub["conn"].execute("FETCH ALL probe WITH (timeout='100ms')").fetchall()
            )
            for timestamp, progress, diff, value in rows:
                if not progress:
                    assert diff == 1 and value not in sub["seen"], rows
                    sub["seen"][value] = int(timestamp)

    def until(check: Callable[[], bool], label: str) -> None:
        deadline = time.monotonic() + 300
        while True:
            pump()
            if check():
                return
            assert time.monotonic() < deadline, label
            time.sleep(0.2)

    try:
        old_writer = connect("mz_old")
        old_writer.execute(
            "CREATE CLUSTER subscription_probe SIZE 'scale=1,workers=1', "
            "REPLICATION FACTOR 2"
        )
        for name in ("old", "new"):
            old_writer.execute(f"CREATE TABLE subscription_{name} (v int)")
            old_writer.execute(
                f"CREATE INDEX subscription_{name}_idx IN CLUSTER subscription_probe "
                f"ON subscription_{name} (v)"
            )
            old_writer.execute(f"INSERT INTO subscription_{name} VALUES (1)")
        c.up("mz_new", wait=False)
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        observers = {service: connect(service) for service in ("mz_old", "mz_new")}
        writers = {"mz_old": old_writer, "mz_new": connect("mz_new")}
        imports = dict(old_writer.execute("""
            SELECT o.name, g.global_id FROM mz_catalog.mz_indexes o
            JOIN mz_internal.mz_object_global_ids g ON g.id = o.id
            WHERE o.name IN ('subscription_old_idx', 'subscription_new_idx')
        """).fetchall())
        assert len(imports) == 2, imports

        def start(service: str) -> None:
            conn = connect(service)
            conn.execute("SET cluster = subscription_probe")
            # mz_session_id() identifies the adapter process, not this SQL
            # session. Resolve the session row using its wire connection ID.
            sessions = conn.execute(
                "SELECT id FROM mz_internal.mz_sessions "
                "WHERE connection_id = pg_backend_pid()"
            ).fetchall()
            assert len(sessions) == 1, sessions
            session = sessions[0][0]
            name = "old" if service == "mz_old" else "new"
            conn.execute("BEGIN")
            conn.execute(
                f"DECLARE probe CURSOR FOR SUBSCRIBE subscription_{name} WITH (PROGRESS)"
            )
            sub = {"conn": conn, "seen": {}, "session": session, "name": name}
            active[service] = sub

            def identified() -> bool:
                rows = (
                    observers[service]
                    .execute(
                        "SELECT id FROM mz_internal.mz_subscriptions WHERE session_id = %s",
                        (session,),
                    )
                    .fetchall()
                )
                if not rows:
                    return False
                assert len(rows) == 1, rows
                sid = rows[0][0]
                match = re.fullmatch(r"t(\d+)@a(\d+)", sid)
                assert match, sid
                sub.update(id=sid, local=int(match[1]), incarnation=match[2])
                return True

            until(identified, f"subscription not published: {service}, {session}")

        def stop(service: str, *, disconnect: bool = False) -> str:
            sub = active.pop(service)
            if not disconnect:
                sub["conn"].execute("CLOSE probe")
                sub["conn"].execute("COMMIT")
            sub["conn"].close()
            return sub["id"]

        def absent(sid: str, service: str, *, logs: bool = True) -> None:
            # A join through mz_subscriptions would hide orphaned rows.
            for observer in observers.values():
                for relation, column in (
                    ("mz_internal.mz_subscriptions", "id"),
                    ("mz_internal.mz_compute_dependencies", "object_id"),
                    ("mz_internal.mz_frontiers", "object_id"),
                    ("mz_catalog.mz_cluster_replica_frontiers", "object_id"),
                ):
                    until(
                        lambda: observer.execute(
                            SQL("SELECT count(*) FROM {} WHERE {} = %s").format(
                                Identifier(*relation.split(".")), Identifier(column)
                            ),
                            (sid,),
                        ).fetchone()
                        == (0,),
                        f"orphaned {relation}: {sid}",
                    )
            with c.sql_connection(
                service=service, user="mz_system", port=6877
            ) as system:
                for relation, column in (
                    ("mz_internal.mz_subscriptions_raw", "id"),
                    ("mz_internal.mz_subscription_frontiers_raw", "object_id"),
                ):
                    until(
                        lambda: system.execute(
                            SQL("SELECT count(*) FROM {} WHERE {} = %s").format(
                                Identifier(*relation.split(".")), Identifier(column)
                            ),
                            (sid,),
                        ).fetchone()
                        == (0,),
                        f"unreclaimed owner rows in {relation}: {sid}",
                    )
            if not logs:
                return
            observer = observers[service]
            observer.execute("SET cluster = subscription_probe")
            replicas = observer.execute("""
                SELECT r.name FROM mz_catalog.mz_cluster_replicas r
                JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
                WHERE c.name = 'subscription_probe'
            """).fetchall()
            assert len(replicas) == 2, replicas
            observer.execute("SET auto_route_catalog_queries = false")
            try:
                for (replica,) in replicas:
                    observer.execute(
                        SQL("SET cluster_replica = {}").format(Identifier(replica))
                    )
                    for log in (
                        "exports",
                        "frontiers",
                        "import_frontiers",
                        "hydration_times",
                    ):
                        until(
                            lambda: observer.execute(
                                f"SELECT count(*) FROM mz_introspection.mz_compute_{log}_per_worker "
                                "WHERE export_id = %s",
                                (sid,),
                            ).fetchone()
                            == (0,),
                            f"orphaned {service}/{replica}/{log}: {sid}",
                        )
            finally:
                observer.execute("RESET cluster_replica")
                observer.execute("RESET auto_route_catalog_queries")

        start("mz_old")
        start("mz_new")
        attempts = []
        for _ in range(256):
            old, new = active["mz_old"], active["mz_new"]
            attempts.append((old["id"], new["id"]))
            if old["local"] == new["local"]:
                break
            lagging = "mz_old" if old["local"] < new["local"] else "mz_new"
            if lagging == "mz_old":
                # SELECT/SUBSCRIBE allocate IDs in pairs. Vary one adapter's
                # stride with a real, non-mutating single-index plan so the
                # two sequences need not remain in disjoint residue classes.
                old_writer.execute(
                    "EXPLAIN PHYSICAL PLAN FOR CREATE INDEX subscription_alignment "
                    "IN CLUSTER subscription_probe ON subscription_old (v + 1)"
                ).fetchall()
            stop(lagging)
            start(lagging)
        else:
            raise AssertionError(
                f"Could not align actual local subscription IDs: {attempts}"
            )
        assert old["id"] != new["id"] and old["incarnation"] != new["incarnation"]

        def progress(service: str, value: int) -> None:
            sub = active[service]
            sid = sub["id"]
            until(lambda: value in sub["seen"], f"missing {service} update {value}")
            timestamp = sub["seen"][value]
            dependency = imports[f"subscription_{sub['name']}_idx"]
            for observer in observers.values():

                def visible() -> bool:
                    rows = observer.execute(
                        """
                        SELECT s.session_id, d.dependency_id,
                               f.read_frontier::text::numeric, f.write_frontier::text::numeric
                        FROM mz_internal.mz_subscriptions s
                        JOIN mz_internal.mz_compute_dependencies d ON d.object_id = s.id
                        JOIN mz_internal.mz_frontiers f ON f.object_id = s.id
                        WHERE s.id = %s
                    """,
                        (sid,),
                    ).fetchall()
                    if not rows:
                        return False
                    assert len(rows) == 1, rows
                    session, dep, read, write = rows[0]
                    assert (session, dep) == (sub["session"], dependency), rows
                    return (
                        read is not None
                        and write is not None
                        and read >= timestamp
                        and write > timestamp
                    )

                until(visible, f"merged progress/join: {sid}, {timestamp}")

                def replicas_advanced() -> bool:
                    rows = observer.execute(
                        """
                        SELECT replica_id, write_frontier::text::numeric
                        FROM mz_catalog.mz_cluster_replica_frontiers WHERE object_id = %s
                    """,
                        (sid,),
                    ).fetchall()
                    assert len(rows) <= 2, rows
                    return len({rid for rid, _ in rows}) == 2 and all(
                        frontier is not None and frontier > timestamp
                        for _, frontier in rows
                    )

                until(replicas_advanced, f"per-replica progress: {sid}, {timestamp}")

            observer = observers[service]
            observer.execute("SET cluster = subscription_probe")
            observer.execute("SET auto_route_catalog_queries = false")
            replicas = observer.execute("""
                SELECT r.name FROM mz_catalog.mz_cluster_replicas r
                JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
                WHERE c.name = 'subscription_probe'
            """).fetchall()
            assert len(replicas) == 2, replicas
            try:
                for (replica,) in replicas:
                    observer.execute(
                        SQL("SET cluster_replica = {}").format(Identifier(replica))
                    )

                    def logged() -> bool:
                        rows = observer.execute(
                            """
                            SELECT e.worker_id::int8, i.import_id, f.time::text::numeric, i.time::text::numeric,
                                   h.hydrated_at IS NOT NULL
                            FROM mz_introspection.mz_compute_exports_per_worker e
                            JOIN mz_introspection.mz_compute_frontiers_per_worker f
                              USING (export_id, worker_id)
                            JOIN mz_introspection.mz_compute_import_frontiers_per_worker i
                              USING (export_id, worker_id)
                            JOIN mz_introspection.mz_compute_hydration_times_per_worker h
                              USING (export_id, worker_id)
                            WHERE e.export_id = %s
                        """,
                            (sid,),
                        ).fetchall()
                        if not rows:
                            return False
                        assert len(rows) == 1 and rows[0][:2] == (0, dependency), rows
                        return (
                            rows[0][2] > timestamp
                            and rows[0][3] > timestamp
                            and rows[0][4]
                        )

                    until(logged, f"log joins/progress: {service}, {replica}, {sid}")
            finally:
                observer.execute("RESET cluster_replica")
                observer.execute("RESET auto_route_catalog_queries")

        for value in (1, 2):
            for service, writer in writers.items():
                if value != 1:
                    writer.execute(
                        f"INSERT INTO subscription_{active[service]['name']} VALUES ({value})"
                    )
                progress(service, value)

        # CLOSE must not remove the colliding predecessor's rows.
        removed = stop("mz_new")
        absent(removed, "mz_new")
        old_writer.execute("INSERT INTO subscription_old VALUES (3)")
        progress("mz_old", 3)
        start("mz_new")
        removed = stop("mz_new", disconnect=True)
        absent(removed, "mz_new")
        old_writer.execute("INSERT INTO subscription_old VALUES (4)")
        progress("mz_old", 4)

        # Candidate connections may restart. The predecessor remains the same
        # session and execution across promotion, not a recreated subscription.
        observers["mz_new"].close()
        writers["mz_new"].close()
        pump()
        c.promote_mz("mz_new", retire_mz_service=None)
        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")
        observers["mz_new"] = connect("mz_new")
        writers["mz_new"] = connect("mz_new")
        start("mz_new")
        writers["mz_new"].execute("INSERT INTO subscription_old VALUES (5)")
        old_writer.execute("INSERT INTO subscription_new VALUES (5)")
        progress("mz_old", 5)
        progress("mz_new", 5)
        removed = stop("mz_old", disconnect=True)
        absent(removed, "mz_old")
        writers["mz_new"].execute("INSERT INTO subscription_new VALUES (6)")
        progress("mz_new", 6)
        # Retire an execution with a live subscription, rather than relying on
        # normal session cleanup to make the retirement case vacuous.
        start("mz_old")
        progress("mz_old", 5)
        retiring = active.pop("mz_old")
        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        try:
            retiring["conn"].execute("FETCH ALL probe WITH (timeout='1s')")
        except psycopg.Error as error:
            assert isinstance(error, OperationalError) or "has been retired" in str(
                error
            ), error
        else:
            raise AssertionError("retired subscription execution still serves FETCH")
        c.kill_fenced_mz("mz_old")
        observers.pop("mz_old").close()
        # Retired replicas cannot serve log queries. The surviving deployment
        # must reclaim durable owner rows and keep its own subscription intact.
        absent(retiring["id"], "mz_new", logs=False)
        writers["mz_new"].execute("INSERT INTO subscription_new VALUES (7)")
        progress("mz_new", 7)
        removed = stop("mz_new")
        absent(removed, "mz_new")
    finally:
        for conn in reversed(connections):
            conn.close()


def workflow_index_read_protection(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Follow shared index permissions in every candidate replica while both adapters write."""
    parser.add_argument(
        "--index-count",
        type=int,
        default=1,
        help="Stable live cohort including keep (default: 1), plus one live drop index",
    )
    parser.add_argument("--publication-interval-ms", type=int, default=1000)
    args = parser.parse_args()
    if args.index_count < 1 or args.publication_interval_ms < 1:
        parser.error("index count and publication interval must be positive")
    c.down(destroy_volumes=True)
    c.up("mz_old", Service("testdrive", idle=True))
    setup(c)

    def query(
        sql: str, service: str = "mz_old", replica: str | None = None
    ) -> list[tuple]:
        with c.sql_connection(
            service=service,
            port=6877,
            user="mz_system",
            startup_params={
                "statement_timeout": "5s",
                "cluster": "cluster",
                **({"cluster_replica": replica} if replica is not None else {}),
            },
        ) as conn:
            cursor = conn.execute(sql.encode())
            return cursor.fetchall() if cursor.description is not None else []

    query("ALTER SYSTEM SET enable_logical_compaction_window = true")
    query("ALTER SYSTEM SET enable_index_options = true")
    query(f"ALTER SYSTEM SET max_objects_per_schema = {args.index_count + 100}")
    query("ALTER SYSTEM SET unsafe_pause_catalog_read_protection_publication = true")
    unpublished_start = time.monotonic()
    c.testdrive(dedent("""
        > CREATE CLUSTER permission_suspended SIZE 'scale=1,workers=1', REPLICATION FACTOR 0;
        > CREATE TABLE permission_suspended_input (a int);
        > INSERT INTO permission_suspended_input VALUES (1);
        > CREATE TABLE permission_input (a int);
        > INSERT INTO permission_input VALUES (1);
        > CREATE VIEW permission_keep AS SELECT a + 1 AS a FROM permission_input;
        > CREATE VIEW permission_drop AS SELECT a + 2 AS a FROM permission_input;
        > CREATE INDEX permission_keep_idx ON permission_keep (a)
          WITH (RETAIN HISTORY = FOR '1s');
        > CREATE INDEX permission_drop_idx ON permission_drop (a)
          WITH (RETAIN HISTORY = FOR '1s');
        > CREATE INDEX permission_suspended_idx IN CLUSTER permission_suspended
          ON permission_suspended_input (a) WITH (RETAIN HISTORY = FOR '1s');
        > CREATE INDEX permission_bootstrap_drop_idx IN CLUSTER permission_suspended
          ON permission_suspended_input (a) WITH (RETAIN HISTORY = FOR '1s');
    """))
    extra_names = [f"permission_cohort_{i}_idx" for i in range(1, args.index_count)]
    if extra_names:
        c.testdrive(
            "\n".join(
                f"> CREATE INDEX {name} IN CLUSTER cluster ON permission_input (a) "
                "WITH (RETAIN HISTORY = FOR '1s');"
                for name in extra_names
            )
        )
    names = [
        "permission_keep_idx",
        "permission_drop_idx",
        "permission_suspended_idx",
        "permission_bootstrap_drop_idx",
        *extra_names,
    ]
    names_sql = ", ".join(f"'{name}'" for name in names)
    ids = dict(query(f"""
        SELECT o.name, g.global_id FROM mz_objects o
        JOIN mz_internal.mz_object_global_ids g ON g.id = o.id
        WHERE o.name IN ({names_sql})
    """))
    assert set(ids) == set(names), ids
    assert len(set(ids.values())) == len(names), ids
    stable_ids = [ids[name] for name in ["permission_keep_idx", *extra_names]]

    def catalog_bounds(service: str) -> dict:
        # Strict-serializable reads certify this adapter's catalog through the
        # query timestamp, so a returned bound has also been applied locally.
        # Avoid serializing the entire catalog just to observe these permissions.
        rows = query(
            """
            SELECT mz_internal.parse_catalog_id(data->'key'->'id'),
                   (data->'value'->>'frontier')::numeric
            FROM mz_internal.mz_catalog_shared
            WHERE data->>'kind' = 'CollectionCompactionBound'
            """,
            service,
        )
        bounds = {}
        for gid, frontier in rows:
            if gid in ids.values():
                assert gid not in bounds, (gid, rows)
                bounds[gid] = {"elements": [] if frontier is None else [int(frontier)]}
        return bounds

    def permission(bounds: dict, gid: str) -> int:
        elements = bounds[gid]["elements"]
        assert len(elements) == 1, (gid, bounds[gid])
        return elements[0]

    def await_bounds(
        targets: dict[str, int],
        service: str = "mz_new",
        *,
        deadline: float | None = None,
    ) -> None:
        if deadline is None:
            deadline = time.monotonic() + 300
        while True:
            bounds = catalog_bounds(service)
            if all(
                gid in bounds and permission(bounds, gid) >= target
                for gid, target in targets.items()
            ):
                return
            assert time.monotonic() < deadline, (service, targets, bounds)
            time.sleep(0.5)

    def assert_dropped(name: str) -> None:
        deadline = time.monotonic() + 300
        for service in ("mz_old", "mz_new"):
            while True:
                absent = query(
                    f"SELECT count(*) FROM mz_indexes WHERE name = '{name}'", service
                ) == [(0,)]
                if absent and ids[name] not in catalog_bounds(service):
                    break
                assert time.monotonic() < deadline, (service, name)
                time.sleep(0.5)

    def replicas(generation: int, cluster: str = "cluster") -> list[tuple]:
        return query(
            f"""
            SELECT mz_internal.parse_catalog_id(c.data->'key'->'id'),
                   c.data->'value'->>'name'
            FROM mz_internal.mz_catalog_shared c
            JOIN mz_clusters cl ON cl.id = mz_internal.parse_catalog_id(c.data->'value'->'cluster_id')
            WHERE c.data->>'kind' = 'ClusterReplica'
              AND COALESCE((c.data->'key'->>'deployment_generation')::uint8, 0) = {generation}
              AND cl.name = '{cluster}'
        """,
            "mz_new",
        )

    bounds = catalog_bounds("mz_old")
    birth = {gid: permission(bounds, gid) for gid in ids.values()}
    # Either ordering of bootstrap and DROP must work. Neither requires a frozen
    # catalog: candidate adapters and replicas follow subsequent committed DDL.
    c.up("mz_new", wait=False)
    query("DROP INDEX permission_bootstrap_drop_idx")
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
    assert_dropped("permission_bootstrap_drop_idx")
    surviving_birth = {
        gid: value
        for gid, value in birth.items()
        if gid != ids["permission_bootstrap_drop_idx"]
    }
    await_bounds(surviving_birth)
    assert time.monotonic() - unpublished_start < 300
    assert replicas(1, "permission_suspended") == []
    candidate_replicas = replicas(1)
    assert len(candidate_replicas) == 2, candidate_replicas
    replica_ids = {rid for rid, _ in candidate_replicas}
    assert len(replica_ids) == 2, candidate_replicas

    def observations(gids: list[str]) -> dict[tuple[str, str], tuple[int, int]]:
        gids_sql = ", ".join(f"'{gid}'" for gid in gids)
        rows = query(
            f"""
            SELECT o.object_id, o.replica_id,
                   o.read_frontier::text::numeric, o.write_frontier::text::numeric
            FROM mz_internal.mz_replica_frontier_observations o
            WHERE o.object_id IN ({gids_sql}) AND o.deployment_generation = 1
              AND o.read_known AND o.write_known
              AND o.read_frontier IS NOT NULL AND o.write_frontier IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM mz_internal.mz_catalog_shared c
                  WHERE c.data->>'kind' = 'ClientIncarnation'
                    AND (c.data->'key'->>'id')::uint8 = o.client_incarnation
                    AND COALESCE((c.data->'value'->>'deployment_generation')::uint8, 0) = o.deployment_generation
                    AND mz_internal.parse_catalog_id(c.data->'value'->'realization'->'Replica') = o.replica_id
              )
        """,
            "mz_new",
        )
        result = {(gid, rid): (int(read), int(write)) for gid, rid, read, write in rows}
        assert len(result) == len(rows), rows
        return result

    def expected_pairs(gids: list[str]) -> set[tuple[str, str]]:
        return {(gid, rid) for gid in gids for rid in replica_ids}

    def synchronize_pause() -> None:
        # An indexed read routed to each replica follows the certified post-pause
        # catalog before answering. Do this for both deployments before taking a cap.
        for generation, service in enumerate(("mz_old", "mz_new")):
            members = replicas(generation)
            assert len(members) == 2, members
            assert len({rid for rid, _ in members}) == 2, members
            for _, name in members:
                query("SELECT * FROM permission_keep", service, replica=name)
                plan = query(
                    "EXPLAIN SELECT * FROM permission_keep", service, replica=name
                )[0][0]
                assert "permission_keep_idx" in plan, plan

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
        c.testdrive(dedent("""
            > SET statement_timeout = '5s';
            > SET TRANSACTION_ISOLATION = 'SERIALIZABLE';
            > SELECT * FROM permission_keep;
            2
            > SELECT * FROM permission_drop;
            3
        """))
        initial_gids = stable_ids + [ids["permission_drop_idx"]]
        deadline = time.monotonic() + 300
        while True:
            observed = observations(initial_gids)
            if all(
                pair in observed and observed[pair][1] > birth[pair[0]]
                for pair in expected_pairs(initial_gids)
            ):
                break
            assert time.monotonic() < deadline, (initial_gids, observed)
            time.sleep(0.5)
        # Startup and grant acquisition must not advance paused permission, even
        # for RF0 indexes. Every installed candidate index must retain that history.
        for service in ("mz_old", "mz_new"):
            bounds = catalog_bounds(service)
            assert all(
                permission(bounds, gid) == initial
                for gid, initial in surviving_birth.items()
            ), (service, surviving_birth, bounds)
        assert all(
            observed[pair][0] <= birth[pair[0]] for pair in expected_pairs(initial_gids)
        ), (birth, observed)
        assert time.monotonic() - unpublished_start < 300
        query(
            "ALTER SYSTEM SET catalog_read_protection_publish_interval = "
            f"'{args.publication_interval_ms}ms'"
        )

        query(
            "ALTER SYSTEM SET unsafe_pause_catalog_read_protection_publication = false"
        )
        # Both adapters write. A peer DROP must not prevent permission delivery
        # to the stable cohort or leave the candidate's catalog frozen.
        for value in (2, 3):
            if value == 3:
                query("DROP INDEX permission_drop_idx")
                assert_dropped("permission_drop_idx")
            query(
                f"INSERT INTO permission_input VALUES ({value})",
                "mz_new" if value == 2 else "mz_old",
            )
            keep = ids["permission_keep_idx"]
            cohort = stable_ids + ([ids["permission_drop_idx"]] if value == 2 else [])
            bounds = catalog_bounds("mz_old")
            baseline = {gid: permission(bounds, gid) for gid in cohort}
            deadline = time.monotonic() + 300
            publication_start = time.monotonic()
            while True:
                writer_request_start = time.monotonic()
                bounds = catalog_bounds("mz_old")
                committed_observed = time.monotonic()
                targets = {gid: permission(bounds, gid) for gid in cohort}
                if all(targets[gid] > baseline[gid] for gid in cohort):
                    break
                assert time.monotonic() < deadline, (baseline, targets)
                time.sleep(0.5)
            committed = targets[keep]
            assert committed > 0
            # Capture one committed target set. Candidate adapter catch-up and
            # replica compaction are separate observations of that permission.
            await_bounds(targets, deadline=deadline)
            follower_requests = 0
            follower_request_seconds = 0.0
            while True:
                follower_request_start = time.monotonic()
                observed = observations(cohort)
                observed_count = sum(
                    all(
                        (gid, rid) in observed
                        and observed[(gid, rid)][0] >= targets[gid]
                        for rid in replica_ids
                    )
                    for gid in cohort
                )
                follower_observed = time.monotonic()
                follower_requests += 1
                follower_request_seconds += follower_observed - follower_request_start
                if observed_count == len(cohort):
                    break
                assert time.monotonic() < deadline, (
                    len(cohort),
                    observed_count,
                    targets,
                )
                time.sleep(0.5)
            c.testdrive(dedent(f"""
                > SET TRANSACTION_ISOLATION = 'SERIALIZABLE';
                > SELECT count(*), sum(a) FROM permission_keep;
                {value} {sum(range(2, value + 2))}
            """))
            print(
                json.dumps(
                    {
                        "workflow": "index-read-protection",
                        "publication_interval_ms": args.publication_interval_ms,
                        "index_count": args.index_count,
                        "initial_live_index_count": len(stable_ids) + 1,
                        "cohort_index_count": len(cohort),
                        "follower_observed_index_count": observed_count,
                        "observer": "certified permission reads on both adapters and generation-qualified current replica observations at 500ms",
                        "measurement_scope": "snapshot observation to all-complete read frontier observation, not isolated subscriber CPU or unobserved commit-to-poll latency",
                        "committed_permission": committed,
                        "writer_permission_observation_seconds": committed_observed
                        - publication_start,
                        "writer_observed_to_follower_frontier_seconds": follower_observed
                        - committed_observed,
                        "max_all_complete_observation_lag_seconds": follower_observed
                        - committed_observed,
                        "writer_snapshot_request_seconds": committed_observed
                        - writer_request_start,
                        "follower_frontier_request_count": follower_requests,
                        "follower_frontier_request_seconds": follower_request_seconds,
                        "follower_final_frontier_request_seconds": follower_observed
                        - follower_request_start,
                        "follower_read_check_seconds": time.monotonic()
                        - follower_observed,
                        "writer_index_dropped": value == 3,
                    },
                    sort_keys=True,
                )
            )
            await_bounds(targets)
            plan = query("EXPLAIN SELECT * FROM permission_keep", "mz_new")[0][0]
            assert "permission_keep_idx" in plan, plan

        query(
            "ALTER SYSTEM SET unsafe_pause_catalog_read_protection_publication = true"
        )
        synchronize_pause()
        capped_start = time.monotonic()
        query("SELECT 1")
        cap = permission(catalog_bounds("mz_old"), keep)
        await_bounds({keep: cap})
        assert permission(catalog_bounds("mz_new"), keep) == cap
        for value in (4, 5, 6):
            previous = observations([keep])
            assert expected_pairs([keep]) <= previous.keys(), previous
            query(f"INSERT INTO permission_input VALUES ({value})")
            deadline = time.monotonic() + 300
            while True:
                observed = observations([keep])
                if all(
                    pair in observed
                    and observed[pair][1] > max(previous[pair][1], cap + 1000)
                    for pair in expected_pairs([keep])
                ):
                    break
                assert time.monotonic() < deadline, (previous, observed, cap)
                time.sleep(0.5)
            assert all(observed[pair][0] <= cap for pair in expected_pairs([keep])), (
                observed,
                cap,
            )
            for service in ("mz_old", "mz_new"):
                assert permission(catalog_bounds(service), keep) == cap
            assert time.monotonic() - capped_start < 300

        query(
            "ALTER SYSTEM SET unsafe_pause_catalog_read_protection_publication = false"
        )
        deadline = time.monotonic() + 300
        await_bounds({keep: cap + 1}, "mz_old", deadline=deadline)
        await_bounds({keep: cap + 1}, deadline=deadline)
        while True:
            observed = observations([keep])
            if all(
                pair in observed and observed[pair][0] > cap
                for pair in expected_pairs([keep])
            ):
                break
            assert time.monotonic() < deadline, (observed, cap)
            time.sleep(0.5)


def workflow_kafka_source_rehydration(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Verify Kafka source rehydration in 0dt deployment"""
    parser.add_argument(
        "--ci-compatible-image-artifact",
        help="Use a build-native-compatible artifact in ordinary CI, retaining same-version instrumented runs",
    )
    args = parser.parse_args()
    artifact = None
    new_image = None
    if args.ci_compatible_image_artifact and (
        sanitizer_enabled() or env_is_truthy("CI_COVERAGE_ENABLED")
    ):
        print("Instrumented native warm promotion uses the current same-version images")
    elif args.ci_compatible_image_artifact:
        with TemporaryDirectory() as directory:
            subprocess.run(
                [
                    "buildkite-agent",
                    "artifact",
                    "download",
                    args.ci_compatible_image_artifact,
                    directory,
                    "--step",
                    "build-native-compatible",
                ],
                check=True,
            )
            artifact = json.loads(
                (Path(directory) / args.ci_compatible_image_artifact).read_text()
            )
        revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        assert artifact["source_revision"] == revision, artifact
        assert artifact["old_version"] != artifact["new_version"], artifact
        new_image = artifact["image"]
        assert isinstance(new_image, str) and new_image, artifact
        print(f"Compatible-version image: {new_image}, source: {revision}")

    print(f"mz_old image: {c.compose['services']['mz_old']['image']}")
    c.down(destroy_volumes=True)
    c.up(
        "kafka",
        "schema-registry",
        "mz_old",
        Service("testdrive", idle=True),
    )
    old_version = c.query_mz_version(service="mz_old")
    print(f"mz_old SQL binary version: {old_version}")
    if artifact:
        assert old_version.split()[0] == f"v{artifact['old_version']}", artifact
    setup(c)

    count = 1000000
    repeats = 20

    start_time = time.time()
    c.testdrive(dedent(f"""
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
        """))
    for i in range(1, repeats):
        c.testdrive(dedent(f"""
        $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-large repeat={count}
        key{i}A,key{i}${{kafka-ingest.iteration}}:value{i}A,${{kafka-ingest.iteration}}
        > SELECT * FROM kafka_source_cnt
        {count*(i+1)}
            """))

    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    with c.override(
        native_mz(
            name="mz_new",
            image=new_image,
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
        try:
            c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        except Exception:
            # Capture the limiting imports without masking the readiness failure.
            try:
                with c.sql_cursor(service="mz_new", user="mz_system", port=6877) as cur:
                    cur.execute("SET statement_timeout = '10s'")
                    cur.execute("SET auto_route_catalog_queries = false")
                    cur.execute("SET cluster = mz_catalog_server")
                    cur.execute("SET cluster_replica = r1")
                    cur.execute("""
                        SELECT i.name, f.export_id, f.import_id, f.worker_id, f.time
                        FROM mz_introspection.mz_compute_import_frontiers_per_worker f
                        JOIN mz_internal.mz_object_global_ids g ON g.global_id = f.export_id
                        JOIN mz_catalog.mz_indexes i ON i.id = g.id
                        WHERE i.name IN ('mz_source_statistics_with_history_ind',
                                         'mz_object_arrangement_sizes_ind',
                                         'mz_compute_hydration_times_ind')
                        ORDER BY i.name, f.import_id, f.worker_id
                    """)
                    print(f"Prewarming import frontiers: {cur.fetchall()}")
            except Exception as diagnostic_error:
                print(f"Prewarming frontier diagnostic failed: {diagnostic_error}")
            raise
        elapsed = time.time() - start_time
        print(f"re-hydration took {elapsed} seconds")
        # Retire mz_old only after timing the promotion.
        promotion_timeout = 2400 if env_is_truthy("CI_COVERAGE_ENABLED") else 1200
        start_time = time.monotonic()
        promotion_deadline = start_time + promotion_timeout
        c.promote_mz("mz_new", retire_mz_service=None)
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER,
            "mz_new",
            timeout=max(1, int(promotion_deadline - time.monotonic())),
            sleep_time=None,
        )
        # Completing the handshake waits for SQL serving without warming a query.
        psycopg.connect(
            host="127.0.0.1",
            port=c.default_port("mz_new"),
            user="materialize",
            dbname="materialize",
            sslmode="disable",
            connect_timeout=max(1, int(promotion_deadline - time.monotonic())),
        ).close()
        elapsed = time.monotonic() - start_time
        print(f"promotion took {elapsed} seconds (through SQL readiness)")
        assert elapsed < promotion_timeout, f"Promotion exceeded {promotion_timeout}s"

        start_time = time.time()
        result = c.sql_query("SELECT * FROM kafka_source_cnt", service="mz_new")
        elapsed = time.time() - start_time
        print(f"final check took {elapsed} seconds")
        assert result[0][0] == count * repeats, f"Wrong result: {result}"
        result = c.sql_query("SELECT count(*) FROM kafka_source_tbl", service="mz_new")
        assert result[0][0] == count * repeats, f"Wrong result: {result}"
        assert (
            elapsed < 3 * HYDRATED_SELECT_FACTOR
        ), f"Took {elapsed}s to SELECT on Kafka source after 0dt upgrade, is it hydrated?"
        # Keep cleanup outside both promotion and first-query timing windows.
        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        c.kill_fenced_mz("mz_old")

        new_version = c.query_mz_version(service="mz_new")
        print(f"mz_new SQL binary version: {new_version}")
        if artifact:
            assert new_version.split()[0] == f"v{artifact['new_version']}", artifact
            assert new_version.split()[0] != old_version.split()[0]

        print("Ingesting again")
        for i in range(repeats, repeats * 2):
            c.testdrive(dedent(f"""
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-large repeat={count}
                key{i}A,key{i}${{kafka-ingest.iteration}}:value{i}A,${{kafka-ingest.iteration}}
                """))

        c.testdrive(dedent(f"""
            > SET CLUSTER = cluster;
            > SELECT * FROM kafka_source_cnt
            {2*count*repeats}
            > SELECT count(*) FROM kafka_source_tbl
            {2*count*repeats}
            """))


def workflow_kafka_source_rehydration_large_initial(c: Composition) -> None:
    """Verify Kafka source rehydration in 0dt deployment"""
    c.down(destroy_volumes=True)
    c.up(
        "kafka",
        "schema-registry",
        "mz_old",
        Service("testdrive", idle=True),
    )
    setup(c)

    count = 1000000
    repeats = 20

    start_time = time.time()
    c.testdrive(dedent("""
        $ kafka-create-topic topic=kafka-large
            """))
    for i in range(repeats):
        c.testdrive(dedent(f"""
            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-large repeat={count}
            key{i}A,key{i}${{kafka-ingest.iteration}}:value{i}A,${{kafka-ingest.iteration}}
            """))

    c.testdrive(dedent(f"""
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
        """))

    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    with c.override(
        native_mz(
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
        # Retire mz_old only after timing the promotion.
        c.promote_mz("mz_new", retire_mz_service=None)
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
            elapsed < 3 * HYDRATED_SELECT_FACTOR
        ), f"Took {elapsed}s to SELECT on Kafka source after 0dt upgrade, is it hydrated?"
        # Keep cleanup outside both promotion and first-query timing windows.
        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        c.kill_fenced_mz("mz_old")

        start_time = time.time()
        result = c.sql_query("SELECT 1", service="mz_new")
        elapsed = time.time() - start_time
        print(f"bootstrapping (checked via SELECT 1) took {elapsed} seconds")
        assert result[0][0] == 1, f"Wrong result: {result}"

        print("Ingesting again")
        for i in range(repeats, repeats * 2):
            c.testdrive(dedent(f"""
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-large repeat={count}
                key{i}A,key{i}${{kafka-ingest.iteration}}:value{i}A,${{kafka-ingest.iteration}}
                """))

        c.testdrive(dedent(f"""
            > SET CLUSTER = cluster;
            > SELECT * FROM kafka_source_cnt
            {2*count*repeats}
            > SELECT count(*) FROM kafka_source_tbl
            {2*count*repeats}
            """))


def workflow_pg_source_rehydration(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Verify shared-source prewarming and reader handover with both deployments alive."""
    parser.add_argument("--rows-per-batch", type=int, default=500000)
    args = parser.parse_args()
    if args.rows_per_batch < 1:
        parser.error("rows per batch must be positive")
    c.down(destroy_volumes=True)
    c.up("postgres", "mz_old", Service("testdrive", idle=True))
    setup(c)

    count = args.rows_per_batch
    repeats = 100
    total = count * repeats

    def upstream(sql: str) -> list[tuple]:
        return c.sql_query(
            sql,
            service="postgres",
            user="postgres",
            password="postgres",
            database="postgres",
        )

    def reader_hosts() -> list[str | None]:
        return [
            row[0]
            for row in upstream(
                "SELECT client_addr::text FROM pg_stat_replication WHERE usename = 'postgres1'"
            )
        ]

    # Setup upstream Postgres table and publication
    c.testdrive(
        dedent("""
        > SET CLUSTER = cluster;

        $ postgres-execute connection=postgres://postgres:postgres@postgres
        CREATE USER postgres1 WITH SUPERUSER PASSWORD 'postgres';
        ALTER USER postgres1 WITH replication;
        DROP PUBLICATION IF EXISTS postgres_source;
        DROP TABLE IF EXISTS postgres_source_table;
        CREATE TABLE postgres_source_table (f1 INTEGER);
        ALTER TABLE postgres_source_table REPLICA IDENTITY FULL;
        CREATE PUBLICATION postgres_source FOR ALL TABLES;
        """),
        quiet=True,
    )

    # Bulk-load data via COPY FROM STDIN (much faster than testdrive INSERT)
    start_time = time.time()
    _pg_direct_ingest(c, "postgres_source_table", count, repeats)
    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    # Create MZ source and wait for snapshot to complete
    c.testdrive(
        dedent(f"""
        > SET CLUSTER = cluster;

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
        {total}
        """),
        quiet=True,
    )

    old_reader = _wait_for_cdc_reader(reader_hosts)

    with c.override(
        native_mz(
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
        _wait_for_cdc_reader(reader_hosts, expected_host=old_reader)
        # Promotion restarts the candidate adapter, not its warmed replicas.
        # Do not cache this prewarming session across that restart.
        with c.sql_connection(service="mz_new") as connection:
            assert connection.execute(
                "SELECT * FROM postgres_source_cnt"
            ).fetchall() == [(total,)]
        # Retire mz_old only after timing the promotion.
        c.promote_mz("mz_new", retire_mz_service=None)
        start_time = time.time()
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )
        # Include the replacement adapter's SQL startup in promotion time,
        # rather than in the separate hydrated-query measurement below.
        c.sql_connection(service="mz_new", reuse_connection=True)
        elapsed = time.time() - start_time
        print(f"promotion took {elapsed} seconds")
        start_time = time.time()
        result = c.sql_query("SELECT * FROM postgres_source_cnt", service="mz_new")
        elapsed = time.time() - start_time
        print(f"final check took {elapsed} seconds")
        assert result[0][0] == total, f"Wrong result: {result}"
        assert (
            elapsed < 4 * HYDRATED_SELECT_FACTOR
        ), f"Took {elapsed}s to SELECT on Postgres source after 0dt upgrade, is it hydrated?"
        promoted_reader = _wait_for_cdc_reader(reader_hosts, excluded_host=old_reader)

        result = c.sql_query(
            "SELECT count(*) FROM postgres_source_table", service="mz_new"
        )
        assert result[0][0] == total, f"Wrong result: {result}"

        # Verify source still works after promotion (small batch is enough)
        print("Ingesting again")
        post_repeats = 10
        post_total = total + count * post_repeats
        _pg_direct_ingest(c, "postgres_source_table", count, post_repeats)
        _wait_for_mz_count(
            c,
            "SELECT * FROM postgres_source_cnt",
            post_total,
            service="mz_new",
        )

        result = c.sql_query("SELECT * FROM postgres_source_cnt", service="mz_new")
        assert result[0][0] == post_total, f"Wrong result: {result}"

        result = c.sql_query(
            "SELECT count(*) FROM postgres_source_table", service="mz_new"
        )
        assert result[0][0] == post_total, f"Wrong result: {result}"

        # Recovery must not revive the former reader while its deployment still
        # serves SQL. Exercise actual connector restart, not process cleanup.
        terminated = upstream(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_replication "
            "WHERE usename = 'postgres1'"
        )
        assert terminated and all(row == (True,) for row in terminated), terminated
        _pg_direct_ingest(c, "postgres_source_table", 1, 1)
        for service in ("mz_new", "mz_old"):
            _wait_for_mz_count(
                c, "SELECT * FROM postgres_source_cnt", post_total + 1, service=service
            )
        _wait_for_cdc_reader(reader_hosts, expected_host=promoted_reader)

        # Keep retirement separate from reader handover and outside timing windows.
        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        c.kill_fenced_mz("mz_old")
        _pg_direct_ingest(c, "postgres_source_table", 1, 1)
        _wait_for_mz_count(
            c, "SELECT * FROM postgres_source_cnt", post_total + 2, service="mz_new"
        )


def workflow_mysql_source_rehydration(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Verify shared-source prewarming and reader handover with both deployments alive."""
    parser.add_argument("--rows-per-batch", type=int, default=1000000)
    args = parser.parse_args()
    if args.rows_per_batch < 1:
        parser.error("rows per batch must be positive")
    c.down(destroy_volumes=True)
    c.up("mysql", "mz_old", Service("testdrive", idle=True))
    setup(c)

    count = args.rows_per_batch
    repeats = 100
    total = count * repeats

    def upstream(sql: str) -> list[tuple]:
        with pymysql.connect(
            host="localhost",
            user="root",
            password=MySql.DEFAULT_ROOT_PASSWORD,
            port=c.default_port("mysql"),
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return list(cursor.fetchall())

    def reader_hosts() -> list[str]:
        return [
            row[0].rsplit(":", 1)[0]
            for row in upstream(
                "SELECT HOST FROM information_schema.PROCESSLIST "
                "WHERE USER = 'mysql1' AND COMMAND LIKE 'Binlog Dump%'"
            )
        ]

    # Setup upstream MySQL table and replication user
    c.testdrive(
        dedent(f"""
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
        """),
        quiet=True,
    )

    # Bulk-load data via batched INSERT (much faster than testdrive)
    start_time = time.time()
    _mysql_direct_ingest(c, "mysql_source_table", count, repeats)
    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    # Create MZ source and wait for snapshot to complete
    c.testdrive(
        dedent(f"""
        > SET CLUSTER = cluster;

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
        {total}
        """),
        quiet=True,
    )

    old_reader = _wait_for_cdc_reader(reader_hosts)

    with c.override(
        native_mz(
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
        _wait_for_cdc_reader(reader_hosts, expected_host=old_reader)
        # The candidate adapter restarts on promotion, but its replicas stay warm.
        with c.sql_connection(service="mz_new") as connection:
            assert connection.execute("SELECT * FROM mysql_source_cnt").fetchall() == [
                (total,)
            ]
        # Retire mz_old only after handover and connector recovery are verified.
        c.promote_mz("mz_new", retire_mz_service=None)
        start_time = time.time()
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )
        # Count replacement SQL startup as promotion, not hydrated query time.
        c.sql_connection(service="mz_new", reuse_connection=True)
        elapsed = time.time() - start_time
        print(f"promotion took {elapsed} seconds")
        start_time = time.time()
        result = c.sql_query("SELECT * FROM mysql_source_cnt", service="mz_new")
        elapsed = time.time() - start_time
        print(f"final check took {elapsed} seconds")
        assert result[0][0] == total, f"Wrong result: {result}"
        assert (
            elapsed < 4 * HYDRATED_SELECT_FACTOR
        ), f"Took {elapsed}s to SELECT on MySQL source after 0dt upgrade, is it hydrated?"
        promoted_reader = _wait_for_cdc_reader(reader_hosts, excluded_host=old_reader)

        result = c.sql_query(
            "SELECT count(*) FROM mysql_source_table", service="mz_new"
        )
        assert result[0][0] == total, f"Wrong result: {result}"

        # Verify source still works after promotion (small batch is enough)
        print("Ingesting again")
        post_repeats = 10
        post_total = total + count * post_repeats
        _mysql_direct_ingest(c, "mysql_source_table", count, post_repeats)
        _wait_for_mz_count(
            c,
            "SELECT * FROM mysql_source_cnt",
            post_total,
            service="mz_new",
        )

        result = c.sql_query("SELECT * FROM mysql_source_cnt", service="mz_new")
        assert result[0][0] == post_total, f"Wrong result: {result}"

        result = c.sql_query(
            "SELECT count(*) FROM mysql_source_table", service="mz_new"
        )
        assert result[0][0] == post_total, f"Wrong result: {result}"

        # A reconnect must not revive the superseded reader while it serves SQL.
        readers = upstream(
            "SELECT ID FROM information_schema.PROCESSLIST "
            "WHERE USER = 'mysql1' AND COMMAND LIKE 'Binlog Dump%'"
        )
        assert readers, "no binlog reader to terminate"
        for (reader_id,) in readers:
            upstream(f"KILL CONNECTION {int(reader_id)}")
        _mysql_direct_ingest(c, "mysql_source_table", 1, 1)
        for service in ("mz_new", "mz_old"):
            _wait_for_mz_count(
                c, "SELECT * FROM mysql_source_cnt", post_total + 1, service=service
            )
        _wait_for_cdc_reader(reader_hosts, expected_host=promoted_reader)

        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        c.kill_fenced_mz("mz_old")
        _mysql_direct_ingest(c, "mysql_source_table", 1, 1)
        _wait_for_mz_count(
            c, "SELECT * FROM mysql_source_cnt", post_total + 2, service="mz_new"
        )


def workflow_sql_server_source_rehydration(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Verify shared-source prewarming and reader handover with both deployments alive."""
    parser.add_argument("--rows-per-batch", type=int, default=1000)
    args = parser.parse_args()
    if not 1 <= args.rows_per_batch <= 1000:
        parser.error("rows per batch must be between 1 and SQL Server's 1000-row limit")
    c.down(destroy_volumes=True)
    c.up("sql-server", "mz_old", Service("testdrive", idle=True))
    setup(c)
    setup_sql_server_testing(c)

    # The number of row value expressions in the INSERT statement exceeds the maximum allowed number of 1000 row values.
    count = args.rows_per_batch
    repeats = 100
    total = count * repeats

    insert_stmt = (
        "INSERT INTO sql_server_source_table VALUES "
        + ", ".join([f"({i})" for i in range(count)])
        + ";"
    )

    def upstream(sql: str) -> list[str]:
        result = c.exec(
            "sql-server",
            "bash",
            "-c",
            "SQLCMD=/opt/mssql-tools18/bin/sqlcmd; "
            '[ -x "$SQLCMD" ] || SQLCMD=/opt/mssql-tools/bin/sqlcmd; '
            'exec "$SQLCMD" -b -C -S localhost -U sa -P "$1" '
            '-h -1 -W -Q "$2"',
            "sqlcmd",
            SqlServer.DEFAULT_SA_PASSWORD,
            "SET NOCOUNT ON; " + sql,
            capture=True,
            silent=True,
        )
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    # Observe CDC polling, not idle auxiliary connections. The recent-activity
    # bound also excludes a detached connection after its reader stopped polling.
    reader_sessions = (
        "FROM sys.dm_exec_sessions s "
        "JOIN sys.dm_exec_connections c ON c.session_id = s.session_id "
        "LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id "
        "CROSS APPLY sys.dm_exec_sql_text(COALESCE(r.sql_handle, c.most_recent_sql_handle)) q "
        "WHERE s.program_name = 'materialize' "
        "AND c.client_net_address IS NOT NULL "
        "AND (r.session_id IS NOT NULL "
        "OR s.last_request_start_time >= DATEADD(second, -5, SYSDATETIME())) "
        "AND (q.text LIKE '%fn_cdc_get_max_lsn%' "
        "OR q.text LIKE '%fn_cdc_get_all_changes_%')"
    )

    def reader_hosts() -> list[str]:
        return upstream("SELECT DISTINCT c.client_net_address " + reader_sessions)

    # Setup table and enable CDC
    c.testdrive(
        dedent(f"""
        > SET CLUSTER = cluster;

        $ sql-server-connect name=sql-server
        server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

        $ sql-server-execute name=sql-server
        USE test;
        CREATE TABLE sql_server_source_table (f1 INTEGER);
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sql_server_source_table', @role_name = 'SA', @supports_net_changes = 0;
        """),
        quiet=True,
    )

    # Bulk-insert all data in one testdrive call (avoid per-call overhead)
    start_time = time.time()
    _sql_server_direct_ingest(c, insert_stmt, repeats)
    elapsed = time.time() - start_time
    print(f"initial ingestion took {elapsed} seconds")

    # Create MZ source and wait for snapshot to complete
    c.testdrive(
        dedent(f"""
        > SET CLUSTER = cluster;

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
        {total}
        """),
        quiet=True,
    )

    old_reader = _wait_for_cdc_reader(reader_hosts)

    with c.override(
        native_mz(
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
        _wait_for_cdc_reader(reader_hosts, expected_host=old_reader)
        with c.sql_connection(service="mz_new") as connection:
            assert connection.execute(
                "SELECT * FROM sql_server_source_cnt"
            ).fetchall() == [(total,)]
        # Keep the old deployment alive through handover and connector recovery.
        c.promote_mz("mz_new", retire_mz_service=None)
        start_time = time.time()
        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )
        # The adapter restarts on promotion, but its replicas stay warm.
        # Include SQL startup in promotion rather than hydrated query time.
        c.sql_connection(service="mz_new", reuse_connection=True)
        elapsed = time.time() - start_time
        print(f"promotion took {elapsed} seconds")
        start_time = time.time()
        result = c.sql_query("SELECT * FROM sql_server_source_cnt", service="mz_new")
        elapsed = time.time() - start_time
        print(f"final check took {elapsed} seconds")
        assert result[0][0] == total, f"Wrong result: {result}"
        assert (
            elapsed < 4 * HYDRATED_SELECT_FACTOR
        ), f"Took {elapsed}s to SELECT on SQL Server source after 0dt upgrade, is it hydrated?"
        promoted_reader = _wait_for_cdc_reader(reader_hosts, excluded_host=old_reader)

        result = c.sql_query(
            "SELECT count(*) FROM sql_server_source_table", service="mz_new"
        )
        assert result[0][0] == total, f"Wrong result: {result}"

        # Verify source still works after promotion (small batch is enough)
        print("Ingesting again")
        post_repeats = 10
        post_total = total + count * post_repeats
        _sql_server_direct_ingest(c, insert_stmt, post_repeats)
        _wait_for_mz_count(
            c,
            "SELECT * FROM sql_server_source_cnt",
            post_total,
            service="mz_new",
        )

        result = c.sql_query("SELECT * FROM sql_server_source_cnt", service="mz_new")
        assert result[0][0] == post_total, f"Wrong result: {result}"

        result = c.sql_query(
            "SELECT count(*) FROM sql_server_source_table", service="mz_new"
        )
        assert result[0][0] == post_total, f"Wrong result: {result}"

        readers = upstream(
            "SELECT TOP 1 c.session_id "
            + reader_sessions
            + f" AND c.client_net_address = '{promoted_reader}'"
        )
        assert len(readers) == 1, "no promoted CDC polling session to terminate"
        # Either polling operator's failure restarts the ingestion. Its teardown
        # can close sibling sessions, so do not KILL a captured list of their IDs.
        upstream(f"KILL {int(readers[0])}")
        _sql_server_direct_ingest(
            c, "INSERT INTO sql_server_source_table VALUES (0);", 1
        )
        for service in ("mz_new", "mz_old"):
            _wait_for_mz_count(
                c,
                "SELECT * FROM sql_server_source_cnt",
                post_total + 1,
                service=service,
            )
        _wait_for_cdc_reader(reader_hosts, expected_host=promoted_reader)

        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        c.kill_fenced_mz("mz_old")
        _sql_server_direct_ingest(
            c, "INSERT INTO sql_server_source_table VALUES (0);", 1
        )
        _wait_for_mz_count(
            c, "SELECT * FROM sql_server_source_cnt", post_total + 2, service="mz_new"
        )


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
        "kafka",
        "schema-registry",
        Service("testdrive", idle=True),
    )

    # Start the original Materialized instance with the failpoint enabled.
    with c.override(
        native_mz(
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

        c.testdrive(dedent("""
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
                """))

    with c.override(
        native_mz(
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
        c.promote_mz("mz_new", retire_mz_service=None)

        c.await_mz_deployment_status(
            DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None
        )
        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        c.kill_fenced_mz("mz_old")

        # Verify that the Kafka source's status is marked as "running" in mz_source_statuses.
        c.testdrive(dedent("""
                > SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'kafka_source_fp';
                running
                """))


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


def workflow_builtin_schema_migrations_replacement(c: Composition) -> None:
    """
    Verify builtin schema migrations by shard replacement.
    """
    raise SkipTest(
        "Unsupported: positive native builtin schema migration (replacement) "
        "coverage is deferred. Native startup rejects forced migrations."
    )

    def get_persist_shard_id(item_id: str, service: str) -> str:
        # `mz_storage_shards` is not linearizable, so we might have to wait for
        # the shard to show up.
        for _ in range(5):
            result = c.sql_query(
                f"SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id = '{item_id}'",
                service=service,
                reuse_connection=False,
            )
            if result:
                return result[0][0]
            time.sleep(1)

        raise RuntimeError(f"no shard found for item {item_id}")

    c.down(destroy_volumes=True)
    c.up("mz_old")
    c.sql(
        "CREATE MATERIALIZED VIEW mv AS SELECT name FROM mz_tables",
        service="mz_old",
        port=6877,
        user="mz_system",
    )

    mz_tables_gid = c.sql_query(
        "SELECT id FROM mz_materialized_views WHERE name = 'mz_tables'",
        service="mz_old",
    )[0][0]
    mv_gid = c.sql_query(
        "SELECT id FROM mz_materialized_views WHERE name = 'mv'",
        service="mz_old",
    )[0][0]

    mz_tables_shard_id = get_persist_shard_id(mz_tables_gid, "mz_old")
    mv_shard_id = get_persist_shard_id(mv_gid, "mz_old")

    with c.override(
        native_mz(
            name="mz_new",
            sanity_restart=False,
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            restart="on-failure",
            external_metadata_store=True,
            force_migrations="replacement",
            healthcheck=LEADER_STATUS_HEALTHCHECK,
            default_replication_factor=2,
        ),
    ):
        c.up("mz_new")
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")

        # The new generation is ready to promote while still read-only, so its migrated builtin MVs
        # must already be hydrated off their replacement shards. If they were excluded from the
        # caught-up gate they would hydrate at cut-over instead, spiking catalog-server CPU. Reading
        # them here from the read-only generation asserts the write-enable-while-read-only path
        # actually filled those shards.
        #
        # NOTE: this covers hydration, not the caught-up gate's lag comparison.
        # `force_migrations="replacement"` replaces `mz_cluster_replica_frontiers` too, and the gate
        # reads the leader's "live" frontiers out of that collection, so here it compares this
        # generation against itself.
        for relation in ["mz_databases", "mz_clusters"]:
            count = c.sql_query(
                f"SELECT count(*) FROM mz_catalog.{relation}",
                service="mz_new",
                reuse_connection=False,
            )[0][0]
            assert count > 0, f"{relation} returned {count} on the read-only generation"

        c.promote_mz("mz_new", retire_mz_service="mz_old")
        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")

        new_mz_tables_gid = c.sql_query(
            "SELECT id FROM mz_materialized_views WHERE name = 'mz_tables'",
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
        new_mz_tables_shard_id = get_persist_shard_id(mz_tables_gid, "mz_new")
        new_mv_shard_id = get_persist_shard_id(mv_gid, "mz_new")
        assert new_mz_tables_shard_id != mz_tables_shard_id
        assert new_mv_shard_id == mv_shard_id

        reused, replaced = fetch_reconciliation_metrics(c, "mz_new")
        assert reused > 0
        assert (
            replaced == 0
        ), f"{replaced} dataflows have been replaced, expected all to be reused"


def workflow_builtin_schema_migrations_evolution(c: Composition) -> None:
    """
    Verify builtin schema migrations by schema evolution.
    """
    raise SkipTest(
        "Unsupported: positive native builtin schema migration (evolution) "
        "coverage is deferred. Native startup rejects forced migrations."
    )

    def get_persist_shard_id(item_id: str, service: str) -> str:
        # `mz_storage_shards` is not linearizable, so we might have to wait for
        # the shard to show up.
        for _ in range(5):
            result = c.sql_query(
                f"SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id = '{item_id}'",
                service=service,
                reuse_connection=False,
            )
            if result:
                return result[0][0]
            time.sleep(1)

        raise RuntimeError(f"no shard found for item {item_id}")

    c.down(destroy_volumes=True)
    c.up("mz_old")
    c.sql(
        "CREATE MATERIALIZED VIEW mv AS SELECT name FROM mz_tables",
        service="mz_old",
        port=6877,
        user="mz_system",
    )

    mz_tables_gid = c.sql_query(
        "SELECT id FROM mz_materialized_views WHERE name = 'mz_tables'",
        service="mz_old",
    )[0][0]
    mv_gid = c.sql_query(
        "SELECT id FROM mz_materialized_views WHERE name = 'mv'",
        service="mz_old",
    )[0][0]

    mz_tables_shard_id = get_persist_shard_id(mz_tables_gid, "mz_old")
    mv_shard_id = get_persist_shard_id(mv_gid, "mz_old")

    with c.override(
        native_mz(
            name="mz_new",
            sanity_restart=False,
            deploy_generation=1,
            system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
            restart="on-failure",
            external_metadata_store=True,
            force_migrations="evolution",
            healthcheck=LEADER_STATUS_HEALTHCHECK,
            default_replication_factor=2,
        ),
    ):
        c.up("mz_new")

        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        c.promote_mz("mz_new", retire_mz_service="mz_old")
        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")

        new_mz_tables_gid = c.sql_query(
            "SELECT id FROM mz_materialized_views WHERE name = 'mz_tables'",
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
        new_mz_tables_shard_id = get_persist_shard_id(mz_tables_gid, "mz_new")
        new_mv_shard_id = get_persist_shard_id(mv_gid, "mz_new")
        assert new_mz_tables_shard_id == mz_tables_shard_id
        assert new_mv_shard_id == mv_shard_id

        reused, replaced = fetch_reconciliation_metrics(c, "mz_new")
        assert reused > 0
        assert (
            replaced == 0
        ), f"{replaced} dataflows have been replaced, expected all to be reused"


def workflow_materialized_view_correction_pruning(c: Composition) -> None:
    """
    Verify that a candidate MV sink prunes its snapshot before output is enabled.

    Candidate adapters serve writes, but their maintained Persist outputs are
    not enabled until promotion. This still exercises non-writing sink hydration.
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
    # SQL listener readiness precedes replica provisioning. Observe the candidate's
    # execution readiness before querying it or timing correction-buffer pruning.
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
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

    # The correction buffer should stabilize in a state where it has seen at
    # least 1000 insertions (the size of the snapshot) and as many deletions:
    # all snapshot updates are consolidated away on the non-writing side, leaving
    # zero records in the correction buffer.
    #
    # Note: the metric counts net length deltas, not raw record insertions, so
    # we don't see 2000 here even though both the desired and the
    # persist-retraction sides flow through the buffer — by the time the
    # second insert's `update_metrics` runs, consolidation has already taken
    # the length back down.
    for _ in range(10):
        time.sleep(1)
        insertions, deletions = get_correction_metrics()
        if insertions >= 1000 and insertions - deletions == 0:
            break
    else:
        raise AssertionError(
            f"unexpected correction metrics: {insertions=}, {deletions=}"
        )


def workflow_materialized_view_candidate_correction_pruning(
    c: Composition,
) -> None:
    """Bound a candidate MV's correction buffer while its peer keeps writing.

    Candidate adapters admit SQL writes, but their maintained Persist outputs
    remain disabled until promotion. A non-writing sink mints no batches, so
    WriteBatch cannot advance consolidation. Forced consolidation must cancel
    desired/persist pairs as forward writes arrive, not just at hydration.
    Otherwise the buffer retains roughly one record per written row (CLU-131).

    Scrape a candidate quickstart replica directly. Container anonymous memory
    also covers its peers, adapter and controller, preserving the memory bound.
    """

    c.down(destroy_volumes=True)
    c.up("mz_old")

    initial_rows = 1_000
    batch_rows = 10_000
    forward_rows = 300_000
    payload_bytes = 1_024
    memory_growth_bound_bytes = 384 * 1024**2

    c.sql(
        f"""
         CREATE TABLE t (a int, payload text);
         INSERT INTO t
           SELECT g, repeat('x', {payload_bytes})
           FROM generate_series(1, {initial_rows}) AS g;
         CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;
         SELECT * FROM mv LIMIT 1;
         """,
        service="mz_old",
    )

    # The candidate is never promoted. Its SQL adapter can write, but the
    # maintained sink stays non-writing throughout the forward-input workload.
    c.up("mz_new")
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
    c.sql("SELECT * FROM mv LIMIT 1", service="mz_new")

    inventory = json.loads(
        c.exec(
            "mz_new",
            "curl",
            "--fail",
            "--silent",
            "--max-time",
            "10",
            "http://localhost:6882/api/inventory",
            capture=True,
        ).stdout
    )
    replicas = [r for r in inventory["replicas"] if r["cluster_name"] == "quickstart"]
    assert replicas, inventory
    replica = min(replicas, key=lambda r: int(r["replica_id"][1:]))
    assert int(replica["generation"]) == 1, replica
    addresses = replica["endpoints"]["internal-http"]
    assert addresses, replica

    def replica_processes() -> str:
        processes = c.exec(
            "mz_new", "ps", "-C", "clusterd", "-o", "pid=,lstart=", capture=True
        ).stdout.strip()
        assert processes, "no candidate replicas"
        return processes

    # A restart would discard the forward-write buffer and measure snapshot
    # hydration instead. It must not count as successful incremental pruning.
    initial_processes = replica_processes()

    def anon_bytes() -> int:
        # Anonymous (heap) memory of the mz_new container from the cgroup, where
        # the correction buffer lives. cgroup v2 reports `anon`; v1 `total_rss`
        # or `rss`.
        return int(
            c.exec(
                "mz_new",
                "sh",
                "-lc",
                r"""awk '$1=="anon" || $1=="total_rss" || $1=="rss" { print $2; exit }' \
                    /sys/fs/cgroup/memory.stat 2>/dev/null || \
                    awk '$1=="anon" || $1=="total_rss" || $1=="rss" { print $2; exit }' \
                    /sys/fs/cgroup/memory/memory.stat""",
                capture=True,
            ).stdout
        )

    def mib(value: int) -> str:
        return f"{value / 1024**2:.1f} MiB"

    def correction_metrics() -> tuple[int, int]:
        insertions = deletions = 0
        for address in addresses:
            transport = (
                ["--unix-socket", address, "http://localhost/metrics"]
                if address.startswith("/")
                else [f"http://{address}/metrics"]
            )
            resp = c.exec(
                "mz_new",
                "curl",
                "--fail",
                "--silent",
                "--max-time",
                "10",
                *transport,
                capture=True,
            ).stdout
            metrics = {}
            for line in resp.splitlines():
                if line.startswith("#"):
                    continue
                name, value = line.rsplit(maxsplit=1)
                if name in (
                    "mz_persist_sink_correction_insertions_total",
                    "mz_persist_sink_correction_deletions_total",
                ):
                    metrics[name] = int(float(value))
            insertions += metrics["mz_persist_sink_correction_insertions_total"]
            deletions += metrics["mz_persist_sink_correction_deletions_total"]
        return (insertions, deletions)

    # Wait for the candidate to process and prune the initial snapshot
    # before measuring the memory baseline. The snapshot is small enough not to
    # matter for RSS, but this preserves the older test's signal.
    insertions = deletions = 0
    for _ in range(30):
        insertions, deletions = correction_metrics()
        if insertions >= initial_rows and insertions - deletions == 0:
            break
        time.sleep(1)
    else:
        raise AssertionError(
            f"snapshot correction buffer did not consolidate: {insertions=}, "
            f"{deletions=}, net={insertions - deletions}"
        )

    baseline_anon = anon_bytes()

    # The active generation writes while the candidate sink remains non-writing.
    # Wide payloads make the retained correction buffer visible in anonymous RSS:
    # on an unfixed build the desired and persist halves retain roughly
    # `2 * forward_rows * payload_bytes`, while the fixed build keeps at most a
    # small batch resident before forced consolidation drains it.
    for lo in range(initial_rows + 1, initial_rows + forward_rows + 1, batch_rows):
        hi = min(lo + batch_rows - 1, initial_rows + forward_rows)
        c.sql(
            f"""
            INSERT INTO t
              SELECT g, repeat('x', {payload_bytes})
              FROM generate_series({lo}, {hi}) AS g;
            """,
            service="mz_old",
        )
        _wait_for_mz_count(c, "SELECT count(*) FROM mv", hi, "mz_old")
        time.sleep(1)

    # The non-writing sink must consolidate the forward writes away rather than
    # retain one record per written row. With the fix the buffer holds at most a
    # batch or so in flight; before it, `insertions - deletions` stays near
    # `forward_rows` (CLU-131) and the wide rows push mz_new's anonymous memory
    # hundreds of MiB above baseline.
    correction_bound = batch_rows * 4
    memory_ceiling = baseline_anon + memory_growth_bound_bytes
    for _ in range(150):
        insertions, deletions = correction_metrics()
        settled_anon = anon_bytes()
        if (
            insertions >= forward_rows
            and insertions - deletions < correction_bound
            and settled_anon <= memory_ceiling
        ):
            break
        time.sleep(1)

    net = insertions - deletions
    memory_growth = settled_anon - baseline_anon
    if (
        insertions < forward_rows
        or net >= correction_bound
        or settled_anon > memory_ceiling
    ):
        raise AssertionError(
            f"candidate correction buffer did not consolidate: {insertions=}, "
            f"{deletions=}, net={net} (bound {correction_bound}, "
            f"forward_rows {forward_rows}, memory_growth={mib(memory_growth)}, "
            f"memory_bound={mib(memory_growth_bound_bytes)}); CLU-131"
        )

    assert replica_processes() == initial_processes, "candidate replicas restarted"
    print(
        f"Candidate correction pruning: {insertions=}, {deletions=}, {net=}, "
        f"memory_growth={mib(memory_growth)}, bound={mib(memory_growth_bound_bytes)}"
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
        "kafka",
        "schema-registry",
        "mz_old",
        Service("testdrive", idle=True),
    )
    num_threads = 50

    setup(c)

    c.testdrive(dedent("""
        > SET CLUSTER = cluster;

        > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL = 'PLAINTEXT';
        > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';
            """))

    end_time = datetime.now(UTC) + timedelta(seconds=200)
    mz1 = "mz_old"
    mz2 = "mz_new"

    stop = Event()
    errors: SimpleQueue[Exception] = SimpleQueue()

    def ingest(i: int) -> None:
        c.testdrive(dedent(f"""
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
                """))

        while not stop.is_set() and datetime.now(UTC) < end_time:
            try:
                c.testdrive(dedent(f"""
                    $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka{i} repeat=10000
                    key1A,key1B:value1A,value1B
                    """))
            except Exception:
                # Ingest can race deployment retirement. Setup failures below
                # are not expected and must fail the parent workflow.
                pass

    def worker(i: int) -> None:
        try:
            if not stop.is_set():
                ingest(i)
        except Exception as error:
            errors.put(error)
            stop.set()

    threads = []
    for i in range(num_threads):
        thread = Thread(name=f"worker_{i}", target=worker, args=(i,))
        threads.append(thread)

    try:
        for thread in threads:
            thread.start()

        i = 1
        while not stop.is_set() and datetime.now(UTC) < end_time:
            with c.override(
                native_mz(
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
                c.promote_mz(mz2, retire_mz_service=None)
                c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, mz2)
                c.retire_mz_deployment(
                    mz2, i - 1, environment_id=DEFAULT_MZ_ENVIRONMENT_ID
                )
                c.kill_fenced_mz(mz1)

            i += 1
            mz1, mz2 = mz2, mz1

    finally:
        # Workers share the Composition. Stop them before its owner can tear
        # down services, or retries can recreate containers during cleanup.
        stop.set()
        for thread in threads:
            if thread.ident is not None:
                thread.join()

    if not errors.empty():
        raise errors.get()


def workflow_ddl(c: Composition) -> None:
    """Keep both deployments writable through DDL and promotion, then retire one."""

    def assert_shared_index(index: str, expected: list[tuple[int, int]]) -> None:
        for service in ("mz_old", "mz_new"):
            with c.sql_connection(service=service) as conn:
                conn.execute("SET cluster = cluster")
                plan = conn.execute(
                    "EXPLAIN PHYSICAL PLAN FOR SELECT * FROM t"
                ).fetchone()
                assert plan and index in plan[0], (service, plan)
                assert conn.execute("SELECT * FROM t ORDER BY a").fetchall() == expected

    def warmed_indexes(services: Sequence[str]) -> dict[str, Any]:
        observed = {}
        for service in services:
            processes = c.exec(
                service, "ps", "-C", "clusterd", "-o", "pid=,lstart=", capture=True
            ).stdout.strip()
            assert processes, (service, "no warmed replicas")
            dataflows = {}
            with c.sql_connection(service=service, port=6877, user="mz_system") as conn:
                replicas = conn.execute("""
                    SELECT r.name FROM mz_catalog.mz_cluster_replicas r
                    JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
                    WHERE c.name = 'cluster' ORDER BY r.name
                """).fetchall()
                assert replicas
                conn.execute("SET auto_route_catalog_queries = false")
                conn.execute("SET cluster = cluster")
                conn.execute("SET statement_timeout = '30s'")
                for (replica,) in replicas:
                    conn.execute(
                        SQL("SET cluster_replica = {}").format(Identifier(replica))
                    )
                    # Installation logs alone do not establish hydration. Make
                    # this replica serve the indexed relation before recording
                    # the execution identity that must survive promotion.
                    plan = conn.execute(
                        "EXPLAIN PHYSICAL PLAN FOR SELECT * FROM t"
                    ).fetchone()
                    assert plan and "t_idx_peer" in plan[0], (service, replica, plan)
                    assert conn.execute("SELECT * FROM t").fetchall()
                    deadline = time.monotonic() + 60
                    while True:
                        rows = conn.execute("""
                            SELECT e.export_id, e.worker_id, e.dataflow_id
                            FROM mz_introspection.mz_compute_exports_per_worker e
                            JOIN mz_internal.mz_object_global_ids g ON g.global_id = e.export_id
                            JOIN mz_catalog.mz_indexes i ON i.id = g.id
                            WHERE i.name = 't_idx_peer'
                            ORDER BY e.export_id, e.worker_id, e.dataflow_id
                        """).fetchall()
                        # setup() uses two processes with four workers each.
                        if {int(row[1]) for row in rows} == set(range(8)):
                            dataflows[replica] = rows
                            break
                        assert time.monotonic() < deadline, (service, replica, rows)
                        time.sleep(1)
            observed[service] = (processes, dataflows)
        return observed

    sink_incarnations: dict[tuple[int, str], str] = {}

    def await_sink_owner(
        generation: int,
        services: Sequence[str],
        after: dict[tuple[int, str, str, int], float] | None = None,
    ) -> dict[tuple[int, str, str, int], float]:
        sink_id = c.sql_query(
            """SELECT g.global_id FROM mz_internal.mz_object_global_ids g
               JOIN mz_catalog.mz_sinks s ON s.id = g.id WHERE s.name = 'kafka_sink'""",
            service="mz_new",
            port=6877,
            user="mz_system",
            reuse_connection=False,
        )[0][0]
        deadline = time.monotonic() + 300
        while True:
            committed = {}
            expected_replicas = set()
            selected = None
            selected_addresses = []
            for service in services:
                inventory = json.loads(
                    c.exec(
                        service,
                        "curl",
                        "--fail",
                        "--silent",
                        "--max-time",
                        "10",
                        "http://localhost:6882/api/inventory",
                        capture=True,
                    ).stdout
                )
                replicas = [
                    r for r in inventory["replicas"] if r["cluster_name"] == "cluster"
                ]
                assert replicas, inventory
                if int(replicas[0]["generation"]) == generation:
                    selected = min(replicas, key=lambda r: int(r["replica_id"][1:]))[
                        "replica_id"
                    ]
                for replica in replicas:
                    identity = (int(replica["generation"]), replica["replica_id"])
                    expected_replicas.add(identity)
                    addresses = replica["endpoints"]["internal-http"]
                    assert len(addresses) == 2, replica
                    if identity == (generation, selected):
                        selected_addresses = addresses
                    for address in addresses:
                        transport = (
                            ["--unix-socket", address, "http://localhost/metrics"]
                            if address.startswith("/")
                            else [f"http://{address}/metrics"]
                        )
                        metrics = c.exec(
                            service,
                            "curl",
                            "--fail",
                            "--silent",
                            "--max-time",
                            "10",
                            *transport,
                            capture=True,
                        ).stdout
                        for line in metrics.splitlines():
                            if (
                                line.startswith("mz_sink_messages_committed{")
                                and f'sink_id="{sink_id}"' in line
                            ):
                                worker = re.search(r'worker_id="(\d+)"', line)
                                assert worker, line
                                key = (*identity, address, int(worker[1]))
                                assert key not in committed, (key, line)
                                committed[key] = float(line.rsplit(" ", 1)[1])
            incarnations = c.sql_query(
                """SELECT COALESCE((data->'value'->>'deployment_generation')::bigint, 0),
                          mz_internal.parse_catalog_id(data->'value'->'realization'->'Replica'),
                          data->'key'->>'id'
                   FROM mz_internal.mz_catalog_shared
                   WHERE data->>'kind' = 'ClientIncarnation'
                     AND data->'value'->'realization'->'Replica' IS NOT NULL""",
                service="mz_new",
                port=6877,
                user="mz_system",
                reuse_connection=False,
            )
            current = {
                (gen, replica): incarnation
                for gen, replica, incarnation in incarnations
                if (gen, replica) in expected_replicas
            }
            assert set(current) == expected_replicas, (current, expected_replicas)
            for identity, incarnation in current.items():
                assert (
                    sink_incarnations.setdefault(identity, incarnation) == incarnation
                ), (identity, sink_incarnations, current)
            rows = c.sql_query(
                SQL("""SELECT deployment_generation::bigint, replica_id, status
                       FROM mz_internal.mz_storage_statuses_raw WHERE id = {}
                       ORDER BY deployment_generation, replica_id, status""")
                .format(Literal(sink_id))
                .as_string(),
                service="mz_new",
                port=6877,
                user="mz_system",
                reuse_connection=False,
            )
            authority = c.sql_query(
                "SELECT active_deployment_generation::bigint FROM mz_internal.mz_catalog_deployment_authority",
                service="mz_new",
                port=6877,
                user="mz_system",
                reuse_connection=False,
            )
            expected = (generation, selected)
            # This sink is never altered/recreated and replicas retain their
            # incarnations. Worker statistics share ownership with the producing
            # future, including across restarts. Their absence on every old
            # process proves that future stopped, not merely that status was
            # hidden or Kafka fenced it. Already-approved blocking Kafka calls
            # may finish later, as allowed by the handover contract.
            if (
                authority == [(generation,)]
                and rows == [(generation, selected, "running")]
                and len(committed) == 8
                and {key[:2] for key in committed} == {expected}
                and {key[3] for key in committed} == set(range(8))
                and all(
                    sum(key[2] == address for key in committed) == 4
                    for address in selected_addresses
                )
                and sum(value > 0 for value in committed.values()) <= 1
                and (
                    after is None
                    or (
                        committed.keys() == after.keys()
                        and all(committed[key] >= after[key] for key in after)
                        and any(committed[key] > after[key] for key in after)
                    )
                )
            ):
                print(f"sink owner: {expected}, worker commits: {committed}")
                return committed
            assert time.monotonic() < deadline, (
                authority,
                rows,
                committed,
                expected,
                after,
            )
            time.sleep(1)

    c.down(destroy_volumes=True)
    c.up(
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

    # Establish sources, a sink and table-backed views before the peer joins.
    c.testdrive(dedent(f"""
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
        """))

    # A compatible candidate serves writes before promotion.
    c.up("mz_new")
    # SQL listeners can be ready before policy provisions deployment-local
    # replicas. Testdrive's initial catalog queries require those replicas.
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")

    # Exercise candidate writes and DDL against the same shared objects.
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
        c.testdrive(dedent(f"""
            $ webhook-append database=materialize schema=public name=webhook_source
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
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (Storage inputs: [<>]): [<> <>]\\n"

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
            <null> <null> 3 4
            <null> <null> 5 6

            > SELECT * FROM webhook_source
            AAA
            BBB

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
            > BEGIN
            > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            > FETCH ALL c WITH (timeout='5s');
            <TIMESTAMP> 1 1
            <TIMESTAMP> 1 3
            <TIMESTAMP> 1 5
            > COMMIT
            # Also exercise a direct finite subscription.
            > SUBSCRIBE (WITH a(x) AS (SELECT 'a') SELECT generate_series(1, 2), x FROM a)
            <TIMESTAMP> 1 1 a
            <TIMESTAMP> 1 2 a
            """))

    assert_shared_index("t_idx2", [(1, 2), (3, 4), (5, 6)])

    # Verify the candidate sees committed peer DDL as well as its own writes.
    c.up(Service("testdrive", idle=True))
    c.testdrive(dedent(f"""
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
        > INSERT INTO t VALUES (7, 8);
        > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
        > SELECT * FROM mv;
        16
        > SELECT max(b) FROM t;
        8
        > SELECT mz_unsafe.mz_sleep(5)
        <null>
        > INSERT INTO t VALUES (9, 10);
        > SELECT * FROM mv;
        25
        > DROP INDEX t_idx2
        > CREATE INDEX t_idx_peer ON t (a, b)
        > CREATE MATERIALIZED VIEW mv_peer AS SELECT sum(a) FROM t;

        $ set-regex match=(s\\d+|\\d{{13}}|[ ]{{12}}0|u\\d{{1,3}}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

        > EXPLAIN TIMESTAMP FOR SELECT * FROM mv;
        "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n\\nbinding constraints:\\nlower:\\n  (Storage inputs: [<>]): [<> <>]\\n"

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
        <null> <null> 7 8
        <null> <null> 9 10
        > SELECT * FROM webhook_source
        AAA
        BBB
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
        <TIMESTAMP> 1 7
        <TIMESTAMP> 1 9
        > COMMIT

        > CREATE TABLE t4 (a INT);
        """))

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
        c.testdrive(dedent("""
            $ webhook-append database=materialize schema=public name=webhook_source
            DDD

            > SELECT count(*) FROM t1;
            0
            > SELECT count(*) FROM t4;
            0
            > SET CLUSTER = cluster;
            > SELECT 1
            1
            > INSERT INTO t VALUES (11, 12);
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM mv;
            36
            > SELECT max(b) FROM t;
            12
            > SELECT * FROM mv;
            36
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
            <null> <null> 7 8
            <null> <null> 9 10
            <null> <null> 11 12
            > SELECT * FROM webhook_source
            AAA
            BBB
            CCC
            DDD

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{13,20} replacement=<TIMESTAMP>
            > BEGIN
            > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            > FETCH ALL c WITH (timeout='5s');
            <TIMESTAMP> 1 1
            <TIMESTAMP> 1 3
            <TIMESTAMP> 1 5
            <TIMESTAMP> 1 7
            <TIMESTAMP> 1 9
            <TIMESTAMP> 1 11
            > COMMIT
            # Also exercise a direct finite subscription.
            > SUBSCRIBE (WITH a(x) AS (SELECT 'a') SELECT generate_series(1, 2), x FROM a)
            <TIMESTAMP> 1 1 a
            <TIMESTAMP> 1 2 a
            """))

        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        assert_shared_index("t_idx_peer", [(n, n + 1) for n in range(1, 12, 2)])
        warm = warmed_indexes(("mz_old", "mz_new"))
        initial_commits = await_sink_owner(0, ("mz_old", "mz_new"))
        assert sum(value > 0 for value in initial_commits.values()) == 1
        with c.sql_connection(service="mz_old") as old_conn:
            assert old_conn.execute("SELECT 1").fetchall() == [(1,)]
            c.promote_mz("mz_new", retire_mz_service=None)
            c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")
            # Preserve the candidate SQL recovery check independently of the
            # predecessor. A query before IsLeader could still hit the candidate
            # adapter that is about to restart rather than its replacement.
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

            assert warmed_indexes(("mz_old", "mz_new")) == warm
            promoted_commits = await_sink_owner(1, ("mz_old", "mz_new"))
            old_conn.execute("INSERT INTO t VALUES (13, 14)")
            old_conn.execute("CREATE TABLE promoted_peer (a int)")
            old_conn.execute("CREATE INDEX promoted_peer_idx ON promoted_peer (a)")
            with c.sql_connection(service="mz_new") as new_conn:
                new_conn.execute("CREATE TABLE promoted_candidate (a int)")
                new_conn.execute(
                    "CREATE INDEX promoted_candidate_idx ON promoted_candidate (a)"
                )
                new_conn.execute("INSERT INTO promoted_peer VALUES (42)")
                old_conn.execute("INSERT INTO promoted_candidate VALUES (43)")
                # Distinct sink markers from both writers while the older
                # deployment remains admitted. Zero preserves the existing sum
                # checkpoints, but must appear in the unfiltered sink envelope.
                new_conn.execute("INSERT INTO t VALUES (0, 0)")
                for conn in (old_conn, new_conn):
                    for table, value in (
                        ("promoted_peer", 42),
                        ("promoted_candidate", 43),
                    ):
                        plan = conn.execute(
                            SQL("EXPLAIN PHYSICAL PLAN FOR SELECT * FROM {}").format(
                                Identifier(table)
                            )
                        ).fetchone()
                        assert plan and f"{table}_idx" in plan[0], plan
                        assert conn.execute(
                            SQL("SELECT * FROM {}").format(Identifier(table))
                        ).fetchall() == [(value,)]
            assert_shared_index(
                "t_idx_peer", [(0, 0), *[(n, n + 1) for n in range(1, 14, 2)]]
            )
            c.testdrive(dedent("""
                > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source_tbl
                <null> <null> 0 0
                <null> <null> 1 2
                <null> <null> 3 4
                <null> <null> 5 6
                <null> <null> 7 8
                <null> <null> 9 10
                <null> <null> 11 12
                <null> <null> 13 14
            """))
            await_sink_owner(1, ("mz_old", "mz_new"), after=promoted_commits)
            assert warmed_indexes(("mz_old", "mz_new")) == warm

            c.retire_mz_deployment(
                "mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID
            )
            try:
                old_conn.execute("INSERT INTO t VALUES (17, 18)")
            except psycopg.Error as error:
                assert isinstance(error, OperationalError) or "has been retired" in str(
                    error
                ), f"Unexpected rejection from retired deployment: {error}"
            else:
                raise AssertionError("retired deployment accepted a new table write")

        c.kill_fenced_mz("mz_old")

        c.testdrive(dedent(f"""
            $ webhook-append database=materialize schema=public name=webhook_source
            EEE
            > SELECT count(*) FROM promoted_peer;
            1
            > SET CLUSTER = cluster;
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > CREATE MATERIALIZED VIEW mv3 AS SELECT sum(a) FROM t;
            > SELECT * FROM mv;
            49
            > SELECT * FROM mv2;
            49
            > SELECT * FROM mv3;
            49
            > SELECT * FROM mv_peer;
            49
            > SELECT max(b) FROM t;
            14
            > INSERT INTO t VALUES (15, 16);
            > SELECT * FROM mv;
            64
            > SELECT * FROM mv2;
            64
            > SELECT max(b) FROM t;
            16
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
            <null> <null> 0 0
            <null> <null> 1 2
            <null> <null> 3 4
            <null> <null> 5 6
            <null> <null> 7 8
            <null> <null> 9 10
            <null> <null> 11 12
            <null> <null> 13 14
            <null> <null> 15 16
            > SELECT * FROM webhook_source
            AAA
            BBB
            CCC
            DDD
            EEE

            $ set-max-tries max-tries=1
            $ set-regex match=\\d{{13,20}} replacement=<TIMESTAMP>
            > BEGIN
            > DECLARE c CURSOR FOR SUBSCRIBE (SELECT a FROM t);
            > FETCH ALL c WITH (timeout='5s');
            <TIMESTAMP> 1 0
            <TIMESTAMP> 1 1
            <TIMESTAMP> 1 3
            <TIMESTAMP> 1 5
            <TIMESTAMP> 1 7
            <TIMESTAMP> 1 9
            <TIMESTAMP> 1 11
            <TIMESTAMP> 1 13
            <TIMESTAMP> 1 15
            > COMMIT
            """))

        await_sink_owner(1, ("mz_new",))
        assert warmed_indexes(("mz_new",)) == {"mz_new": warm["mz_new"]}


def workflow_stuck_collection(c: Composition) -> None:
    """Verify that stuck collections don't hold up a 0dt deployment."""

    c.down(destroy_volumes=True)

    c.up("mz_old")

    c.sql(
        "ALTER SYSTEM SET with_0dt_caught_up_check_cutoff = '10s'",
        service="mz_old",
        port=6877,
        user="mz_system",
    )

    c.sql(
        """
        CREATE CLUSTER paused SIZE 'scale=1,workers=1', REPLICATION FACTOR 0;
        CREATE CLUSTER test SIZE 'scale=1,workers=1';

        CREATE TABLE t (a int);
        CREATE MATERIALIZED VIEW mv IN CLUSTER paused AS SELECT * FROM t;
        CREATE INDEX idx IN CLUSTER test ON mv (a);
        """,
        service="mz_old",
    )

    c.up("mz_new")
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
    c.promote_mz("mz_new", retire_mz_service=None)
    c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None)

    c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
    c.kill_fenced_mz("mz_old")


def workflow_caught_up_stability(c: Composition) -> None:
    """Verify the 0dt caught-up stability gate end-to-end.

    With a non-trivial stability period, a healthy deployment must still reach
    ReadyToPromote (the gate does not deadlock healthy clusters) and can be
    promoted without revoking the previous deployment's write authority.
    Retirement is explicit and leaves the warmed candidate and relay intact.
    The gate's reset-on-unhealthy behavior is hard to
    reproduce deterministically here, so it is covered by adapter unit tests
    that drive synthetic replica-health transitions directly.
    """

    c.down(destroy_volumes=True)

    c.up("mz_old")

    # Require clusters to stay caught-up and healthy for a while before we cut
    # over. mz_new reads this from the shared catalog.
    c.sql(
        "ALTER SYSTEM SET with_0dt_caught_up_check_stability_period = '20s'",
        service="mz_old",
        port=6877,
        user="mz_system",
    )

    c.sql(
        """
        CREATE TABLE t (a int);
        CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;
        CREATE INDEX mv_idx ON mv (a);
        INSERT INTO t VALUES (1), (2), (3);
        """,
        service="mz_old",
    )

    c.up("mz_new")
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")

    def warmed_processes() -> str:
        processes = c.exec(
            "mz_new", "ps", "-C", "clusterd", "-o", "pid=,lstart=", capture=True
        ).stdout.strip()
        assert processes, "candidate has no warmed replicas"
        return processes

    def index_dataflows() -> list[tuple[str, int]]:
        with c.sql_connection(service="mz_new", port=6877, user="mz_system") as conn:
            conn.execute("SET auto_route_catalog_queries = false")
            conn.execute("SET cluster = quickstart")
            conn.execute("SET cluster_replica = r1")
            conn.execute("SET statement_timeout = '30s'")
            deadline = time.monotonic() + 60
            while True:
                rows = conn.execute("""
                    SELECT e.export_id, e.dataflow_id
                    FROM mz_introspection.mz_compute_exports e
                    JOIN mz_internal.mz_object_global_ids g ON g.global_id = e.export_id
                    JOIN mz_catalog.mz_indexes i ON i.id = g.id
                    WHERE i.name = 'mv_idx'
                    ORDER BY e.export_id, e.dataflow_id
                    """).fetchall()
                if rows:
                    return rows
                assert time.monotonic() < deadline, "candidate index is not observed"
                time.sleep(1)

    relay = c.container_id("pubsub")
    assert relay is not None

    def relay_process() -> str:
        # Composition.exec prefixes the service entrypoint. Diagnostics must
        # execute directly, not become arguments to pubsubd.
        process = c.invoke(
            "exec",
            "-T",
            "pubsub",
            "ps",
            "-C",
            "pubsubd",
            "-o",
            "pid=,lstart=",
            capture=True,
        ).stdout.strip()
        assert process, "shared relay is not running"
        return process

    relay_start = relay_process()
    processes = warmed_processes()
    dataflows = index_dataflows()

    # Keep the incumbent session open across promotion. Candidate sessions may
    # restart at promotion, so use separate candidate connections on each side.
    with c.sql_connection(service="mz_old") as incumbent:
        incumbent.execute("CREATE TEMP TABLE session_state (a int)")
        incumbent.execute("INSERT INTO session_state VALUES (42)")
        incumbent.execute("CREATE TABLE incumbent_before (a int)")
        incumbent.execute("INSERT INTO t VALUES (4)")
        with c.sql_connection(service="mz_new") as candidate:
            candidate.execute("CREATE TABLE candidate_before (a int)")
            candidate.execute("INSERT INTO t VALUES (5)")
            assert candidate.execute("SELECT * FROM mv ORDER BY a").fetchall() == [
                (i,) for i in range(1, 6)
            ]

        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        c.promote_mz("mz_new", retire_mz_service=None)
        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")
        assert warmed_processes() == processes
        assert index_dataflows() == dataflows

        assert incumbent.execute("SELECT * FROM session_state").fetchall() == [(42,)]
        incumbent.execute("CREATE TABLE incumbent_after (a int)")
        incumbent.execute("INSERT INTO t VALUES (6)")
        with c.sql_connection(service="mz_new") as candidate:
            candidate.execute("CREATE TABLE candidate_after (a int)")
            candidate.execute("INSERT INTO t VALUES (7)")
            assert candidate.execute("SELECT * FROM mv ORDER BY a").fetchall() == [
                (i,) for i in range(1, 8)
            ]
        assert incumbent.execute("SELECT * FROM mv ORDER BY a").fetchall() == [
            (i,) for i in range(1, 8)
        ]

        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        try:
            incumbent.execute("INSERT INTO t VALUES (99)")
        except psycopg.Error as error:
            assert isinstance(error, OperationalError) or "has been retired" in str(
                error
            ), f"Unexpected rejection from retired deployment: {error}"
        else:
            raise AssertionError("retired deployment accepted a new table write")

    c.kill_fenced_mz("mz_old")
    # A disconnected session alone does not prove durable exclusion. Reopen the
    # same deployment identity and require retirement-specific admission failure.
    restart = c.run(
        "mz_old",
        "--foreground",
        "--kill-after=10s",
        DEFAULT_TIMEOUT,
        "tini",
        "-s",
        "--",
        "/usr/local/bin/entrypoint.sh",
        *c.compose["services"]["mz_old"]["command"],
        entrypoint="timeout",
        rm=True,
        capture=True,
        capture_stderr=True,
        check=False,
        env_extra={"MZ_RESTART_ON_FAILURE": "0"},
    )
    output = (restart.stdout or "") + (restart.stderr or "")
    assert restart.returncode != 0, "retired deployment restarted successfully"
    assert any(
        message in output
        for message in (
            "catalog deployment generation 0 has been retired",
            "deployment 0 has been retired",
        )
    ), f"Restart failed for a reason other than retirement: {output}"
    c.sql("INSERT INTO t VALUES (8)", service="mz_new")
    assert c.sql_query("SELECT * FROM mv ORDER BY a", service="mz_new") == [
        (i,) for i in range(1, 9)
    ]
    assert warmed_processes() == processes
    assert index_dataflows() == dataflows
    assert c.container_id("pubsub") == relay
    assert relay_process() == relay_start
    c.invoke(
        "exec", "-T", "pubsub", "curl", "--fail", "--silent", "localhost:6880/api/livez"
    )
    assert c.is_running("pubsub")


def _leader_status(c: Composition, mz_service: str) -> str:
    """Reads the current leader-promotion status of *mz_service*."""
    return json.loads(
        c.exec(
            mz_service,
            "curl",
            "-s",
            "localhost:6878/api/leader/status",
            capture=True,
            silent=True,
        ).stdout
    )["status"]


def workflow_caught_up_stability_crash_loop(c: Composition) -> None:
    """Verify the stability gate blocks cutover while a replica crash-loops.

    A cluster with two replicas hosts a materialized view. One of mz_new's
    replicas is repeatedly killed while the other stays healthy, so the cluster
    keeps hydrating and counts as caught-up. Because it is caught-up, a
    point-in-time check would see it ready and cut over into the crashing
    replica. The stability gate must refuse while any replica is unhealthy, and
    report ready only once the killed replica recovers and stays healthy for the
    stability period.
    """

    c.down(destroy_volumes=True)

    c.up("mz_old")

    c.sql(
        "ALTER SYSTEM SET with_0dt_caught_up_check_stability_period = '15s'",
        service="mz_old",
        port=6877,
        user="mz_system",
    )

    c.sql(
        """
        CREATE CLUSTER crashy SIZE 'scale=1,workers=1', REPLICATION FACTOR 2;
        CREATE TABLE t (a int);
        CREATE MATERIALIZED VIEW mv IN CLUSTER crashy AS SELECT * FROM t;
        CREATE INDEX mv_idx IN CLUSTER crashy ON mv (a);
        INSERT INTO t VALUES (1), (2), (3);
        """,
        service="mz_old",
    )

    # Target a single replica of `crashy` so the cluster stays caught-up on the
    # other one. Replica IDs come from the shared catalog, so they match across
    # generations. We kill the matching child process inside mz_new's container.
    replica_id = c.sql_query(
        "SELECT r.id FROM mz_cluster_replicas r JOIN mz_clusters c ON c.id = r.cluster_id "
        "WHERE c.name = 'crashy' ORDER BY r.name LIMIT 1",
        service="mz_old",
    )[0][0]

    c.up("mz_new")

    # SIGKILL the replica's clusterd repeatedly. The process orchestrator
    # relaunches it ~5s later, producing Online/Offline flapping. SIGKILL is not
    # classified as a crash, so it's safe with the default propagate_crashes.
    crash_until = time.time() + 60

    def crash_loop() -> None:
        while time.time() < crash_until:
            c.exec(
                "mz_new",
                "bash",
                "-c",
                f"ps aux | grep -v grep | grep -w 'replica_id={replica_id}' "
                f"| awk '{{print $2}}' | xargs -r kill -9 || true",
            )
            time.sleep(3)

    crasher = Thread(target=crash_loop)
    crasher.start()
    try:
        # While a replica is crash-looping, mz_new must never report ready. The
        # surviving replica hydrates the tiny view within seconds, so the cluster
        # is caught-up almost immediately. Without the gate, mz_new would promote
        # within a check interval. The status endpoint may not answer in the
        # first moments after boot, so we only assert once we read a status.
        deadline = time.time() + 45
        while time.time() < deadline:
            try:
                status = _leader_status(c, "mz_new")
            except Exception:
                time.sleep(2)
                continue
            assert (
                status == DeploymentStatus.INITIALIZING.value
            ), f"mz_new reached status {status} while a replica was crash-looping"
            time.sleep(2)
    finally:
        crasher.join()

    # The replica recovers. After the stability period mz_new becomes ready.
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
    c.promote_mz("mz_new", retire_mz_service=None)
    c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None)

    c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
    c.kill_fenced_mz("mz_old")


def _pids(c: Composition, mz_service: str, command: str) -> set[str]:
    """Returns the PIDs of processes named *command* in *mz_service*."""
    return set(
        c.exec(
            mz_service,
            "bash",
            "-c",
            f"ps -eo pid=,comm= | awk '$2 == \"{command}\" {{ print $1 }}'",
            capture=True,
            silent=True,
        ).stdout.split()
    )


def workflow_caught_up_stability_survives_restart(c: Composition) -> None:
    """Verify an adapterd-only restart preserves replica hydration age.

    Establish readiness before restarting the adapter. The replicas keep their
    hydration timestamps, so recovery must not require another full stability
    period. Initial warmup has no fixed duration, especially in debug builds.
    """
    period = 120

    c.down(destroy_volumes=True)
    c.up("mz_old")

    c.sql(
        f"ALTER SYSTEM SET with_0dt_caught_up_check_stability_period = '{period}s'",
        service="mz_old",
        port=6877,
        user="mz_system",
    )
    c.sql(
        """
        CREATE CLUSTER stable SIZE 'scale=1,workers=1';
        CREATE TABLE t (a int);
        CREATE MATERIALIZED VIEW mv IN CLUSTER stable AS SELECT * FROM t;
        CREATE INDEX mv_idx IN CLUSTER stable ON mv (a);
        INSERT INTO t VALUES (1), (2), (3);
        """,
        service="mz_old",
    )

    started = time.monotonic()
    c.up("mz_new")
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
    assert (
        time.monotonic() - started >= period
    ), "mz_new became ready before its initial stability period elapsed"
    controllerd = _pids(c, "mz_new", "controllerd")
    adapterd = _pids(c, "mz_new", "adapterd")
    replicas = _pids(c, "mz_new", "clusterd")
    assert controllerd, "mz_new has no controllerd process before restart"
    assert adapterd, "mz_new has no adapterd process before restart"
    assert replicas, "mz_new has no clusterd processes before restart"

    # Controllerd restarts the adapter independently of the container entrypoint.
    # Keep controllerd and clusterd alive so replica hydration age is retained.
    c.exec("mz_new", "bash", "-c", f"kill -9 {' '.join(sorted(adapterd))}")

    deadline = time.monotonic() + 60
    while True:
        pids = _pids(c, "mz_new", "adapterd")
        if pids and pids != adapterd:
            break
        assert time.monotonic() < deadline, "mz_new's adapterd did not restart"
        time.sleep(0.5)
    restarted = time.monotonic()

    try:
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
    except Exception:
        # Readiness substitutes zero for missing qualifying observations.
        # Capture compute-owned counts without replacing the original failure.
        try:
            c.exec(
                "mz_new",
                "timeout",
                "5s",
                "bash",
                "-c",
                r"""
                set -o pipefail
                date -u
                ps aux | grep -v grep | grep "cluster_id=s2" |
                    sed -n 's#.* --internal-http-listen-addr=\([^ ]*\) .*#\1#p' |
                    while read -r socket; do
                        echo "System replica metrics: $socket"
                        curl --fail --silent --show-error --max-time 2 \
                            --unix-socket "$socket" localhost/metrics |
                            grep '^mz_compute_collection_count'
                    done
                """,
            )
        except Exception as error:
            print(f"Could not capture system replica hydration counts: {error}")
        raise
    ready_after = time.monotonic() - restarted

    assert (
        _pids(c, "mz_new", "controllerd") == controllerd
    ), "mz_new's controller restarted with its adapter"
    assert (
        _pids(c, "mz_new", "clusterd") == replicas
    ), "mz_new's replicas did not survive its restart"
    assert (
        ready_after < period
    ), f"mz_new became ready {ready_after:.0f}s after its restart, exceeding a full {period}s stability period"

    c.promote_mz("mz_new", retire_mz_service=None)
    c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None)

    c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
    c.kill_fenced_mz("mz_old")


def workflow_caught_up_stability_without_dataflows(c: Composition) -> None:
    """Verify the stability gate does not wait on hydration no replica reports.

    A materialized view past its last refresh has a sealed shard, so mz_new
    never runs a dataflow for it. A materialized view pinned to one replica
    runs only there. Neither may keep any replica of the cluster from counting
    as hydrated.
    """
    c.down(destroy_volumes=True)
    c.up("mz_old")

    c.sql(
        """
        ALTER SYSTEM SET with_0dt_caught_up_check_stability_period = '0s';
        ALTER SYSTEM SET enable_replica_targeted_materialized_views = true;
        """,
        service="mz_old",
        port=6877,
        user="mz_system",
    )
    c.sql(
        """
        CREATE CLUSTER pinned REPLICAS (
            r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=1,workers=1')
        );
        CREATE TABLE t (a int);
        INSERT INTO t VALUES (1), (2), (3);
        CREATE MATERIALIZED VIEW sealed IN CLUSTER pinned
            WITH (REFRESH AT mz_now()::string::int8) AS SELECT * FROM t;
        CREATE MATERIALIZED VIEW on_r1 IN CLUSTER pinned REPLICA r1
            AS SELECT count(*) FROM t;
        """,
        service="mz_old",
    )

    # Wait for the refresh to seal `sealed`, so mz_new starts with an empty
    # as_of for it.
    for _ in range(120):
        sealed = c.sql_query(
            """
            SELECT f.write_frontier IS NULL
            FROM mz_internal.mz_frontiers f
            JOIN mz_materialized_views mv ON f.object_id = mv.id
            WHERE mv.name = 'sealed'
            """,
            service="mz_old",
        )
        if sealed and sealed[0][0]:
            break
        time.sleep(1)
    else:
        raise RuntimeError("materialized view `sealed` never sealed its shard")

    c.up("mz_new")
    c.await_mz_deployment_status(
        DeploymentStatus.READY_TO_PROMOTE, "mz_new", timeout=300
    )
    c.promote_mz("mz_new", retire_mz_service=None)
    c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new", sleep_time=None)

    c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
    c.kill_fenced_mz("mz_old")


def workflow_ddl_detection_with_id_pool(c: Composition) -> None:
    """Verify a candidate follows peer DDL with batch-allocated user IDs.

    IDs below the allocator counter must not hide new committed objects.
    They become queryable without an adapter restart, before promotion, and
    remain queryable after promotion and explicit predecessor retirement.
    """
    c.down(destroy_volumes=True)
    c.up(
        "mz_old",
    )

    # Use a small pool batch size so we can trigger pool refills predictably.
    c.sql(
        """
        ALTER SYSTEM SET user_id_pool_batch_size = 5;
        ALTER SYSTEM SET cluster = quickstart;
        """,
        service="mz_old",
        port=6877,
        user="mz_system",
    )

    # Create initial objects to trigger pool allocation. The allocator counter
    # will jump ahead by the batch size (5), but only a few IDs will be used.
    c.sql(
        """
        CREATE TABLE pool_t1 (a INT);
        CREATE TABLE pool_t2 (a INT);
        INSERT INTO pool_t1 VALUES (1);
        INSERT INTO pool_t2 VALUES (2);
        """,
        service="mz_old",
    )

    # Establish the candidate's initial catalog before the peer changes it.
    c.up("mz_new")
    c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
    adapter_pids = _pids(c, "mz_new", "adapterd")
    assert adapter_pids, "mz_new has no adapterd process before peer DDL"

    # The allocator counter is not a catalog-change watermark. New objects can
    # use IDs from a range the peer reserved before the candidate started.
    c.sql(
        """
        CREATE TABLE pool_t3 (a INT);
        CREATE TABLE pool_t4 (a INT);
        CREATE MATERIALIZED VIEW pool_mv AS SELECT sum(a) FROM pool_t1;
        INSERT INTO pool_t3 VALUES (3);
        INSERT INTO pool_t4 VALUES (4);
        """,
        service="mz_old",
    )

    # Check the same contents before promotion and after retirement.
    with c.override(
        Testdrive(
            materialize_url="postgres://materialize@mz_new:6875",
            materialize_url_internal="postgres://materialize@mz_new:6877",
            mz_service="mz_new",
            materialize_params={"cluster": "quickstart"},
            no_reset=True,
            seed=1,
            default_timeout=DEFAULT_TIMEOUT,
        )
    ):
        c.up(Service("testdrive", idle=True))
        checks = dedent("""
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM pool_t1;
            1
            > SELECT * FROM pool_t2;
            2
            > SELECT * FROM pool_t3;
            3
            > SELECT * FROM pool_t4;
            4
            > SELECT * FROM pool_mv;
            1
            """)
        c.testdrive(checks)
        assert (
            _pids(c, "mz_new", "adapterd") == adapter_pids
        ), "mz_new restarted to incorporate peer DDL"

        c.promote_mz("mz_new", retire_mz_service=None)
        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")
        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        c.kill_fenced_mz("mz_old")
        c.testdrive(checks)


def workflow_ddl_detection_ephemeral_items(c: Composition) -> None:
    """Temporary objects follow session/execution lifetime, not promotion."""
    c.down(destroy_volumes=True)
    c.up("mz_old")
    c.sql(
        "ALTER SYSTEM SET enable_alter_table_add_column = true",
        service="mz_old",
        port=6877,
        user="mz_system",
    )
    connections = []

    def metadata(service: str, query: str) -> Any:
        return c.sql_query(
            query, service=service, port=6877, user="mz_system", reuse_connection=False
        )

    def item_predicate(session: str) -> str:
        return (
            SQL(
                "data->>'kind' = 'Item' AND data->'value'->>'ephemeral_owner_session' = {}"
            )
            .format(Literal(session))
            .as_string()
        )

    def assert_owner(service: str, session: str, incarnation: str) -> None:
        rows = metadata(
            service,
            f"SELECT data->'value'->>'ephemeral_owner_incarnation' FROM mz_internal.mz_catalog_raw WHERE {item_predicate(session)}",
        )
        assert rows == [(incarnation,)] * 4, rows

    def collection_ids(service: str, session: str) -> set[str]:
        rows = metadata(
            service,
            f"SELECT data::text FROM mz_internal.mz_catalog_raw WHERE {item_predicate(session)}",
        )
        ids = set()
        for (raw,) in rows:
            value = json.loads(raw)["value"]
            ids.add(json.dumps(value["global_id"]))
            ids.update(
                json.dumps(version["global_id"]) for version in value["extra_versions"]
            )
        assert ids, (session, rows)
        return ids

    def await_removed(service: str, session: str, ids: set[str]) -> None:
        # Use the production maintenance cadence. No publication interval override.
        deadline = time.monotonic() + 300
        values = (
            SQL(", ")
            .join(SQL("{}::jsonb").format(Literal(id)) for id in sorted(ids))
            .as_string()
        )
        while True:
            rows = metadata(
                service,
                f"""SELECT count(*) FROM mz_internal.mz_catalog_raw
                WHERE ({item_predicate(session)})
                   OR (data->>'kind' IN ('StorageCollectionMetadata', 'WrittenPlan',
                                        'MaintainedReadRequirement', 'CollectionCompactionBound')
                       AND data->'key'->'id' IN ({values}))""",
            )
            if rows == [(0,)]:
                return
            assert time.monotonic() < deadline, (session, rows)
            time.sleep(1)

    def await_sql(service: str) -> None:
        deadline = time.monotonic() + 300
        while True:
            try:
                assert c.sql_query(
                    "SELECT 1", service=service, reuse_connection=False
                ) == [(1,)]
                return
            except OperationalError:
                assert time.monotonic() < deadline, f"{service} SQL did not recover"
                time.sleep(1)

    def create(service: str, value: int) -> tuple[psycopg.Connection, str, str]:
        connection = c.sql_connection(service=service)
        connections.append(connection)
        connection.execute("SET CLUSTER = quickstart")
        connection.execute("SET statement_timeout = '60s'")
        connection.execute("CREATE TEMP TABLE temp_t (a int)")
        connection.execute("CREATE TEMP VIEW temp_v AS SELECT a FROM temp_t")
        connection.execute("CREATE TEMP VIEW temp_nested AS SELECT a FROM temp_v")
        # A constant view has no creator read grant from which to infer ownership.
        marker = f"temp_constant_{value}"
        connection.execute(
            SQL("CREATE TEMP VIEW {} AS SELECT 1").format(Identifier(marker))
        )
        connection.execute("INSERT INTO temp_t VALUES (%s)", (value,))
        assert connection.execute("SELECT * FROM temp_nested").fetchall() == [(value,)]
        rows = metadata(
            service,
            SQL(
                "SELECT data->'value'->>'ephemeral_owner_session', data->'value'->>'ephemeral_owner_incarnation' FROM mz_internal.mz_catalog_raw WHERE data->>'kind' = 'Item' AND data->'value'->>'name' = {}"
            )
            .format(Literal(marker))
            .as_string(),
        )
        assert len(rows) == 1 and all(rows[0]), rows
        session, incarnation = rows[0]
        assert_owner(service, session, incarnation)
        return connection, session, incarnation

    try:
        old, old_session, old_incarnation = create("mz_old", 7)
        c.up("mz_new")
        c.await_mz_deployment_status(DeploymentStatus.READY_TO_PROMOTE, "mz_new")
        candidate_pids = _pids(c, "mz_new", "adapterd")
        assert candidate_pids
        candidate, candidate_session, _ = create("mz_new", 9)
        # The same names resolve independently on the two adapters.
        assert old.execute("SELECT * FROM temp_v").fetchall() == [(7,)]
        old.execute("ALTER TABLE temp_t ADD COLUMN b int")
        old.execute("ALTER VIEW temp_nested RENAME TO temp_nested_renamed")
        old.execute("GRANT SELECT ON temp_t TO PUBLIC")
        assert_owner("mz_new", old_session, old_incarnation)
        assert candidate.execute("SELECT * FROM temp_v").fetchall() == [(9,)]
        assert (
            _pids(c, "mz_new", "adapterd") == candidate_pids
        ), "following temporary DDL restarted the candidate"

        candidate_ids = collection_ids("mz_old", candidate_session)
        candidate.close()
        await_removed("mz_old", candidate_session, candidate_ids)
        assert (
            _pids(c, "mz_new", "adapterd") == candidate_pids
        ), "normal disconnect restarted the candidate"
        c.promote_mz("mz_new", retire_mz_service=None)
        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, "mz_new")
        await_sql("mz_new")
        # Promotion may restart the candidate, but does not retire incumbent sessions.
        old.execute("INSERT INTO temp_t (a) VALUES (8)")
        assert old.execute(
            "SELECT a FROM temp_nested_renamed ORDER BY a"
        ).fetchall() == [(7,), (8,)]
        assert_owner("mz_new", old_session, old_incarnation)
        survivor, survivor_session, survivor_incarnation = create("mz_new", 11)
        survivor_pids = _pids(c, "mz_new", "adapterd")

        adapter_pids = _pids(c, "mz_old", "adapterd")
        controller_pids = _pids(c, "mz_old", "controllerd")
        replica_pids = _pids(c, "mz_old", "clusterd")
        assert adapter_pids and controller_pids and replica_pids
        old_ids = collection_ids("mz_new", old_session)
        assert len(old_ids) == 5, "ALTER's additional table version is missing"
        c.exec("mz_old", "bash", "-c", f"kill -9 {' '.join(sorted(adapter_pids))}")
        await_removed("mz_new", old_session, old_ids)
        await_sql("mz_old")
        assert _pids(c, "mz_old", "adapterd") != adapter_pids
        assert _pids(c, "mz_old", "controllerd") == controller_pids
        assert _pids(c, "mz_old", "clusterd") == replica_pids
        assert_owner("mz_new", survivor_session, survivor_incarnation)

        replacement, replacement_session, replacement_incarnation = create("mz_old", 13)
        assert replacement_incarnation != old_incarnation
        replacement_ids = collection_ids("mz_new", replacement_session)
        c.retire_mz_deployment("mz_new", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
        try:
            replacement.execute("INSERT INTO temp_t VALUES (14)")
        except psycopg.Error as error:
            assert isinstance(error, OperationalError) or "has been retired" in str(
                error
            ), error
        else:
            raise AssertionError("retired deployment accepted a temporary-table write")
        c.kill_fenced_mz("mz_old")
        await_removed("mz_new", replacement_session, replacement_ids)
        assert survivor.execute("SELECT * FROM temp_nested").fetchall() == [(11,)]
        assert_owner("mz_new", survivor_session, survivor_incarnation)
        survivor_ids = collection_ids("mz_new", survivor_session)
        survivor.close()
        await_removed("mz_new", survivor_session, survivor_ids)
        assert (
            _pids(c, "mz_new", "adapterd") == survivor_pids
        ), "peer retirement or normal disconnect restarted the survivor"
    finally:
        for connection in connections:
            connection.close()
