# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import time
from textwrap import dedent

from pg8000.exceptions import InterfaceError

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.ui import CommandFailureCausedUIError

DEFAULT_TIMEOUT = "300s"

SERVICES = [
    MySql(),
    Postgres(),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Cockroach(),
    Materialized(
        name="mz_old",
        sanity_restart=False,
        deploy_generation=0,
        external_cockroach=True,
    ),
    Materialized(
        name="mz_new",
        sanity_restart=False,
        deploy_generation=1,
        restart="on-failure",
        external_cockroach=True,
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
    for name in buildkite.shard_list(
        list(c.workflows.keys()), lambda workflow: workflow
    ):
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_read_only(c: Composition) -> None:
    """Verify read-only mode."""
    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "postgres", "mysql", "mz_old")
    c.up("testdrive", persistent=True)

    # Make sure cluster is owned by the system so it doesn't get dropped
    # between testdrive runs.
    c.sql(
        """
        DROP CLUSTER IF EXISTS cluster CASCADE;
        CREATE CLUSTER cluster SIZE '2-1';
        GRANT ALL ON CLUSTER cluster TO materialize;
        ALTER SYSTEM SET cluster = cluster;
        ALTER SYSTEM SET enable_0dt_deployment = true;
    """,
        service="mz_old",
        port=6877,
        user="mz_system",
    )

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
        > CREATE SOURCE kafka_source (key1, key2, value1, value2)
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-${{testdrive.seed}}')
          KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          ENVELOPE UPSERT;
        > SELECT * FROM kafka_source
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
          (PUBLICATION 'postgres_source')
          FOR TABLES (postgres_source_table);
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
          FROM MYSQL CONNECTION mysql
          FOR TABLES (public.mysql_source_table AS mysql_source_table);
        > SELECT * FROM mysql_source_table;
        A 0

        > CREATE SOURCE kafka_sink_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${{testdrive.seed}}')
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE NONE

        > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source
        <null> <null> 1 2

        > CREATE SOURCE webhook_source
          IN CLUSTER cluster
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
        Materialized(name="mz_old", deploy_generation=1, external_cockroach=True)
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
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n"

            > SELECT * FROM kafka_source
            key1A key1B value1A value1B
            > SELECT * FROM postgres_source_table
            A 0
            > SELECT * FROM mysql_source_table;
            A 0
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source
            <null> <null> 1 2
            > SELECT * FROM webhook_source
            AAA
            """
            )
        )

        c.up("mz_old")
        # Wait up to 60s for the new deployment to become ready for promotion.
        for _ in range(1, 60):
            result = json.loads(
                c.exec(
                    "mz_old",
                    "curl",
                    "-s",
                    "localhost:6878/api/leader/status",
                    capture=True,
                ).stdout
            )
            if result["status"] == "ReadyToPromote":
                break
            assert result["status"] == "Initializing", f"Unexpected status {result}"
            print("Not ready yet, waiting 1s")
            time.sleep(1)
        result = json.loads(
            c.exec(
                "mz_old",
                "curl",
                "-s",
                "-X",
                "POST",
                "localhost:6878/api/leader/promote",
                capture=True,
            ).stdout
        )
        assert result["result"] == "Success", f"Unexpected result {result}"

    # After promotion, the deployment should boot with writes allowed.
    with c.override(
        Materialized(
            name="mz_old",
            healthcheck=[
                "CMD-SHELL",
                """[ "$(curl -f localhost:6878/api/leader/status)" = '{"status":"IsLeader"}' ]""",
            ],
            deploy_generation=1,
            external_cockroach=True,
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
            > SELECT * FROM kafka_source
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source
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

            > SELECT * FROM kafka_source
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
            > SELECT * FROM webhook_source
            AAA
            CCC
            """
            )
        )


def workflow_basic(c: Composition) -> None:
    """Verify basic 0dt deployment flow."""
    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "postgres", "mysql", "mz_old")
    c.up("testdrive", persistent=True)

    # Make sure cluster is owned by the system so it doesn't get dropped
    # between testdrive runs.
    c.sql(
        """
        DROP CLUSTER IF EXISTS cluster CASCADE;
        CREATE CLUSTER cluster SIZE '2-1';
        GRANT ALL ON CLUSTER cluster TO materialize;
        ALTER SYSTEM SET cluster = cluster;
        ALTER SYSTEM SET enable_0dt_deployment = true;
    """,
        service="mz_old",
        port=6877,
        user="mz_system",
    )

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
        > CREATE SOURCE kafka_source (key1, key2, value1, value2)
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-${{testdrive.seed}}')
          KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
          ENVELOPE UPSERT;
        > SELECT * FROM kafka_source
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
          (PUBLICATION 'postgres_source')
          FOR TABLES (postgres_source_table);
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
          FROM MYSQL CONNECTION mysql
          FOR TABLES (public.mysql_source_table AS mysql_source_table);
        > SELECT * FROM mysql_source_table;
        A 0

        > CREATE SOURCE kafka_sink_source
          IN CLUSTER cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${{testdrive.seed}}')
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE NONE

        > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source
        <null> <null> 1 2

        > CREATE SOURCE webhook_source
          IN CLUSTER cluster
          FROM WEBHOOK BODY FORMAT TEXT

        $ webhook-append database=materialize schema=public name=webhook_source
        AAA
        > SELECT * FROM webhook_source
        AAA
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
        c.up("testdrive", persistent=True)
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
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n"

            > SELECT * FROM kafka_source
            key1A key1B value1A value1B
            key2A key2B value2A value2B
            > SELECT * FROM postgres_source_table
            A 0
            B 1
            > SELECT * FROM mysql_source_table;
            A 0
            B 1
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source
            <null> <null> 1 2

            > SELECT * FROM webhook_source
            AAA
            """
            )
        )

    # But the old Materialize can still run writes
    c.up("testdrive", persistent=True)
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
        "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: true\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n"

        > SELECT * FROM kafka_source
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
        > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source
        <null> <null> 1 2
        <null> <null> 3 4
        <null> <null> 5 6
        > SELECT * FROM webhook_source
        AAA
        CCC
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
        c.up("testdrive", persistent=True)
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
            > SELECT * FROM kafka_source
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
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source
            <null> <null> 1 2
            <null> <null> 3 4
            <null> <null> 5 6
            > SELECT * FROM webhook_source
            AAA
            CCC
            """
            )
        )

        # Wait up to 60s for the new deployment to become ready for promotion.
        for _ in range(1, 60):
            result = json.loads(
                c.exec(
                    "mz_new",
                    "curl",
                    "-s",
                    "localhost:6878/api/leader/status",
                    capture=True,
                ).stdout
            )
            if result["status"] == "ReadyToPromote":
                break
            assert result["status"] == "Initializing", f"Unexpected status {result}"
            print("Not ready yet, waiting 1s")
            time.sleep(1)
        result = json.loads(
            c.exec(
                "mz_new",
                "curl",
                "-s",
                "-X",
                "POST",
                "localhost:6878/api/leader/promote",
                capture=True,
            ).stdout
        )
        assert result["result"] == "Success", f"Unexpected result {result}"

        # Give some time for Mz to restart after promotion
        for i in range(10):
            try:
                c.sql("SELECT 1", service="mz_old")
            except InterfaceError as e:
                assert "network error" in str(
                    e
                ) or "Can't create a connection to host" in str(
                    e
                ), f"Unexpected error: {e}"
            except CommandFailureCausedUIError as e:
                # service "mz_old" is not running
                assert "running docker compose failed" in str(
                    e
                ), f"Unexpected error: {e}"
                break
            time.sleep(1)
        else:
            assert False, "mz_old didn't stop running within 10 seconds"

        for i in range(10):
            try:
                c.sql("SELECT 1", service="mz_new")
                break
            except CommandFailureCausedUIError:
                pass
            except InterfaceError:
                pass
            time.sleep(1)
        else:
            assert False, "mz_new didn't come up within 10 seconds"

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
            > SELECT * FROM kafka_source
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

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka
            key4A,key4B:value4A,value4B

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO postgres_source_table VALUES ('D', 3);

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            USE public;
            INSERT INTO mysql_source_table VALUES ('D', 3);

            > SELECT * FROM kafka_source
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
            > SELECT (before).a, (before).b, (after).a, (after).b FROM kafka_sink_source
            <null> <null> 1 2
            <null> <null> 3 4
            <null> <null> 5 6
            <null> <null> 7 8
            > SELECT * FROM webhook_source
            AAA
            CCC
            EEE
            """
            )
        )
