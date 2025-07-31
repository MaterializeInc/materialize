# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Tests with limited amount of memory, makes sure that the scenarios keep working
and do not regress. Contains tests for large data ingestions.
"""
import argparse
import math
from dataclasses import dataclass
from string import ascii_lowercase
from textwrap import dedent

from materialize import buildkite
from materialize.buildkite import shard_list
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.test_analytics.config.test_analytics_db_config import (
    create_dummy_test_analytics_config,
    create_test_analytics_config,
)
from materialize.test_analytics.data.bounded_memory.bounded_memory_minimal_search_storage import (
    BOUNDED_MEMORY_STATUS_CONFIGURED,
    BOUNDED_MEMORY_STATUS_FAILURE,
    BOUNDED_MEMORY_STATUS_SUCCESS,
    BoundedMemoryMinimalSearchEntry,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb

# Those defaults have been carefully chosen to avoid known OOMs
# such as materialize#15093 and database-issues#4297 while hopefully catching any further
# regressions in memory usage
PAD_LEN = 1024
STRING_PAD = "x" * PAD_LEN
REPEAT = 16 * 1024
ITERATIONS = 128

BOUNDED_MEMORY_FRAMEWORK_VERSION = "1.0.0"

SERVICES = [
    Materialized(),  # overridden below
    Testdrive(
        no_reset=True,
        seed=1,
        default_timeout="3600s",
        entrypoint_extra=[
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        ],
    ),
    Redpanda(),
    Postgres(),
    MySql(),
    Clusterd(),
    Mz(app_password=""),
]


@dataclass
class Scenario:
    name: str
    pre_restart: str
    post_restart: str
    materialized_memory: str
    clusterd_memory: str
    disabled: bool = False


class PgCdcScenario(Scenario):
    PG_SETUP = dedent(
        """
        > CREATE SECRET pgpass AS 'postgres'
        > CREATE CONNECTION pg FOR POSTGRES
          HOST postgres,
          DATABASE postgres,
          USER postgres,
          PASSWORD SECRET pgpass

        $ postgres-execute connection=postgres://postgres:postgres@postgres
        ALTER USER postgres WITH replication;

        CREATE TABLE t1 (f1 SERIAL PRIMARY KEY, f2 INTEGER DEFAULT 0, f3 TEXT);
        ALTER TABLE t1 REPLICA IDENTITY FULL;

        CREATE PUBLICATION mz_source FOR ALL TABLES;
        """
    )
    MZ_SETUP = dedent(
        """
        > CREATE SOURCE mz_source
          IN CLUSTER clusterd
          FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

        > CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);

        > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM t1;
        """
    )


class MySqlCdcScenario(Scenario):
    MYSQL_SETUP = dedent(
        f"""
        > CREATE SECRET mysqlpass AS '${{arg.mysql-root-password}}'
        > CREATE CONNECTION mysql_conn TO MYSQL (
            HOST mysql,
            USER root,
            PASSWORD SECRET mysqlpass
          )

        $ mysql-connect name=mysql url=mysql://root@mysql password=${{arg.mysql-root-password}}

        $ mysql-execute name=mysql
        # needed for MySQL 5.7
        SET GLOBAL max_allowed_packet=67108864;

        # reconnect
        $ mysql-connect name=mysql url=mysql://root@mysql password=${{arg.mysql-root-password}}

        $ mysql-execute name=mysql
        DROP DATABASE IF EXISTS public;
        CREATE DATABASE public;
        USE public;

        SET @i:=0;
        CREATE TABLE series_helper (i INT);
        INSERT INTO series_helper (i) SELECT @i:=@i+1 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT {REPEAT};

        CREATE TABLE t1 (f1 SERIAL PRIMARY KEY, f2 INTEGER DEFAULT 0, f3 TEXT);
        """
    )
    MZ_SETUP = dedent(
        """
        > CREATE SOURCE mz_source
          IN CLUSTER clusterd
          FROM MYSQL CONNECTION mysql_conn;

        > CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);

        > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM t1;
        """
    )


class KafkaScenario(Scenario):
    SCHEMAS = dedent(
        """
        $ set key-schema={
            "type": "string"
          }

        $ set value-schema={
            "type" : "record",
            "name" : "test",
          "fields" : [
            {"name":"f1", "type":"string"}
          ]
          }
        """
    )

    CONNECTIONS = dedent(
        """
        $ kafka-create-topic topic=topic1

        $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${value-schema} key-schema=${key-schema}
        "AAA" {"f1": "START MARKER"}

        > CREATE CONNECTION IF NOT EXISTS csr_conn
          FOR CONFLUENT SCHEMA REGISTRY
          URL '${testdrive.schema-registry-url}';

        > CREATE CONNECTION IF NOT EXISTS kafka_conn
          FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT;
        """
    )

    SOURCE = dedent(
        """
        > CREATE SOURCE s1
          IN CLUSTER clusterd
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}');

        > CREATE TABLE s1_tbl FROM SOURCE s1 (REFERENCE "testdrive-topic1-${testdrive.seed}")
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE UPSERT;

        > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM s1_tbl;
        """
    )

    END_MARKER = dedent(
        """
        $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${value-schema} key-schema=${key-schema}
        "ZZZ" {"f1": "END MARKER"}
        """
    )

    POST_RESTART = dedent(
        f"""
        # Delete all rows except markers
        $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${{value-schema}} key-schema=${{key-schema}} repeat={REPEAT}
        "${{kafka-ingest.iteration}}"

        $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${{value-schema}} key-schema=${{key-schema}} repeat={REPEAT}
        "MMM"

        # Expect that only markers are left
        > SELECT * FROM v1;
        2
        """
    )


SCENARIOS = [
    PgCdcScenario(
        name="pg-cdc-snapshot",
        pre_restart=PgCdcScenario.PG_SETUP
        + "$ postgres-execute connection=postgres://postgres:postgres@postgres\n"
        + "\n".join(
            [
                dedent(
                    f"""
                    INSERT INTO t1 (f3) SELECT '{i}' || REPEAT('a', {PAD_LEN}) FROM generate_series(1, {REPEAT});
                    """
                )
                for i in range(0, ITERATIONS * 10)
            ]
        )
        + PgCdcScenario.MZ_SETUP
        + dedent(
            f"""
            > SELECT * FROM v1; /* expect {ITERATIONS * 10 * REPEAT} */
            {ITERATIONS * 10 * REPEAT}

            > SELECT COUNT(*) FROM t1; /* expect {ITERATIONS * 10 * REPEAT} */
            {ITERATIONS * 10 * REPEAT}
            """
        ),
        post_restart=dedent(
            f"""
            # We do not do DELETE post-restart, as it will cause postgres to go out of disk

            > SELECT * FROM v1; /* expect {ITERATIONS * 10 * REPEAT} */
            {ITERATIONS * 10 * REPEAT}

            > SELECT COUNT(*) FROM t1; /* expect {ITERATIONS * 10 * REPEAT} */
            {ITERATIONS * 10 * REPEAT}
            """
        ),
        materialized_memory="4.5Gb",
        clusterd_memory="1Gb",
    ),
    PgCdcScenario(
        name="pg-cdc-update",
        pre_restart=PgCdcScenario.PG_SETUP
        + dedent(
            f"""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO t1 (f3) VALUES ('START');
            INSERT INTO t1 (f3) SELECT REPEAT('a', {PAD_LEN}) FROM generate_series(1, {REPEAT});
            """
        )
        + PgCdcScenario.MZ_SETUP
        + "\n".join(
            [
                dedent(
                    """
                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    UPDATE t1 SET f2 = f2 + 1;
                    """
                )
                for letter in ascii_lowercase[:ITERATIONS]
            ]
        )
        + dedent(
            f"""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            INSERT INTO t1 (f3) VALUES ('END');

            > SELECT * FROM v1 /* expect: {REPEAT + 2} */;
            {REPEAT + 2}

            > SELECT COUNT(*) FROM t1 /* expect: {REPEAT + 2} */;
            {REPEAT + 2}
            """
        ),
        post_restart=dedent(
            """
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            DELETE FROM t1;

            > SELECT * FROM v1;
            0

            > SELECT COUNT(*) FROM t1;
            0
            """
        ),
        materialized_memory="4.5Gb",
        clusterd_memory="3.5Gb",
    ),
    Scenario(
        name="pg-cdc-gh-15044",
        pre_restart=dedent(
            f"""
            > CREATE SECRET pgpass AS 'postgres'
            > CREATE CONNECTION pg FOR POSTGRES
              HOST postgres,
              DATABASE postgres,
              USER postgres,
              PASSWORD SECRET pgpass

            # Insert data pre-snapshot
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            ALTER USER postgres WITH replication;
            DROP SCHEMA IF EXISTS public CASCADE;
            DROP PUBLICATION IF EXISTS mz_source;

            CREATE SCHEMA public;

            CREATE TABLE t1 (f1 SERIAL PRIMARY KEY, f2 INTEGER DEFAULT 0, f3 TEXT);
            ALTER TABLE t1 REPLICA IDENTITY FULL;

            INSERT INTO t1 (f3) SELECT REPEAT('a', 1024 * 1024) FROM generate_series(1, 16);

            CREATE PUBLICATION mz_source FOR ALL TABLES;

            > CREATE SOURCE mz_source
              FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
            > CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);

            > SELECT COUNT(*) > 0 FROM t1;
            true

            # > CREATE MATERIALIZED VIEW v1 AS SELECT f1 + 1, f2 FROM t1;

            > CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) FROM t1;

            # Update data post-snapshot
            $ postgres-execute connection=postgres://postgres:postgres@postgres

            {'UPDATE t1 SET f2 = f2 + 1;' * 300}
            INSERT INTO t1 (f3) VALUES ('eof');

            > SELECT * FROM v2;
            17

            > SELECT COUNT(*) FROM t2;
            """
        ),
        post_restart=dedent(
            """
            > SELECT * FROM v2;
            17

            > SELECT COUNT(*) FROM t2;
            0

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            DELETE FROM t1;

            > SELECT * FROM v2;
            0

            > SELECT COUNT(*) FROM t2;
            0
            """
        ),
        materialized_memory="8Gb",
        clusterd_memory="6Gb",
        disabled=True,
    ),
    PgCdcScenario(
        name="pg-cdc-large-tx",
        pre_restart=PgCdcScenario.PG_SETUP
        + PgCdcScenario.MZ_SETUP
        + "$ postgres-execute connection=postgres://postgres:postgres@postgres\n"
        + "BEGIN;\n"
        + "\n".join(
            [
                dedent(
                    f"""
                    INSERT INTO t1 (f3) SELECT '{i}' || REPEAT('a', {PAD_LEN}) FROM generate_series(1, {int(REPEAT / 16)});
                    """
                )
                for i in range(0, ITERATIONS * 20)
            ]
        )
        + "COMMIT;\n"
        + dedent(
            f"""
            > SELECT * FROM v1; /* expect {int(ITERATIONS * 20 * REPEAT / 16)} */
            {int(ITERATIONS * 20 * REPEAT / 16)}

            > SELECT COUNT(*) FROM t1; /* expect {int(ITERATIONS * 20 * REPEAT / 16)} */
            {int(ITERATIONS * 20 * REPEAT / 16)}
            """
        ),
        post_restart=dedent(
            f"""
            > SELECT * FROM v1; /* expect {int(ITERATIONS * 20 * REPEAT / 16)} */
            {int(ITERATIONS * 20 * REPEAT / 16)}

            > SELECT COUNT(*) FROM t1; /* expect {int(ITERATIONS * 20 * REPEAT / 16)} */
            {int(ITERATIONS * 20 * REPEAT / 16)}

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            DELETE FROM t1;

            > SELECT * FROM v1;
            0

            > SELECT COUNT(*) FROM t1;
            0
            """
        ),
        materialized_memory="4.5Gb",
        # TODO: Reduce to 1Gb when https://github.com/MaterializeInc/database-issues/issues/9515 is fixed
        clusterd_memory="2Gb",
    ),
    MySqlCdcScenario(
        name="mysql-cdc-snapshot",
        pre_restart=MySqlCdcScenario.MYSQL_SETUP
        + "$ mysql-execute name=mysql\n"
        + "\n".join(
            [
                dedent(
                    f"""
                    INSERT INTO t1 (f3) SELECT CONCAT('{i}', REPEAT('a', {PAD_LEN})) FROM series_helper;
                    """
                )
                for i in range(0, ITERATIONS)
            ]
        )
        + MySqlCdcScenario.MZ_SETUP
        + dedent(
            f"""
            > SELECT * FROM v1; /* expect {ITERATIONS * REPEAT} */
            {ITERATIONS * REPEAT}

            > SELECT COUNT(*) FROM t1; /* expect {ITERATIONS * REPEAT} */
            {ITERATIONS * REPEAT}
            """
        ),
        post_restart=dedent(
            f"""
            > SELECT * FROM v1; /* expect {ITERATIONS * REPEAT} */
            {ITERATIONS * REPEAT}

            > SELECT COUNT(*) FROM t1; /* expect {ITERATIONS * REPEAT} */
            {ITERATIONS * REPEAT}

            $ mysql-connect name=mysql url=mysql://root@mysql password=${{arg.mysql-root-password}}
            $ mysql-execute name=mysql
            USE public;
            DELETE FROM t1;

            > SELECT * FROM v1;
            0

            > SELECT COUNT(*) FROM t1;
            0
            """
        ),
        materialized_memory="4.5Gb",
        clusterd_memory="3.5Gb",
    ),
    MySqlCdcScenario(
        name="mysql-cdc-update",
        pre_restart=MySqlCdcScenario.MYSQL_SETUP
        + dedent(
            f"""
            $ mysql-execute name=mysql
            INSERT INTO t1 (f3) VALUES ('START');
            INSERT INTO t1 (f3) SELECT REPEAT('a', {PAD_LEN}) FROM series_helper;
            """
        )
        + MySqlCdcScenario.MZ_SETUP
        + "\n".join(
            [
                dedent(
                    """
                    $ mysql-execute name=mysql
                    UPDATE t1 SET f2 = f2 + 1;
                    """
                )
                for letter in ascii_lowercase[:ITERATIONS]
            ]
        )
        + dedent(
            f"""
            $ mysql-execute name=mysql
            INSERT INTO t1 (f3) VALUES ('END');

            > SELECT * FROM v1 /* expect: {REPEAT + 2} */;
            {REPEAT + 2}

            > SELECT COUNT(*) FROM t1 /* expect: {REPEAT + 2} */;
            {REPEAT + 2}
            """
        ),
        post_restart=dedent(
            """
            $ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}
            $ mysql-execute name=mysql
            USE public;
            DELETE FROM t1;

            > SELECT * FROM v1;
            0

            > SELECT COUNT(*) FROM t1;
            0
            """
        ),
        materialized_memory="4.5Gb",
        clusterd_memory="3.5Gb",
    ),
    MySqlCdcScenario(
        name="mysql-cdc-large-tx",
        pre_restart=MySqlCdcScenario.MYSQL_SETUP
        + MySqlCdcScenario.MZ_SETUP
        + "$ mysql-execute name=mysql\n"
        + "SET AUTOCOMMIT = FALSE;\n"
        + "\n".join(
            [
                dedent(
                    f"""
                    INSERT INTO t1 (f3) SELECT CONCAT('{i}', REPEAT('a', {PAD_LEN})) FROM series_helper LIMIT {int(REPEAT / 128)};
                    """
                )
                for i in range(0, ITERATIONS * 10)
            ]
        )
        + "COMMIT;\n"
        + dedent(
            f"""
            > SELECT * FROM v1; /* expect {int(ITERATIONS * 10) * int(REPEAT / 128)} */
            {int(ITERATIONS * 10) * int(REPEAT / 128)}

            > SELECT COUNT(*) FROM t1; /* expect {int(ITERATIONS * 10) * int(REPEAT / 128)} */
            {int(ITERATIONS * 10) * int(REPEAT / 128)}
            """
        ),
        post_restart=dedent(
            f"""
            > SELECT * FROM v1; /* expect {int(ITERATIONS * 10) * int(REPEAT / 128)} */
            {int(ITERATIONS * 10) * int(REPEAT / 128)}

            > SELECT COUNT(*) FROM t1; /* expect {int(ITERATIONS * 10) * int(REPEAT / 128)} */
            {int(ITERATIONS * 10) * int(REPEAT / 128)}

            $ mysql-connect name=mysql url=mysql://root@mysql password=${{arg.mysql-root-password}}
            $ mysql-execute name=mysql
            USE public;
            DELETE FROM t1;

            > SELECT * FROM v1;
            0

            > SELECT COUNT(*) FROM t1;
            0
            """
        ),
        materialized_memory="3.5Gb",
        clusterd_memory="8.5Gb",
    ),
    KafkaScenario(
        name="upsert-snapshot",
        pre_restart=KafkaScenario.SCHEMAS
        + KafkaScenario.CONNECTIONS
        + "\n".join(
            [
                dedent(
                    f"""
                    $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${{value-schema}} key-schema=${{key-schema}} repeat={REPEAT}
                    "MMM" {{"f1": "{i}{STRING_PAD}"}}
                    """
                )
                for i in range(0, ITERATIONS)
            ]
        )
        + KafkaScenario.END_MARKER
        # Ensure this config works.
        + dedent(
            """
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET storage_upsert_max_snapshot_batch_buffering = 2;
            """
        )
        + KafkaScenario.SOURCE
        + dedent(
            """
            # Expect all ingested data + two MARKERs
            > SELECT * FROM v1;
            3

            > SELECT COUNT(*) FROM s1_tbl;
            3
            """
        ),
        post_restart=KafkaScenario.SCHEMAS + KafkaScenario.POST_RESTART,
        materialized_memory="2Gb",
        clusterd_memory="3.5Gb",
    ),
    # Perform updates while the source is ingesting
    KafkaScenario(
        name="upsert-update",
        pre_restart=KafkaScenario.SCHEMAS
        + KafkaScenario.CONNECTIONS
        + KafkaScenario.SOURCE
        + "\n".join(
            [
                dedent(
                    f"""
                    $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${{value-schema}} key-schema=${{key-schema}} repeat={REPEAT}
                    "${{kafka-ingest.iteration}}" {{"f1": "{i}{STRING_PAD}"}}
                    """
                )
                for i in range(0, ITERATIONS)
            ]
        )
        + KafkaScenario.END_MARKER
        + dedent(
            f"""
            # Expect all ingested data + two MARKERs
            > SELECT * FROM v1;
            {REPEAT + 2}

            > SELECT COUNT(*) FROM s1_tbl;
            {REPEAT + 2}
            """
        ),
        post_restart=KafkaScenario.SCHEMAS + KafkaScenario.POST_RESTART,
        materialized_memory="4.5Gb",
        clusterd_memory="3.5Gb",
    ),
    # Perform inserts+deletes while the source is ingesting
    KafkaScenario(
        name="upsert-insert-delete",
        pre_restart=KafkaScenario.SCHEMAS
        + KafkaScenario.CONNECTIONS
        + KafkaScenario.SOURCE
        + "\n".join(
            [
                dedent(
                    f"""
                    $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${{value-schema}} key-schema=${{key-schema}} repeat={REPEAT}
                    "${{kafka-ingest.iteration}}" {{"f1": "{letter}{STRING_PAD}"}}

                    $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${{value-schema}} key-schema=${{key-schema}} repeat={REPEAT}
                    "${{kafka-ingest.iteration}}"
                    """
                )
                for letter in ascii_lowercase[:ITERATIONS]
            ]
        )
        + KafkaScenario.END_MARKER
        + dedent(
            """
            # Expect just the two MARKERs
            > SELECT * FROM v1;
            2

            > SELECT COUNT(*) FROM s1_tbl;
            2
            """
        ),
        post_restart=KafkaScenario.SCHEMAS + KafkaScenario.POST_RESTART,
        materialized_memory="4.5Gb",
        clusterd_memory="3.5Gb",
    ),
    Scenario(
        name="table-insert-delete",
        pre_restart=dedent(
            """
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET max_result_size = 2147483648;

            > CREATE TABLE t1 (f1 STRING, f2 STRING)
            > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM t1;
            """
        )
        + "\n".join(
            [
                dedent(
                    f"""
                    > INSERT INTO t1 (f1, f2) SELECT '{letter}', REPEAT('a', {PAD_LEN}) || generate_series::text FROM generate_series(1, {REPEAT});
                    > DELETE FROM t1 WHERE f1 = '{letter}';
                    """
                )
                for letter in ascii_lowercase[:ITERATIONS]
            ]
        )
        + dedent(
            """
            > SELECT * FROM v1;
            0

            > SELECT COUNT(*) FROM t1;
            0
            """
        ),
        post_restart=dedent(
            """
           > SELECT * FROM v1;
           0

           > SELECT COUNT(*) FROM t1;
           0
           """
        ),
        materialized_memory="4.5Gb",
        clusterd_memory="3.5Gb",
    ),
    Scenario(
        name="table-index-hydration",
        pre_restart=dedent(
            """
            > DROP CLUSTER REPLICA clusterd.r1;

            > CREATE TABLE t (a bigint, b bigint);

            > CREATE INDEX idx IN CLUSTER clusterd ON t (a);

            # We do not want to get a 'canceling statement due to statement timeout' error
            > SET statement_timeout = '300s'

            # And we do not want the DMLs to be retried in any circumstance
            $ set-max-tries max-tries=1

            > INSERT INTO t SELECT a, a FROM generate_series(1, 2000000) AS a;
            > UPDATE t SET b = b + 100000;
            > UPDATE t SET b = b + 1000000;
            > UPDATE t SET b = b + 10000000;
            > UPDATE t SET b = b + 100000000;
            > UPDATE t SET b = b + 1000000000;
            > UPDATE t SET a = a + 100000;
            > UPDATE t SET a = a + 1000000;
            > UPDATE t SET a = a + 10000000;
            > UPDATE t SET a = a + 100000000;
            > UPDATE t SET a = a + 1000000000;

            > CREATE CLUSTER REPLICA clusterd.r1
              STORAGECTL ADDRESSES ['clusterd:2100'],
              STORAGE ADDRESSES ['clusterd:2103'],
              COMPUTECTL ADDRESSES ['clusterd:2101'],
              COMPUTE ADDRESSES ['clusterd:2102'];

            > SET CLUSTER = clusterd

            > SELECT COUNT(*) FROM t;
            2000000
            """
        ),
        post_restart=dedent(
            """
            > SET CLUSTER = clusterd

            > SELECT COUNT(*) FROM t;
            2000000
            """
        ),
        materialized_memory="10Gb",
        clusterd_memory="3.5Gb",
    ),
    Scenario(
        name="accumulate-reductions",
        pre_restart=dedent(
            """
            > DROP TABLE IF EXISTS t CASCADE;
            > CREATE TABLE t (a int, b int, c int, d int);

            > CREATE MATERIALIZED VIEW data AS
              SELECT a, a AS b FROM generate_series(1, 10000000) AS a
              UNION ALL
              SELECT a, b FROM t;

            > INSERT INTO t (a, b) VALUES (1, 1);
            > INSERT INTO t (a, b) VALUES (0, 0);

            > DROP CLUSTER IF EXISTS idx_cluster CASCADE;
            > CREATE CLUSTER idx_cluster SIZE '1-8G', REPLICATION FACTOR 2;

            > CREATE VIEW accumulable AS
              SELECT
                a,
                sum(a) AS sum_a, COUNT(a) as cnt_a,
                sum(b) AS sum_b, COUNT(b) as cnt_b
              FROM data
              GROUP BY a;

            > CREATE INDEX i_accumulable IN CLUSTER idx_cluster ON accumulable(a);

            > SET CLUSTER = idx_cluster;

            > SELECT COUNT(*) FROM accumulable;
            10000001
            """
        ),
        post_restart=dedent(
            """
            > SET CLUSTER = idx_cluster;

            > SELECT COUNT(*) FROM accumulable;
            10000001
            """
        ),
        materialized_memory="8.5Gb",
        clusterd_memory="3.5Gb",
    ),
    KafkaScenario(
        name="upsert-index-hydration",
        pre_restart=KafkaScenario.SCHEMAS
        + KafkaScenario.CONNECTIONS
        + dedent(
            f"""
            $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${{value-schema}} key-schema=${{key-schema}} repeat={90 * REPEAT}
            "${{kafka-ingest.iteration}}" {{"f1": "{STRING_PAD}"}}
            """
        )
        + KafkaScenario.END_MARKER
        + dedent(
            """
            > CREATE SOURCE s1
              IN CLUSTER clusterd
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}');

            > CREATE TABLE s1_tbl FROM SOURCE s1 (REFERENCE "testdrive-topic1-${testdrive.seed}")
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE UPSERT;

            > DROP CLUSTER REPLICA clusterd.r1;

            > CREATE INDEX i1 IN CLUSTER clusterd ON s1_tbl (f1);
            """
        ),
        post_restart=KafkaScenario.SCHEMAS
        + dedent(
            f"""
            > CREATE CLUSTER REPLICA clusterd.r1
              STORAGECTL ADDRESSES ['clusterd:2100'],
              STORAGE ADDRESSES ['clusterd:2103'],
              COMPUTECTL ADDRESSES ['clusterd:2101'],
              COMPUTE ADDRESSES ['clusterd:2102'];

            > SET CLUSTER = clusterd;

            > SELECT COUNT(*) FROM s1_tbl;
            {90 * REPEAT + 2}
            # Delete all rows except markers
            $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${{value-schema}} key-schema=${{key-schema}} repeat={REPEAT}
            "${{kafka-ingest.iteration}}"
            """
        ),
        materialized_memory="7.2Gb",
        clusterd_memory="3.5Gb",
    ),
    Scenario(
        name="table-aggregate",
        pre_restart=dedent(
            f"""
            > SET statement_timeout = '600 s';

            > CREATE TABLE t1 (key1 INTEGER, key2 INTEGER, key3 INTEGER, key4 INTEGER)

            > CREATE MATERIALIZED VIEW v1 IN CLUSTER clusterd AS SELECT key1, MIN(key2), MAX(key3) FROM t1 GROUP BY key1;
            > CREATE DEFAULT INDEX ON v1;

            > CREATE MATERIALIZED VIEW v2 IN CLUSTER clusterd AS SELECT key2, MIN(key1), MAX(key1) FROM t1 GROUP BY key2;
            > CREATE DEFAULT INDEX ON v2;

            > CREATE MATERIALIZED VIEW v3 IN CLUSTER clusterd AS SELECT key3, MIN(key1), MAX(key1) FROM t1 GROUP BY key3;
            > CREATE DEFAULT INDEX ON v3;

            > CREATE MATERIALIZED VIEW v4 IN CLUSTER clusterd AS SELECT key4, MIN(key1), MAX(key1) FROM t1 GROUP BY key4;
            > CREATE DEFAULT INDEX ON v4;

            > INSERT INTO t1 (key1, key2, key3, key4)
              SELECT
                generate_series,
                MOD(generate_series, 10),
                MOD(generate_series, 100),
                MOD(generate_series, 1000)
                FROM generate_series(1, {REPEAT} * {ITERATIONS})

            > SELECT COUNT(*) > 0 FROM v1;
            true
            > SELECT COUNT(*) > 0 FROM v2;
            true
            > SELECT COUNT(*) > 0 FROM v3;
            true
            > SELECT COUNT(*) > 0 FROM v4;
            true
            """
        ),
        post_restart=dedent(
            """
            > SELECT COUNT(*) > 0 FROM v1;
            true
            > SELECT COUNT(*) > 0 FROM v2;
            true
            > SELECT COUNT(*) > 0 FROM v3;
            true
            > SELECT COUNT(*) > 0 FROM v4;
            true
            """
        ),
        materialized_memory="4.5Gb",
        clusterd_memory="5.5Gb",
    ),
    Scenario(
        name="table-outer-join",
        pre_restart=dedent(
            f"""
            > SET statement_timeout = '600 s';

            > CREATE TABLE t1 (key1 INTEGER, f1 STRING DEFAULT 'abcdefghi')

            > CREATE TABLE t2 (key2 INTEGER, f2 STRING DEFAULT 'abcdefghi')

            > CREATE MATERIALIZED VIEW v1
              IN CLUSTER clusterd AS
              SELECT * FROM t1 LEFT JOIN t2 ON (key1 = key2)

            > CREATE DEFAULT INDEX ON v1;

            > CREATE MATERIALIZED VIEW v2
              IN CLUSTER clusterd AS
              SELECT * FROM t2 LEFT JOIN t1 ON (key1 = key2)

            > CREATE DEFAULT INDEX ON v2;

            > INSERT INTO t1 (key1)
              SELECT generate_series FROM generate_series(1, {REPEAT} * {ITERATIONS})

            > INSERT INTO t2 (key2)
              SELECT MOD(generate_series, 10) FROM generate_series(1, {REPEAT} * {ITERATIONS})

            # Records have no match in t2
            > INSERT INTO t1 (key1)
              SELECT generate_series + 1 * ({REPEAT} * {ITERATIONS})
              FROM generate_series(1, {REPEAT} * {ITERATIONS})

            # Records have no match in t1
            > INSERT INTO t2 (key2)
              SELECT generate_series + 2 * ({REPEAT} * {ITERATIONS})
              FROM generate_series(1, {REPEAT} * {ITERATIONS})

            > SELECT COUNT(*) > 0 FROM v1;
            true

            > SELECT COUNT(*) > 0 FROM v2;
            true
            """
        ),
        post_restart=dedent(
            """
            > SELECT COUNT(*) > 0 FROM v1;
            true

            > SELECT COUNT(*) > 0 FROM v2;
            true
            """
        ),
        materialized_memory="4.5Gb",
        clusterd_memory="3.5Gb",
    ),
    Scenario(
        name="cardinality-estimate-disjunction",
        pre_restart=dedent(
            """
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET ENABLE_CARDINALITY_ESTIMATES TO TRUE;

            > SET ENABLE_SESSION_CARDINALITY_ESTIMATES TO TRUE;

            > CREATE TABLE tab0_raw(pk INTEGER, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);
            > CREATE MATERIALIZED VIEW tab0 AS SELECT DISTINCT ON(pk) * FROM tab0_raw;
            > INSERT INTO tab0_raw VALUES(0,6,72.32,'diidw',65,65.1,'uudvn'), (1,57,90.1,'jvnyz',84,48.99,'raktj'), (2,68,91.83,'wefta',37,71.86,'zddoc'), (3,10,78.14,'zwjtc',7,9.96,'epmyn'), (4,63,24.41,'rwaus',66,53.7,'gbgmw'), (5,87,70.88,'rwpww',46,26.5,'bvbew'), (6,76,46.18,'lfvrf',99,92.47,'hqpgb'), (7,25,81.99,'khylz',54,73.22,'qaonp'), (8,93,17.58,'clxlk',88,59.16,'ziwhr'), (9,64,18.54,'fgkop',82,18.73,'lztum');

            > CREATE TABLE tab1_raw(pk INTEGER, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);
            > CREATE MATERIALIZED VIEW tab1 AS SELECT DISTINCT ON(pk) * FROM tab1_raw;
            > CREATE INDEX idx_tab1_0 on tab1 (col0);
            > CREATE INDEX idx_tab1_1 on tab1 (col1);
            > CREATE INDEX idx_tab1_3 on tab1 (col3);
            > CREATE INDEX idx_tab1_4 on tab1 (col4);
            > INSERT INTO tab1_raw SELECT * FROM tab0;

            > CREATE TABLE tab2_raw(pk INTEGER, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);
            > CREATE MATERIALIZED VIEW tab2 AS SELECT DISTINCT ON(pk) * FROM tab2_raw;
            > CREATE INDEX idx_tab2_0 ON tab2 (col4);
            > CREATE INDEX idx_tab2_2 ON tab2 (col1);
            > CREATE INDEX idx_tab2_4 ON tab2 (col0,col3 DESC);
            > CREATE INDEX idx_tab2_5 ON tab2 (col3);
            > INSERT INTO tab2_raw SELECT * FROM tab0;

            > CREATE TABLE tab3_raw(pk INTEGER, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);
            > CREATE MATERIALIZED VIEW tab3 AS SELECT DISTINCT ON(pk) * FROM tab3_raw;
            > CREATE INDEX idx_tab3_0 ON tab3 (col0,col1,col3);
            > CREATE INDEX idx_tab3_1 ON tab3 (col4);
            > INSERT INTO tab3_raw SELECT * FROM tab0;

            > CREATE TABLE tab4_raw(pk INTEGER, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);
            > CREATE MATERIALIZED VIEW tab4 AS SELECT DISTINCT ON(pk) * FROM tab4_raw;
            > CREATE INDEX idx_tab4_0 ON tab4 (col4 DESC,col3 DESC);
            > CREATE INDEX idx_tab4_2 ON tab4 (col1);
            > INSERT INTO tab4_raw SELECT * FROM tab0;

            > SELECT pk FROM tab0 WHERE col0 <= 88 AND (((col4 <= 97.11 AND col0 IN (11,85,87,63,88) OR (col0 <= 45 AND ((((((((col0 > 79)) OR col1 <= 30.14 OR col3 >= 12))) OR col0 >= 89 OR col1 < 20.99 OR col1 >= 74.51 AND col3 > 77) AND (col0 IN (67,97,94,86,81))) AND ((col1 <= 10.70 AND col1 IS NULL AND col3 > 49 AND col3 > 66 AND (((col4 > 42.2) AND ((((col4 < 86.27) AND col3 >= 77 AND col3 < 48))) AND col3 >= 49)) AND col0 IN (SELECT col3 FROM tab0 WHERE col4 BETWEEN 20.3 AND 97.63))) AND col0 >= 25) OR ((col0 <= 35)) AND col0 < 68 OR ((col0 = 98))) OR (col1 <= 17.96) AND ((((col0 IS NULL))) OR col4 <= 2.63 AND (col0 > 2) AND col3 > 8) OR col3 <= 88 AND (((col0 IS NULL))) OR col0 >= 30)) AND col0 > 5) OR col0 > 3;
            0
            1
            2
            3
            4
            5
            6
            7
            8
            9

            ?[version>=14400] EXPLAIN OPTIMIZED PLAN WITH(cardinality) AS VERBOSE TEXT FOR SELECT pk FROM tab0 WHERE col0 <= 88 AND (((col4 <= 97.11 AND col0 IN (11,85,87,63,88) OR (col0 <= 45 AND ((((((((col0 > 79)) OR col1 <= 30.14 OR col3 >= 12))) OR col0 >= 89 OR col1 < 20.99 OR col1 >= 74.51 AND col3 > 77) AND (col0 IN (67,97,94,86,81))) AND ((col1 <= 10.70 AND col1 IS NULL AND col3 > 49 AND col3 > 66 AND (((col4 > 42.2) AND ((((col4 < 86.27) AND col3 >= 77 AND col3 < 48))) AND col3 >= 49)) AND col0 IN (SELECT col3 FROM tab0 WHERE col4 BETWEEN 20.3 AND 97.63))) AND col0 >= 25) OR ((col0 <= 35)) AND col0 < 68 OR ((col0 = 98))) OR (col1 <= 17.96) AND ((((col0 IS NULL))) OR col4 <= 2.63 AND (col0 > 2) AND col3 > 8) OR col3 <= 88 AND (((col0 IS NULL))) OR col0 >= 30)) AND col0 > 5) OR col0 > 3;
            Explained Query:
              With
                cte l0 =
                  Distinct project=[#0] // { cardinality: \"<UNKNOWN>\" }
                    Project (#1) // { cardinality: \"<UNKNOWN>\" }
                      ReadStorage materialize.public.tab0 // { cardinality: \"<UNKNOWN>\" }
                cte l1 =
                  Reduce group_by=[#0] aggregates=[any((#0{col0} = #1{right_col0_0}))] // { cardinality: \"<UNKNOWN>\" }
                    CrossJoin type=differential // { cardinality: \"<UNKNOWN>\" }
                      ArrangeBy keys=[[]] // { cardinality: \"<UNKNOWN>\" }
                        Get l0 // { cardinality: \"<UNKNOWN>\" }
                      ArrangeBy keys=[[]] // { cardinality: \"<UNKNOWN>\" }
                        Project (#4) // { cardinality: \"<UNKNOWN>\" }
                          Filter (#5{col4} <= 97.63) AND (#5{col4} >= 20.3) // { cardinality: \"<UNKNOWN>\" }
                            ReadStorage materialize.public.tab0 // { cardinality: \"<UNKNOWN>\" }
                cte l2 =
                  Union // { cardinality: \"<UNKNOWN>\" }
                    Get l1 // { cardinality: \"<UNKNOWN>\" }
                    Map (false) // { cardinality: \"<UNKNOWN>\" }
                      Union // { cardinality: \"<UNKNOWN>\" }
                        Negate // { cardinality: \"<UNKNOWN>\" }
                          Project (#0) // { cardinality: \"<UNKNOWN>\" }
                            Get l1 // { cardinality: \"<UNKNOWN>\" }
                        Get l0 // { cardinality: \"<UNKNOWN>\" }
              Return // { cardinality: \"<UNKNOWN>\" }
                Project (#0) // { cardinality: \"<UNKNOWN>\" }
                  Filter ((#1{col0} > 3) OR ((#1{col0} <= 88) AND (#1{col0} > 5) AND ((#1{col0} = 98) OR (#1{col0} >= 30) OR (#6 AND (#2{col1}) IS NULL AND (#3{col3} < 48) AND (#4{col4} < 86.27) AND (#1{col0} <= 45) AND (#2{col1} <= 10.7) AND (#3{col3} > 49) AND (#3{col3} > 66) AND (#4{col4} > 42.2) AND (#1{col0} >= 25) AND (#3{col3} >= 49) AND (#3{col3} >= 77) AND ((#1{col0} = 67) OR (#1{col0} = 81) OR (#1{col0} = 86) OR (#1{col0} = 94) OR (#1{col0} = 97)) AND ((#2{col1} < 20.99) OR (#2{col1} <= 30.14) OR (#1{col0} > 79) OR (#1{col0} >= 89) OR (#3{col3} >= 12) OR ((#3{col3} > 77) AND (#2{col1} >= 74.51)))) OR (#7 AND (#3{col3} <= 88)) OR ((#1{col0} < 68) AND (#1{col0} <= 35)) OR ((#2{col1} <= 17.96) AND (#7 OR ((#4{col4} <= 2.63) AND (#1{col0} > 2) AND (#3{col3} > 8)))) OR ((#4{col4} <= 97.11) AND ((#1{col0} = 11) OR (#1{col0} = 63) OR (#1{col0} = 85) OR (#1{col0} = 87) OR (#1{col0} = 88)))))) // { cardinality: \"<UNKNOWN>\" }
                    Map ((#1{col0}) IS NULL) // { cardinality: \"<UNKNOWN>\" }
                      Join on=(#1 = #5) type=differential // { cardinality: \"<UNKNOWN>\" }
                        ArrangeBy keys=[[#1]] // { cardinality: \"<UNKNOWN>\" }
                          Project (#0..=#2, #4, #5) // { cardinality: \"<UNKNOWN>\" }
                            Filter ((#1{col0} > 3) OR ((#1{col0} <= 88) AND (#1{col0} > 5) AND ((#1{col0} = 98) OR (#1{col0} >= 30) OR (#7 AND (#4{col3} <= 88)) OR ((#2{col1}) IS NULL AND (#4{col3} < 48) AND (#5{col4} < 86.27) AND (#1{col0} <= 45) AND (#2{col1} <= 10.7) AND (#4{col3} > 49) AND (#4{col3} > 66) AND (#5{col4} > 42.2) AND (#1{col0} >= 25) AND (#4{col3} >= 49) AND (#4{col3} >= 77) AND ((#1{col0} = 67) OR (#1{col0} = 81) OR (#1{col0} = 86) OR (#1{col0} = 94) OR (#1{col0} = 97)) AND ((#2{col1} < 20.99) OR (#2{col1} <= 30.14) OR (#1{col0} > 79) OR (#1{col0} >= 89) OR (#4{col3} >= 12) OR ((#4{col3} > 77) AND (#2{col1} >= 74.51)))) OR ((#1{col0} < 68) AND (#1{col0} <= 35)) OR ((#2{col1} <= 17.96) AND (#7 OR ((#5{col4} <= 2.63) AND (#1{col0} > 2) AND (#4{col3} > 8)))) OR ((#5{col4} <= 97.11) AND ((#1{col0} = 11) OR (#1{col0} = 63) OR (#1{col0} = 85) OR (#1{col0} = 87) OR (#1{col0} = 88)))))) // { cardinality: \"<UNKNOWN>\" }
                              Map ((#1{col0}) IS NULL) // { cardinality: \"<UNKNOWN>\" }
                                ReadStorage materialize.public.tab0 // { cardinality: \"<UNKNOWN>\" }
                        ArrangeBy keys=[[#0]] // { cardinality: \"<UNKNOWN>\" }
                          Union // { cardinality: \"<UNKNOWN>\" }
                            Filter ((#0 > 3) OR ((#0 <= 88) AND (#0 > 5) AND ((#0) IS NULL OR (#0 = 11) OR (#0 = 63) OR (#0 = 85) OR (#0 = 87) OR (#0 = 88) OR (#0 = 98) OR (#0 > 2) OR (#0 >= 30) OR (#1 AND (#0 <= 45) AND (#0 >= 25) AND ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97))) OR ((#0 < 68) AND (#0 <= 35))))) // { cardinality: \"<UNKNOWN>\" }
                              Get l2 // { cardinality: \"<UNKNOWN>\" }
                            Project (#0, #18) // { cardinality: \"<UNKNOWN>\" }
                              Filter (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16)))) AND (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16 AND null)))) // { cardinality: \"<UNKNOWN>\" }
                                Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35)), null) // { cardinality: \"<UNKNOWN>\" }
                                  Join on=(#0 = #1) type=differential // { cardinality: \"<UNKNOWN>\" }
                                    ArrangeBy keys=[[#0]] // { cardinality: \"<UNKNOWN>\" }
                                      Union // { cardinality: \"<UNKNOWN>\" }
                                        Negate // { cardinality: \"<UNKNOWN>\" }
                                          Project (#0) // { cardinality: \"<UNKNOWN>\" }
                                            Filter (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16)))) AND (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16 AND null)))) // { cardinality: \"<UNKNOWN>\" }
                                              Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35))) // { cardinality: \"<UNKNOWN>\" }
                                                Get l2 // { cardinality: \"<UNKNOWN>\" }
                                        Project (#0) // { cardinality: \"<UNKNOWN>\" }
                                          Filter (#1 OR (#2 AND #3 AND (#4 OR #5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #16 OR (#13 AND #14 AND #15)))) AND (#1 OR (#2 AND #3 AND (#4 OR #5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #16 OR (#13 AND #14 AND #15 AND null)))) // { cardinality: \"<UNKNOWN>\" }
                                            Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35))) // { cardinality: \"<UNKNOWN>\" }
                                              Get l0 // { cardinality: \"<UNKNOWN>\" }
                                    ArrangeBy keys=[[#0]] // { cardinality: \"<UNKNOWN>\" }
                                      Get l0 // { cardinality: \"<UNKNOWN>\" }

            Source materialize.public.tab0

            Target cluster: quickstart

            ?[version<14400] EXPLAIN OPTIMIZED PLAN WITH(cardinality) AS VERBOSE TEXT FOR SELECT pk FROM tab0 WHERE col0 <= 88 AND (((col4 <= 97.11 AND col0 IN (11,85,87,63,88) OR (col0 <= 45 AND ((((((((col0 > 79)) OR col1 <= 30.14 OR col3 >= 12))) OR col0 >= 89 OR col1 < 20.99 OR col1 >= 74.51 AND col3 > 77) AND (col0 IN (67,97,94,86,81))) AND ((col1 <= 10.70 AND col1 IS NULL AND col3 > 49 AND col3 > 66 AND (((col4 > 42.2) AND ((((col4 < 86.27) AND col3 >= 77 AND col3 < 48))) AND col3 >= 49)) AND col0 IN (SELECT col3 FROM tab0 WHERE col4 BETWEEN 20.3 AND 97.63))) AND col0 >= 25) OR ((col0 <= 35)) AND col0 < 68 OR ((col0 = 98))) OR (col1 <= 17.96) AND ((((col0 IS NULL))) OR col4 <= 2.63 AND (col0 > 2) AND col3 > 8) OR col3 <= 88 AND (((col0 IS NULL))) OR col0 >= 30)) AND col0 > 5) OR col0 > 3;
            Explained Query:
              With
                cte l0 =
                  Distinct project=[#0] // { cardinality: "<UNKNOWN>" }
                    Project (#1) // { cardinality: "<UNKNOWN>" }
                      ReadStorage materialize.public.tab0 // { cardinality: "<UNKNOWN>" }
                cte l1 =
                  Reduce group_by=[#0] aggregates=[any((#0 = #1))] // { cardinality: "<UNKNOWN>" }
                    CrossJoin type=differential // { cardinality: "<UNKNOWN>" }
                      ArrangeBy keys=[[]] // { cardinality: "<UNKNOWN>" }
                        Get l0 // { cardinality: "<UNKNOWN>" }
                      ArrangeBy keys=[[]] // { cardinality: "<UNKNOWN>" }
                        Project (#4) // { cardinality: "<UNKNOWN>" }
                          Filter (#5 <= 97.63) AND (#5 >= 20.3) // { cardinality: "<UNKNOWN>" }
                            ReadStorage materialize.public.tab0 // { cardinality: "<UNKNOWN>" }
                cte l2 =
                  Union // { cardinality: "<UNKNOWN>" }
                    Get l1 // { cardinality: "<UNKNOWN>" }
                    Map (false) // { cardinality: "<UNKNOWN>" }
                      Union // { cardinality: "<UNKNOWN>" }
                        Negate // { cardinality: "<UNKNOWN>" }
                          Project (#0) // { cardinality: "<UNKNOWN>" }
                            Get l1 // { cardinality: "<UNKNOWN>" }
                        Get l0 // { cardinality: "<UNKNOWN>" }
              Return // { cardinality: "<UNKNOWN>" }
                Project (#0) // { cardinality: "<UNKNOWN>" }
                  Filter ((#1 > 3) OR ((#1 <= 88) AND (#1 > 5) AND ((#1 = 98) OR (#1 >= 30) OR (#6 AND (#2) IS NULL AND (#3 < 48) AND (#4 < 86.27) AND (#1 <= 45) AND (#2 <= 10.7) AND (#3 > 49) AND (#3 > 66) AND (#4 > 42.2) AND (#1 >= 25) AND (#3 >= 49) AND (#3 >= 77) AND ((#1 = 67) OR (#1 = 81) OR (#1 = 86) OR (#1 = 94) OR (#1 = 97)) AND ((#2 < 20.99) OR (#2 <= 30.14) OR (#1 > 79) OR (#1 >= 89) OR (#3 >= 12) OR ((#3 > 77) AND (#2 >= 74.51)))) OR (#7 AND (#3 <= 88)) OR ((#1 < 68) AND (#1 <= 35)) OR ((#2 <= 17.96) AND (#7 OR ((#4 <= 2.63) AND (#1 > 2) AND (#3 > 8)))) OR ((#4 <= 97.11) AND ((#1 = 11) OR (#1 = 63) OR (#1 = 85) OR (#1 = 87) OR (#1 = 88)))))) // { cardinality: "<UNKNOWN>" }
                    Map ((#1) IS NULL) // { cardinality: "<UNKNOWN>" }
                      Join on=(#1 = #5) type=differential // { cardinality: "<UNKNOWN>" }
                        ArrangeBy keys=[[#1]] // { cardinality: "<UNKNOWN>" }
                          Project (#0..=#2, #4, #5) // { cardinality: "<UNKNOWN>" }
                            Filter ((#1 > 3) OR ((#1 <= 88) AND (#1 > 5) AND ((#1 = 98) OR (#1 >= 30) OR (#7 AND (#4 <= 88)) OR ((#2) IS NULL AND (#4 < 48) AND (#5 < 86.27) AND (#1 <= 45) AND (#2 <= 10.7) AND (#4 > 49) AND (#4 > 66) AND (#5 > 42.2) AND (#1 >= 25) AND (#4 >= 49) AND (#4 >= 77) AND ((#1 = 67) OR (#1 = 81) OR (#1 = 86) OR (#1 = 94) OR (#1 = 97)) AND ((#2 < 20.99) OR (#2 <= 30.14) OR (#1 > 79) OR (#1 >= 89) OR (#4 >= 12) OR ((#4 > 77) AND (#2 >= 74.51)))) OR ((#1 < 68) AND (#1 <= 35)) OR ((#2 <= 17.96) AND (#7 OR ((#5 <= 2.63) AND (#1 > 2) AND (#4 > 8)))) OR ((#5 <= 97.11) AND ((#1 = 11) OR (#1 = 63) OR (#1 = 85) OR (#1 = 87) OR (#1 = 88)))))) // { cardinality: "<UNKNOWN>" }
                              Map ((#1) IS NULL) // { cardinality: "<UNKNOWN>" }
                                ReadStorage materialize.public.tab0 // { cardinality: "<UNKNOWN>" }
                        ArrangeBy keys=[[#0]] // { cardinality: "<UNKNOWN>" }
                          Union // { cardinality: "<UNKNOWN>" }
                            Filter ((#0 > 3) OR ((#0 <= 88) AND (#0 > 5) AND ((#0) IS NULL OR (#0 = 11) OR (#0 = 63) OR (#0 = 85) OR (#0 = 87) OR (#0 = 88) OR (#0 = 98) OR (#0 > 2) OR (#0 >= 30) OR (#1 AND (#0 <= 45) AND (#0 >= 25) AND ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97))) OR ((#0 < 68) AND (#0 <= 35))))) // { cardinality: "<UNKNOWN>" }
                              Get l2 // { cardinality: "<UNKNOWN>" }
                            Project (#0, #18) // { cardinality: "<UNKNOWN>" }
                              Filter (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16)))) AND (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16 AND null)))) // { cardinality: "<UNKNOWN>" }
                                Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35)), null) // { cardinality: "<UNKNOWN>" }
                                  Join on=(#0 = #1) type=differential // { cardinality: "<UNKNOWN>" }
                                    ArrangeBy keys=[[#0]] // { cardinality: "<UNKNOWN>" }
                                      Union // { cardinality: "<UNKNOWN>" }
                                        Negate // { cardinality: "<UNKNOWN>" }
                                          Project (#0) // { cardinality: "<UNKNOWN>" }
                                            Filter (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16)))) AND (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16 AND null)))) // { cardinality: "<UNKNOWN>" }
                                              Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35))) // { cardinality: "<UNKNOWN>" }
                                                Get l2 // { cardinality: "<UNKNOWN>" }
                                        Project (#0) // { cardinality: "<UNKNOWN>" }
                                          Filter (#1 OR (#2 AND #3 AND (#4 OR #5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #16 OR (#13 AND #14 AND #15)))) AND (#1 OR (#2 AND #3 AND (#4 OR #5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #16 OR (#13 AND #14 AND #15 AND null)))) // { cardinality: "<UNKNOWN>" }
                                            Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35))) // { cardinality: "<UNKNOWN>" }
                                              Get l0 // { cardinality: "<UNKNOWN>" }
                                    ArrangeBy keys=[[#0]] // { cardinality: "<UNKNOWN>" }
                                      Get l0 // { cardinality: "<UNKNOWN>" }

            Source materialize.public.tab0

            Target cluster: quickstart
            """
        ),
        post_restart=dedent(
            """
            > SELECT pk FROM tab0 WHERE col0 <= 88 AND (((col4 <= 97.11 AND col0 IN (11,85,87,63,88) OR (col0 <= 45 AND ((((((((col0 > 79)) OR col1 <= 30.14 OR col3 >= 12))) OR col0 >= 89 OR col1 < 20.99 OR col1 >= 74.51 AND col3 > 77) AND (col0 IN (67,97,94,86,81))) AND ((col1 <= 10.70 AND col1 IS NULL AND col3 > 49 AND col3 > 66 AND (((col4 > 42.2) AND ((((col4 < 86.27) AND col3 >= 77 AND col3 < 48))) AND col3 >= 49)) AND col0 IN (SELECT col3 FROM tab0 WHERE col4 BETWEEN 20.3 AND 97.63))) AND col0 >= 25) OR ((col0 <= 35)) AND col0 < 68 OR ((col0 = 98))) OR (col1 <= 17.96) AND ((((col0 IS NULL))) OR col4 <= 2.63 AND (col0 > 2) AND col3 > 8) OR col3 <= 88 AND (((col0 IS NULL))) OR col0 >= 30)) AND col0 > 5) OR col0 > 3;
            0
            1
            2
            3
            4
            5
            6
            7
            8
            9

            ?[version>=14400] EXPLAIN OPTIMIZED PLAN WITH(cardinality) AS VERBOSE TEXT FOR SELECT pk FROM tab0 WHERE col0 <= 88 AND (((col4 <= 97.11 AND col0 IN (11,85,87,63,88) OR (col0 <= 45 AND ((((((((col0 > 79)) OR col1 <= 30.14 OR col3 >= 12))) OR col0 >= 89 OR col1 < 20.99 OR col1 >= 74.51 AND col3 > 77) AND (col0 IN (67,97,94,86,81))) AND ((col1 <= 10.70 AND col1 IS NULL AND col3 > 49 AND col3 > 66 AND (((col4 > 42.2) AND ((((col4 < 86.27) AND col3 >= 77 AND col3 < 48))) AND col3 >= 49)) AND col0 IN (SELECT col3 FROM tab0 WHERE col4 BETWEEN 20.3 AND 97.63))) AND col0 >= 25) OR ((col0 <= 35)) AND col0 < 68 OR ((col0 = 98))) OR (col1 <= 17.96) AND ((((col0 IS NULL))) OR col4 <= 2.63 AND (col0 > 2) AND col3 > 8) OR col3 <= 88 AND (((col0 IS NULL))) OR col0 >= 30)) AND col0 > 5) OR col0 > 3;
            Explained Query:
              With
                cte l0 =
                  Distinct project=[#0] // { cardinality: \"<UNKNOWN>\" }
                    Project (#1) // { cardinality: \"<UNKNOWN>\" }
                      ReadStorage materialize.public.tab0 // { cardinality: \"<UNKNOWN>\" }
                cte l1 =
                  Reduce group_by=[#0] aggregates=[any((#0{col0} = #1{right_col0_0}))] // { cardinality: \"<UNKNOWN>\" }
                    CrossJoin type=differential // { cardinality: \"<UNKNOWN>\" }
                      ArrangeBy keys=[[]] // { cardinality: \"<UNKNOWN>\" }
                        Get l0 // { cardinality: \"<UNKNOWN>\" }
                      ArrangeBy keys=[[]] // { cardinality: \"<UNKNOWN>\" }
                        Project (#4) // { cardinality: \"<UNKNOWN>\" }
                          Filter (#5{col4} <= 97.63) AND (#5{col4} >= 20.3) // { cardinality: \"<UNKNOWN>\" }
                            ReadStorage materialize.public.tab0 // { cardinality: \"<UNKNOWN>\" }
                cte l2 =
                  Union // { cardinality: \"<UNKNOWN>\" }
                    Get l1 // { cardinality: \"<UNKNOWN>\" }
                    Map (false) // { cardinality: \"<UNKNOWN>\" }
                      Union // { cardinality: \"<UNKNOWN>\" }
                        Negate // { cardinality: \"<UNKNOWN>\" }
                          Project (#0) // { cardinality: \"<UNKNOWN>\" }
                            Get l1 // { cardinality: \"<UNKNOWN>\" }
                        Get l0 // { cardinality: \"<UNKNOWN>\" }
              Return // { cardinality: \"<UNKNOWN>\" }
                Project (#0) // { cardinality: \"<UNKNOWN>\" }
                  Filter ((#1{col0} > 3) OR ((#1{col0} <= 88) AND (#1{col0} > 5) AND ((#1{col0} = 98) OR (#1{col0} >= 30) OR (#6 AND (#2{col1}) IS NULL AND (#3{col3} < 48) AND (#4{col4} < 86.27) AND (#1{col0} <= 45) AND (#2{col1} <= 10.7) AND (#3{col3} > 49) AND (#3{col3} > 66) AND (#4{col4} > 42.2) AND (#1{col0} >= 25) AND (#3{col3} >= 49) AND (#3{col3} >= 77) AND ((#1{col0} = 67) OR (#1{col0} = 81) OR (#1{col0} = 86) OR (#1{col0} = 94) OR (#1{col0} = 97)) AND ((#2{col1} < 20.99) OR (#2{col1} <= 30.14) OR (#1{col0} > 79) OR (#1{col0} >= 89) OR (#3{col3} >= 12) OR ((#3{col3} > 77) AND (#2{col1} >= 74.51)))) OR (#7 AND (#3{col3} <= 88)) OR ((#1{col0} < 68) AND (#1{col0} <= 35)) OR ((#2{col1} <= 17.96) AND (#7 OR ((#4{col4} <= 2.63) AND (#1{col0} > 2) AND (#3{col3} > 8)))) OR ((#4{col4} <= 97.11) AND ((#1{col0} = 11) OR (#1{col0} = 63) OR (#1{col0} = 85) OR (#1{col0} = 87) OR (#1{col0} = 88)))))) // { cardinality: \"<UNKNOWN>\" }
                    Map ((#1{col0}) IS NULL) // { cardinality: \"<UNKNOWN>\" }
                      Join on=(#1 = #5) type=differential // { cardinality: \"<UNKNOWN>\" }
                        ArrangeBy keys=[[#1]] // { cardinality: \"<UNKNOWN>\" }
                          Project (#0..=#2, #4, #5) // { cardinality: \"<UNKNOWN>\" }
                            Filter ((#1{col0} > 3) OR ((#1{col0} <= 88) AND (#1{col0} > 5) AND ((#1{col0} = 98) OR (#1{col0} >= 30) OR (#7 AND (#4{col3} <= 88)) OR ((#2{col1}) IS NULL AND (#4{col3} < 48) AND (#5{col4} < 86.27) AND (#1{col0} <= 45) AND (#2{col1} <= 10.7) AND (#4{col3} > 49) AND (#4{col3} > 66) AND (#5{col4} > 42.2) AND (#1{col0} >= 25) AND (#4{col3} >= 49) AND (#4{col3} >= 77) AND ((#1{col0} = 67) OR (#1{col0} = 81) OR (#1{col0} = 86) OR (#1{col0} = 94) OR (#1{col0} = 97)) AND ((#2{col1} < 20.99) OR (#2{col1} <= 30.14) OR (#1{col0} > 79) OR (#1{col0} >= 89) OR (#4{col3} >= 12) OR ((#4{col3} > 77) AND (#2{col1} >= 74.51)))) OR ((#1{col0} < 68) AND (#1{col0} <= 35)) OR ((#2{col1} <= 17.96) AND (#7 OR ((#5{col4} <= 2.63) AND (#1{col0} > 2) AND (#4{col3} > 8)))) OR ((#5{col4} <= 97.11) AND ((#1{col0} = 11) OR (#1{col0} = 63) OR (#1{col0} = 85) OR (#1{col0} = 87) OR (#1{col0} = 88)))))) // { cardinality: \"<UNKNOWN>\" }
                              Map ((#1{col0}) IS NULL) // { cardinality: \"<UNKNOWN>\" }
                                ReadStorage materialize.public.tab0 // { cardinality: \"<UNKNOWN>\" }
                        ArrangeBy keys=[[#0]] // { cardinality: \"<UNKNOWN>\" }
                          Union // { cardinality: \"<UNKNOWN>\" }
                            Filter ((#0 > 3) OR ((#0 <= 88) AND (#0 > 5) AND ((#0) IS NULL OR (#0 = 11) OR (#0 = 63) OR (#0 = 85) OR (#0 = 87) OR (#0 = 88) OR (#0 = 98) OR (#0 > 2) OR (#0 >= 30) OR (#1 AND (#0 <= 45) AND (#0 >= 25) AND ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97))) OR ((#0 < 68) AND (#0 <= 35))))) // { cardinality: \"<UNKNOWN>\" }
                              Get l2 // { cardinality: \"<UNKNOWN>\" }
                            Project (#0, #18) // { cardinality: \"<UNKNOWN>\" }
                              Filter (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16)))) AND (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16 AND null)))) // { cardinality: \"<UNKNOWN>\" }
                                Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35)), null) // { cardinality: \"<UNKNOWN>\" }
                                  Join on=(#0 = #1) type=differential // { cardinality: \"<UNKNOWN>\" }
                                    ArrangeBy keys=[[#0]] // { cardinality: \"<UNKNOWN>\" }
                                      Union // { cardinality: \"<UNKNOWN>\" }
                                        Negate // { cardinality: \"<UNKNOWN>\" }
                                          Project (#0) // { cardinality: \"<UNKNOWN>\" }
                                            Filter (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16)))) AND (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16 AND null)))) // { cardinality: \"<UNKNOWN>\" }
                                              Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35))) // { cardinality: \"<UNKNOWN>\" }
                                                Get l2 // { cardinality: \"<UNKNOWN>\" }
                                        Project (#0) // { cardinality: \"<UNKNOWN>\" }
                                          Filter (#1 OR (#2 AND #3 AND (#4 OR #5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #16 OR (#13 AND #14 AND #15)))) AND (#1 OR (#2 AND #3 AND (#4 OR #5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #16 OR (#13 AND #14 AND #15 AND null)))) // { cardinality: \"<UNKNOWN>\" }
                                            Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35))) // { cardinality: \"<UNKNOWN>\" }
                                              Get l0 // { cardinality: \"<UNKNOWN>\" }
                                    ArrangeBy keys=[[#0]] // { cardinality: \"<UNKNOWN>\" }
                                      Get l0 // { cardinality: \"<UNKNOWN>\" }

            Source materialize.public.tab0

            Target cluster: quickstart

            ?[version<14400] EXPLAIN OPTIMIZED PLAN WITH(cardinality) AS VERBOSE TEXT FOR SELECT pk FROM tab0 WHERE col0 <= 88 AND (((col4 <= 97.11 AND col0 IN (11,85,87,63,88) OR (col0 <= 45 AND ((((((((col0 > 79)) OR col1 <= 30.14 OR col3 >= 12))) OR col0 >= 89 OR col1 < 20.99 OR col1 >= 74.51 AND col3 > 77) AND (col0 IN (67,97,94,86,81))) AND ((col1 <= 10.70 AND col1 IS NULL AND col3 > 49 AND col3 > 66 AND (((col4 > 42.2) AND ((((col4 < 86.27) AND col3 >= 77 AND col3 < 48))) AND col3 >= 49)) AND col0 IN (SELECT col3 FROM tab0 WHERE col4 BETWEEN 20.3 AND 97.63))) AND col0 >= 25) OR ((col0 <= 35)) AND col0 < 68 OR ((col0 = 98))) OR (col1 <= 17.96) AND ((((col0 IS NULL))) OR col4 <= 2.63 AND (col0 > 2) AND col3 > 8) OR col3 <= 88 AND (((col0 IS NULL))) OR col0 >= 30)) AND col0 > 5) OR col0 > 3;
            Explained Query:
              With
                cte l0 =
                  Distinct project=[#0] // { cardinality: "<UNKNOWN>" }
                    Project (#1) // { cardinality: "<UNKNOWN>" }
                      ReadStorage materialize.public.tab0 // { cardinality: "<UNKNOWN>" }
                cte l1 =
                  Reduce group_by=[#0] aggregates=[any((#0 = #1))] // { cardinality: "<UNKNOWN>" }
                    CrossJoin type=differential // { cardinality: "<UNKNOWN>" }
                      ArrangeBy keys=[[]] // { cardinality: "<UNKNOWN>" }
                        Get l0 // { cardinality: "<UNKNOWN>" }
                      ArrangeBy keys=[[]] // { cardinality: "<UNKNOWN>" }
                        Project (#4) // { cardinality: "<UNKNOWN>" }
                          Filter (#5 <= 97.63) AND (#5 >= 20.3) // { cardinality: "<UNKNOWN>" }
                            ReadStorage materialize.public.tab0 // { cardinality: "<UNKNOWN>" }
                cte l2 =
                  Union // { cardinality: "<UNKNOWN>" }
                    Get l1 // { cardinality: "<UNKNOWN>" }
                    Map (false) // { cardinality: "<UNKNOWN>" }
                      Union // { cardinality: "<UNKNOWN>" }
                        Negate // { cardinality: "<UNKNOWN>" }
                          Project (#0) // { cardinality: "<UNKNOWN>" }
                            Get l1 // { cardinality: "<UNKNOWN>" }
                        Get l0 // { cardinality: "<UNKNOWN>" }
              Return // { cardinality: "<UNKNOWN>" }
                Project (#0) // { cardinality: "<UNKNOWN>" }
                  Filter ((#1 > 3) OR ((#1 <= 88) AND (#1 > 5) AND ((#1 = 98) OR (#1 >= 30) OR (#6 AND (#2) IS NULL AND (#3 < 48) AND (#4 < 86.27) AND (#1 <= 45) AND (#2 <= 10.7) AND (#3 > 49) AND (#3 > 66) AND (#4 > 42.2) AND (#1 >= 25) AND (#3 >= 49) AND (#3 >= 77) AND ((#1 = 67) OR (#1 = 81) OR (#1 = 86) OR (#1 = 94) OR (#1 = 97)) AND ((#2 < 20.99) OR (#2 <= 30.14) OR (#1 > 79) OR (#1 >= 89) OR (#3 >= 12) OR ((#3 > 77) AND (#2 >= 74.51)))) OR (#7 AND (#3 <= 88)) OR ((#1 < 68) AND (#1 <= 35)) OR ((#2 <= 17.96) AND (#7 OR ((#4 <= 2.63) AND (#1 > 2) AND (#3 > 8)))) OR ((#4 <= 97.11) AND ((#1 = 11) OR (#1 = 63) OR (#1 = 85) OR (#1 = 87) OR (#1 = 88)))))) // { cardinality: "<UNKNOWN>" }
                    Map ((#1) IS NULL) // { cardinality: "<UNKNOWN>" }
                      Join on=(#1 = #5) type=differential // { cardinality: "<UNKNOWN>" }
                        ArrangeBy keys=[[#1]] // { cardinality: "<UNKNOWN>" }
                          Project (#0..=#2, #4, #5) // { cardinality: "<UNKNOWN>" }
                            Filter ((#1 > 3) OR ((#1 <= 88) AND (#1 > 5) AND ((#1 = 98) OR (#1 >= 30) OR (#7 AND (#4 <= 88)) OR ((#2) IS NULL AND (#4 < 48) AND (#5 < 86.27) AND (#1 <= 45) AND (#2 <= 10.7) AND (#4 > 49) AND (#4 > 66) AND (#5 > 42.2) AND (#1 >= 25) AND (#4 >= 49) AND (#4 >= 77) AND ((#1 = 67) OR (#1 = 81) OR (#1 = 86) OR (#1 = 94) OR (#1 = 97)) AND ((#2 < 20.99) OR (#2 <= 30.14) OR (#1 > 79) OR (#1 >= 89) OR (#4 >= 12) OR ((#4 > 77) AND (#2 >= 74.51)))) OR ((#1 < 68) AND (#1 <= 35)) OR ((#2 <= 17.96) AND (#7 OR ((#5 <= 2.63) AND (#1 > 2) AND (#4 > 8)))) OR ((#5 <= 97.11) AND ((#1 = 11) OR (#1 = 63) OR (#1 = 85) OR (#1 = 87) OR (#1 = 88)))))) // { cardinality: "<UNKNOWN>" }
                              Map ((#1) IS NULL) // { cardinality: "<UNKNOWN>" }
                                ReadStorage materialize.public.tab0 // { cardinality: "<UNKNOWN>" }
                        ArrangeBy keys=[[#0]] // { cardinality: "<UNKNOWN>" }
                          Union // { cardinality: "<UNKNOWN>" }
                            Filter ((#0 > 3) OR ((#0 <= 88) AND (#0 > 5) AND ((#0) IS NULL OR (#0 = 11) OR (#0 = 63) OR (#0 = 85) OR (#0 = 87) OR (#0 = 88) OR (#0 = 98) OR (#0 > 2) OR (#0 >= 30) OR (#1 AND (#0 <= 45) AND (#0 >= 25) AND ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97))) OR ((#0 < 68) AND (#0 <= 35))))) // { cardinality: "<UNKNOWN>" }
                              Get l2 // { cardinality: "<UNKNOWN>" }
                            Project (#0, #18) // { cardinality: "<UNKNOWN>" }
                              Filter (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16)))) AND (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16 AND null)))) // { cardinality: "<UNKNOWN>" }
                                Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35)), null) // { cardinality: "<UNKNOWN>" }
                                  Join on=(#0 = #1) type=differential // { cardinality: "<UNKNOWN>" }
                                    ArrangeBy keys=[[#0]] // { cardinality: "<UNKNOWN>" }
                                      Union // { cardinality: "<UNKNOWN>" }
                                        Negate // { cardinality: "<UNKNOWN>" }
                                          Project (#0) // { cardinality: "<UNKNOWN>" }
                                            Filter (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16)))) AND (#2 OR (#3 AND #4 AND (#5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #13 OR #17 OR (#14 AND #15 AND #16 AND null)))) // { cardinality: "<UNKNOWN>" }
                                              Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35))) // { cardinality: "<UNKNOWN>" }
                                                Get l2 // { cardinality: "<UNKNOWN>" }
                                        Project (#0) // { cardinality: "<UNKNOWN>" }
                                          Filter (#1 OR (#2 AND #3 AND (#4 OR #5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #16 OR (#13 AND #14 AND #15)))) AND (#1 OR (#2 AND #3 AND (#4 OR #5 OR #6 OR #7 OR #8 OR #9 OR #10 OR #11 OR #12 OR #16 OR (#13 AND #14 AND #15 AND null)))) // { cardinality: "<UNKNOWN>" }
                                            Map ((#0 > 3), (#0 <= 88), (#0 > 5), (#0) IS NULL, (#0 = 11), (#0 = 63), (#0 = 85), (#0 = 87), (#0 = 88), (#0 = 98), (#0 > 2), (#0 >= 30), (#0 <= 45), (#0 >= 25), ((#0 = 67) OR (#0 = 81) OR (#0 = 86) OR (#0 = 94) OR (#0 = 97)), ((#0 < 68) AND (#0 <= 35))) // { cardinality: "<UNKNOWN>" }
                                              Get l0 // { cardinality: "<UNKNOWN>" }
                                    ArrangeBy keys=[[#0]] // { cardinality: "<UNKNOWN>" }
                                      Get l0 // { cardinality: "<UNKNOWN>" }

            Source materialize.public.tab0

            Target cluster: quickstart
            """
        ),
        materialized_memory="4.5Gb",
        clusterd_memory="3.5Gb",
    ),
    Scenario(
        name="dataflow-logical-backpressure",
        pre_restart=dedent(
            """
            # * Timestamp interval to quickly create a source with many distinct timestamps.
            # * Lgalloc disabled to force more memory pressure.
            # * Index options to enable retained history.
            # * Finally, enable backpressure.
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET min_timestamp_interval = '10ms';
            ALTER SYSTEM SET enable_lgalloc = false;
            ALTER SYSTEM SET enable_index_options = true;
            ALTER SYSTEM SET enable_compute_logical_backpressure = true;

            > DROP CLUSTER REPLICA clusterd.r1;

            # Table to hold back frontiers.
            > CREATE TABLE t (a int);
            > INSERT INTO t VALUES (1);

            # Create a source with 512 distinct timestamps.
            > CREATE SOURCE counter FROM LOAD GENERATOR COUNTER (TICK INTERVAL '100ms', UP TO 512) WITH (TIMESTAMP INTERVAL '100ms', RETAIN HISTORY FOR '10d');

            > CREATE MATERIALIZED VIEW cv WITH (RETAIN HISTORY FOR '10d') AS SELECT counter FROM counter, t;

            # Wait until counter is fully ingested.
            > SELECT COUNT(*) FROM counter;
            512

            > CREATE CLUSTER REPLICA clusterd.r1
              STORAGECTL ADDRESSES ['clusterd:2100'],
              STORAGE ADDRESSES ['clusterd:2103'],
              COMPUTECTL ADDRESSES ['clusterd:2101'],
              COMPUTE ADDRESSES ['clusterd:2102'];

            > SET CLUSTER = clusterd

            # Ballast is the concatenation of two 32-byte strings, for readability.
            > CREATE VIEW v AS
                SELECT
                    c1.counter + c2.counter * 10 + c3.counter * 100 AS c,
                    '01234567890123456789012345678901'||'01234567890123456789012345678901' AS ballast
                FROM
                    cv c1,
                    cv c2,
                    cv c3;
            > CREATE DEFAULT INDEX ON v WITH (RETAIN HISTORY FOR '10d');
            > SELECT COUNT(*) > 0 FROM v;
            true
            """
        ),
        post_restart=dedent(
            """
            > SET CLUSTER = clusterd

            > SELECT COUNT(*) > 0 FROM v;
            true
            """
        ),
        materialized_memory="10Gb",
        clusterd_memory="3.5Gb",
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    def process(name: str) -> None:
        if name in ["default", "minimization-search"]:
            return
        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_main(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Process various datasets in a memory-constrained environment in order
    to exercise compaction/garbage collection and confirm no OOMs or thrashing."""

    parser.add_argument(
        "scenarios", nargs="*", default=None, help="run specified Scenarios"
    )
    args = parser.parse_args()

    for scenario in shard_list(SCENARIOS, lambda scenario: scenario.name):
        if shall_skip_scenario(scenario, args):
            continue

        if scenario.disabled:
            print(f"+++ Scenario {scenario.name} is disabled, skipping.")
            continue

        c.override_current_testcase_name(f"Scenario '{scenario.name}'")

        print(
            f"+++ Running scenario {scenario.name} with materialized_memory={scenario.materialized_memory} and clusterd_memory={scenario.clusterd_memory} ..."
        )

        run_scenario(
            c,
            scenario,
            materialized_memory=scenario.materialized_memory,
            clusterd_memory=scenario.clusterd_memory,
        )


def workflow_minimization_search(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Find the minimal working memory configurations."""

    parser.add_argument(
        "scenarios", nargs="*", default=None, help="run specified Scenarios"
    )
    parser.add_argument(
        "--materialized-memory-search-step",
        default=0.2,
        type=float,
    )
    parser.add_argument(
        "--clusterd-memory-search-step",
        default=0.2,
        type=float,
    )
    parser.add_argument(
        "--materialized-memory-lower-bound-in-gb",
        default=1.5,
        type=float,
    )
    parser.add_argument(
        "--clusterd-memory-lower-bound-in-gb",
        default=0.5,
        type=float,
    )
    args = parser.parse_args()

    if buildkite.is_in_buildkite():
        test_analytics_config = create_test_analytics_config(c)
    else:
        test_analytics_config = create_dummy_test_analytics_config()

    test_analytics = TestAnalyticsDb(test_analytics_config)
    # will be updated to True at the end
    test_analytics.builds.add_build_job(was_successful=False)

    for scenario in shard_list(SCENARIOS, lambda scenario: scenario.name):
        if shall_skip_scenario(scenario, args):
            continue

        if scenario.disabled:
            print(f"+++ Scenario {scenario.name} is disabled, skipping.")
            continue

        c.override_current_testcase_name(f"Scenario '{scenario.name}'")

        print(f"+++ Starting memory search for scenario {scenario.name}")

        run_memory_search(
            c,
            scenario,
            args.materialized_memory_search_step,
            args.clusterd_memory_search_step,
            args.materialized_memory_lower_bound_in_gb,
            args.clusterd_memory_lower_bound_in_gb,
            test_analytics,
        )

    try:
        test_analytics.builds.update_build_job_success(True)
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


def shall_skip_scenario(scenario: Scenario, args: argparse.Namespace) -> bool:
    return (
        args.scenarios is not None
        and len(args.scenarios) > 0
        and scenario.name not in args.scenarios
    )


def run_scenario(
    c: Composition, scenario: Scenario, materialized_memory: str, clusterd_memory: str
) -> None:
    c.down(destroy_volumes=True)

    with c.override(
        Materialized(memory=materialized_memory, support_external_clusterd=True),
        Clusterd(memory=clusterd_memory),
    ):
        c.up(
            "redpanda",
            "materialized",
            "postgres",
            "mysql",
            "clusterd",
            Service("testdrive", idle=True),
        )

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

        c.sql(
            """
            CREATE CLUSTER clusterd REPLICAS (r1 (
                STORAGECTL ADDRESSES ['clusterd:2100'],
                STORAGE ADDRESSES ['clusterd:2103'],
                COMPUTECTL ADDRESSES ['clusterd:2101'],
                COMPUTE ADDRESSES ['clusterd:2102']
            ))
        """
        )

        testdrive_timeout_arg = "--default-timeout=5m"
        statement_timeout = "> SET statement_timeout = '600s';\n"

        c.testdrive(
            statement_timeout + scenario.pre_restart, args=[testdrive_timeout_arg]
        )

        # Restart Mz to confirm that re-hydration is also bounded memory
        c.kill("materialized", "clusterd")
        c.up("materialized", "clusterd")

        c.testdrive(
            statement_timeout + scenario.post_restart, args=[testdrive_timeout_arg]
        )


def try_run_scenario(
    c: Composition, scenario: Scenario, materialized_memory: str, clusterd_memory: str
) -> bool:
    try:
        run_scenario(c, scenario, materialized_memory, clusterd_memory)
        return True
    except:
        return False


def run_memory_search(
    c: Composition,
    scenario: Scenario,
    materialized_search_step_in_gb: float,
    clusterd_search_step_in_gb: float,
    materialized_memory_lower_bound_in_gb: float,
    clusterd_memory_lower_bound_in_gb: float,
    test_analytics: TestAnalyticsDb,
) -> None:
    assert materialized_search_step_in_gb > 0 or clusterd_search_step_in_gb > 0
    materialized_memory = scenario.materialized_memory
    clusterd_memory = scenario.clusterd_memory

    none_minimization_target = "none"
    search_entry = BoundedMemoryMinimalSearchEntry(
        scenario_name=scenario.name,
        tested_memory_mz_in_gb=_get_memory_in_gb(materialized_memory),
        tested_memory_clusterd_in_gb=_get_memory_in_gb(clusterd_memory),
    )
    test_analytics.bounded_memory_search.add_entry(
        BOUNDED_MEMORY_FRAMEWORK_VERSION,
        search_entry,
        minimization_target=none_minimization_target,
        flush=True,
    )
    test_analytics.bounded_memory_search.update_status(
        search_entry,
        minimization_target=none_minimization_target,
        status=BOUNDED_MEMORY_STATUS_CONFIGURED,
        flush=True,
    )

    if materialized_search_step_in_gb > 0:
        materialized_memory, clusterd_memory = find_minimal_memory(
            c,
            test_analytics,
            scenario,
            initial_materialized_memory=materialized_memory,
            initial_clusterd_memory=clusterd_memory,
            reduce_materialized_memory_by_gb=materialized_search_step_in_gb,
            reduce_clusterd_memory_by_gb=0,
            materialized_memory_lower_bound_in_gb=materialized_memory_lower_bound_in_gb,
            clusterd_memory_lower_bound_in_gb=clusterd_memory_lower_bound_in_gb,
        )
    if clusterd_search_step_in_gb > 0:
        materialized_memory, clusterd_memory = find_minimal_memory(
            c,
            test_analytics,
            scenario,
            initial_materialized_memory=materialized_memory,
            initial_clusterd_memory=clusterd_memory,
            reduce_materialized_memory_by_gb=0,
            reduce_clusterd_memory_by_gb=clusterd_search_step_in_gb,
            materialized_memory_lower_bound_in_gb=materialized_memory_lower_bound_in_gb,
            clusterd_memory_lower_bound_in_gb=clusterd_memory_lower_bound_in_gb,
        )

    print(f"Found minimal memory for scenario {scenario.name}:")
    print(
        f"* materialized_memory={materialized_memory} (specified was: {scenario.materialized_memory})"
    )
    print(
        f"* clusterd_memory={clusterd_memory} (specified was: {scenario.clusterd_memory})"
    )
    print("Consider adding some buffer to avoid flakiness.")


def find_minimal_memory(
    c: Composition,
    test_analytics: TestAnalyticsDb,
    scenario: Scenario,
    initial_materialized_memory: str,
    initial_clusterd_memory: str,
    reduce_materialized_memory_by_gb: float,
    reduce_clusterd_memory_by_gb: float,
    materialized_memory_lower_bound_in_gb: float,
    clusterd_memory_lower_bound_in_gb: float,
) -> tuple[str, str]:
    if reduce_materialized_memory_by_gb > 0 and reduce_clusterd_memory_by_gb > 0:
        raise RuntimeError(
            "Cannot reduce both materialized and clusterd memory at once"
        )
    elif reduce_materialized_memory_by_gb >= 0.1:
        minimalization_target = "materialized_memory"
    elif reduce_clusterd_memory_by_gb >= 0.1:
        minimalization_target = "clusterd_memory"
    else:
        raise RuntimeError("No valid reduction set")

    materialized_memory = initial_materialized_memory
    clusterd_memory = initial_clusterd_memory
    materialized_memory_steps = [materialized_memory]
    clusterd_memory_steps = [clusterd_memory]

    while True:
        new_materialized_memory = _reduce_memory(
            materialized_memory,
            reduce_materialized_memory_by_gb,
            materialized_memory_lower_bound_in_gb,
        )
        new_clusterd_memory = _reduce_memory(
            clusterd_memory,
            reduce_clusterd_memory_by_gb,
            clusterd_memory_lower_bound_in_gb,
        )

        if new_materialized_memory is None or new_clusterd_memory is None:
            # limit undercut
            break

        scenario_desc = f"{scenario.name} with materialized_memory={new_materialized_memory} and clusterd_memory={new_clusterd_memory}"

        search_entry = BoundedMemoryMinimalSearchEntry(
            scenario_name=scenario.name,
            tested_memory_mz_in_gb=_get_memory_in_gb(new_materialized_memory),
            tested_memory_clusterd_in_gb=_get_memory_in_gb(new_clusterd_memory),
        )
        test_analytics.bounded_memory_search.add_entry(
            BOUNDED_MEMORY_FRAMEWORK_VERSION,
            search_entry,
            minimization_target=minimalization_target,
            flush=True,
        )

        print(f"Trying scenario {scenario_desc}")
        success = try_run_scenario(
            c,
            scenario,
            materialized_memory=new_materialized_memory,
            clusterd_memory=new_clusterd_memory,
        )

        if success:
            print(f"Scenario {scenario_desc} succeeded.")
            materialized_memory = new_materialized_memory
            clusterd_memory = new_clusterd_memory
            materialized_memory_steps.append(new_materialized_memory)
            clusterd_memory_steps.append(new_clusterd_memory)
            test_analytics.bounded_memory_search.update_status(
                search_entry,
                status=BOUNDED_MEMORY_STATUS_SUCCESS,
                minimization_target=minimalization_target,
                flush=True,
            )
        else:
            print(f"Scenario {scenario_desc} failed.")
            test_analytics.bounded_memory_search.update_status(
                search_entry,
                status=BOUNDED_MEMORY_STATUS_FAILURE,
                minimization_target=minimalization_target,
                flush=True,
            )
            break

    if (
        materialized_memory < initial_materialized_memory
        or clusterd_memory < initial_clusterd_memory
    ):
        print(f"Validating again the memory configuration for {scenario.name}")
        materialized_memory, clusterd_memory = _validate_new_memory_configuration(
            c,
            scenario,
            materialized_memory,
            clusterd_memory,
            materialized_memory_steps,
            clusterd_memory_steps,
        )

    return materialized_memory, clusterd_memory


def _validate_new_memory_configuration(
    c: Composition,
    scenario: Scenario,
    materialized_memory: str,
    clusterd_memory: str,
    materialized_memory_steps: list[str],
    clusterd_memory_steps: list[str],
) -> tuple[str, str]:
    success = try_run_scenario(
        c,
        scenario,
        materialized_memory=materialized_memory,
        clusterd_memory=clusterd_memory,
    )

    scenario_desc = f"{scenario.name} with materialized_memory={materialized_memory} and clusterd_memory={clusterd_memory}"

    if success:
        print(f"Successfully validated {scenario_desc}")
    else:
        print(f"Validation of {scenario_desc} failed")

        assert len(materialized_memory_steps) > 1 and len(clusterd_memory_steps) > 1
        materialized_memory = materialized_memory_steps[-2]
        clusterd_memory = clusterd_memory_steps[-2]

        print(
            f"Going back one step to materialized_memory={materialized_memory} and clusterd_memory={clusterd_memory}"
        )

    return materialized_memory, clusterd_memory


def _reduce_memory(
    memory_spec: str, reduce_by_gb: float, lower_bound_in_gb: float
) -> str | None:
    if math.isclose(reduce_by_gb, 0.0, abs_tol=0.01):
        # allow staying at the same value
        return memory_spec

    current_gb = _get_memory_in_gb(memory_spec)

    if math.isclose(current_gb, lower_bound_in_gb, abs_tol=0.01):
        # lower bound already reached
        return None

    new_gb = current_gb - reduce_by_gb

    if new_gb < lower_bound_in_gb:
        new_gb = lower_bound_in_gb

    return f"{round(new_gb, 2)}Gb"


def _get_memory_in_gb(memory_spec: str) -> float:
    if not memory_spec.endswith("Gb"):
        raise RuntimeError(f"Unsupported memory specification: {memory_spec}")

    return float(memory_spec.removesuffix("Gb"))
