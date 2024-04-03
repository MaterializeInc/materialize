# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import math
from dataclasses import dataclass
from string import ascii_lowercase
from textwrap import dedent

from materialize.buildkite import accepted_by_shard
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.testdrive import Testdrive

# Those defaults have been carefully chosen to avoid known OOMs
# such as #15093 and #15044 while hopefully catching any further
# regressions in memory usage
PAD_LEN = 1024
STRING_PAD = "x" * PAD_LEN
REPEAT = 16 * 1024
ITERATIONS = 128

SERVICES = [
    Cockroach(setup_materialize=True),
    Materialized(external_cockroach=True),
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
]


@dataclass
class Scenario:
    name: str
    pre_restart: str
    post_restart: str
    disabled: bool = False
    materialized_memory: str = "5Gb"
    clusterd_memory: str = "3.5Gb"


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
        > DROP CLUSTER IF EXISTS single_replica_cluster;
        > CREATE CLUSTER single_replica_cluster SIZE '${arg.default-storage-size}';
        > CREATE SOURCE mz_source
          IN CLUSTER single_replica_cluster
          FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
          FOR ALL TABLES;

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
        > DROP CLUSTER IF EXISTS single_replica_cluster;
        > CREATE CLUSTER single_replica_cluster SIZE '${arg.default-storage-size}';
        > CREATE SOURCE mz_source
          IN CLUSTER single_replica_cluster
          FROM MYSQL CONNECTION mysql_conn
          FOR ALL TABLES;

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
        > DROP CLUSTER IF EXISTS single_replica_cluster;
        > CREATE CLUSTER single_replica_cluster SIZE '${arg.default-storage-size}';
        > CREATE SOURCE s1
          IN CLUSTER single_replica_cluster
          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}')
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE UPSERT;

        > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM s1;
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
                for i in range(0, ITERATIONS)
            ]
        )
        + PgCdcScenario.MZ_SETUP
        + dedent(
            f"""
            > SELECT * FROM v1; /* expect {ITERATIONS * REPEAT} */
            {ITERATIONS * REPEAT}
            """
        ),
        post_restart=dedent(
            f"""
            # We do not do DELETE post-restart, as it will cause OOM for clusterd
            > SELECT * FROM v1; /* expect {ITERATIONS * REPEAT} */
            {ITERATIONS * REPEAT}
            """
        ),
        clusterd_memory="3.6Gb",
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
            """
        ),
        post_restart=dedent(
            """
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            DELETE FROM t1;

            > SELECT * FROM v1;
            0
            """
        ),
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
            """
        ),
        post_restart=dedent(
            f"""
            # We do not do DELETE post-restart, as it will cause OOM for clusterd
            > SELECT * FROM v1; /* expect {int(ITERATIONS * 20 * REPEAT / 16)} */
            {int(ITERATIONS * 20 * REPEAT / 16)}
            """
        ),
        materialized_memory="4.5Gb",
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
            """
        ),
        post_restart=dedent(
            f"""
            # We do not do DELETE post-restart, as it will cause OOM for clusterd
            > SELECT * FROM v1; /* expect {ITERATIONS * REPEAT} */
            {ITERATIONS * REPEAT}
            """
        ),
        clusterd_memory="3.6Gb",
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
            """
        ),
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
                    INSERT INTO t1 (f3) SELECT CONCAT('{i}', REPEAT('a', {PAD_LEN})) FROM series_helper LIMIT {int(REPEAT / 24)};
                    """
                )
                for i in range(0, ITERATIONS * 20)
            ]
        )
        + "COMMIT;\n"
        + dedent(
            f"""
            > SELECT * FROM v1; /* expect {int(ITERATIONS * 20) * int(REPEAT / 24)} */
            {int(ITERATIONS * 20) * int(REPEAT / 24)}
            """
        ),
        post_restart=dedent(
            f"""
            # We do not do DELETE post-restart, as it will cause OOM for clusterd
            > SELECT * FROM v1; /* expect {int(ITERATIONS * 20) * int(REPEAT / 24)} */
            {int(ITERATIONS * 20) * int(REPEAT / 24)}
            """
        ),
        clusterd_memory="8.0Gb",
        disabled=True,
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
                    "${{kafka-ingest.iteration}}" {{"f1": "{i}{STRING_PAD}"}}
                    """
                )
                for i in range(0, ITERATIONS)
            ]
        )
        + KafkaScenario.END_MARKER
        + KafkaScenario.SOURCE
        + dedent(
            f"""
            # Expect all ingested data + two MARKERs
            > SELECT * FROM v1;
            {REPEAT + 2}
            """
        ),
        post_restart=KafkaScenario.SCHEMAS + KafkaScenario.POST_RESTART,
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
            """
        ),
        post_restart=KafkaScenario.SCHEMAS + KafkaScenario.POST_RESTART,
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
            """
        ),
        post_restart=KafkaScenario.SCHEMAS + KafkaScenario.POST_RESTART,
    ),
    Scenario(
        name="table-insert-delete",
        pre_restart=dedent(
            """
            > SET statement_timeout = '600 s';

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
            """
        ),
        post_restart=dedent(
            """
           > SELECT * FROM v1;
           0
           """
        ),
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

            > SELECT count(*) FROM t;
            2000000
            """
        ),
        post_restart=dedent(
            """
            > SET CLUSTER = clusterd

            > SELECT count(*) FROM t;
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
            > CREATE CLUSTER idx_cluster SIZE '1-8G', REPLICATION FACTOR 1;

            > CREATE VIEW accumulable AS
              SELECT
                a,
                sum(a) AS sum_a, count(a) as cnt_a,
                sum(b) AS sum_b, count(b) as cnt_b
              FROM data
              GROUP BY a;

            > CREATE INDEX i_accumulable IN CLUSTER idx_cluster ON accumulable(a);

            > SET CLUSTER = idx_cluster;

            > SELECT count(*) FROM accumulable;
            10000001
            """
        ),
        post_restart=dedent(
            """
            > SET CLUSTER = idx_cluster;

            > SELECT count(*) FROM accumulable;
            10000001
            """
        ),
        materialized_memory="9.5Gb",
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
            > DROP CLUSTER IF EXISTS single_replica_cluster;
            > CREATE CLUSTER single_replica_cluster SIZE '${arg.default-storage-size}';
            > CREATE SOURCE s1
              IN CLUSTER single_replica_cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE UPSERT;

            > DROP CLUSTER REPLICA clusterd.r1;

            > CREATE INDEX i1 IN CLUSTER clusterd ON s1 (f1);
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

            > SELECT count(*) FROM s1;
            {90 * REPEAT + 2}
            # Delete all rows except markers
            $ kafka-ingest format=avro key-format=avro topic=topic1 schema=${{value-schema}} key-schema=${{key-schema}} repeat={REPEAT}
            "${{kafka-ingest.iteration}}"
            """
        ),
        materialized_memory="10Gb",
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
        clusterd_memory="7Gb",
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
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Process various datasets in a memory-constrained environment in order
    to exercise compaction/garbage collection and confirm no OOMs or thrashing."""

    parser.add_argument(
        "scenarios", nargs="*", default=None, help="run specified Scenarios"
    )
    parser.add_argument("--find-minimal-memory", action="store_true")
    parser.add_argument("--memory-search-step", default=0.2, type=float)
    args = parser.parse_args()

    for scenario in SCENARIOS:
        if (
            args.scenarios is not None
            and len(args.scenarios) > 0
            and scenario.name not in args.scenarios
        ):
            continue

        if scenario.disabled:
            print(f"+++ Scenario {scenario.name} is disabled, skipping.")
            continue
        if not accepted_by_shard(scenario.name):
            continue
        else:
            print(f"+++ Running scenario {scenario.name} ...")

        if args.find_minimal_memory:
            run_memory_search(c, scenario, args.memory_search_step)
        else:
            run_scenario(
                c,
                scenario,
                materialized_memory=scenario.materialized_memory,
                clusterd_memory=scenario.clusterd_memory,
            )


def run_scenario(
    c: Composition, scenario: Scenario, materialized_memory: str, clusterd_memory: str
) -> None:
    c.down(destroy_volumes=True)

    with c.override(
        Materialized(memory=materialized_memory),
        Clusterd(memory=clusterd_memory),
    ):
        c.up("redpanda", "materialized", "postgres", "mysql", "clusterd")

        c.sql(
            "ALTER SYSTEM SET enable_unorchestrated_cluster_replicas = true;",
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

        c.up("testdrive", persistent=True)
        c.testdrive(scenario.pre_restart, args=[testdrive_timeout_arg])

        # Restart Mz to confirm that re-hydration is also bounded memory
        c.kill("materialized", "clusterd")
        c.up("materialized", "clusterd")

        c.testdrive(scenario.post_restart, args=[testdrive_timeout_arg])


def try_run_scenario(
    c: Composition, scenario: Scenario, materialized_memory: str, clusterd_memory: str
) -> bool:
    try:
        run_scenario(c, scenario, materialized_memory, clusterd_memory)
        return True
    except:
        return False


def run_memory_search(
    c: Composition, scenario: Scenario, memory_search_step_in_gb: float
) -> None:
    assert memory_search_step_in_gb > 0
    materialized_memory = scenario.materialized_memory
    clusterd_memory = scenario.clusterd_memory

    print(f"Starting memory search for scenario {scenario.name}")

    materialized_memory, clusterd_memory = find_minimal_memory(
        c,
        scenario,
        initial_materialized_memory=materialized_memory,
        initial_clusterd_memory=clusterd_memory,
        reduce_materialized_memory_by_gb=memory_search_step_in_gb,
        reduce_clusterd_memory_by_gb=0,
    )
    materialized_memory, clusterd_memory = find_minimal_memory(
        c,
        scenario,
        initial_materialized_memory=materialized_memory,
        initial_clusterd_memory=clusterd_memory,
        reduce_materialized_memory_by_gb=0,
        reduce_clusterd_memory_by_gb=memory_search_step_in_gb,
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
    scenario: Scenario,
    initial_materialized_memory: str,
    initial_clusterd_memory: str,
    reduce_materialized_memory_by_gb: float,
    reduce_clusterd_memory_by_gb: float,
) -> tuple[str, str]:
    assert reduce_materialized_memory_by_gb > 0 or reduce_clusterd_memory_by_gb > 0

    materialized_memory = initial_materialized_memory
    clusterd_memory = initial_clusterd_memory
    materialized_memory_steps = [materialized_memory]
    clusterd_memory_steps = [clusterd_memory]

    while True:
        new_materialized_memory = _reduce_memory(
            materialized_memory, reduce_materialized_memory_by_gb
        )
        new_clusterd_memory = _reduce_memory(
            clusterd_memory, reduce_clusterd_memory_by_gb
        )

        if new_materialized_memory is None or new_clusterd_memory is None:
            # limit undercut
            break

        scenario_desc = f"{scenario.name} with materialized_memory={new_materialized_memory} and clusterd_memory={new_clusterd_memory}"

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
        else:
            print(f"Scenario {scenario_desc} failed.")
            break

    if (
        materialized_memory != initial_materialized_memory
        or clusterd_memory != initial_clusterd_memory
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


def _reduce_memory(memory_spec: str, reduce_by_gb: float) -> str | None:
    if not memory_spec.endswith("Gb"):
        raise RuntimeError(f"Unsupported memory specification: {memory_spec}")

    if math.isclose(reduce_by_gb, 0.0, abs_tol=0.01):
        return memory_spec

    current_gb = float(memory_spec.removesuffix("Gb"))
    new_gb = current_gb - reduce_by_gb

    if new_gb <= 0.2:
        return None

    return f"{new_gb}Gb"
