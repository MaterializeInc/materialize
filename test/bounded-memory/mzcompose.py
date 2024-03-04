# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from string import ascii_lowercase
from textwrap import dedent

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
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
        ],
    ),
    Redpanda(),
    Postgres(),
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
        else:
            print(f"+++ Running scenario {scenario.name} ...")

        c.down(destroy_volumes=True)

        with c.override(
            Materialized(memory=scenario.materialized_memory),
            Clusterd(memory=scenario.clusterd_memory),
        ):
            c.up("redpanda", "materialized", "postgres", "clusterd")

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

            c.up("testdrive", persistent=True)
            c.testdrive(scenario.pre_restart)

            # Restart Mz to confirm that re-hydration is also bounded memory
            c.kill("materialized", "clusterd")
            c.up("materialized", "clusterd")

            c.testdrive(scenario.post_restart)
