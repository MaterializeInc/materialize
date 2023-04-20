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

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    Materialized,
    Postgres,
    Redpanda,
    Testdrive,
)

# Those defaults have been carefully chosen to avoid known OOMs
# such as #15093 and #15044 while hopefully catching any further
# regressions in memory usage
PAD_LEN = 1024
STRING_PAD = "x" * PAD_LEN
REPEAT = 16 * 1024
ITERATIONS = 128
MATERIALIZED_MEMORY = "5Gb"
CLUSTERD_MEMORY = "3.5Gb"

SERVICES = [
    Materialized(memory=MATERIALIZED_MEMORY),
    Testdrive(no_reset=True, seed=1, default_timeout="3600s"),
    Redpanda(),
    Postgres(),
    Clusterd(memory=CLUSTERD_MEMORY),
]


@dataclass
class Scenario:
    name: str
    pre_restart: str
    post_restart: str
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
          FOR KAFKA BROKER '${testdrive.kafka-addr}';
        """
    )

    SOURCE = dedent(
        """
        > CREATE SOURCE s1
          IN CLUSTER clusterd
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
    # Peform updates while the source is ingesting
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
    # Peform inserts+deletes while the source is ingesting
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
            print(f"Scenario {scenario.name} is disabled, skipping.")
            continue
        else:
            print(f"Running scenario {scenario.name} ...")

        c.down(destroy_volumes=True)

        c.up("redpanda", "materialized", "postgres", "clusterd")

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
