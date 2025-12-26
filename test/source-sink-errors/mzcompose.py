# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test the detection and reporting of source/sink errors by introducing a
Disruption and then checking the mz_internal.mz_*_statuses tables
"""

import random
from collections.abc import Callable
from dataclasses import dataclass
from textwrap import dedent
from typing import Protocol

from materialize import buildkite
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import selected_by_name


def schema() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


SERVICES = [
    Redpanda(),
    Materialized(support_external_clusterd=True),
    Testdrive(),
    Clusterd(),
    Postgres(),
    Zookeeper(),
    Kafka(
        name="badkafka",
        environment=[
            "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
            # Setting the following values to 3 to trigger a failure
            # sets the transaction.state.log.min.isr config
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=3",
            # sets the transaction.state.log.replication.factor config
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3",
        ],
    ),
    SchemaRegistry(kafka_servers=[("badkafka", "9092")]),
]


class Disruption(Protocol):
    name: str

    def run_test(self, c: Composition) -> None: ...


@dataclass
class KafkaTransactionLogGreaterThan1:
    name: str

    # override the `run_test`, as we need `Kafka` (not `Redpanda`), and need to change some other things
    def run_test(self, c: Composition) -> None:
        print(f"+++ Running disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.up(Service("testdrive", idle=True))

        with c.override(
            Testdrive(
                no_reset=True,
                seed=seed,
                kafka_url="badkafka",
                entrypoint_extra=[
                    "--initial-backoff=1s",
                    "--backoff-factor=0",
                ],
            ),
        ):
            c.up("zookeeper", "badkafka", "schema-registry", "materialized")
            self.populate(c)
            self.assert_error(c, "transaction error", "running a single Kafka broker")

    def populate(self, c: Composition) -> None:
        # Create a source and a sink
        c.testdrive(
            dedent(
                """
                > CREATE CONNECTION kafka_conn
                  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

                > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                    URL '${testdrive.schema-registry-url}'
                  );

                > CREATE TABLE sink_table (f1 INTEGER);

                > INSERT INTO sink_table VALUES (1);

                > INSERT INTO sink_table VALUES (2);

                > CREATE SINK kafka_sink FROM sink_table
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """
            ),
        )

    def assert_error(self, c: Composition, error: str, hint: str) -> None:
        c.testdrive(
            dedent(
                f"""
                $ set-sql-timeout duration=120s
                > SELECT bool_or(error ~* '{error}'), bool_or(details::json#>>'{{hints,0}}' ~* '{hint}')
                  FROM mz_internal.mz_sink_status_history
                  JOIN mz_sinks ON mz_sinks.id = sink_id
                  WHERE name = 'kafka_sink' and status = 'stalled'
                true true
                """
            )
        )


@dataclass
class KafkaDisruption:
    name: str
    breakage: Callable
    expected_error: str
    fixage: Callable | None

    def run_test(self, c: Composition) -> None:
        print(f"+++ Running disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.up(
            "redpanda",
            "materialized",
            "clusterd",
            Service("testdrive", idle=True),
        )

        with c.override(
            Testdrive(
                no_reset=True,
                seed=seed,
                entrypoint_extra=["--initial-backoff=1s", "--backoff-factor=0"],
            )
        ):
            self.populate(c)
            self.breakage(c, seed)
            self.assert_error(c, self.expected_error)

            if self.fixage:
                self.fixage(c, seed)
                self.assert_recovery(c)

    def populate(self, c: Composition) -> None:
        # Create a source and a sink
        c.testdrive(
            dedent(
                """
                # We specify the progress topic explicitly so we can delete it in a test later,
                # and confirm that the sink stalls. (Deleting the output topic is not enough if
                # we're not actively publishing new messages to the sink.)
                > CREATE CONNECTION kafka_conn
                  TO KAFKA (
                    BROKER '${testdrive.kafka-addr}',
                    SECURITY PROTOCOL PLAINTEXT,
                    PROGRESS TOPIC 'testdrive-progress-topic-${testdrive.seed}'
                  );

                > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                  URL '${testdrive.schema-registry-url}'
                  );

                $ kafka-create-topic topic=source-topic

                $ kafka-ingest topic=source-topic format=bytes
                ABC

                > CREATE SOURCE source1
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-source-topic-${testdrive.seed}')

                > CREATE TABLE source1_tbl FROM SOURCE source1 (REFERENCE "testdrive-source-topic-${testdrive.seed}")
                  FORMAT BYTES
                  ENVELOPE NONE
                # WITH ( REMOTE 'clusterd:2100' ) https://github.com/MaterializeInc/database-issues/issues/4800

                # Ensure the source makes _real_ progress before we disrupt it. This also
                # ensures the sink makes progress, which is required to hit certain stalls.
                # As of implementing correctness property #2, this is required.
                > SELECT count(*) from source1_tbl
                1

                > CREATE SINK sink1 FROM source1_tbl
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-topic-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                # WITH ( REMOTE 'clusterd:2100' ) https://github.com/MaterializeInc/database-issues/issues/4800

                $ kafka-verify-topic sink=materialize.public.sink1
                """
            )
        )

    def assert_error(self, c: Composition, error: str) -> None:
        c.testdrive(
            dedent(
                f"""
                $ set-sql-timeout duration=60s
                > SELECT status, error ~* '{error}'
                  FROM mz_internal.mz_source_statuses
                  WHERE name = 'source1'
                stalled true
                """
            )
        )

    def assert_recovery(self, c: Composition) -> None:
        c.testdrive(
            dedent(
                """
                $ kafka-ingest topic=source-topic format=bytes
                ABC

                > SELECT COUNT(*) FROM source1_tbl;
                2

                > SELECT status, error
                  FROM mz_internal.mz_source_statuses
                  WHERE name = 'source1'
                running <null>
                """
            )
        )


@dataclass
class KafkaSinkDisruption:
    name: str
    breakage: Callable
    expected_error: str
    fixage: Callable | None

    def run_test(self, c: Composition) -> None:
        print(f"+++ Running Kafka sink disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.up(
            "redpanda",
            "materialized",
            "clusterd",
            Service("testdrive", idle=True),
        )

        with c.override(
            Testdrive(
                no_reset=True,
                seed=seed,
                entrypoint_extra=["--initial-backoff=1s", "--backoff-factor=0"],
            )
        ):
            self.populate(c)
            self.breakage(c, seed)
            self.assert_error(c, self.expected_error)

            if self.fixage:
                self.fixage(c, seed)
                self.assert_recovery(c)

    def populate(self, c: Composition) -> None:
        # Create a source and a sink
        c.testdrive(
            schema()
            + dedent(
                """
                # We specify the progress topic explicitly so we can delete it in a test later,
                # and confirm that the sink stalls. (Deleting the output topic is not enough if
                # we're not actively publishing new messages to the sink.)
                > CREATE CONNECTION kafka_conn
                  TO KAFKA (
                    BROKER '${testdrive.kafka-addr}',
                    SECURITY PROTOCOL PLAINTEXT,
                    PROGRESS TOPIC 'testdrive-progress-topic-${testdrive.seed}'
                  );

                > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                  URL '${testdrive.schema-registry-url}'
                  );

                $ kafka-create-topic topic=source-topic

                $ kafka-ingest topic=source-topic format=avro schema=${schema}
                {"f1": "A"}

                > CREATE SOURCE source1
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-source-topic-${testdrive.seed}')

                > CREATE TABLE source1_tbl FROM SOURCE source1 (REFERENCE "testdrive-source-topic-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE
                # WITH ( REMOTE 'clusterd:2100' ) https://github.com/MaterializeInc/database-issues/issues/4800

                > CREATE SINK sink1 FROM source1_tbl
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-topic-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                # WITH ( REMOTE 'clusterd:2100' ) https://github.com/MaterializeInc/database-issues/issues/4800

                $ kafka-verify-data format=avro sink=materialize.public.sink1 sort-messages=true
                {"before": null, "after": {"row":{"f1": "A"}}}
                """
            )
        )

    def assert_error(self, c: Composition, error: str) -> None:
        c.testdrive(
            dedent(
                f"""
                $ set-sql-timeout duration=60s
                # Sinks generally halt after receiving an error, which means that they may alternate
                # between `stalled` and `starting`. Instead of relying on the current status, we
                # check that there is a stalled status with the expected error.
                > SELECT bool_or(error ~* '{error}'), bool_or(details->'namespaced'->>'kafka' ~* '{error}')
                  FROM mz_internal.mz_sink_status_history
                  JOIN mz_sinks ON mz_sinks.id = sink_id
                  WHERE name = 'sink1' and status = 'stalled'
                true true
                """
            )
        )

    def assert_recovery(self, c: Composition) -> None:
        c.testdrive(
            dedent(
                """
                > SELECT status, error
                  FROM mz_internal.mz_sink_statuses
                  WHERE name = 'sink1'
                running <null>
                """
            )
        )


@dataclass
class PgDisruption:
    name: str
    breakage: Callable
    expected_error: str
    fixage: Callable | None

    def run_test(self, c: Composition) -> None:
        print(f"+++ Running disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.up(
            "postgres",
            "materialized",
            "clusterd",
            Service("testdrive", idle=True),
        )

        with c.override(
            Testdrive(
                no_reset=True,
                seed=seed,
                entrypoint_extra=["--initial-backoff=1s", "--backoff-factor=0"],
            )
        ):
            self.populate(c)
            self.breakage(c, seed)
            self.assert_error(c, self.expected_error)

            if self.fixage:
                self.fixage(c, seed)
                self.assert_recovery(c)

    def populate(self, c: Composition) -> None:
        # Create a source and a sink
        c.testdrive(
            dedent(
                """
                > CREATE SECRET pgpass AS 'postgres'
                > CREATE CONNECTION pg TO POSTGRES (
                    HOST postgres,
                    DATABASE postgres,
                    USER postgres,
                    PASSWORD SECRET pgpass
                  )

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                ALTER USER postgres WITH replication;
                DROP SCHEMA IF EXISTS public CASCADE;
                CREATE SCHEMA public;

                DROP PUBLICATION IF EXISTS mz_source;
                CREATE PUBLICATION mz_source FOR ALL TABLES;

                CREATE TABLE source1 (f1 INTEGER PRIMARY KEY, f2 integer[]);
                INSERT INTO source1 VALUES (1, NULL);
                ALTER TABLE source1 REPLICA IDENTITY FULL;
                INSERT INTO source1 VALUES (2, NULL);

                > CREATE SOURCE "pg_source"
                  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
                > CREATE TABLE "source1_tbl" FROM SOURCE "pg_source" (REFERENCE "source1");
                """
            )
        )

    def assert_error(self, c: Composition, error: str) -> None:
        c.testdrive(
            dedent(
                f"""
                $ set-sql-timeout duration=60s
                # Postgres sources may halt after receiving an error, which means that they may alternate
                # between `stalled` and `starting`. Instead of relying on the current status, we
                # check that the latest stall has the error we expect.
                > SELECT error ~* '{error}'
                    FROM mz_internal.mz_source_status_history
                    JOIN (
                      SELECT name, id FROM mz_sources UNION SELECT name, id FROM mz_tables
                    ) ON id = source_id
                    WHERE (
                        name = 'source1_tbl' OR name = 'pg_source'
                    ) AND (status = 'stalled' OR status = 'ceased')
                    ORDER BY occurred_at DESC LIMIT 1;
                true
                """
            )
        )

    def assert_recovery(self, c: Composition) -> None:
        c.testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO source1 VALUES (3);

                > SELECT status, error
                  FROM mz_internal.mz_source_statuses
                  WHERE name = 'source1_tbl'
                  AND type = 'table'
                running <null>

                > SELECT f1 FROM source1_tbl;
                1
                2
                3
                """
            )
        )


disruptions: list[Disruption] = [
    KafkaSinkDisruption(
        name="delete-sink-topic-delete-progress-fix",
        breakage=lambda c, seed: delete_sink_topic(c, seed),
        expected_error="topic testdrive-sink-topic-\\d+ does not exist",
        # If we delete the progress topic, we will re-create the sink as if it is new.
        fixage=lambda c, seed: c.exec(
            "redpanda", "rpk", "topic", "delete", f"testdrive-progress-topic-{seed}"
        ),
    ),
    KafkaSinkDisruption(
        name="delete-sink-topic-recreate-topic-fix",
        breakage=lambda c, seed: delete_sink_topic(c, seed),
        expected_error="topic testdrive-sink-topic-\\d+ does not exist",
        # If we recreate the sink topic, the sink will work but will likely be inconsistent.
        fixage=lambda c, seed: c.exec(
            "redpanda", "rpk", "topic", "create", f"testdrive-sink-topic-{seed}"
        ),
    ),
    KafkaDisruption(
        name="delete-source-topic",
        breakage=lambda c, seed: c.exec(
            "redpanda", "rpk", "topic", "delete", f"testdrive-source-topic-{seed}"
        ),
        expected_error="UnknownTopicOrPartition|topic",
        fixage=None,
        # Re-creating the topic does not restart the source
        # fixage=lambda c,seed: redpanda_topics(c, "create", seed),
    ),
    # docker compose pause has become unreliable recently
    # KafkaDisruption(
    #     name="pause-redpanda",
    #     breakage=lambda c, _: c.pause("redpanda"),
    #     expected_error="OperationTimedOut|BrokerTransportFailure|transaction",
    #     fixage=lambda c, _: c.unpause("redpanda"),
    # ),
    KafkaDisruption(
        name="sigstop-redpanda",
        breakage=lambda c, _: c.kill("redpanda", signal="SIGSTOP", wait=False),
        expected_error="OperationTimedOut|BrokerTransportFailure|transaction",
        fixage=lambda c, _: c.kill("redpanda", signal="SIGCONT", wait=False),
    ),
    KafkaDisruption(
        name="kill-redpanda",
        breakage=lambda c, _: c.kill("redpanda"),
        expected_error="BrokerTransportFailure|Resolve|Broker transport failure|Timed out",
        fixage=lambda c, _: c.up("redpanda"),
    ),
    # https://github.com/MaterializeInc/database-issues/issues/4800
    # KafkaDisruption(
    #     name="kill-redpanda-clusterd",
    #     breakage=lambda c, _: c.kill("redpanda", "clusterd"),
    #     expected_error="???",
    #     fixage=lambda c, _: c.up("redpanda", "clusterd"),
    # ),
    PgDisruption(
        name="kill-postgres",
        breakage=lambda c, _: c.kill("postgres"),
        expected_error="error connecting to server|connection closed|deadline has elapsed|failed to lookup address information",
        fixage=lambda c, _: c.up("postgres"),
    ),
    PgDisruption(
        name="drop-publication-postgres",
        breakage=lambda c, _: c.testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP PUBLICATION mz_source;
                INSERT INTO source1 VALUES (3, NULL);
                """
            )
        ),
        expected_error="publication .+ does not exist",
        # Can't recover when publication state is deleted.
        fixage=None,
    ),
    PgDisruption(
        name="alter-postgres",
        breakage=lambda c, _: alter_pg_table(c),
        expected_error="source table source1 with oid .+ has been altered",
        fixage=None,
    ),
    PgDisruption(
        name="unsupported-postgres",
        breakage=lambda c, _: unsupported_pg_table(c),
        expected_error="invalid input syntax for type array",
        fixage=None,
    ),
    # One-off disruption with a badly configured kafka sink
    KafkaTransactionLogGreaterThan1(
        name="bad-kafka-sink",
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("disruptions", nargs="*", default=[d.name for d in disruptions])

    args = parser.parse_args()

    sharded_disruptions = buildkite.shard_list(
        list(selected_by_name(args.disruptions, disruptions)), lambda s: s.name
    )
    print(
        f"Disruptions in shard with index {buildkite.get_parallelism_index()}: {[d.name for d in sharded_disruptions]}"
    )
    for disruption in sharded_disruptions:
        c.override_current_testcase_name(
            f"Disruption '{disruption.name}' in workflow_default"
        )
        disruption.run_test(c)
        c.down(sanity_restart_mz=False)


def delete_sink_topic(c: Composition, seed: int) -> None:
    c.exec("redpanda", "rpk", "topic", "delete", f"testdrive-sink-topic-{seed}")

    # Write new data to source otherwise nothing will encounter the missing topic
    c.testdrive(
        schema()
        + dedent(
            """
            $ kafka-ingest topic=source-topic format=avro schema=${schema}
            {"f1": "B"}

            > SELECT COUNT(*) FROM source1_tbl;
            2
            """
        )
    )


def alter_pg_table(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
                 $ postgres-execute connection=postgres://postgres:postgres@postgres
                 ALTER TABLE source1 DROP COLUMN f1;
                 INSERT INTO source1 VALUES (NULL)
                 """
        )
    )


def unsupported_pg_table(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
                 $ postgres-execute connection=postgres://postgres:postgres@postgres
                 INSERT INTO source1 VALUES (3, '[2:3]={2,2}')
                 """
        )
    )
