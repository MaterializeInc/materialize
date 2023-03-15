# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from dataclasses import dataclass
from textwrap import dedent
from typing import Callable, List, Optional, Protocol

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Clusterd,
    Kafka,
    Materialized,
    Postgres,
    Redpanda,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Redpanda(),
    Materialized(),
    Testdrive(),
    Clusterd(),
    Postgres(),
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
]


class Disruption(Protocol):
    def run_test(self, c: Composition) -> None:
        ...


@dataclass
class KafkaTransactionLogGreaterThan1:
    name: str

    # override the `run_test`, as we need `Kafka` (not `Redpanda`), and need to change some other things
    def run_test(self, c: Composition) -> None:
        print(f"+++ Running disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.up("testdrive", persistent=True)

        with c.override(
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
            Testdrive(
                no_reset=True,
                seed=seed,
                entrypoint_extra=[
                    "--initial-backoff=1s",
                    "--backoff-factor=0",
                    "--kafka-addr=badkafka",
                ],
            ),
        ):
            c.up("zookeeper", "badkafka", "schema-registry", "materialized")
            self.populate(c)
            self.assert_error(
                c, "retriable transaction error", "running a single Kafka broker"
            )

    def populate(self, c: Composition) -> None:
        # Create a source and a sink
        c.testdrive(
            dedent(
                """
                > CREATE CONNECTION kafka_conn
                  TO KAFKA (BROKER '${testdrive.kafka-addr}');
               
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
                > SELECT status, error ~* '{error}', details::json#>>'{{hint}}' ~* '{hint}'
                  FROM mz_internal.mz_sink_status_history
                  JOIN mz_sinks ON mz_sinks.id = sink_id
                  WHERE name = 'kafka_sink' and status = 'stalled'
                  ORDER BY occurred_at DESC LIMIT 1
                stalled true true
                """
            )
        )


@dataclass
class KafkaDisruption:
    name: str
    breakage: Callable
    expected_error: str
    fixage: Optional[Callable]

    def run_test(self, c: Composition) -> None:
        print(f"+++ Running disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.down(destroy_volumes=True)
        c.up("testdrive", persistent=True)
        c.up("redpanda", "materialized", "clusterd")

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
                  FORMAT BYTES
                  ENVELOPE NONE
                # WITH ( REMOTE 'clusterd:2100' ) https://github.com/MaterializeInc/materialize/issues/16582

                > CREATE SINK sink1 FROM source1
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-topic-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                # WITH ( REMOTE 'clusterd:2100' ) https://github.com/MaterializeInc/materialize/issues/16582
                """
            )
        )

    def assert_error(self, c: Composition, error: str) -> None:
        c.testdrive(
            dedent(
                f"""
                > SELECT status, error ~* '{error}'
                  FROM mz_internal.mz_source_statuses
                  WHERE name = 'source1'
                stalled true

                # Sinks generally halt after receiving an error, which means that they may alternate
                # between `stalled` and `starting`. Instead of relying on the current status, we
                # check that the latest stall has the error we expect.
                > SELECT status, error ~* '{error}'
                  FROM mz_internal.mz_sink_status_history
                  JOIN mz_sinks ON mz_sinks.id = sink_id
                  WHERE name = 'sink1' and status = 'stalled'
                  ORDER BY occurred_at DESC LIMIT 1
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

                > SELECT COUNT(*) FROM source1;
                2

                > SELECT status, error
                  FROM mz_internal.mz_source_statuses
                  WHERE name = 'source1'
                running <null>

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
    fixage: Optional[Callable]

    def run_test(self, c: Composition) -> None:
        print(f"+++ Running disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.down(destroy_volumes=True)
        c.up("testdrive", persistent=True)
        c.up("postgres", "materialized", "clusterd")

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

                CREATE TABLE t1 (f1 INTEGER PRIMARY KEY, f2 integer[]);
                INSERT INTO t1 VALUES (1, NULL);
                ALTER TABLE t1 REPLICA IDENTITY FULL;
                INSERT INTO t1 VALUES (2, NULL);

                > CREATE SOURCE "source1"
                  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
                  FOR TABLES ("t1");
                """
            )
        )

    def assert_error(self, c: Composition, error: str) -> None:
        c.testdrive(
            dedent(
                f"""
                # Postgres sources may halt after receiving an error, which means that they may alternate
                # between `stalled` and `starting`. Instead of relying on the current status, we
                # check that the latest stall has the error we expect.
                > SELECT status, error ~* '{error}'
                  FROM mz_internal.mz_source_status_history
                  JOIN mz_sources ON mz_sources.id = source_id
                  WHERE name = 'source1' and status = 'stalled'
                  ORDER BY occurred_at DESC LIMIT 1
                stalled true
                """
            )
        )

    def assert_recovery(self, c: Composition) -> None:
        c.testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO t1 VALUES (3);

                > SELECT status, error
                  FROM mz_internal.mz_source_statuses
                  WHERE name = 'source1'
                running <null>

                > SELECT f1 FROM t1;
                1
                2
                3
                """
            )
        )


disruptions: List[Disruption] = [
    KafkaDisruption(
        name="delete-topic",
        breakage=lambda c, seed: redpanda_topics(c, "delete", seed),
        expected_error="UnknownTopicOrPartition|topic",
        fixage=None
        # Re-creating the topic does not restart the source
        # fixage=lambda c,seed: redpanda_topics(c, "create", seed),
    ),
    KafkaDisruption(
        name="pause-redpanda",
        breakage=lambda c, _: c.pause("redpanda"),
        expected_error="OperationTimedOut|BrokerTransportFailure|transaction",
        fixage=lambda c, _: c.unpause("redpanda"),
    ),
    KafkaDisruption(
        name="kill-redpanda",
        breakage=lambda c, _: c.kill("redpanda"),
        expected_error="BrokerTransportFailure|Resolve",
        fixage=lambda c, _: c.up("redpanda"),
    ),
    # https://github.com/MaterializeInc/materialize/issues/16582
    # KafkaDisruption(
    #     name="kill-redpanda-clusterd",
    #     breakage=lambda c, _: c.kill("redpanda", "clusterd"),
    #     expected_error="???",
    #     fixage=lambda c, _: c.up("redpanda", "clusterd"),
    # ),
    PgDisruption(
        name="kill-postgres",
        breakage=lambda c, _: c.kill("postgres"),
        expected_error="error connecting to server",
        fixage=lambda c, _: c.up("postgres"),
    ),
    PgDisruption(
        name="drop-publication-postgres",
        breakage=lambda c, _: c.testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP PUBLICATION mz_source;
                INSERT INTO t1 VALUES (3, NULL);
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
        expected_error="source table t1 with oid .+ has been altered",
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


def workflow_default(c: Composition) -> None:
    """Test the detection and reporting of source/sink errors by
    introducing a Disruption and then checking the mz_internal.mz_*_statuses tables
    """

    for id, disruption in enumerate(disruptions):
        disruption.run_test(c)


def redpanda_topics(c: Composition, action: str, seed: int) -> None:
    for topic in ["source", "sink", "progress"]:
        c.exec("redpanda", "rpk", "topic", action, f"testdrive-{topic}-topic-{seed}")


def alter_pg_table(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
                 $ postgres-execute connection=postgres://postgres:postgres@postgres
                 ALTER TABLE t1 ADD COLUMN f3 INTEGER;
                 INSERT INTO t1 VALUES (3, NULL, 4)
                 """
        )
    )


def unsupported_pg_table(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
                 $ postgres-execute connection=postgres://postgres:postgres@postgres
                 INSERT INTO t1 VALUES (3, '{{1},{2}}')
                 """
        )
    )
