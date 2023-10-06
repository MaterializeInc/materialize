# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from collections.abc import Callable
from dataclasses import dataclass
from textwrap import dedent
from typing import Protocol

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

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
class KafkaDisruption:
    name: str
    breakage: Callable
    expected_error: str
    fixage: Callable | None

    def run_test(self, c: Composition) -> None:
        print(f"+++ Running disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.down(destroy_volumes=True, sanity_restart_mz=False)
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
                # check that there is a stalled status with the expected error.
                > SELECT occurred_at, status, error, error ~* 'UnknownTopicOrPartition|topic' as hmm
                  FROM mz_internal.mz_sink_status_history
                  JOIN mz_sinks ON mz_sinks.id = sink_id
                  WHERE name = 'sink1' ORDER BY occurred_at ASC
                occurred_at status error hmm
                ----------------------------
                gus         gus    gus   true
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
    fixage: Callable | None

    def run_test(self, c: Composition) -> None:
        print(f"+++ Running disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.down(destroy_volumes=True, sanity_restart_mz=False)
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

                CREATE TABLE source1 (f1 INTEGER PRIMARY KEY, f2 integer[]);
                INSERT INTO source1 VALUES (1, NULL);
                ALTER TABLE source1 REPLICA IDENTITY FULL;
                INSERT INTO source1 VALUES (2, NULL);

                > CREATE SOURCE "pg_source"
                  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
                  FOR TABLES ("source1");
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
                INSERT INTO source1 VALUES (3);

                > SELECT status, error
                  FROM mz_internal.mz_source_statuses
                  WHERE name = 'source1'
                running <null>

                > SELECT f1 FROM source1;
                1
                2
                3
                """
            )
        )


disruptions: list[Disruption] = [
    KafkaDisruption(
        name="delete-topic",
        breakage=lambda c, seed: redpanda_topics(c, "delete", seed),
        expected_error="UnknownTopicOrPartition|topic",
        fixage=None
        # Re-creating the topic does not restart the source
        # fixage=lambda c,seed: redpanda_topics(c, "create", seed),
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
