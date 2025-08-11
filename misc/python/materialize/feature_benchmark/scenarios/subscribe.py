# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from random import getrandbits
from textwrap import dedent

from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario


class SubscribeParallel(Scenario):
    """Feature benchmarks related to SUBSCRIBE"""

    SCALE = (
        2  # So 100 concurrent SUBSCRIBEs by default, limited by database-issues#5376
    )
    FIXED_SCALE = True

    def benchmark(self) -> MeasurementSource:
        return Td(
            self.create_subscribe_source()
            + "\n".join(
                [
                    dedent(
                        f"""
                        $ postgres-connect name=conn{i} url=postgres://materialize:materialize@${{testdrive.materialize-sql-addr}}
                        $ postgres-execute connection=conn{i}
                        # STRICT SERIALIZABLE is affected by database-issues#5407
                        START TRANSACTION ISOLATION LEVEL SERIALIZABLE;
                        DECLARE c{i} CURSOR FOR SUBSCRIBE s1
                        """
                    )
                    for i in range(0, self.n())
                ]
            )
            + self.insert()
            # We measure from here ...
            + dedent(
                """
                > SELECT COUNT(*) FROM s1;
                  /* A */
                1
                """
            )
            + "\n".join(
                [
                    dedent(
                        f"""
                        $ postgres-execute connection=conn{i}
                        FETCH ALL FROM c{i};
                        """
                    )
                    for i in range(0, self.n())
                ]
            )
            # ... to here
            + dedent(
                """
                > SELECT 1
                  /* B */
                1
                """
            )
        )

    def create_subscribe_source(self) -> str:
        raise NotImplementedError

    def insert(self) -> str:
        raise NotImplementedError


class SubscribeParallelTable(SubscribeParallel):
    def create_subscribe_source(self) -> str:
        return dedent(
            """
             > DROP TABLE IF EXISTS s1;
             > CREATE TABLE s1 (f1 TEXT);
             """
        )

    def insert(self) -> str:
        return "> INSERT INTO s1 VALUES (REPEAT('x', 1024))\n"


class SubscribeParallelTableWithIndex(SubscribeParallel):
    def create_subscribe_source(self) -> str:
        return dedent(
            """
             > DROP TABLE IF EXISTS s1;
             > CREATE TABLE s1 (f1 INTEGER);
             > CREATE DEFAULT INDEX ON s1;
             """
        )

    def insert(self) -> str:
        return "> INSERT INTO s1 VALUES (123)\n"


class SubscribeParallelKafka(SubscribeParallel):
    def create_subscribe_source(self) -> str:
        # As we are doing `kafka-ingest` in the middle of the benchmark() method
        # we must always use a unique topic to ensure isolation between the individal
        # measurements
        self._unique_topic_id = getrandbits(64)
        return dedent(
            f"""
             # Separate topic for each Mz instance
             $ kafka-create-topic topic=subscribe-kafka-{self._unique_topic_id}

             > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);

             > DROP CLUSTER IF EXISTS source_cluster CASCADE;
             > CREATE CLUSTER source_cluster SIZE 'scale={self._default_size},workers=1', REPLICATION FACTOR 1;

             > DROP SOURCE IF EXISTS s1 CASCADE;

             > CREATE SOURCE s1_source
               IN CLUSTER source_cluster
               FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-subscribe-kafka-{self._unique_topic_id}-${{testdrive.seed}}');

             > CREATE TABLE s1 FROM SOURCE s1_source (REFERENCE "testdrive-subscribe-kafka-{self._unique_topic_id}-${{testdrive.seed}}")
               FORMAT BYTES ENVELOPE NONE;

             > CREATE DEFAULT INDEX ON s1;
             """
        )

    def insert(self) -> str:
        return dedent(
            f"""
            $ kafka-ingest format=bytes topic=subscribe-kafka-{self._unique_topic_id}
            123
            """
        )
