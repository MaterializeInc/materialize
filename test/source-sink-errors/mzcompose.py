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
from typing import Callable, Optional

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized, Redpanda, Storaged, Testdrive

SERVICES = [Redpanda(), Materialized(), Testdrive(), Storaged()]


@dataclass
class Disruption:
    name: str
    breakage: Callable
    expected_error: str
    fixage: Optional[Callable]

    def run_test(self, c: Composition) -> None:
        print(f"+++ Running disruption scenario {self.name}")
        seed = random.randint(0, 256**4)

        c.down(destroy_volumes=True)
        c.up("testdrive", persistent=True)
        c.start_and_wait_for_tcp(services=["redpanda", "materialized", "storaged"])
        c.wait_for_materialized()

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
                  /* TODO(bkirwi) WITH ( REMOTE 'storaged:2100' ) REMOTE causes the status source to be empty */

                > CREATE SINK sink1 FROM source1
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-topic-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                  /* WITH ( REMOTE 'storaged:2100' ) REMOTE causes the status source to be empty */
                """
            )
        )

    def assert_error(self, c: Composition, error: str) -> None:
        c.testdrive(
            dedent(
                f"""
                > SELECT status, error ~* '{error}'
                  FROM mz_internal.mz_source_status
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
                  FROM mz_internal.mz_source_status
                  WHERE name = 'source1'
                running <null>

                > SELECT status, error
                  FROM mz_internal.mz_sink_status
                  WHERE name = 'sink1'
                running <null>
                """
            )
        )


disruptions = [
    Disruption(
        name="delete-topic",
        breakage=lambda c, seed: redpanda_topics(c, "delete", seed),
        expected_error="UnknownTopicOrPartition|topic",
        fixage=None
        # Re-creating the topic does not restart the source
        # fixage=lambda c,seed: redpanda_topics(c, "create", seed),
    ),
    Disruption(
        name="pause-redpanda",
        breakage=lambda c, _: c.pause("redpanda"),
        expected_error="OperationTimedOut|BrokerTransportFailure|transaction",
        fixage=lambda c, _: c.unpause("redpanda"),
    ),
    Disruption(
        name="kill-redpanda",
        breakage=lambda c, _: c.kill("redpanda"),
        expected_error="BrokerTransportFailure|Resolve",
        fixage=lambda c, _: c.up("redpanda"),
    ),
    # Can not be tested ATM due to REMOTE breaking things
    # Disruption(
    #     name="kill-redpanda-storaged",
    #     breakage=lambda c, _: c.kill("redpanda", "storaged"),
    #     expected_error="???",
    #     fixage=lambda c, _: c.up("redpanda", "storaged"),
    # ),
]


def workflow_default(c: Composition) -> None:
    """Test the detection and reporting of source/sink errors by
    introducing a Disruption and then checking the mz_internal.mz_*_status tables
    """

    for id, disruption in enumerate(disruptions):
        disruption.run_test(c)


def redpanda_topics(c: Composition, action: str, seed: int) -> None:
    for topic in ["source", "sink", "progress"]:
        c.exec("redpanda", "rpk", "topic", action, f"testdrive-{topic}-topic-{seed}")
