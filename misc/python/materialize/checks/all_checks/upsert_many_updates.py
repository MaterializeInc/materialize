# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.common import KAFKA_SCHEMA

INCREMENTS = 100000


class UpsertManyUpdates(Check):
    """Update the same row over and over"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            KAFKA_SCHEMA
            + dedent(
                """
                $ kafka-create-topic topic=upsert-many-updates
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-updates key-schema=${keyschema} schema=${schema}
                {"key1": "A"} {"f1": "0"}

                > CREATE SOURCE upsert_many_updates_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-many-updates-${testdrive.seed}')
                > CREATE TABLE upsert_many_updates FROM SOURCE upsert_many_updates_src (REFERENCE "testdrive-upsert-many-updates-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_many_updates_view AS
                  SELECT f1 FROM upsert_many_updates
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        # Construct inputs for $kafka-ingest where every update is a separate Kafka message to be ingested
        increment1 = "\n".join(
            [f"""{{"key1": "A"}} {{"f1": "{i+1}"}}""" for i in range(INCREMENTS)]
        )
        increment2 = "\n".join(
            [
                f"""{{"key1": "A"}} {{"f1": "{INCREMENTS+i+1}"}}"""
                for i in range(INCREMENTS)
            ]
        )

        return [
            Testdrive(
                KAFKA_SCHEMA
                + dedent(
                    """
                    $ kafka-ingest format=avro key-format=avro topic=upsert-many-updates key-schema=${keyschema} schema=${schema}
                    """
                )
                + increment1
            ),
            Testdrive(
                KAFKA_SCHEMA
                + dedent(
                    """
                    $ kafka-ingest format=avro key-format=avro topic=upsert-many-updates key-schema=${keyschema} schema=${schema}
                    """
                )
                + increment2
            ),
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                > SELECT * FROM upsert_many_updates
                A {INCREMENTS*2}
                """
            )
        )
