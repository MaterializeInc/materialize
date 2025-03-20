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
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD


class UpsertManyRows(Check):
    """Upsert 1M rows"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)
            + dedent(
                """
                $ kafka-create-topic topic=upsert-many-rows
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-rows key-schema=${keyschema} schema=${schema} repeat=1000000
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "X"}
                {"key1": "B${kafka-ingest.iteration}"} {"f1": "X"}
                {"key1": "C${kafka-ingest.iteration}"} {"f1": "X"}

                > CREATE SOURCE upsert_many_rows_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-many-rows-${testdrive.seed}')
                > CREATE TABLE upsert_many_rows FROM SOURCE upsert_many_rows_src (REFERENCE "testdrive-upsert-many-rows-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_many_rows_view AS
                  SELECT f1, COUNT(*) AS count_rows, COUNT(DISTINCT key1) AS count_keys
                  FROM upsert_many_rows
                  GROUP BY f1
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD) + dedent(s))
            for s in [
                """
                # Update the As
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-rows key-schema=${keyschema} schema=${schema} repeat=1000000
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "Y"}

                # Delete the Bs
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-rows key-schema=${keyschema} schema=${schema} repeat=1000000
                {"key1": "B${kafka-ingest.iteration}"}
                """,
                """
                # Update the As again
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-rows key-schema=${keyschema} schema=${schema} repeat=1000000
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "Z"}

                # Delete the Cs
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-rows key-schema=${keyschema} schema=${schema} repeat=1000000
                {"key1": "C${kafka-ingest.iteration}"}

                # Insert some more
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-rows key-schema=${keyschema} schema=${schema} repeat=1000000
                {"key1": "D${kafka-ingest.iteration}"} {"f1": "Z"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM upsert_many_rows_view
                Z 2000000 2000000
                """
            )
        )
