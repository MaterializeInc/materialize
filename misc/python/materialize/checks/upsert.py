# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


def schemas() -> str:
    return dedent(
        """
       $ set keyschema={
           "type": "record",
           "name": "Key",
           "fields": [
               {"name": "key1", "type": "string"}
           ]
         }

       $ set schema={
           "type" : "record",
           "name" : "test",
           "fields" : [
               {"name":"f1", "type":"string"}
           ]
         }
    """
    )


class UpsertInsert(Check):
    """Test that repeated inserts of the same record are properly handled"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=upsert-insert

                $ kafka-ingest format=avro key-format=avro topic=upsert-insert key-schema=${keyschema} schema=${schema} repeat=10000
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE upsert_insert
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-insert-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_insert_view AS SELECT COUNT(DISTINCT key1 || ' ' || f1) FROM upsert_insert;
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro key-format=avro topic=upsert-insert key-schema=${keyschema} schema=${schema} repeat=10000
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}
                """,
                """
                $ kafka-ingest format=avro key-format=avro topic=upsert-insert key-schema=${keyschema} schema=${schema} repeat=10000
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*), COUNT(DISTINCT key1), COUNT(DISTINCT f1) FROM upsert_insert
                10000 10000 10000

                > SELECT * FROM upsert_insert_view;
                10000
           """
            )
        )


class UpsertUpdate(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=upsert-update

                $ kafka-ingest format=avro key-format=avro topic=upsert-update key-schema=${keyschema} schema=${schema} repeat=10000
                {"key1": "${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE upsert_update
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-update-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_update_view AS SELECT LEFT(f1, 1), COUNT(*) AS c1, COUNT(DISTINCT key1) AS c2, COUNT(DISTINCT f1) AS c3 FROM upsert_update GROUP BY LEFT(f1, 1);
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro key-format=avro topic=upsert-update key-schema=${keyschema} schema=${schema} repeat=10000
                {"key1": "${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}
                """,
                """
                $ kafka-ingest format=avro key-format=avro topic=upsert-update key-schema=${keyschema} schema=${schema} repeat=10000
                {"key1": "${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM upsert_update_view;
                C 10000 10000 10000
           """
            )
        )


class UpsertDelete(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=upsert-delete

                $ kafka-ingest format=avro key-format=avro topic=upsert-delete key-schema=${keyschema} schema=${schema} repeat=30000
                {"key1": "${kafka-ingest.iteration}"} {"f1": "${kafka-ingest.iteration}"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE upsert_delete
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-delete-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_delete_view AS SELECT COUNT(*), MIN(key1), MAX(key1) FROM upsert_delete;
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro key-format=avro topic=upsert-delete key-schema=${keyschema} schema=${schema} repeat=10000
                {"key1": "${kafka-ingest.iteration}"}
                """,
                """
                $ kafka-ingest format=avro key-format=avro topic=upsert-delete key-schema=${keyschema} schema=${schema} start-iteration=20000 repeat=10000
                {"key1": "${kafka-ingest.iteration}"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM upsert_delete_view;
                10000 10000 19999
           """
            )
        )
