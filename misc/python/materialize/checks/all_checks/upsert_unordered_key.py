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

# A schema that allows null values
SCHEMA = dedent(
    """
        # Must be a subset of the keys in the rows AND
        # in a different order than the value.
        $ set keyschema={
            "type": "record",
            "name": "Key",
            "fields": [
                {"name": "b", "type": "string"},
                {"name": "a", "type": "long"}
            ]
          }

        $ set schema={
            "type" : "record",
            "name" : "envelope",
            "fields" : [
              {
                "name": "before",
                "type": [
                  {
                    "name": "row",
                    "type": "record",
                    "fields": [
                      {
                          "name": "a",
                          "type": "long"
                      },
                      {
                        "name": "data",
                        "type": "string"
                      },
                      {
                          "name": "b",
                          "type": "string"
                      }]
                   },
                   "null"
                 ]
              },
              {
                "name": "after",
                "type": ["row", "null"]
              }
            ]
          }
    """
)


class UpsertUnorderedKey(Check):
    """Upsert with keys in a different order than values."""

    def initialize(self) -> Testdrive:
        return Testdrive(
            SCHEMA
            + dedent(
                """
                $ kafka-create-topic topic=upsert-unordered-key
                $ kafka-ingest format=avro topic=upsert-unordered-key key-format=avro key-schema=${keyschema} schema=${schema}
                {"b": "bdata", "a": 1} {"before": {"row": {"a": 1, "data": "fish", "b": "bdata"}}, "after": {"row": {"a": 1, "data": "fish2", "b": "bdata"}}}

                > CREATE SOURCE upsert_unordered_key_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-unordered-key-${testdrive.seed}')
                > CREATE TABLE upsert_unordered_key
                  FROM SOURCE upsert_unordered_key_src (REFERENCE "testdrive-upsert-unordered-key-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(SCHEMA + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro topic=upsert-unordered-key key-format=avro key-schema=${keyschema} schema=${schema}
                {"b": "bdata", "a": 1} {"before": {"row": {"a": 1, "data": "fish2", "b": "bdata"}}, "after": {"row": {"a": 1, "data": "fish3", "b": "bdata"}}}
                """,
                """
                $ kafka-ingest format=avro topic=upsert-unordered-key key-format=avro key-schema=${keyschema} schema=${schema}
                {"b": "bdata", "a": 1} {"before": {"row": {"a": 1, "data": "fish3", "b": "bdata"}}, "after": {"row": {"a": 1, "data": "fish4", "b": "bdata"}}}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM upsert_unordered_key
                1 fish4 bdata
                """
            )
        )
