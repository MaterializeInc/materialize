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

MANY_KEYS = ", ".join(
    [f'{{"name": "key{i+1}", "type": "string"}}' for i in range(1000)]
)
MANY_VALUES = ", ".join(
    [f'{{"name": "f{i+1}", "type": "string"}}' for i in range(1000)]
)

MANY_KEYS_SCHEMA = dedent(
    f"""
    $ set keyschema={{
      "type" : "record",
      "name" : "Key",
      "fields" : [ {MANY_KEYS} ]
      }}

    $ set schema={{
      "type" : "record",
      "name" : "test",
      "fields" : [
        {{"name":"f1", "type":"string"}}
      ]
      }}
    """
)

MANY_VALUES_SCHEMA = dedent(
    f"""
   $ set keyschema={{
     "type": "record",
     "name": "Key",
     "fields": [
       {{"name": "key1", "type": "string"}}
     ]
     }}

   $ set schema={{
     "type" : "record",
     "name" : "test",
     "fields" : [ {MANY_VALUES} ]
     }}
   """
)


class UpsertManyValueColumns(Check):
    """Upsert 1K value columns"""

    DATA_A = ", ".join([f'"f{i+1}": "A{i+1}XYZ"' for i in range(1000)])
    DATA_B = ", ".join([f'"f{i+1}": "B{i+1}XYZ"' for i in range(1000)])
    DATA_C = ", ".join([f'"f{i+1}": "C{i+1}XYZ"' for i in range(1000)])

    def initialize(self) -> Testdrive:
        return Testdrive(
            MANY_VALUES_SCHEMA
            + dedent(
                f"""
                $ kafka-create-topic topic=upsert-many-value-columns
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-value-columns key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "1"}} {{ {UpsertManyValueColumns.DATA_A} }}
                {{"key1": "2"}} {{ {UpsertManyValueColumns.DATA_A} }}
                {{"key1": "3"}} {{ {UpsertManyValueColumns.DATA_A} }}

                > CREATE SOURCE upsert_many_value_columns_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-many-value-columns-${{testdrive.seed}}')
                > CREATE TABLE upsert_many_value_columns FROM SOURCE upsert_many_value_columns_source_src (REFERENCE "testdrive-upsert-many-value-columns-${{testdrive.seed}}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_many_value_columns_view AS
                  SELECT key1, f1, f1000
                  FROM upsert_many_value_columns
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(MANY_VALUES_SCHEMA + dedent(s))
            for s in [
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-value-columns key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "1"}} {{ {UpsertManyValueColumns.DATA_B} }}
                {{"key1": "2"}}
                """,
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-value-columns key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "1"}} {{ {UpsertManyValueColumns.DATA_C} }}
                {{"key1": "3"}}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM upsert_many_value_columns_view
                1 C1XYZ C1000XYZ
                """
            )
        )


class UpsertManyKeyColumns(Check):
    """Upsert 1K key columns"""

    KEYS_A = ", ".join([f'"key{i+1}": "A{i+1}XYZ"' for i in range(1000)])
    KEYS_B = ", ".join([f'"key{i+1}": "B{i+1}XYZ"' for i in range(1000)])
    KEYS_C = ", ".join([f'"key{i+1}": "C{i+1}XYZ"' for i in range(1000)])

    def initialize(self) -> Testdrive:
        return Testdrive(
            MANY_KEYS_SCHEMA
            + dedent(
                f"""
                $ kafka-create-topic topic=upsert-many-key-columns
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-key-columns key-schema=${{keyschema}} schema=${{schema}}
                {{ {UpsertManyKeyColumns.KEYS_A} }} {{ "f1" : "X" }}
                {{ {UpsertManyKeyColumns.KEYS_B} }} {{ "f1" : "X" }}
                {{ {UpsertManyKeyColumns.KEYS_C} }} {{ "f1" : "X" }}

                > CREATE SOURCE upsert_many_key_columns_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-many-key-columns-${{testdrive.seed}}')
                > CREATE TABLE upsert_many_key_columns FROM SOURCE upsert_many_key_columns_source_src (REFERENCE "testdrive-upsert-many-key-columns-${{testdrive.seed}}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_many_key_columns_view AS
                  SELECT key1, key1000, f1
                  FROM upsert_many_key_columns
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(MANY_KEYS_SCHEMA + dedent(s))
            for s in [
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-key-columns key-schema=${{keyschema}} schema=${{schema}}
                {{ {UpsertManyKeyColumns.KEYS_A} }} {{ "f1" : "Y" }}
                {{ {UpsertManyKeyColumns.KEYS_B} }}
                """,
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-many-key-columns key-schema=${{keyschema}} schema=${{schema}}
                {{ {UpsertManyKeyColumns.KEYS_A} }} {{ "f1" : "Z" }}
                {{ {UpsertManyKeyColumns.KEYS_C} }}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM upsert_many_key_columns_view
                A1XYZ A1000XYZ Z
                """
            )
        )
