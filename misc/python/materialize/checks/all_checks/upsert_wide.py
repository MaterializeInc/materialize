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
from materialize.checks.checks import Check, externally_idempotent
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD

PAD_100K = "X" * (100 * 1024)
PAD_500K = "Y" * (500 * 1024)
PAD_1M = "Z" * (1000 * 1024)


@externally_idempotent(False)
class UpsertWideValue(Check):
    """Perform upsert over records with a very long/wide value."""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)
            + dedent(
                f"""
                $ kafka-create-topic topic=upsert-wide-value

                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-value key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "A"}} {{"f1": "{PAD_100K}"}}

                > CREATE SOURCE upsert_wide_value_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-wide-value-${{testdrive.seed}}')
                > CREATE TABLE upsert_wide_value FROM SOURCE upsert_wide_value_src (REFERENCE "testdrive-upsert-wide-value-${{testdrive.seed}}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_wide_value_view AS
                  SELECT LEFT(f1, 1), RIGHT(f1, 1),
                  LENGTH(f1)
                  FROM upsert_wide_value
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD) + dedent(s))
            for s in [
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-value key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "A"}} {{"f1": "{PAD_1M}"}}

                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-value key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "B"}} {{"f1": "{PAD_500K}"}}

                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-value key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "C"}} {{"f1": "{PAD_1M}"}}
                """,
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-value key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "A"}} {{"f1": "{PAD_500K}"}}

                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-value key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "B"}} {{"f1": "{PAD_1M}"}}

                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-value key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "C"}} {{"f1": "{PAD_100K}"}}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM upsert_wide_value_view
                X X 102400
                Y Y 512000
                Z Z 1024000
                """
            )
        )


@externally_idempotent(False)
class UpsertWideKey(Check):
    """Perform upsert over records with a very long/wide key."""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)
            + dedent(
                f"""
                $ kafka-create-topic topic=upsert-wide-key

                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-key key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "A{PAD_1M}"}} {{"f1": "A1"}}
                {{"key1": "B{PAD_1M}"}} {{"f1": "B1"}}
                {{"key1": "C{PAD_1M}"}} {{"f1": "C1"}}

                > CREATE SOURCE upsert_wide_key_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-wide-key-${{testdrive.seed}}')
                > CREATE TABLE upsert_wide_key FROM SOURCE upsert_wide_key_src (REFERENCE "testdrive-upsert-wide-key-${{testdrive.seed}}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_wide_key_view AS
                  SELECT LEFT(key1, 1), RIGHT(key1, 1),
                  LENGTH(key1), f1
                  FROM upsert_wide_key
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD) + dedent(s))
            for s in [
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-key key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "A{PAD_1M}"}} {{"f1": "A2"}}
                {{"key1": "D{PAD_1M}"}} {{"f1": "D1"}}

                # Delete B ...
                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-key key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "B{PAD_1M}"}}
                """,
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-key key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "A{PAD_1M}"}} {{"f1": "A3"}}
                {{"key1": "E{PAD_1M}"}} {{"f1": "E1"}}

                # Delete C ...
                $ kafka-ingest format=avro key-format=avro topic=upsert-wide-key key-schema=${{keyschema}} schema=${{schema}}
                {{"key1": "C{PAD_1M}"}}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM upsert_wide_key_view
                A Z 1024001 A3
                D Z 1024001 D1
                E Z 1024001 E1
                """
            )
        )
