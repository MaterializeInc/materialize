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

PAD_128B = "X" * 128
# 1000 and not 1024 to keep entire Kafka message under 1M
PAD_1K = "Y" * 1000

# A schema that allows null values
SCHEMA = dedent(
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
               {"name":"f1", "type": ["null", "string"], "default": null }
           ]
         }
    """
)


class UpsertEnrichValue(Check):
    """Progressively enrich records that started their lives as null values.
    Progressively empoverish records that started as very large values.
    """

    def initialize(self) -> Testdrive:
        return Testdrive(
            SCHEMA
            + dedent(
                f"""
                $ kafka-create-topic topic=upsert-enrich-value
                # 'A...' records start as NULLs
                $ kafka-ingest format=avro key-format=avro topic=upsert-enrich-value key-schema=${{keyschema}} schema=${{schema}} repeat=1000
                {{"key1": "A${{kafka-ingest.iteration}}"}} {{"f1": null}}

                # 'B...' records start as 1Ks
                $ kafka-ingest format=avro key-format=avro topic=upsert-enrich-value key-schema=${{keyschema}} schema=${{schema}} repeat=1000
                {{"key1": "B${{kafka-ingest.iteration}}"}} {{"f1": {{"string":"{PAD_1K}"}}}}

                > CREATE SOURCE upsert_enrich_value_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-enrich-value-${{testdrive.seed}}')
                > CREATE TABLE upsert_enrich_value FROM SOURCE upsert_enrich_value_src (REFERENCE "testdrive-upsert-enrich-value-${{testdrive.seed}}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_enrich_value_view AS
                  SELECT LEFT(key1, 1) AS key_left, LEFT(f1, 1) AS value_left, RIGHT(f1, 1),
                  LENGTH(f1), COUNT(*), SUM(CASE WHEN f1 IS NULL THEN 1 ELSE 0 END) AS nulls, COUNT(f1) AS not_nulls
                  FROM upsert_enrich_value
                  GROUP BY LEFT(key1, 1), f1
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(SCHEMA + dedent(s))
            for s in [
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-enrich-value key-schema=${{keyschema}} schema=${{schema}} repeat=1000
                {{"key1": "A${{kafka-ingest.iteration}}"}} {{"f1": {{"string":"{PAD_128B}"}}}}

                $ kafka-ingest format=avro key-format=avro topic=upsert-enrich-value key-schema=${{keyschema}} schema=${{schema}} repeat=1000
                {{"key1": "B${{kafka-ingest.iteration}}"}} {{"f1": {{"string":"{PAD_128B}"}}}}
                """,
                f"""
                # 'A...' records will now be enriched to 1Ks
                $ kafka-ingest format=avro key-format=avro topic=upsert-enrich-value key-schema=${{keyschema}} schema=${{schema}} repeat=1000
                {{"key1": "A${{kafka-ingest.iteration}}"}} {{"f1": {{"string":"{PAD_1K}"}}}}

                # 'B...' records will now be enpoverished to NULLs
                $ kafka-ingest format=avro key-format=avro topic=upsert-enrich-value key-schema=${{keyschema}} schema=${{schema}} repeat=1000
                {{"key1": "B${{kafka-ingest.iteration}}"}} {{"f1": null}}

                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM upsert_enrich_value_view
                A Y Y 1000 1000 0 1000
                B <null> <null> <null> 1000 1000 0
                """
            )
        )
