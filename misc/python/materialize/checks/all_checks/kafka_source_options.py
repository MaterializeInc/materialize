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


@externally_idempotent(False)
class KafkaSourceOptions(Check):
    """Kafka source surface not covered elsewhere: the INCLUDE
    PARTITION/OFFSET/TIMESTAMP/HEADERS metadata columns, START OFFSET, GROUP
    ID PREFIX, and ENVELOPE UPSERT (VALUE DECODING ERRORS = INLINE)."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_envelope_upsert_inline_errors = true

            $ kafka-create-topic topic=kafka-source-options partitions=1

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-source-options
            skipped:{"f1": 0}

            $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-source-options headers={"h1": "v1"}
            key1:{"f1": 1}

            > CREATE SOURCE kafka_source_options_src
              FROM KAFKA CONNECTION kafka_conn (
                TOPIC 'testdrive-kafka-source-options-${testdrive.seed}',
                START OFFSET (1),
                GROUP ID PREFIX 'kafka-source-options-'
              )

            > CREATE TABLE kafka_source_options_tbl
              FROM SOURCE kafka_source_options_src (REFERENCE "testdrive-kafka-source-options-${testdrive.seed}")
              KEY FORMAT TEXT
              VALUE FORMAT JSON
              INCLUDE
                PARTITION AS meta_partition,
                OFFSET AS meta_offset,
                TIMESTAMP AS meta_timestamp,
                HEADERS AS meta_headers
              ENVELOPE UPSERT (VALUE DECODING ERRORS = (INLINE AS decode_error))
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-source-options headers={"h2": "v2"}
                key2:{"f1": 2}

                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-source-options
                badkey:not-json
                """,
                """
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-source-options
                key3:{"f1": 3}

                # Fixing the bad value clears the inline decode error.
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=kafka-source-options
                badkey:{"f1": 99}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            # The record at offset 0 is excluded by START OFFSET (1).
            > SELECT
                key,
                data ->> 'f1',
                meta_partition,
                meta_offset >= 1,
                meta_timestamp IS NOT NULL,
                decode_error IS NULL
              FROM kafka_source_options_tbl
            key1 1 0 true true true
            key2 2 0 true true true
            key3 3 0 true true true
            badkey 99 0 true true true

            > SELECT key, map_build(meta_headers) -> 'h1', map_build(meta_headers) -> 'h2'
              FROM kafka_source_options_tbl
              WHERE key IN ('key1', 'key2')
            key1 v1 <null>
            key2 <null> v2
            """))
