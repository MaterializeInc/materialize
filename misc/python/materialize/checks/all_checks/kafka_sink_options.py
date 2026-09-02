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
class KafkaSinkOptions(Check):
    """Kafka sink options not covered by kafka_sink.py: COMPRESSION TYPE,
    AVRO KEY/VALUE FULLNAME, PROGRESS GROUP ID PREFIX, TRANSACTIONAL ID
    PREFIX, and the HEADERS option."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_kafka_sink_headers = true

            > CREATE TABLE sink_options_table (key INT, val STRING, hdrs MAP[TEXT => TEXT])
            > INSERT INTO sink_options_table VALUES (1, 'one', '{h1 => v1}')

            > CREATE MATERIALIZED VIEW sink_options_mv AS
              SELECT key, val, hdrs FROM sink_options_table

            > CREATE SINK sink_options_sink1 FROM sink_options_mv
              INTO KAFKA CONNECTION kafka_conn (
                TOPIC 'sink-options-sink1-${testdrive.seed}',
                COMPRESSION TYPE = 'gzip',
                PROGRESS GROUP ID PREFIX 'sink-options-progress',
                TRANSACTIONAL ID PREFIX 'sink-options-txn'
              )
              KEY (key) NOT ENFORCED
              HEADERS hdrs
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn (
                AVRO KEY FULLNAME 'com.example.SinkOptionsKey',
                AVRO VALUE FULLNAME 'com.example.SinkOptionsValue'
              )
              ENVELOPE UPSERT
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO sink_options_table VALUES (2, 'two', '{h2 => v2}')
                """,
                """
                > INSERT INTO sink_options_table VALUES (3, 'three', '{}')
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            # The sink's generated Avro schemas carry the configured fullnames.
            $ schema-registry-verify schema-type=avro subject=sink-options-sink1-${testdrive.seed}-key
            {"type":"record","name":"SinkOptionsKey","namespace":"com.example","fields":[{"name":"key","type":["null","int"]}]}

            > SELECT status FROM mz_internal.mz_sink_statuses
              WHERE name = 'sink_options_sink1'
            running

            # Round-trip the sunk topic to prove data (compressed with gzip)
            # arrives intact.
            > CREATE SOURCE sink_options_verify_src
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-options-sink1-${testdrive.seed}')
            # Each key is written exactly once above, so ENVELOPE NONE sees
            # exactly one row per key.
            > CREATE TABLE sink_options_verify_tbl
              FROM SOURCE sink_options_verify_src (REFERENCE "sink-options-sink1-${testdrive.seed}")
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              INCLUDE HEADERS
              ENVELOPE NONE

            > SELECT key, val, map_build(headers) -> 'h1', map_build(headers) -> 'h2'
              FROM sink_options_verify_tbl
            1 one v1 <null>
            2 two <null> v2
            3 three <null> <null>

            > DROP SOURCE sink_options_verify_src CASCADE
            """))
