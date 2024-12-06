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
class KafkaProtocols(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-from-file ca-crt=/share/secrets/ca.crt
                $ set-from-file kafka-crt=/share/secrets/materialized-kafka.crt
                $ set-from-file kafka-key=/share/secrets/materialized-kafka.key
                $ set-from-file kafka1-crt=/share/secrets/materialized-kafka1.crt
                $ set-from-file kafka1-key=/share/secrets/materialized-kafka1.key

                > CREATE SECRET kafka_key AS '${kafka-key}'
                > CREATE SECRET kafka1_key AS '${kafka1-key}'
                > CREATE SECRET garbage_key AS 'garbage'

                $ kafka-create-topic topic=kafka-protocols-1
                $ kafka-ingest topic=kafka-protocols-1 format=bytes
                one

                $ kafka-create-topic topic=kafka-protocols-2
                $ kafka-ingest topic=kafka-protocols-2 format=bytes
                one

                $ kafka-create-topic topic=kafka-protocols-3
                $ kafka-ingest topic=kafka-protocols-3 format=bytes
                one

                > CREATE SECRET kafka_protocols_password AS 'sekurity'

                > CREATE CONNECTION kafka_plaintext TO KAFKA (
                    BROKER 'kafka:9093' USING SSH TUNNEL ssh_tunnel_0,
                    SSL CERTIFICATE AUTHORITY '${ca-crt}'
                  )

                > CREATE CONNECTION kafka_ssl TO KAFKA (
                    BROKER 'kafka:9094',
                    SSL CERTIFICATE '${kafka-crt}',
                    SSL KEY SECRET kafka_key,
                    SSL CERTIFICATE AUTHORITY '${ca-crt}'
                  )

                > CREATE CONNECTION kafka_scram_sha_512 TO KAFKA (
                    BROKER 'kafka:9095' USING SSH TUNNEL ssh_tunnel_0,
                    SASL MECHANISMS 'SCRAM-SHA-512',
                    SASL USERNAME 'materialize',
                    SASL PASSWORD SECRET kafka_protocols_password,
                    SECURITY PROTOCOL SASL_PLAINTEXT
                  )

                > CREATE CONNECTION kafka_ssl_scram_sha_512 TO KAFKA (
                    BROKER 'kafka:9096',
                    SASL MECHANISMS 'SCRAM-SHA-512',
                    SASL USERNAME 'materialize',
                    SASL PASSWORD SECRET kafka_protocols_password,
                    SSL CERTIFICATE AUTHORITY '${ca-crt}'
                  )

                > CREATE CONNECTION kafka_sasl TO KAFKA (
                    BROKER 'kafka:9097' USING SSH TUNNEL ssh_tunnel_0,
                    SASL MECHANISMS 'PLAIN',
                    SASL USERNAME 'materialize',
                    SASL PASSWORD SECRET kafka_protocols_password,
                    SSL CERTIFICATE '${kafka-crt}',
                    SSL KEY SECRET kafka_key,
                    SSL CERTIFICATE AUTHORITY '${ca-crt}'
                  )

                > CREATE SOURCE kafka_plaintext_1_src FROM KAFKA CONNECTION kafka_plaintext (
                    TOPIC 'testdrive-kafka-protocols-1-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_plaintext_1 FROM SOURCE kafka_plaintext_1_src (REFERENCE "testdrive-kafka-protocols-1-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_ssl_1_src FROM KAFKA CONNECTION kafka_ssl (
                    TOPIC 'testdrive-kafka-protocols-1-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_ssl_1 FROM SOURCE kafka_ssl_1_src (REFERENCE "testdrive-kafka-protocols-1-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_scram_sha_512_1_src FROM KAFKA CONNECTION kafka_scram_sha_512 (
                    TOPIC 'testdrive-kafka-protocols-1-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_scram_sha_512_1 FROM SOURCE kafka_scram_sha_512_1_src (REFERENCE "testdrive-kafka-protocols-1-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_ssl_scram_sha_512_1_src FROM KAFKA CONNECTION kafka_ssl_scram_sha_512 (
                    TOPIC 'testdrive-kafka-protocols-1-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_ssl_scram_sha_512_1 FROM SOURCE kafka_ssl_scram_sha_512_1_src (REFERENCE "testdrive-kafka-protocols-1-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_sasl_1_src FROM KAFKA CONNECTION kafka_sasl (
                    TOPIC 'testdrive-kafka-protocols-1-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_sasl_1 FROM SOURCE kafka_sasl_1_src (REFERENCE "testdrive-kafka-protocols-1-${testdrive.seed}")
                  FORMAT TEXT
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE SOURCE kafka_plaintext_2_src FROM KAFKA CONNECTION kafka_plaintext (
                    TOPIC 'testdrive-kafka-protocols-2-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_plaintext_2 FROM SOURCE kafka_plaintext_2_src (REFERENCE "testdrive-kafka-protocols-2-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_ssl_2_src FROM KAFKA CONNECTION kafka_ssl (
                    TOPIC 'testdrive-kafka-protocols-2-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_ssl_2 FROM SOURCE kafka_ssl_2_src (REFERENCE "testdrive-kafka-protocols-2-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_scram_sha_512_2_src FROM KAFKA CONNECTION kafka_scram_sha_512 (
                    TOPIC 'testdrive-kafka-protocols-2-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_scram_sha_512_2 FROM SOURCE kafka_scram_sha_512_2_src (REFERENCE "testdrive-kafka-protocols-2-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_ssl_scram_sha_512_2_src FROM KAFKA CONNECTION kafka_ssl_scram_sha_512 (
                    TOPIC 'testdrive-kafka-protocols-2-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_ssl_scram_sha_512_2 FROM SOURCE kafka_ssl_scram_sha_512_2_src (REFERENCE "testdrive-kafka-protocols-2-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_sasl_2_src FROM KAFKA CONNECTION kafka_sasl (
                    TOPIC 'testdrive-kafka-protocols-2-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_sasl_2 FROM SOURCE kafka_sasl_2_src (REFERENCE "testdrive-kafka-protocols-2-${testdrive.seed}")
                  FORMAT TEXT

                $ kafka-ingest topic=kafka-protocols-1 format=bytes
                two

                $ kafka-ingest topic=kafka-protocols-2 format=bytes
                two

                $ kafka-ingest topic=kafka-protocols-3 format=bytes
                two
                """,
                """
                > CREATE SOURCE kafka_plaintext_3_src FROM KAFKA CONNECTION kafka_plaintext (
                    TOPIC 'testdrive-kafka-protocols-3-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_plaintext_3 FROM SOURCE kafka_plaintext_3_src (REFERENCE "testdrive-kafka-protocols-3-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_ssl_3_src FROM KAFKA CONNECTION kafka_ssl (
                    TOPIC 'testdrive-kafka-protocols-3-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_ssl_3 FROM SOURCE kafka_ssl_3_src (REFERENCE "testdrive-kafka-protocols-3-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_scram_sha_512_3_src FROM KAFKA CONNECTION kafka_scram_sha_512 (
                    TOPIC 'testdrive-kafka-protocols-3-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_scram_sha_512_3 FROM SOURCE kafka_scram_sha_512_3_src (REFERENCE "testdrive-kafka-protocols-3-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_ssl_scram_sha_512_3_src FROM KAFKA CONNECTION kafka_ssl_scram_sha_512 (
                    TOPIC 'testdrive-kafka-protocols-3-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_ssl_scram_sha_512_3 FROM SOURCE kafka_ssl_scram_sha_512_3_src (REFERENCE "testdrive-kafka-protocols-3-${testdrive.seed}")
                  FORMAT TEXT

                > CREATE SOURCE kafka_sasl_3_src FROM KAFKA CONNECTION kafka_sasl (
                    TOPIC 'testdrive-kafka-protocols-3-${testdrive.seed}'
                  )
                > CREATE TABLE kafka_sasl_3 FROM SOURCE kafka_sasl_3_src (REFERENCE "testdrive-kafka-protocols-3-${testdrive.seed}")
                  FORMAT TEXT

                $ kafka-ingest topic=kafka-protocols-1 format=bytes
                three

                $ kafka-ingest topic=kafka-protocols-2 format=bytes
                three

                $ kafka-ingest topic=kafka-protocols-3 format=bytes
                three
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM kafka_plaintext_1
                one
                two
                three

                > SELECT * FROM kafka_ssl_1
                one
                two
                three

                > SELECT * FROM kafka_scram_sha_512_1
                one
                two
                three

                > SELECT * FROM kafka_ssl_scram_sha_512_1
                one
                two
                three

                > SELECT * FROM kafka_sasl_1
                one
                two
                three

                > SELECT * FROM kafka_plaintext_2
                one
                two
                three

                > SELECT * FROM kafka_ssl_2
                one
                two
                three

                > SELECT * FROM kafka_scram_sha_512_2
                one
                two
                three

                > SELECT * FROM kafka_ssl_scram_sha_512_2
                one
                two
                three

                > SELECT * FROM kafka_sasl_2
                one
                two
                three

                > SELECT * FROM kafka_plaintext_3
                one
                two
                three

                > SELECT * FROM kafka_ssl_3
                one
                two
                three

                > SELECT * FROM kafka_scram_sha_512_3
                one
                two
                three

                > SELECT * FROM kafka_ssl_scram_sha_512_3
                one
                two
                three

                > SELECT * FROM kafka_sasl_3
                one
                two
                three
                """
            )
        )
