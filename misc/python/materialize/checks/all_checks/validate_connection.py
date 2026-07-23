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


class ValidateConnection(Check):
    """VALIDATE CONNECTION against Kafka, Confluent Schema Registry, and
    Postgres connections (the AWS variant is covered by aws.py)."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE CONNECTION validate_conn_kafka TO KAFKA (
                BROKER '${testdrive.kafka-addr}',
                SECURITY PROTOCOL PLAINTEXT
              )

            > CREATE CONNECTION validate_conn_csr TO CONFLUENT SCHEMA REGISTRY (
                URL '${testdrive.schema-registry-url}'
              )

            > CREATE SECRET validate_conn_pg_pass AS 'postgres'
            > CREATE CONNECTION validate_conn_pg TO POSTGRES (
                HOST 'postgres',
                DATABASE postgres,
                USER postgres,
                PASSWORD SECRET validate_conn_pg_pass
              )
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > VALIDATE CONNECTION validate_conn_kafka
                > VALIDATE CONNECTION validate_conn_pg
                # A connection created WITH (VALIDATE = false) can be validated
                # explicitly later.
                > CREATE CONNECTION validate_conn_kafka2 TO KAFKA (
                    BROKER '${testdrive.kafka-addr}',
                    SECURITY PROTOCOL PLAINTEXT
                  ) WITH (VALIDATE = false)
                > VALIDATE CONNECTION validate_conn_kafka2
                """,
                """
                > VALIDATE CONNECTION validate_conn_csr
                > VALIDATE CONNECTION validate_conn_kafka2
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > VALIDATE CONNECTION validate_conn_kafka
            > VALIDATE CONNECTION validate_conn_kafka2
            > VALIDATE CONNECTION validate_conn_csr
            > VALIDATE CONNECTION validate_conn_pg
            """))
