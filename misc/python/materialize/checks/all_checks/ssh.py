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


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


@externally_idempotent(False)
class SshPg(Check):
    """
    Testing Postgres CDC source with SSH tunnel
    """

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE SECRET pgpass AS 'postgres'

                > CREATE CONNECTION pg_ssh1 TO POSTGRES (
                  HOST postgres,
                  DATABASE postgres,
                  USER postgres,
                  PASSWORD SECRET pgpass,
                  SSL MODE require,
                  SSH TUNNEL ssh_tunnel_0);

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                ALTER USER postgres WITH replication;
                CREATE TABLE t_ssh1 (f1 INTEGER);
                ALTER TABLE t_ssh1 REPLICA IDENTITY FULL;
                CREATE TABLE t_ssh2 (f1 INTEGER);
                ALTER TABLE t_ssh2 REPLICA IDENTITY FULL;
                CREATE TABLE t_ssh3 (f1 INTEGER);
                ALTER TABLE t_ssh3 REPLICA IDENTITY FULL;
                CREATE PUBLICATION mz_source_ssh FOR ALL TABLES;
                INSERT INTO t_ssh1 VALUES (1), (2), (3), (4), (5);

                > CREATE SOURCE mz_source_ssh1
                  FROM POSTGRES CONNECTION pg_ssh1
                  (PUBLICATION 'mz_source_ssh')
                > CREATE TABLE t_ssh1 FROM SOURCE mz_source_ssh1 (REFERENCE t_ssh1);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                > CREATE CONNECTION pg_ssh2 TO POSTGRES (
                  HOST postgres,
                  DATABASE postgres,
                  USER postgres,
                  PASSWORD SECRET pgpass,
                  SSL MODE require,
                  SSH TUNNEL ssh_tunnel_0);

                > CREATE SOURCE mz_source_ssh2
                  FROM POSTGRES CONNECTION pg_ssh2
                  (PUBLICATION 'mz_source_ssh');
                > CREATE TABLE t_ssh2 FROM SOURCE mz_source_ssh2 (REFERENCE t_ssh2);

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO t_ssh1 VALUES (6), (7), (8), (9), (10);

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO t_ssh2 VALUES (6), (7), (8), (9), (10);
                """,
                """
                > CREATE CONNECTION pg_ssh3 TO POSTGRES (
                  HOST postgres,
                  DATABASE postgres,
                  USER postgres,
                  PASSWORD SECRET pgpass,
                  SSL MODE require,
                  SSH TUNNEL ssh_tunnel_0);

                > CREATE SOURCE mz_source_ssh3
                  FROM POSTGRES CONNECTION pg_ssh3
                  (PUBLICATION 'mz_source_ssh');
                > CREATE TABLE t_ssh3 FROM SOURCE mz_source_ssh3 (REFERENCE t_ssh3);

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO t_ssh1 VALUES (11), (12), (13), (14), (15);

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO t_ssh2 VALUES (11), (12), (13), (14), (15);

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO t_ssh3 VALUES (11), (12), (13), (14), (15);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*) FROM t_ssh1;
                15

                > SELECT COUNT(*) FROM t_ssh2;
                10

                > SELECT COUNT(*) FROM t_ssh3;
                5
           """
            )
        )


@externally_idempotent(False)
class SshKafka(Check):
    """
    Testing Kafka source with SSH tunnel
    """

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=ssh1

                $ kafka-create-topic topic=ssh2

                $ kafka-create-topic topic=ssh3

                $ kafka-ingest topic=ssh1 format=bytes
                one

                > CREATE CONNECTION kafka_conn_ssh1
                  TO KAFKA (BROKER '${testdrive.kafka-addr}' USING SSH TUNNEL ssh_tunnel_0, SECURITY PROTOCOL PLAINTEXT);

                > CREATE SOURCE ssh1_src
                  FROM KAFKA CONNECTION kafka_conn_ssh1 (TOPIC 'testdrive-ssh1-${testdrive.seed}');
                > CREATE TABLE ssh1 FROM SOURCE ssh1_src (REFERENCE "testdrive-ssh1-${testdrive.seed}")
                  FORMAT TEXT
                  ENVELOPE NONE;
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                > CREATE CONNECTION kafka_conn_ssh2
                  TO KAFKA (BROKER '${testdrive.kafka-addr}' USING SSH TUNNEL ssh_tunnel_0, SECURITY PROTOCOL PLAINTEXT);

                > CREATE SOURCE ssh2_src
                  FROM KAFKA CONNECTION kafka_conn_ssh2 (TOPIC 'testdrive-ssh2-${testdrive.seed}');
                > CREATE TABLE ssh2 FROM SOURCE ssh2_src (REFERENCE "testdrive-ssh2-${testdrive.seed}")
                  FORMAT TEXT
                  ENVELOPE NONE;

                $ kafka-ingest topic=ssh1 format=bytes
                two

                $ kafka-ingest topic=ssh2 format=bytes
                two
                """,
                """
                > CREATE CONNECTION kafka_conn_ssh3
                  TO KAFKA (BROKER '${testdrive.kafka-addr}' USING SSH TUNNEL ssh_tunnel_0, SECURITY PROTOCOL PLAINTEXT);

                > CREATE SOURCE ssh3_src
                  FROM KAFKA CONNECTION kafka_conn_ssh3 (TOPIC 'testdrive-ssh3-${testdrive.seed}');
                > CREATE TABLE ssh3 FROM SOURCE ssh3_src (REFERENCE "testdrive-ssh3-${testdrive.seed}")
                  FORMAT TEXT
                  ENVELOPE NONE;

                $ kafka-ingest topic=ssh1 format=bytes
                three

                $ kafka-ingest topic=ssh2 format=bytes
                three

                $ kafka-ingest topic=ssh3 format=bytes
                three
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM ssh1;
                one
                two
                three

                > SELECT * FROM ssh2;
                two
                three

                > SELECT * FROM ssh3;
                three
           """
            )
        )
