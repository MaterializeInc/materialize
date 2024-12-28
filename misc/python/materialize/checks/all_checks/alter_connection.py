# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from enum import Enum
from random import Random
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, externally_idempotent
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.mz_version import MzVersion


def schema() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


class SshChange(Enum):
    ADD_SSH = 1
    DROP_SSH = 2
    CHANGE_SSH_HOST = 3


# {i} will be replaced later
SHOW_CONNECTION_SSH_TUNNEL = """materialize.public.ssh_tunnel_{i} "CREATE CONNECTION \\"materialize\\".\\"public\\".\\"ssh_tunnel_{i}\\" TO SSH TUNNEL (HOST = 'ssh-bastion-host', PORT = 22, USER = 'mz')" """
WITH_SSH_SUFFIX = "USING SSH TUNNEL ssh_tunnel_{i}"


class AlterConnectionSshChangeBase(Check):
    def __init__(
        self,
        ssh_change: SshChange,
        index: int,
        base_version: MzVersion,
        rng: Random | None,
    ):
        super().__init__(base_version, rng)
        self.ssh_change = ssh_change
        self.index = index

    def initialize(self) -> Testdrive:
        i = self.index

        return Testdrive(
            schema()
            + dedent(
                f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET enable_connection_validation_syntax = true

                $ kafka-create-topic topic=alter-connection-{i}a

                $ kafka-ingest topic=alter-connection-{i}a format=bytes
                one

                > CREATE CONNECTION kafka_conn_alter_connection_{i}a
                  TO KAFKA (SECURITY PROTOCOL = "plaintext", BROKER '${{testdrive.kafka-addr}}' {WITH_SSH_SUFFIX.replace('{i}', str(i)) if self.ssh_change in {SshChange.DROP_SSH, SshChange.CHANGE_SSH_HOST} else ''});

                > CREATE SOURCE alter_connection_source_{i}a_src
                  FROM KAFKA CONNECTION kafka_conn_alter_connection_{i}a (TOPIC 'testdrive-alter-connection-{i}a-${{testdrive.seed}}');
                > CREATE TABLE alter_connection_source_{i}a FROM SOURCE alter_connection_source_{i}a_src (REFERENCE "testdrive-alter-connection-{i}a-${{testdrive.seed}}")
                  FORMAT TEXT
                  ENVELOPE NONE;

                > SELECT count(regexp_match(create_sql, 'USING SSH TUNNEL')) > 0 FROM (SHOW CREATE CONNECTION kafka_conn_alter_connection_{i}a);
                {'false' if self.ssh_change == SshChange.ADD_SSH else 'true'}

                > SELECT count(regexp_match(create_sql, 'ssh-bastion-host')) > 0 FROM (SHOW CREATE CONNECTION ssh_tunnel_{i});
                true

                > CREATE TABLE alter_connection_table_{i} (f1 INTEGER, PRIMARY KEY (f1));
                > INSERT INTO alter_connection_table_{i} VALUES (1);

                > CREATE MATERIALIZED VIEW mv_alter_connection_{i} AS SELECT f1 FROM alter_connection_table_{i};

                > CREATE SINK alter_connection_sink_{i} FROM mv_alter_connection_{i}
                  INTO KAFKA CONNECTION kafka_conn_alter_connection_{i}a (TOPIC 'testdrive-alter-connection-sink-{i}-${{testdrive.seed}}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM;

                $ kafka-verify-topic sink=materialize.public.alter_connection_sink_{i} await-value-schema=true
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        i = self.index

        return [
            Testdrive(schema() + dedent(s))
            for s in [
                f"""
                $ kafka-ingest topic=alter-connection-{i}a format=bytes
                two

                > ALTER CONNECTION kafka_conn_alter_connection_{i}a SET (BROKER '${{testdrive.kafka-addr}}' {WITH_SSH_SUFFIX.replace('{i}', str(i)) if self.ssh_change in {SshChange.ADD_SSH, SshChange.CHANGE_SSH_HOST} else ''});

                $ kafka-ingest topic=alter-connection-{i}a format=bytes
                three

                $ kafka-create-topic topic=alter-connection-{i}b

                $ kafka-ingest topic=alter-connection-{i}b format=bytes
                ten

                > CREATE CONNECTION kafka_conn_alter_connection_{i}b
                  TO KAFKA (SECURITY PROTOCOL = "plaintext", BROKER '${{testdrive.kafka-addr}}' {WITH_SSH_SUFFIX.replace('{i}', str(i)) if self.ssh_change in {SshChange.DROP_SSH, SshChange.CHANGE_SSH_HOST} else ''});

                > CREATE SOURCE alter_connection_source_{i}b_src
                  FROM KAFKA CONNECTION kafka_conn_alter_connection_{i}b (TOPIC 'testdrive-alter-connection-{i}b-${{testdrive.seed}}');
                > CREATE TABLE alter_connection_source_{i}b FROM SOURCE alter_connection_source_{i}b_src (REFERENCE "testdrive-alter-connection-{i}b-${{testdrive.seed}}")
                  FORMAT TEXT
                  ENVELOPE NONE;

                $ kafka-ingest topic=alter-connection-{i}b format=bytes
                twenty

                {f"> ALTER CONNECTION ssh_tunnel_{i} SET (HOST = 'other_ssh_bastion') WITH (VALIDATE = true);" if self.ssh_change == SshChange.CHANGE_SSH_HOST else "$ nop"}

                > INSERT INTO alter_connection_table_{i} VALUES (2);
                """,
                f"""
                $ kafka-ingest topic=alter-connection-{i}a format=bytes
                four

                $ kafka-ingest topic=alter-connection-{i}b format=bytes
                thirty

                > ALTER CONNECTION kafka_conn_alter_connection_{i}b SET (BROKER '${{testdrive.kafka-addr}}' {WITH_SSH_SUFFIX.replace('{i}', str(i)) if self.ssh_change in {SshChange.ADD_SSH, SshChange.CHANGE_SSH_HOST} else ''});

                $ kafka-ingest topic=alter-connection-{i}b format=bytes
                fourty

                > INSERT INTO alter_connection_table_{i} VALUES (3);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        i = self.index

        return Testdrive(
            dedent(
                f"""
                > SELECT regexp_match(create_sql, '(ssh-bastion-host|other_ssh_bastion)') FROM (SHOW CREATE CONNECTION ssh_tunnel_{i});
                {"{other_ssh_bastion}" if self.ssh_change == SshChange.CHANGE_SSH_HOST else "{ssh-bastion-host}"}

                > SELECT count(regexp_match(create_sql, 'USING SSH TUNNEL')) > 0 FROM (SHOW CREATE CONNECTION kafka_conn_alter_connection_{i}a);
                {'true' if self.ssh_change in {SshChange.ADD_SSH, SshChange.CHANGE_SSH_HOST} else 'false'}

                > SELECT * FROM alter_connection_source_{i}a;
                one
                two
                three
                four

                > SELECT * FROM alter_connection_source_{i}b;
                ten
                twenty
                thirty
                fourty

                > CREATE SOURCE alter_connection_sink_source_{i}_src
                  FROM KAFKA CONNECTION kafka_conn_alter_connection_{i}a (TOPIC 'testdrive-alter-connection-sink-{i}-${{testdrive.seed}}');
                > CREATE TABLE alter_connection_sink_source_{i} FROM SOURCE alter_connection_sink_source_{i}_src (REFERENCE "testdrive-alter-connection-sink-{i}-${{testdrive.seed}}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE;

                # Table has expected data
                > SELECT * FROM alter_connection_table_{i};
                1
                2
                3

                # TODO: Reenable this check when kafka-verify-data can deal with validate being run twice
                # Sink Kafka topic has expected data
                # $ kafka-verify-data format=avro sink=materialize.public.alter_connection_sink_{i} sort-messages=true
                # {{"before": null, "after": {{"row":{{"f1": 1}}}}}}
                # {{"before": null, "after": {{"row":{{"f1": 2}}}}}}
                # {{"before": null, "after": {{"row":{{"f1": 3}}}}}}

                # Source based on sink topic has ingested data; data must be text-formatted because records are not
                # supported in testdrive
                > SELECT before::text, after::text FROM alter_connection_sink_source_{i};
                <null> (1)
                <null> (2)
                <null> (3)

                > DROP SOURCE IF EXISTS alter_connection_sink_source_{i}_src CASCADE
                """
            )
        )


@externally_idempotent(False)
class AlterConnectionToSsh(AlterConnectionSshChangeBase):
    def __init__(self, base_version: MzVersion, rng: Random | None):
        super().__init__(SshChange.ADD_SSH, 1, base_version, rng)


@externally_idempotent(False)
class AlterConnectionToNonSsh(AlterConnectionSshChangeBase):
    def __init__(self, base_version: MzVersion, rng: Random | None):
        super().__init__(SshChange.DROP_SSH, 2, base_version, rng)


@externally_idempotent(False)
class AlterConnectionHost(AlterConnectionSshChangeBase):
    def __init__(self, base_version: MzVersion, rng: Random | None):
        super().__init__(SshChange.CHANGE_SSH_HOST, 3, base_version, rng)
