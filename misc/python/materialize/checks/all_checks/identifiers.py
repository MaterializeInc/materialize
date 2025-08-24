# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from random import Random
from textwrap import dedent
from typing import Any

from pg8000.converters import literal  # type: ignore

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.mz_version import MzVersion
from materialize.util import naughty_strings


def dq(ident: str) -> str:
    ident = ident.replace('"', '""')
    return f'"{ident}"'


def dq_print(ident: str) -> str:
    ident = ident.replace("\\", "\\\\")
    ident = ident.replace('"', '\\"')
    return f'"{ident}"'


def sq(ident: str) -> Any:
    return literal(ident)


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


def cluster() -> str:
    return "> CREATE CLUSTER identifiers SIZE 'scale=1,workers=4'\n"


class Identifiers(Check):
    IDENT_KEYS = [
        "db",
        "schema",
        "type",
        "table",
        "column",
        "value1",
        "value2",
        # "source",
        "source_view",
        "kafka_conn",
        "csr_conn",
        "secret",
        # "secret_value",
        "mv0",
        "mv1",
        "mv2",
        "sink0",
        "sink1",
        "sink2",
        "alias",
        "role",
        "comment_table",
        "comment_column",
    ]

    def __init__(self, base_version: MzVersion, rng: Random | None) -> None:
        strings = naughty_strings()
        values = (rng or Random(0)).sample(strings, len(self.IDENT_KEYS))
        self.ident = {
            key: value.encode()[:255].decode("utf-8", "ignore")
            for key, value in zip(self.IDENT_KEYS, values)
        }
        # ERROR: invalid input syntax for type bytea: invalid escape sequence
        self.ident["secret_value"] = "secret_value"
        # https://github.com/MaterializeInc/database-issues/issues/6813
        self.ident["source"] = "source"
        super().__init__(base_version, rng)

    def initialize(self) -> Testdrive:
        cmds = f"""
            > SET cluster=identifiers;
            > CREATE ROLE {dq(self.ident["role"])};
            > CREATE DATABASE {dq(self.ident["db"])};
            > SET DATABASE={dq(self.ident["db"])};
            > CREATE SCHEMA {dq(self.ident["schema"])};
            > CREATE TYPE {dq(self.ident["type"])} AS LIST (ELEMENT TYPE = text);
            > CREATE TABLE {dq(self.ident["schema"])}.{dq(self.ident["table"])} ({dq(self.ident["column"])} TEXT, c2 {dq(self.ident["type"])});
            > INSERT INTO {dq(self.ident["schema"])}.{dq(self.ident["table"])} VALUES ({sq(self.ident["value1"])}, LIST[{sq(self.ident["value2"])}]::{dq(self.ident["type"])});
            > CREATE MATERIALIZED VIEW {dq(self.ident["schema"])}.{dq(self.ident["mv0"])} IN CLUSTER {self._default_cluster()} AS
              SELECT COUNT({dq(self.ident["column"])}) FROM {dq(self.ident["schema"])}.{dq(self.ident["table"])};

            $ kafka-create-topic topic=sink-source-ident

            $ kafka-ingest format=avro key-format=avro topic=sink-source-ident key-schema=${{keyschema}} schema=${{schema}} repeat=1000
            {{"key1": "U2${{kafka-ingest.iteration}}"}} {{"f1": "A${{kafka-ingest.iteration}}"}}

            > CREATE CONNECTION IF NOT EXISTS {dq(self.ident["kafka_conn"])} FOR KAFKA {self._kafka_broker()};
            > CREATE CONNECTION IF NOT EXISTS {dq(self.ident["csr_conn"])} FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';

            > CREATE SOURCE {dq(self.ident["source"] + "_src")}
              IN CLUSTER identifiers
              FROM KAFKA CONNECTION {dq(self.ident["kafka_conn"])} (TOPIC 'testdrive-sink-source-ident-${{testdrive.seed}}');
            > CREATE TABLE {dq(self.ident["source"])} FROM SOURCE {dq(self.ident["source"] + "_src")} (REFERENCE "testdrive-sink-source-ident-${{testdrive.seed}}")
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {dq(self.ident["csr_conn"])}
              ENVELOPE UPSERT;

            > CREATE MATERIALIZED VIEW {dq(self.ident["source_view"])} IN CLUSTER {self._default_cluster()} AS
              SELECT LEFT(key1, 2) as l_k, LEFT(f1, 1) AS l_v, COUNT(*) AS c FROM {dq(self.ident["source"])} GROUP BY LEFT(key1, 2), LEFT(f1, 1);
            > CREATE SINK {dq(self.ident["schema"])}.{dq(self.ident["sink0"])}
              IN CLUSTER identifiers
              FROM {dq(self.ident["source_view"])}
              INTO KAFKA CONNECTION {dq(self.ident["kafka_conn"])} (TOPIC 'sink-sink-ident0')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {dq(self.ident["csr_conn"])}
              ENVELOPE DEBEZIUM;

            > CREATE SECRET {dq(self.ident["secret"])} as {sq(self.ident["secret_value"])};

            > COMMENT ON TABLE {dq(self.ident["schema"])}.{dq(self.ident["table"])} IS {sq(self.ident["comment_table"])};

            > COMMENT ON COLUMN {dq(self.ident["schema"])}.{dq(self.ident["table"])}.{dq(self.ident["column"])} IS {sq(self.ident["comment_column"])};
            """

        return Testdrive(schemas() + cluster() + dedent(cmds))

    def manipulate(self) -> list[Testdrive]:
        cmds = [
            f"""
            > SET CLUSTER=identifiers;
            > SET DATABASE={dq(self.ident["db"])};
            > CREATE MATERIALIZED VIEW {dq(self.ident["schema"])}.{dq(self.ident["mv" + i])} IN CLUSTER {self._default_cluster()} AS
              SELECT {dq(self.ident["column"])}, c2 as {dq(self.ident["alias"])} FROM {dq(self.ident["schema"])}.{dq(self.ident["table"])};
            > INSERT INTO {dq(self.ident["schema"])}.{dq(self.ident["table"])} VALUES ({sq(self.ident["value1"])}, LIST[{sq(self.ident["value2"])}]::{dq(self.ident["type"])});
            > CREATE SINK {dq(self.ident["schema"])}.{dq(self.ident["sink" + i])}
              IN CLUSTER identifiers
              FROM {dq(self.ident["source_view"])}
              INTO KAFKA CONNECTION {dq(self.ident["kafka_conn"])} (TOPIC 'sink-sink-ident')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {dq(self.ident["csr_conn"])}
              ENVELOPE DEBEZIUM;
            """
            for i in ["1", "2"]
        ]
        return [Testdrive(dedent(s)) for s in cmds]

    def validate(self) -> Testdrive:
        cmds = f"""
        > SHOW DATABASES WHERE name NOT LIKE 'to_be_created%' AND name NOT LIKE 'owner_db%' AND name NOT LIKE 'privilege_db%' AND name <> 'defpriv_db';
        materialize ""
        {dq_print(self.ident["db"])} ""

        > SET DATABASE={dq(self.ident["db"])};

        > SELECT name FROM mz_roles WHERE name = {sq(self.ident["role"])}
        {dq_print(self.ident["role"])}

        > SHOW TYPES;
        {dq_print(self.ident["type"])} ""

        > SHOW SCHEMAS FROM {dq(self.ident["db"])};
        public ""
        information_schema ""
        mz_catalog ""
        mz_catalog_unstable ""
        mz_unsafe ""
        mz_internal ""
        mz_introspection ""
        pg_catalog ""
        {dq_print(self.ident["schema"])} ""

        > SHOW SINKS FROM {dq(self.ident["schema"])};
        {dq_print(self.ident["sink0"])} kafka identifiers ""
        {dq_print(self.ident["sink1"])} kafka identifiers ""
        {dq_print(self.ident["sink2"])} kafka identifiers ""

        > SELECT * FROM {dq(self.ident["schema"])}.{dq(self.ident["mv0"])};
        3

        > SELECT {dq(self.ident["column"])}, {dq(self.ident["alias"])}[1] FROM {dq(self.ident["schema"])}.{dq(self.ident["mv1"])};
        {dq_print(self.ident["value1"])} {dq_print(self.ident["value2"])}
        {dq_print(self.ident["value1"])} {dq_print(self.ident["value2"])}
        {dq_print(self.ident["value1"])} {dq_print(self.ident["value2"])}

        > SELECT {dq(self.ident["column"])}, {dq(self.ident["alias"])}[1] FROM {dq(self.ident["schema"])}.{dq(self.ident["mv2"])};
        {dq_print(self.ident["value1"])} {dq_print(self.ident["value2"])}
        {dq_print(self.ident["value1"])} {dq_print(self.ident["value2"])}
        {dq_print(self.ident["value1"])} {dq_print(self.ident["value2"])}

        > SELECT * FROM {dq(self.ident["source_view"])};
        U2 A 1000

        > SELECT object_sub_id, comment FROM mz_internal.mz_comments JOIN mz_tables ON mz_internal.mz_comments.id = mz_tables.id WHERE name = {sq(self.ident["table"])};
        <null> {dq_print(self.ident["comment_table"])}
        1 {dq_print(self.ident["comment_column"])}

        > SHOW SECRETS;
        {dq_print(self.ident["secret"])} ""
        """
        return Testdrive(dedent(cmds))
