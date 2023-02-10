# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import Any, List

from pg8000.converters import literal  # type: ignore

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


def dq(ident: Any) -> str:
    ident = ident.replace('"', '""')
    return f'"{ident}"'


def sq(ident: Any) -> Any:
    return literal(ident)


def schemas() -> str:
    return dedent(
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
               {"name":"f1", "type":"string"}
           ]
         }
       """
    )


class Identifiers(Check):
    # Identifiers taken from https://github.com/minimaxir/big-list-of-naughty-strings
    # Under MIT license, Copyright (c) 2015-2020 Max Woolf
    IDENTS = [
        "1.00",
        "\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f",
        "찦차를 타고 온 펲시맨과 쑛다리 똠방각하",
        "❤️ 💔 💌 💕 💞 💓 💗 💖 💘 💝 💟 💜 💛 💚 💙",
        "ثم نفس سقطت وبالتحديد،, جزيرتي باستخدام أن دنو. إذ هنا؟ الستار وتنصيب كان. أهّل ايطاليا، بريطانيا-فرنسا قد أخذ. سليمان، إتفاقية بين ما, يذكر الحدود أي بعد, معاملة بولندا، الإطلاق عل إيو.",
        "בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ",
        "הָיְתָהtestالصفحات التّحول",
        "﷽",
    ]

    def initialize(self) -> Testdrive:
        cmds = "\n".join(
            [
                f"""
            > CREATE DATABASE {dq(ident + "_db")};
            > SET DATABASE={dq(ident + "_db")};
            > CREATE SCHEMA {dq(ident + "_schema")};
            > CREATE TYPE {dq(ident + "_type")} AS LIST (ELEMENT TYPE = text);
            > CREATE TABLE {dq(ident + "_schema")}.{dq(ident + "_table")} ({dq(ident)} TEXT, c2 {dq(ident + "_type")});
            > INSERT INTO {dq(ident + "_schema")}.{dq(ident + "_table")} VALUES ({sq(ident)}, LIST[{sq(ident)}]::{dq(ident + "_type")});
            > CREATE MATERIALIZED VIEW {dq(ident + "_schema")}.{dq(ident + "_mv0")} AS SELECT COUNT({dq(ident)}) FROM {dq(ident + "_schema")}.{dq(ident + "_table")};

            $ kafka-create-topic topic=sink-source

            $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${{keyschema}} schema=${{schema}} repeat=1000
            {{"key1": "U2${{kafka-ingest.iteration}}"}} {{"f1": "A${{kafka-ingest.iteration}}"}}

            > CREATE CONNECTION IF NOT EXISTS {dq(ident + "_kafka_conn")} FOR KAFKA BROKER '${{testdrive.kafka-addr}}';
            > CREATE CONNECTION IF NOT EXISTS {dq(ident + "_csr_conn")} FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';
            > CREATE SOURCE {dq(ident + "_source")}
              FROM KAFKA CONNECTION {dq(ident + "_kafka_conn")} (TOPIC 'testdrive-sink-source-${{testdrive.seed}}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {dq(ident + "_csr_conn")}
              ENVELOPE UPSERT;
            > CREATE MATERIALIZED VIEW {dq(ident + "_source_view")} AS SELECT LEFT(key1, 2) as l_k, LEFT(f1, 1) AS l_v, COUNT(*) AS c FROM {dq(ident + "_source")} GROUP BY LEFT(key1, 2), LEFT(f1, 1);
            > CREATE SINK {dq(ident + "_schema")}.{dq(ident + "_sink0")} FROM {dq(ident + "_source_view")}
              INTO KAFKA CONNECTION {dq(ident + "_kafka_conn")} (TOPIC 'sink-sink0')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {dq(ident + "_csr_conn")}
              ENVELOPE DEBEZIUM;

            > CREATE SECRET {dq(ident + "_secret")} as {sq(ident)};
            """
                for ident in self.IDENTS
            ]
        )
        return Testdrive(schemas() + dedent(cmds))

    def manipulate(self) -> List[Testdrive]:
        cmds = [
            "\n".join(
                [
                    f"""
            > SET DATABASE={dq(ident + "_db")};
            > CREATE MATERIALIZED VIEW {dq(ident + "_schema")}.{dq(ident + "_mv" + i)} AS SELECT {dq(ident)}, c2 as {dq(ident + "_alias")} FROM {dq(ident + "_schema")}.{dq(ident + "_table")};
            > INSERT INTO {dq(ident + "_schema")}.{dq(ident + "_table")} VALUES ({sq(ident)}, LIST[{sq(ident)}]::{dq(ident + "_type")});
            > CREATE SINK {dq(ident + "_schema")}.{dq(ident + "_sink" + i)} FROM {dq(ident + "_source_view")}
              INTO KAFKA CONNECTION {dq(ident + "_kafka_conn")} (TOPIC 'sink-sink{i}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {dq(ident + "_csr_conn")}
              ENVELOPE DEBEZIUM;
            """
                    for ident in self.IDENTS
                ]
            )
            for i in ["1", "2"]
        ]
        return [Testdrive(dedent(s)) for s in cmds]

    def validate(self) -> Testdrive:
        cmds = "> SHOW DATABASES LIKE '%_db';\n"
        cmds += "\n".join([dq(ident + "_db") for ident in self.IDENTS])
        for ident in self.IDENTS:
            cmds += f"""
> SET DATABASE={dq(ident + "_db")};

> SHOW TYPES;
{dq(ident + "_type")}

> SHOW SCHEMAS FROM {dq(ident + "_db")};
public
information_schema
mz_catalog
mz_internal
pg_catalog
{dq(ident + "_schema")}

> SHOW SINKS FROM {dq(ident + "_schema")};
{dq(ident + "_sink0")} kafka 4
{dq(ident + "_sink1")} kafka 4
{dq(ident + "_sink2")} kafka 4

> SELECT * FROM {dq(ident + "_schema")}.{dq(ident + "_mv0")};
3

> SELECT {dq(ident)}, {dq(ident + "_alias")}[1] FROM {dq(ident + "_schema")}.{dq(ident + "_mv1")};
{dq(ident)} {dq(ident)}
{dq(ident)} {dq(ident)}
{dq(ident)} {dq(ident)}

> SELECT {dq(ident)}, {dq(ident + "_alias")}[1] FROM {dq(ident + "_schema")}.{dq(ident + "_mv2")};
{dq(ident)} {dq(ident)}
{dq(ident)} {dq(ident)}
{dq(ident)} {dq(ident)}

> SELECT * FROM {dq(ident + "_source_view")};
U2 A 1000

> SHOW SECRETS;
{dq(ident + "_secret")}
"""
        return Testdrive(dedent(cmds))
