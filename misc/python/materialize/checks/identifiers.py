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
from materialize.checks.mz_version import MzVersion


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
    # Some identifiers taken from https://github.com/minimaxir/big-list-of-naughty-strings
    # Under MIT license, Copyright (c) 2015-2020 Max Woolf
    IDENTS = [
        {
            "db": "-1",
            "schema": "0",
            "type": "1",
            "table": "2",
            "column": "3",
            "value1": "4",
            "value2": "5",
            "source": "6",
            "source_view": "7",
            "kafka_conn": "8",
            "csr_conn": "9",
            "secret": "10",
            "secret_value": "11",
            "mv0": "12",
            "mv1": "13",
            "mv2": "14",
            "sink0": "15",
            "sink1": "16",
            "sink2": "17",
            "alias": "18",
        },
        # Disabled because of timeouts, revisit if we want to accept longer runtime
        # {
        #     "db": "-1.0",
        #     "schema": "0.0",
        #     "type": "1.0",
        #     "table": "2.0",
        #     "column": "3.0",
        #     "value1": "4.0",
        #     "value2": "5.0",
        #     "source": "6.0",
        #     "source_view": "7.0",
        #     "kafka_conn": "8.0",
        #     "csr_conn": "9.0",
        #     "secret": "10.0",
        #     "secret_value": "11.0",
        #     "mv0": "12.0",
        #     "mv1": "13.0",
        #     "mv2": "14.0",
        #     "sink0": "15.0",
        #     "sink1": "16.0",
        #     "sink2": "17.0",
        #     "alias": "18.0",
        # },
        {
            "db": "\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f^?",
            "schema": ",./;'[]\\-=",
            "type": '<>?:"{}|_+',
            "table": '<>?:"{}|_+',
            "column": "!@#$%^&*()`~",
            "value1": "",
            "value2": "\t\u000b\f              ​    　",
            "source": "Ω≈ç√∫˜µ≤≥÷",
            "source_view": "œ∑´®†¥¨ˆøπ“‘",
            "kafka_conn": "¡™£¢∞§¶•ªº–≠",
            "csr_conn": "¸˛Ç◊ı˜Â¯˘¿",
            "secret": "ÅÍÎÏ˝ÓÔÒÚÆ☃",
            "secret_value": "Œ„´‰ˇÁ¨ˆØ∏”’",
            "mv0": "┬─┬ノ( º _ ºノ)",
            "mv1": "( ͡° ͜ʖ ͡°)",
            "mv2": "¯\\_(ツ)_/¯",
            "sink0": "åß∂ƒ©˙∆˚¬…æ",
            "sink1": "￾",
            "sink2": "﻿",
            "alias": "₀₁₂",
        },
        {
            "db": "찦차를 타고 온 펲시맨과 쑛다리 똠방각하",
            "schema": "田中さんにあげて下さい",
            "type": "パーティーへ行かないか",
            "table": "和製漢語",
            "column": "部落格",
            "value1": "사회과학원 어학연구소",
            "value2": "社會科學院語學研究所",
            "source": "울란바토르",
            "source_view": "𐐜 𐐔𐐇𐐝𐐀𐐡𐐇𐐓 𐐙𐐊𐐡𐐝𐐓/𐐝𐐇𐐗𐐊𐐤𐐔 𐐒𐐋𐐗 𐐒𐐌 𐐜 𐐡𐐀𐐖𐐇𐐤𐐓𐐝 𐐱𐑂 𐑄 𐐔𐐇𐐝𐐀𐐡𐐇𐐓 𐐏𐐆𐐅𐐤𐐆𐐚𐐊𐐡𐐝𐐆𐐓𐐆",
            "kafka_conn": "表ポあA鷗ŒéＢ逍Üßªąñ丂㐀𠀀",
            "csr_conn": "Ⱥ",
            "secret": "ヽ༼ຈل͜ຈ༽ﾉ ヽ༼ຈل͜ຈ༽ﾉ",
            "secret_value": "(｡◕ ∀ ◕｡)",
            "mv0": "Ṱ̺̺o͞ ̷i̲̬n̝̗v̟̜o̶̙kè͚̮ ̖t̝͕h̼͓e͇̣ ̢̼h͚͎i̦̲v̻͍e̺̭-m̢iͅn̖̺d̵̼ ̞̥r̛̗e͙p͠r̼̞e̺̠s̘͇e͉̥ǹ̬͎t͍̬i̪̱n͠g̴͉ ͏͉c̬̟h͡a̫̻o̫̟s̗̦.̨̹",
            "mv1": "I̗̘n͇͇v̮̫ok̲̫i̖͙n̡̻g̲͈ ̰t͔̦h̞̲e̢̤ ͍̬f̴̘è͖ẹ̥̩l͖͔i͓͚n͖͍g͍ ̨o͚̪f̘̣ ̖̘c҉͔h̵̤á̗̼o̼̣s̱͈.̛̖",
            "mv2": "Ṯ̤͍h̲́e͏͓ ͇̜N͕͠e̗̱z̘̝p̤̺e̠̻r̨̤d̠̟i̦͖a̠̗n͚͜ ̻̞h̵͉i̳̞v̢͇ḙ͎͟-҉̭m̤̭i͕͇n̗͙ḍ̟ ̯̲ǫ̟̯f ̪̰c̦͍ḥ͚a̮͎ơ̩̹s̤.̝̝ ҉Z̡̖a͖̰l̲̫g̡̟o̗͜.̟",
            "sink0": "𠜎𠜱𠝹𠱓𠱸𠲖𠳏",
            "sink1": "Ⱦ",
            "sink2": "｀ｨ(´∀｀∩",
            "alias": "⅛⅜⅝⅞",
        },
        # Disabled because of timeouts, revisit if we want to accept longer runtime
        # {
        #     "db": "❤️ 💔 💌 💕 💞 💓 💗 💖 💘 💝 💟 💜 💛 💚 💙",
        #     "schema": "😍",
        #     "type": "👩🏽",
        #     "table": "👨‍🦰 👨🏿‍🦰 👨‍🦱 👨🏿‍🦱 🦹🏿‍♂️",
        #     "column": "👾 🙇 💁 🙅 🙆 🙋 🙎 🙍",
        #     "value1": "🐵 🙈 🙉 🙊",
        #     "value2": "✋🏿 💪🏿   👐🏿   🙌🏿   👏🏿   🙏🏿",
        #     "source": "🚾 🆒 🆓 🆕 🆖 🆗 🆙 🏧",
        #     "source_view": "🇺 🇸 🇷 🇺 🇸  🇦 🇫 🇦 🇲 🇸",
        #     "kafka_conn": "🇺 🇸 🇷 🇺 🇸 🇦 🇫 🇦 🇲",
        #     "csr_conn": "🇺 🇸 🇷 🇺 🇸 🇦",
        #     "secret": "１２３",
        #     "secret_value": "١٢٣",
        #     "mv0": "🇺s🇸r🇷p🇺>🇸l🇦r",
        #     "mv1": "Ⱦ",
        #     "mv2": "👨‍👩‍👦 👨‍👩‍👧‍👦 👨‍👨‍👦 👩‍👩‍👧 👨‍👦 👨‍👧‍👦 👩‍👦 👩‍👧‍👦",
        #     "sink0": "0️⃣ 1️⃣ 2️⃣ 3️⃣ 4️⃣ 5️⃣ 6️⃣ 7️⃣ 8️⃣ 9️⃣ 🔟",
        #     "sink1": " test ",
        #     "sink2": "‫test‫",
        #     "alias": "1#INF",
        # },
        # {
        #     "db": "ﺚﻣ ﻦﻔﺳ ﺲﻘﻄﺗ ﻮﺑﺎﻠﺘﺣﺪﻳﺩ،, ﺝﺰﻳﺮﺘﻳ ﺏﺎﺴﺘﺧﺩﺎﻣ ﺄﻧ ﺪﻧﻭ. ﺇﺫ ﻪﻧﺍ؟ ﺎﻠﺴﺗﺍﺭ ﻮﺘﻨﺼﻴﺑ ﻙﺎﻧ. ﺄﻬّﻟ ﺎﻴﻃﺎﻠﻳﺍ، ﺏﺮﻴﻃﺎﻨﻳﺍ-ﻑﺮﻨﺳﺍ ﻕﺩ ﺄﺧﺫ. ﺲﻠﻴﻣﺎﻧ، ﺈﺘﻓﺎﻘﻳﺓ ﺐﻴﻧ ﻡﺍ, ﻱﺬﻛﺭ ﺎﻠﺣﺩﻭﺩ ﺄﻳ ﺐﻋﺩ, ﻢﻋﺎﻤﻟﺓ ﺏﻮﻠﻧﺩﺍ، ﺍﻺﻃﻼﻗ ﻊﻟ ﺈﻳﻭ.",
        #     "schema": "בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּמַיִם, וְאֵת הָאָרֶץ",
        #     "type": "הָיְתָהtestﺎﻠﺼﻔﺣﺎﺗ ﺎﻠﺘّﺣﻮﻟ",
        #     "table": "﷽",
        #     "column": "undefined",
        #     "value1": "undef",
        #     "value2": "NULL",
        #     "source": "(null)",
        #     "source_view": "NIL",
        #     "kafka_conn": "true",
        #     "csr_conn": "FALSE",
        #     "secret": "None",
        #     "secret_value": "'",
        #     "mv0": "\\",
        #     "mv1": "\\\\",
        #     "mv2": '"',
        #     "sink0": "nil",
        #     "sink1": "⁦test⁧",
        #     "sink2": "‪‪᚛                 ᚜‪",
        #     "alias": "0xabad1dea",
        # },
    ]

    def initialize(self) -> Testdrive:
        cmds = "\n".join(
            [
                f"""
            > CREATE DATABASE {dq(ident["db"])};
            > SET DATABASE={dq(ident["db"])};
            > CREATE SCHEMA {dq(ident["schema"])};
            > CREATE TYPE {dq(ident["type"])} AS LIST (ELEMENT TYPE = text);
            > CREATE TABLE {dq(ident["schema"])}.{dq(ident["table"])} ({dq(ident["column"])} TEXT, c2 {dq(ident["type"])});
            > INSERT INTO {dq(ident["schema"])}.{dq(ident["table"])} VALUES ({sq(ident["value1"])}, LIST[{sq(ident["value2"])}]::{dq(ident["type"])});
            > CREATE MATERIALIZED VIEW {dq(ident["schema"])}.{dq(ident["mv0"])} AS SELECT COUNT({dq(ident["column"])}) FROM {dq(ident["schema"])}.{dq(ident["table"])};

            $ kafka-create-topic topic=sink-source-ident{i}

            $ kafka-ingest format=avro key-format=avro topic=sink-source-ident{i} key-schema=${{keyschema}} schema=${{schema}} repeat=1000
            {{"key1": "U2${{kafka-ingest.iteration}}"}} {{"f1": "A${{kafka-ingest.iteration}}"}}

            > CREATE CONNECTION IF NOT EXISTS {dq(ident["kafka_conn"])} FOR KAFKA BROKER '${{testdrive.kafka-addr}}';
            > CREATE CONNECTION IF NOT EXISTS {dq(ident["csr_conn"])} FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';
            > CREATE SOURCE {dq(ident["source"])}
              FROM KAFKA CONNECTION {dq(ident["kafka_conn"])} (TOPIC 'testdrive-sink-source-ident{i}-${{testdrive.seed}}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {dq(ident["csr_conn"])}
              ENVELOPE UPSERT;
            > CREATE MATERIALIZED VIEW {dq(ident["source_view"])} AS SELECT LEFT(key1, 2) as l_k, LEFT(f1, 1) AS l_v, COUNT(*) AS c FROM {dq(ident["source"])} GROUP BY LEFT(key1, 2), LEFT(f1, 1);
            > CREATE SINK {dq(ident["schema"])}.{dq(ident["sink0"])} FROM {dq(ident["source_view"])}
              INTO KAFKA CONNECTION {dq(ident["kafka_conn"])} (TOPIC 'sink-sink-ident0')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {dq(ident["csr_conn"])}
              ENVELOPE DEBEZIUM;
            """
                + (
                    f"""
            > CREATE SECRET {dq(ident["secret"])} as {sq(ident["secret_value"])};
            """
                    if self.base_version >= MzVersion("v0.44.0")
                    else ""
                )
                for i, ident in enumerate(self.IDENTS)
            ]
        )
        return Testdrive(schemas() + dedent(cmds))

    def manipulate(self) -> List[Testdrive]:
        cmds = [
            "\n".join(
                [
                    f"""
            > SET DATABASE={dq(ident["db"])};
            > CREATE MATERIALIZED VIEW {dq(ident["schema"])}.{dq(ident["mv" + i])} AS SELECT {dq(ident["column"])}, c2 as {dq(ident["alias"])} FROM {dq(ident["schema"])}.{dq(ident["table"])};
            > INSERT INTO {dq(ident["schema"])}.{dq(ident["table"])} VALUES ({sq(ident["value1"])}, LIST[{sq(ident["value2"])}]::{dq(ident["type"])});
            > CREATE SINK {dq(ident["schema"])}.{dq(ident["sink" + i])} FROM {dq(ident["source_view"])}
              INTO KAFKA CONNECTION {dq(ident["kafka_conn"])} (TOPIC 'sink-sink-ident{i}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION {dq(ident["csr_conn"])}
              ENVELOPE DEBEZIUM;
            """
                    for ident in self.IDENTS
                ]
            )
            for i in ["1", "2"]
        ]
        return [Testdrive(dedent(s)) for s in cmds]

    def validate(self) -> Testdrive:
        cmds = "> SHOW DATABASES WHERE name NOT LIKE 'to_be_created%';\nmaterialize\n"
        cmds += "\n".join([dq(ident["db"]) for ident in self.IDENTS])
        for ident in self.IDENTS:
            cmds += f"""
> SET DATABASE={dq(ident["db"])};

> SHOW TYPES;
{dq_print(ident["type"])}

> SHOW SCHEMAS FROM {dq(ident["db"])};
public
information_schema
mz_catalog
mz_internal
pg_catalog
{dq_print(ident["schema"])}

> SHOW SINKS FROM {dq(ident["schema"])};
{dq_print(ident["sink0"])} kafka 4
{dq_print(ident["sink1"])} kafka 4
{dq_print(ident["sink2"])} kafka 4

> SELECT * FROM {dq(ident["schema"])}.{dq(ident["mv0"])};
3

> SELECT {dq(ident["column"])}, {dq(ident["alias"])}[1] FROM {dq(ident["schema"])}.{dq(ident["mv1"])};
{dq_print(ident["value1"])} {dq_print(ident["value2"])}
{dq_print(ident["value1"])} {dq_print(ident["value2"])}
{dq_print(ident["value1"])} {dq_print(ident["value2"])}

> SELECT {dq(ident["column"])}, {dq(ident["alias"])}[1] FROM {dq(ident["schema"])}.{dq(ident["mv2"])};
{dq_print(ident["value1"])} {dq_print(ident["value2"])}
{dq_print(ident["value1"])} {dq_print(ident["value2"])}
{dq_print(ident["value1"])} {dq_print(ident["value2"])}

> SELECT * FROM {dq(ident["source_view"])};
U2 A 1000
"""
        if self.base_version >= MzVersion("v0.44.0"):
            cmds += f"""
> SHOW SECRETS;
{dq_print(ident["secret"])}
"""
        return Testdrive(dedent(cmds))
