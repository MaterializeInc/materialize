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
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


class Webhook(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE '1'));

                > CREATE SOURCE webhook_text IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT TEXT;

                > CREATE SOURCE webhook_json IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT JSON INCLUDE HEADERS;

                > CREATE SOURCE webhook_bytes IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT BYTES;

                $ webhook-append database=materialize schema=public name=webhook_text
                fooä

                $ webhook-append database=materialize schema=public name=webhook_json content-type=application/json app=platform-checks1
                {
                  "hello": "wörld"
                }

                $ webhook-append database=materialize schema=public name=webhook_bytes
                \u0001
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ webhook-append database=materialize schema=public name=webhook_text
                bar❤️

                $ webhook-append database=materialize schema=public name=webhook_json content-type=application/json app=
                {
                  "still": 123,
                  "foo": []
                }

                $ webhook-append database=materialize schema=public name=webhook_bytes
                \u0000\u0000\u0000\u0000
                """,
                """
                $ webhook-append database=materialize schema=public name=webhook_text
                baz１２３

                $ webhook-append database=materialize schema=public name=webhook_json content-type=application/json app=null
                [{"good": "bye"}, 42, null]

                $ webhook-append database=materialize schema=public name=webhook_bytes
                \u0001\u0002\u0003\u0004
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW COLUMNS FROM webhook_text
                body false text ""

                > SHOW COLUMNS FROM webhook_json
                body false jsonb ""
                headers false map ""

                > SHOW COLUMNS FROM webhook_bytes
                body false bytea ""

                > SELECT * FROM webhook_text
                fooä
                bar❤️
                baz１２３

                > SELECT body FROM webhook_json WHERE headers -> 'app' = 'platform-checks1'
                "{\\"hello\\":\\"wörld\\"}"

                > SELECT body FROM webhook_json WHERE headers -> 'app' = ''
                "{\\"foo\\":[],\\"still\\":123}"

                > SELECT body FROM webhook_json WHERE headers -> 'app' = 'null'
                "[{\\"good\\":\\"bye\\"},42,null]"

                > SELECT * FROM webhook_bytes
                \\\\x00\\x00\\x00\\x00
                \\\\x01
                \\\\x01\\x02\\x03\\x04

                > SHOW CREATE SOURCE webhook_text
                materialize.public.webhook_text "CREATE SOURCE \\"materialize\\".\\"public\\".\\"webhook_text\\" IN CLUSTER \\"webhook_cluster\\" FROM WEBHOOK BODY FORMAT TEXT"

                > SHOW CREATE SOURCE webhook_json
                materialize.public.webhook_json "CREATE SOURCE \\"materialize\\".\\"public\\".\\"webhook_json\\" IN CLUSTER \\"webhook_cluster\\" FROM WEBHOOK BODY FORMAT JSON INCLUDE HEADERS"

                > SHOW CREATE SOURCE webhook_bytes
                materialize.public.webhook_bytes "CREATE SOURCE \\"materialize\\".\\"public\\".\\"webhook_bytes\\" IN CLUSTER \\"webhook_cluster\\" FROM WEBHOOK BODY FORMAT BYTES"
           """
            )
        )


class WebhookTable(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.130.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE TABLE webhook_table_text FROM WEBHOOK BODY FORMAT TEXT;

                $ webhook-append database=materialize schema=public name=webhook_table_text
                hello_world
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ webhook-append database=materialize schema=public name=webhook_table_text
                anotha_one!
                """,
                """
                $ webhook-append database=materialize schema=public name=webhook_table_text
                threeeeeee
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM webhook_table_text
                hello_world
                anotha_one!
                threeeeeee

                > SHOW CREATE TABLE webhook_table_text
                materialize.public.webhook_table_text "CREATE TABLE \\"materialize\\".\\"public\\".\\"webhook_table_text\\" FROM WEBHOOK BODY FORMAT TEXT"
           """
            )
        )
