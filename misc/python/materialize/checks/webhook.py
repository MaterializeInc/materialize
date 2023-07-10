# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.util import MzVersion


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


class Webhook(Check):
    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.59.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_webhook_sources = true

                > CREATE SOURCE webhook_text FROM WEBHOOK BODY FORMAT TEXT;

                > CREATE SOURCE webhook_json FROM WEBHOOK BODY FORMAT JSON INCLUDE HEADERS;

                > CREATE SOURCE webhook_bytes FROM WEBHOOK BODY FORMAT BYTES;

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

    def manipulate(self) -> List[Testdrive]:
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
                body false text

                > SHOW COLUMNS FROM webhook_json
                body false jsonb
                headers false map

                > SHOW COLUMNS FROM webhook_bytes
                body false bytea

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
           """
            )
        )
