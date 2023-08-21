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
from materialize.util import MzVersion


class JsonSource(Check):
    """Test CREATE SOURCE ... FORMAT JSON"""

    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.60.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ kafka-create-topic topic=format-json partitions=1

                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=format-json
                "object":{"a":"b","c":"d"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE SOURCE format_jsonA
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-json-${testdrive.seed}')
                  KEY FORMAT JSON
                  VALUE FORMAT JSON
                  ENVELOPE UPSERT

                > CREATE SOURCE format_jsonB
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-json-${testdrive.seed}')
                  KEY FORMAT JSON
                  VALUE FORMAT JSON
                  ENVELOPE UPSERT
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=format-json
                "float":1.23
                "str":"hello"
                """,
                """
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=format-json
                "array":[1,2,3]
                "int":1
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM format_jsonA ORDER BY key
                "\\"array\\"" [1,2,3]
                "\\"float\\"" 1.23
                "\\"int\\"" 1
                "\\"object\\"" "{\\"a\\":\\"b\\",\\"c\\":\\"d\\"}"
                "\\"str\\"" "\\"hello\\""

                > SELECT * FROM format_jsonB ORDER BY key
                "\\"array\\"" [1,2,3]
                "\\"float\\"" 1.23
                "\\"int\\"" 1
                "\\"object\\"" "{\\"a\\":\\"b\\",\\"c\\":\\"d\\"}"
                "\\"str\\"" "\\"hello\\""

                > SHOW CREATE SOURCE format_jsonB;
                materialize.public.format_jsonb "CREATE SOURCE \\"materialize\\".\\"public\\".\\"format_jsonb\\" FROM KAFKA CONNECTION \\"materialize\\".\\"public\\".\\"kafka_conn\\" (TOPIC = 'testdrive-format-json-${testdrive.seed}') KEY FORMAT JSON VALUE FORMAT JSON ENVELOPE UPSERT EXPOSE PROGRESS AS \\"materialize\\".\\"public\\".\\"format_jsonb_progress\\""
           """
            )
        )
