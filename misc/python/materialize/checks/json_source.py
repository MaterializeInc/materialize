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
        return self.base_version >= MzVersion.parse("0.52.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ kafka-create-topic topic=format-json

                $ kafka-ingest format=bytes topic=format-json repeat=10000
                {"f1": 1}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE format_jsonA
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-json-${testdrive.seed}')
                  FORMAT JSON

                > CREATE SOURCE format_jsonB
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-json-${testdrive.seed}')
                  FORMAT JSON
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ kafka-ingest format=bytes topic=format-json repeat=10000
                {"f2": 1}
                """,
                """
                $ kafka-ingest format=bytes topic=format-json repeat=10000
                {"f3": 1}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT SUM((data -> 'f1')::STRING::INTEGER) + SUM((data -> 'f2')::STRING::INTEGER) + SUM((data -> 'f3')::STRING::INTEGER) FROM format_jsonA;
                30000

                > SHOW CREATE SOURCE format_jsonA;
                materialize.public.format_jsona "CREATE SOURCE \\"materialize\\".\\"public\\".\\"format_jsona\\" FROM KAFKA CONNECTION \\"materialize\\".\\"public\\".\\"kafka_conn\\" (TOPIC = 'testdrive-format-json-1') FORMAT JSON EXPOSE PROGRESS AS \\"materialize\\".\\"public\\".\\"format_jsonA_progress\\""

                > SELECT SUM((data -> 'f1')::STRING::INTEGER) + SUM((data -> 'f2')::STRING::INTEGER) + SUM((data -> 'f3')::STRING::INTEGER) FROM format_jsonB;
                30000

                > SHOW CREATE SOURCE format_jsonB;
                materialize.public.format_jsonb "CREATE SOURCE \\"materialize\\".\\"public\\".\\"format_jsonb\\" FROM KAFKA CONNECTION \\"materialize\\".\\"public\\".\\"kafka_conn\\" (TOPIC = 'testdrive-format-json-1') FORMAT JSON EXPOSE PROGRESS AS \\"materialize\\".\\"public\\".\\"format_jsonB_progress\\""
           """
            )
        )
