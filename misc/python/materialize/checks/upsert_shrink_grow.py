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


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


class ShrinkGrow:
    def initialize(self) -> Testdrive:
        name = self.name()
        pads = self.pads()
        return Testdrive(
            schemas()
            + dedent(
                f"""
                $ kafka-create-topic topic=upsert-update-{name}

                $ kafka-ingest format=avro key-format=avro topic=upsert-update-{name} key-schema=${{keyschema}} schema=${{schema}} repeat=10000
                {{"key1": "${{kafka-ingest.iteration}}"}} {{"f1": "A${{kafka-ingest.iteration}}{pads[0]}A"}}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${{testdrive.kafka-addr}}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';

                > CREATE SOURCE upsert_update_{name}
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-update-{name}-${{testdrive.seed}}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW upsert_update_{name}_view AS
                  SELECT LEFT(f1, 1), RIGHT(f1, 1),
                  COUNT(*) AS c1, COUNT(DISTINCT key1) AS c2, COUNT(DISTINCT f1) AS c3,
                  MIN(LENGTH(f1)) AS l1, MAX(LENGTH(f1)) AS l2
                  FROM upsert_update_{name}
                  GROUP BY LEFT(f1, 1), RIGHT(f1, 1);
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        name = self.name()
        pads = self.pads()
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-update-{name} key-schema=${{keyschema}} schema=${{schema}} repeat=10000
                {{"key1": "${{kafka-ingest.iteration}}"}} {{"f1": "B${{kafka-ingest.iteration}}{pads[1]}B"}}
                """,
                f"""
                $ kafka-ingest format=avro key-format=avro topic=upsert-update-{name} key-schema=${{keyschema}} schema=${{schema}} repeat=10000
                {{"key1": "${{kafka-ingest.iteration}}"}} {{"f1": "C${{kafka-ingest.iteration}}{pads[2]}C"}}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        name = self.name()
        last_pad_length = len(self.pads()[-1])
        return Testdrive(
            dedent(
                f"""
                > SELECT * FROM upsert_update_{name}_view;
                C C 10000 10000 10000 {last_pad_length+3} {last_pad_length+6}
                """
            )
        )

    def name(self) -> str:
        raise NotImplementedError

    def pads(self) -> List[str]:
        raise NotImplementedError


class UpsertUpdateShrink(ShrinkGrow, Check):
    """Upserts where the data length shrinks"""

    def name(self) -> str:
        return "shrink"

    def pads(self) -> List[str]:
        return ["x" * 1024, "x" * 512, "x" * 256]


class UpsertUpdateGrow(ShrinkGrow, Check):
    """Upserts where the data lenth grows"""

    def name(self) -> str:
        return "grow"

    def pads(self) -> List[str]:
        return ["x" * 256, "x" * 512, "x" * 1024]
