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


@externally_idempotent(False)
class RenameSource(Check):
    def _source_schema(self) -> str:
        return dedent(
            """
            $ set rename-source-schema={
                 "type" : "record",
                 "name" : "test",
                 "fields" : [
                     {"name":"f1", "type":"string"}
                 ]
              }
        """
        )

    def initialize(self) -> Testdrive:
        return Testdrive(
            self._source_schema()
            + dedent(
                """
                $ kafka-create-topic topic=rename-source

                $ kafka-ingest format=avro topic=rename-source schema=${rename-source-schema}
                {"f1": "A"}

                > CREATE SOURCE rename_source1_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-rename-source-${testdrive.seed}')
                > CREATE TABLE rename_source1_tbl FROM SOURCE rename_source1_src (REFERENCE "testdrive-rename-source-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                $ kafka-ingest format=avro topic=rename-source schema=${rename-source-schema}
                {"f1": "B"}

                > CREATE MATERIALIZED VIEW rename_source_view AS SELECT DISTINCT f1 FROM rename_source1_tbl;

                $ kafka-ingest format=avro topic=rename-source schema=${rename-source-schema}
                {"f1": "C"}
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(self._source_schema() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro topic=rename-source schema=${rename-source-schema}
                {"f1": "D"}
                > ALTER SOURCE rename_source1_src RENAME to rename_source2_src;
                $ kafka-ingest format=avro topic=rename-source schema=${rename-source-schema}
                {"f1": "E"}
                """,
                """
                $ kafka-ingest format=avro topic=rename-source schema=${rename-source-schema}
                {"f1": "F"}
                > ALTER SOURCE rename_source2_src RENAME to rename_source3_src;
                $ kafka-ingest format=avro topic=rename-source schema=${rename-source-schema}
                {"f1": "G"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM rename_source1_tbl;
                A
                B
                C
                D
                E
                F
                G

                > SELECT * FROM rename_source_view;
                A
                B
                C
                D
                E
                F
                G
           """
            )
        )
