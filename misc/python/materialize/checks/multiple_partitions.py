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


class MultiplePartitions(Check):
    """Test that adds new partitions to a Kafka source"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=multiple-partitions-topic

                # ingest A-key entries
                $ kafka-ingest format=avro key-format=avro topic=multiple-partitions-topic key-schema=${keyschema} schema=${schema} repeat=100
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE multiple_partitions_source
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-multiple-partitions-topic-${testdrive.seed}', TOPIC METADATA REFRESH INTERVAL MS 500)
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                $ kafka-add-partitions topic=multiple-partitions-topic total-partitions=2

                > CREATE MATERIALIZED VIEW mv_multiple_partitions AS SELECT * FROM multiple_partitions_source;
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                # ingest B-key entries
                $ kafka-ingest format=avro key-format=avro topic=multiple-partitions-topic key-schema=${keyschema} schema=${schema} repeat=60
                {"key1": "B${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}

                # Make sure that source is up and complete
                > SELECT LEFT(f1, 1), COUNT(*) FROM multiple_partitions_source GROUP BY LEFT(f1, 1);
                A 100
                B 60

                $ kafka-add-partitions topic=multiple-partitions-topic total-partitions=3

                # ingest some more B-key entries
                $ kafka-ingest format=avro key-format=avro topic=multiple-partitions-topic key-schema=${keyschema} schema=${schema} repeat=60
                {"key1": "B${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}

                # delete some A-key entries
                $ kafka-ingest format=avro key-format=avro topic=multiple-partitions-topic key-schema=${keyschema} schema=${schema} repeat=50
                {"key1": "A${kafka-ingest.iteration}"}
                """,
                """
                # ingest C-key entries
                $ kafka-ingest format=avro key-format=avro topic=multiple-partitions-topic key-schema=${keyschema} schema=${schema} repeat=60
                {"key1": "C${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}

                # Make sure that source is up and complete
                > SELECT LEFT(f1, 1), COUNT(*) FROM multiple_partitions_source GROUP BY LEFT(f1, 1);
                A 50
                B 60
                C 60

                $ kafka-add-partitions topic=multiple-partitions-topic total-partitions=4

                # ingest some more C-key entries
                $ kafka-ingest format=avro key-format=avro topic=multiple-partitions-topic key-schema=${keyschema} schema=${schema} repeat=40
                {"key1": "C${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}

                # delete some A-key entries
                $ kafka-ingest format=avro key-format=avro topic=multiple-partitions-topic key-schema=${keyschema} schema=${schema} repeat=50
                {"key1": "A${kafka-ingest.iteration}"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT partition FROM multiple_partitions_source_progress;
                (3,)
                [0,0]
                [1,1]
                [2,2]
                [3,3]

                # alias is needed to avoid error due to reserved keyword
                > SELECT SUM(p.offset) FROM multiple_partitions_source_progress p;
                420

                > SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'multiple_partitions_source';
                running

                > SELECT LEFT(f1, 1), COUNT(*) FROM multiple_partitions_source GROUP BY LEFT(f1, 1);
                A 50
                B 60
                C 60

                > SELECT LEFT(f1, 1), COUNT(*) FROM mv_multiple_partitions GROUP BY LEFT(f1, 1);
                A 50
                B 60
                C 60
                """
            )
        )
