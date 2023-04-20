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


class MultiplePartitions(Check):
    """Test that adds new partitions to a Kafka source"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=movies-topic

                $ kafka-ingest format=avro key-format=avro topic=movies-topic key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE movies_source
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-movies-topic-${testdrive.seed}', TOPIC METADATA REFRESH INTERVAL MS 500)
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW mv_movies AS SELECT * FROM movies_source;
                
                $ kafka-add-partitions topic=movies-topic total-partitions=4
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro key-format=avro topic=movies-topic key-schema=${keyschema} schema=${schema} repeat=40
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}
                """,
                """
                $ kafka-ingest format=avro key-format=avro topic=movies-topic key-schema=${keyschema} schema=${schema} repeat=60
                {"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM movies_source_progress;
                (3,) 0
                [0,0] 1025
                [1,1] 25
                [2,2] 25
                [3,3] 25
                
                > SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'movies_source';
                running
                
                # > SELECT COUNT(*) FROM movies_source;
                # 1100
                
                # > SELECT COUNT(*) FROM mv_movies;
                # 1100
           """
            )
        )
