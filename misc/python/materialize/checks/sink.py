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


class Sink(Check):
    """Test that repeated inserts of the same record are properly handled"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=sink-source

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} publish=true repeat=1000
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} publish=true repeat=1000
                {"key1": "D2${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} publish=true repeat=1000
                {"key1": "U3${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} publish=true repeat=1000
                {"key1": "D3${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                > CREATE SOURCE sink_source
                  FROM KAFKA BROKER '${testdrive.kafka-addr}'
                  TOPIC 'testdrive-sink-source-${testdrive.seed}'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW sink_source_view AS SELECT LEFT(key1, 2) as l_k, LEFT(f1, 1) AS l_v, COUNT(*) AS c FROM sink_source GROUP BY LEFT(key1, 2), LEFT(f1, 1);

                > CREATE SINK sink_sink1 FROM sink_source_view
                  INTO KAFKA BROKER '${testdrive.kafka-addr}'
                  TOPIC 'sink-sink1' WITH (reuse_topic=true)
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} publish=true repeat=1000
                {"key1": "I2${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}
                {"key1": "D2${kafka-ingest.iteration}"}

                > CREATE SINK sink_sink2 FROM sink_source_view
                  INTO KAFKA BROKER '${testdrive.kafka-addr}'
                  TOPIC 'sink-sink2' WITH (reuse_topic=true)
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
                """,
                """
                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} publish=true repeat=1000
                {"key1": "I3${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}
                {"key1": "U3${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}
                {"key1": "D3${kafka-ingest.iteration}"}

                > CREATE SINK sink_sink3 FROM sink_source_view
                  INTO KAFKA BROKER '${testdrive.kafka-addr}'
                  TOPIC 'sink-sink3' WITH (reuse_topic=true)
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM sink_source_view;
                I2 B 1000
                I3 C 1000
                U2 B 1000
                U3 C 1000

                # We check the contents of the sink topics by re-ingesting them.

                > CREATE SOURCE sink_view1
                  FROM KAFKA BROKER '${testdrive.kafka-addr}'
                  TOPIC 'sink-sink1'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
                  ENVELOPE NONE

                > CREATE SOURCE sink_view2
                  FROM KAFKA BROKER '${testdrive.kafka-addr}'
                  TOPIC 'sink-sink2'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
                  ENVELOPE NONE

                > CREATE SOURCE sink_view3
                  FROM KAFKA BROKER '${testdrive.kafka-addr}'
                  TOPIC 'sink-sink3'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
                  ENVELOPE NONE

                # Validate the sink by aggregating all the 'before' and 'after' records using SQL
                > SELECT l_v, l_k, SUM(c)
                  FROM (
                    SELECT (after).l_v, (after).l_k, (after).c FROM sink_view1
                    UNION ALL
                    SELECT (before).l_v, (before).l_k, -(before).c FROM sink_view1
                  ) GROUP BY l_v, l_k
                  HAVING SUM(c) > 0;
                B I2 1000
                B U2 1000
                C I3 1000
                C U3 1000

                > SELECT l_v, l_k, SUM(c)
                  FROM (
                    SELECT (after).l_v, (after).l_k, (after).c FROM sink_view2
                    UNION ALL
                    SELECT (before).l_v, (before).l_k, -(before).c FROM sink_view2
                  ) GROUP BY l_v, l_k
                  HAVING SUM(c) > 0;
                B I2 1000
                B U2 1000
                C I3 1000
                C U3 1000

                > SELECT l_v, l_k, SUM(c)
                  FROM (
                    SELECT (after).l_v, (after).l_k, (after).c FROM sink_view3
                    UNION ALL
                    SELECT (before).l_v, (before).l_k, -(before).c FROM sink_view3
                  ) GROUP BY l_v, l_k
                  HAVING SUM(c) > 0;
                B I2 1000
                B U2 1000
                C I3 1000
                C U3 1000

                > DROP SOURCE sink_view1;

                > DROP SOURCE sink_view2;

                > DROP SOURCE sink_view3;
            """
            )
        )
