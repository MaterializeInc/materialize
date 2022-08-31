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


class SinkUpsert(Check):
    """Basic Check on sinks from an upsert source"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=sink-source

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "D2${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "U3${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "D3${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE sink_source
                  FROM KAFKA CONNECTION kafka_conn
                  TOPIC 'testdrive-sink-source-${testdrive.seed}'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW sink_source_view AS SELECT LEFT(key1, 2) as l_k, LEFT(f1, 1) AS l_v, COUNT(*) AS c FROM sink_source GROUP BY LEFT(key1, 2), LEFT(f1, 1);

                > CREATE SINK sink_sink1 FROM sink_source_view
                  INTO KAFKA CONNECTION kafka_conn
                  TOPIC 'sink-sink1'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I2${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}
                {"key1": "D2${kafka-ingest.iteration}"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SINK sink_sink2 FROM sink_source_view
                  INTO KAFKA CONNECTION kafka_conn
                  TOPIC 'sink-sink2'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                """,
                """
                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I3${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}
                {"key1": "U3${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}
                {"key1": "D3${kafka-ingest.iteration}"}

                > CREATE SINK sink_sink3 FROM sink_source_view
                  INTO KAFKA CONNECTION kafka_conn
                  TOPIC 'sink-sink3'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
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

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE sink_view1
                  FROM KAFKA CONNECTION kafka_conn
                  TOPIC 'sink-sink1'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE SOURCE sink_view2
                  FROM KAFKA CONNECTION kafka_conn
                  TOPIC 'sink-sink2'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE SOURCE sink_view3
                  FROM KAFKA CONNECTION kafka_conn
                  TOPIC 'sink-sink3'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
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


class SinkTables(Check):
    """Sink and re-ingest a large transaction from a table source"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE TABLE sink_large_transaction_table (f1 INTEGER, f2 TEXT, PRIMARY KEY (f1));
                > CREATE DEFAULT INDEX ON sink_large_transaction_table;

                > INSERT INTO sink_large_transaction_table SELECT generate_series, REPEAT('x', 1024) FROM generate_series(1, 100000);

                > CREATE MATERIALIZED VIEW sink_large_transaction_view AS SELECT f1 - 1 AS f1 , f2 FROM sink_large_transaction_table;

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SINK sink_large_transaction_sink1 FROM sink_large_transaction_view
                  INTO KAFKA CONNECTION kafka_conn
                  TOPIC 'testdrive-sink-large-transaction-sink-${testdrive.seed}'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn;
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                > UPDATE sink_large_transaction_table SET f2 = REPEAT('y', 1024)
                """,
                """
                > UPDATE sink_large_transaction_table SET f2 = REPEAT('z', 1024)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                # We check the contents of the sink topics by re-ingesting them.
                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE sink_large_transaction_source
                  FROM KAFKA CONNECTION kafka_conn
                  TOPIC 'testdrive-sink-large-transaction-sink-${testdrive.seed}'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE MATERIALIZED VIEW sink_large_transaction_view2
                  AS
                  SELECT COUNT(*) AS c1 , COUNT(f1) AS c2, COUNT(DISTINCT f1) AS c3 , MIN(f1), MAX(f1)
                  FROM (
                    SELECT (before).f1, (before).f2 FROM sink_large_transaction_source
                  )

                > CREATE MATERIALIZED VIEW sink_large_transaction_view3
                  AS
                  SELECT COUNT(*) AS c1 , COUNT(f1) AS c2, COUNT(DISTINCT f1) AS c3 , MIN(f1), MAX(f1)
                  FROM (
                    SELECT (after).f1, (after).f2 FROM sink_large_transaction_source
                  )

                > CREATE MATERIALIZED VIEW sink_large_transaction_view4
                  AS
                  SELECT LEFT(f2, 1), SUM(c)
                  FROM (
                    SELECT (after).f2, COUNT(*) AS c FROM sink_large_transaction_source GROUP BY (after).f2
                    UNION ALL
                    SELECT (before).f2, -COUNT(*) AS c  FROM sink_large_transaction_source GROUP BY (before).f2
                  )
                  GROUP BY f2

                > SELECT * FROM sink_large_transaction_view2
                500000 200000 100000 0 99999

                > SELECT * FROM sink_large_transaction_view3
                500000 300000 100000 0 99999

                > SELECT * FROM sink_large_transaction_view4
                <null> -100000
                x 0
                y 0
                z 100000

                > DROP SOURCE sink_large_transaction_source CASCADE;
            """
            )
        )
