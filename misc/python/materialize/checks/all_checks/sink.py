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
from materialize.checks.checks import Check, disabled, externally_idempotent
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


def schemas_null() -> str:
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
                   {"name":"f1", "type":["null", "string"]},
                   {"name":"f2", "type":["long", "null"]}
               ]
             }
    """
    )


@externally_idempotent(False)
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

                > CREATE SOURCE sink_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-source-${testdrive.seed}')
                > CREATE TABLE sink_source FROM SOURCE sink_source_src (REFERENCE "testdrive-sink-source-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW sink_source_view AS SELECT LEFT(key1, 2) as l_k, LEFT(f1, 1) AS l_v, COUNT(*) AS c FROM sink_source GROUP BY LEFT(key1, 2), LEFT(f1, 1);

                > CREATE SINK sink_sink1 FROM sink_source_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink1')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON sink_source_view TO materialize
                GRANT USAGE ON CONNECTION kafka_conn TO materialize
                GRANT USAGE ON CONNECTION csr_conn TO materialize

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I2${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}
                {"key1": "D2${kafka-ingest.iteration}"}

                > CREATE SINK sink_sink2 FROM sink_source_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink2')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """,
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON sink_source_view TO materialize
                GRANT USAGE ON CONNECTION kafka_conn TO materialize
                GRANT USAGE ON CONNECTION csr_conn TO materialize

                $ kafka-ingest format=avro key-format=avro topic=sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I3${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}
                {"key1": "U3${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}
                {"key1": "D3${kafka-ingest.iteration}"}

                > CREATE SINK sink_sink3 FROM sink_source_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink3')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON sink_source_view TO materialize
                GRANT USAGE ON CONNECTION kafka_conn TO materialize
                GRANT USAGE ON CONNECTION csr_conn TO materialize

                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATECLUSTER ON SYSTEM TO materialize

                > SELECT * FROM sink_source_view;
                I2 B 1000
                I3 C 1000
                U2 B 1000
                U3 C 1000

                # We check the contents of the sink topics by re-ingesting them.

                > CREATE SOURCE sink_view1_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink1')
                > CREATE TABLE sink_view1 FROM SOURCE sink_view1_src (REFERENCE "sink-sink1")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE SOURCE sink_view2_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink2')
                > CREATE TABLE sink_view2 FROM SOURCE sink_view2_src (REFERENCE "sink-sink2")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE SOURCE sink_view3_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink3')
                > CREATE TABLE sink_view3 FROM SOURCE sink_view3_src (REFERENCE "sink-sink3")
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

                > DROP SOURCE sink_view1_src CASCADE;
                > DROP SOURCE sink_view2_src CASCADE;
                > DROP SOURCE sink_view3_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
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

                # Can be slow with a large transaction
                $ set-sql-timeout duration=240s

                > CREATE MATERIALIZED VIEW sink_large_transaction_view AS SELECT f1 - 1 AS f1 , f2 FROM sink_large_transaction_table;

                > CREATE CLUSTER sink_large_transaction_sink1_cluster SIZE 'scale=1,workers=4';

                > CREATE SINK sink_large_transaction_sink1
                  IN CLUSTER sink_large_transaction_sink1_cluster
                  FROM sink_large_transaction_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-large-transaction-sink-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM;
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
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
                $ set-sql-timeout duration=60s
                $ schema-registry-verify schema-type=avro subject=testdrive-sink-large-transaction-sink-${testdrive.seed}-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","fields":[{"name":"f1","type":"int"},{"name":"f2","type":["null","string"]}]}]},{"name":"after","type":["null","row"]}]}

                # We check the contents of the sink topics by re-ingesting them.
                > CREATE SOURCE sink_large_transaction_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-large-transaction-sink-${testdrive.seed}')
                > CREATE TABLE sink_large_transaction_source FROM SOURCE sink_large_transaction_source_src (REFERENCE "testdrive-sink-large-transaction-sink-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                # Can be slow with a large transaction
                $ set-sql-timeout duration=240s

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

                > DROP SOURCE sink_large_transaction_source_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
class SinkNullDefaults(Check):
    """Check on an Avro sink with NULL DEFAULTS"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas_null()
            + dedent(
                """
                $ kafka-create-topic topic=sink-source-null

                $ kafka-ingest format=avro key-format=avro topic=sink-source-null key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": {"string": "A${kafka-ingest.iteration}"}, "f2": null}

                $ kafka-ingest format=avro key-format=avro topic=sink-source-null key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "D2${kafka-ingest.iteration}"} {"f1": null, "f2": {"long": ${kafka-ingest.iteration}}}

                $ kafka-ingest format=avro key-format=avro topic=sink-source-null key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "U3${kafka-ingest.iteration}"} {"f1": {"string": "A${kafka-ingest.iteration}"}, "f2": null}

                $ kafka-ingest format=avro key-format=avro topic=sink-source-null key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "D3${kafka-ingest.iteration}"} {"f1": null, "f2": {"long": ${kafka-ingest.iteration}}}

                > CREATE SOURCE sink_source_null_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-source-null-${testdrive.seed}')
                > CREATE TABLE sink_source_null FROM SOURCE sink_source_null_src (REFERENCE "testdrive-sink-source-null-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW sink_source_null_view AS SELECT LEFT(key1, 2) as l_k, LEFT(f1, 1) AS l_v1, f2 / 100 AS l_v2, COUNT(*) AS c FROM sink_source_null GROUP BY LEFT(key1, 2), LEFT(f1, 1), f2 / 100;

                > CREATE SINK sink_sink_null1 FROM sink_source_null_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-null1')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ( NULL DEFAULTS )
                  ENVELOPE DEBEZIUM
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas_null() + dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON sink_source_null_view TO materialize
                GRANT USAGE ON CONNECTION kafka_conn TO materialize
                GRANT USAGE ON CONNECTION csr_conn TO materialize

                $ kafka-ingest format=avro key-format=avro topic=sink-source-null key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I2${kafka-ingest.iteration}"} {"f1": {"string": "B${kafka-ingest.iteration}"}, "f2": null}
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": null, "f2": {"long": ${kafka-ingest.iteration}}}
                {"key1": "D2${kafka-ingest.iteration}"}

                > CREATE SINK sink_sink_null2 FROM sink_source_null_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-null2')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ( NULL DEFAULTS )
                  ENVELOPE DEBEZIUM
                """,
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON sink_source_null_view TO materialize
                GRANT USAGE ON CONNECTION kafka_conn TO materialize
                GRANT USAGE ON CONNECTION csr_conn TO materialize

                $ kafka-ingest format=avro key-format=avro topic=sink-source-null key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I2${kafka-ingest.iteration}"} {"f1": {"string": "B${kafka-ingest.iteration}"}, "f2": null}
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": null, "f2": {"long": ${kafka-ingest.iteration}}}
                {"key1": "D2${kafka-ingest.iteration}"}

                > CREATE SINK sink_sink_null3 FROM sink_source_null_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-null3')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ( NULL DEFAULTS )
                  ENVELOPE DEBEZIUM
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                $ schema-registry-verify schema-type=avro subject=sink-sink-null1-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","fields":[{"name":"l_k","type":"string"},{"name":"l_v1","type":["null","string"],"default":null},{"name":"l_v2","type":["null","long"],"default":null},{"name":"c","type":"long"}]}],"default":null},{"name":"after","type":["null","row"],"default":null}]}

                $ schema-registry-verify schema-type=avro subject=sink-sink-null2-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","fields":[{"name":"l_k","type":"string"},{"name":"l_v1","type":["null","string"],"default":null},{"name":"l_v2","type":["null","long"],"default":null},{"name":"c","type":"long"}]}],"default":null},{"name":"after","type":["null","row"],"default":null}]}

                $ schema-registry-verify schema-type=avro subject=sink-sink-null3-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","fields":[{"name":"l_k","type":"string"},{"name":"l_v1","type":["null","string"],"default":null},{"name":"l_v2","type":["null","long"],"default":null},{"name":"c","type":"long"}]}],"default":null},{"name":"after","type":["null","row"],"default":null}]}

                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON sink_source_null_view TO materialize
                GRANT USAGE ON CONNECTION kafka_conn TO materialize
                GRANT USAGE ON CONNECTION csr_conn TO materialize

                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATECLUSTER ON SYSTEM TO materialize

                > SELECT * FROM sink_source_null_view;
                D3 <null> 0 100
                D3 <null> 1 100
                D3 <null> 2 100
                D3 <null> 3 100
                D3 <null> 4 100
                D3 <null> 5 100
                D3 <null> 6 100
                D3 <null> 7 100
                D3 <null> 8 100
                D3 <null> 9 100
                I2 B <null> 1000
                U2 <null> 0 100
                U2 <null> 1 100
                U2 <null> 2 100
                U2 <null> 3 100
                U2 <null> 4 100
                U2 <null> 5 100
                U2 <null> 6 100
                U2 <null> 7 100
                U2 <null> 8 100
                U2 <null> 9 100
                U3 A <null> 1000

                # We check the contents of the sink topics by re-ingesting them.

                > CREATE SOURCE sink_view_null1_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-null1')
                > CREATE TABLE sink_view_null1 FROM SOURCE sink_view_null1_src (REFERENCE "sink-sink-null1")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE SOURCE sink_view_null2_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-null2')
                > CREATE TABLE sink_view_null2 FROM SOURCE sink_view_null2_src (REFERENCE "sink-sink-null2")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE SOURCE sink_view_null3_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-null3')
                > CREATE TABLE sink_view_null3 FROM SOURCE sink_view_null3_src (REFERENCE "sink-sink-null3")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                # Validate the sink by aggregating all the 'before' and 'after' records using SQL
                > SELECT l_v1, l_v2, l_k, SUM(c)
                  FROM (
                    SELECT (after).l_v1, (after).l_v2, (after).l_k, (after).c FROM sink_view_null1
                    UNION ALL
                    SELECT (before).l_v1, (before).l_v2, (before).l_k, -(before).c FROM sink_view_null1
                  ) GROUP BY l_v1, l_v2, l_k
                  HAVING SUM(c) > 0;
                <null> 0 D3 100
                <null> 0 U2 100
                <null> 1 D3 100
                <null> 1 U2 100
                <null> 2 D3 100
                <null> 2 U2 100
                <null> 3 D3 100
                <null> 3 U2 100
                <null> 4 D3 100
                <null> 4 U2 100
                <null> 5 D3 100
                <null> 5 U2 100
                <null> 6 D3 100
                <null> 6 U2 100
                <null> 7 D3 100
                <null> 7 U2 100
                <null> 8 D3 100
                <null> 8 U2 100
                <null> 9 D3 100
                <null> 9 U2 100
                A <null> U3 1000
                B <null> I2 1000

                > SELECT l_v1, l_v2, l_k, SUM(c)
                  FROM (
                    SELECT (after).l_v1, (after).l_v2, (after).l_k, (after).c FROM sink_view_null2
                    UNION ALL
                    SELECT (before).l_v1, (before).l_v2, (before).l_k, -(before).c FROM sink_view_null2
                  ) GROUP BY l_v1, l_v2, l_k
                  HAVING SUM(c) > 0;
                <null> 0 D3 100
                <null> 0 U2 100
                <null> 1 D3 100
                <null> 1 U2 100
                <null> 2 D3 100
                <null> 2 U2 100
                <null> 3 D3 100
                <null> 3 U2 100
                <null> 4 D3 100
                <null> 4 U2 100
                <null> 5 D3 100
                <null> 5 U2 100
                <null> 6 D3 100
                <null> 6 U2 100
                <null> 7 D3 100
                <null> 7 U2 100
                <null> 8 D3 100
                <null> 8 U2 100
                <null> 9 D3 100
                <null> 9 U2 100
                A <null> U3 1000
                B <null> I2 1000

                > SELECT l_v1, l_v2, l_k, SUM(c)
                  FROM (
                    SELECT (after).l_v1, (after).l_v2, (after).l_k, (after).c FROM sink_view_null3
                    UNION ALL
                    SELECT (before).l_v1, (before).l_v2, (before).l_k, -(before).c FROM sink_view_null3
                  ) GROUP BY l_v1, l_v2, l_k
                  HAVING SUM(c) > 0;
                <null> 0 D3 100
                <null> 0 U2 100
                <null> 1 D3 100
                <null> 1 U2 100
                <null> 2 D3 100
                <null> 2 U2 100
                <null> 3 D3 100
                <null> 3 U2 100
                <null> 4 D3 100
                <null> 4 U2 100
                <null> 5 D3 100
                <null> 5 U2 100
                <null> 6 D3 100
                <null> 6 U2 100
                <null> 7 D3 100
                <null> 7 U2 100
                <null> 8 D3 100
                <null> 8 U2 100
                <null> 9 D3 100
                <null> 9 U2 100
                A <null> U3 1000
                B <null> I2 1000

                > DROP SOURCE sink_view_null1_src CASCADE;
                > DROP SOURCE sink_view_null2_src CASCADE;
                > DROP SOURCE sink_view_null3_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
class SinkComments(Check):
    """Check on an Avro sink with comments"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas_null()
            + dedent(
                """
                $ kafka-create-topic topic=sink-sourcecomments

                $ kafka-ingest format=avro key-format=avro topic=sink-source-comments key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": {"string": "A${kafka-ingest.iteration}"}, "f2": null}

                $ kafka-ingest format=avro key-format=avro topic=sink-source-comments key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "D2${kafka-ingest.iteration}"} {"f1": null, "f2": {"long": ${kafka-ingest.iteration}}}

                $ kafka-ingest format=avro key-format=avro topic=sink-source-comments key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "U3${kafka-ingest.iteration}"} {"f1": {"string": "A${kafka-ingest.iteration}"}, "f2": null}

                $ kafka-ingest format=avro key-format=avro topic=sink-source-comments key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "D3${kafka-ingest.iteration}"} {"f1": null, "f2": {"long": ${kafka-ingest.iteration}}}

                > CREATE SOURCE sink_source_comments_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-source-comments-${testdrive.seed}')
                > CREATE TABLE sink_source_comments FROM SOURCE sink_source_comments_src (REFERENCE "testdrive-sink-source-comments-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW sink_source_comments_view AS SELECT LEFT(key1, 2) as l_k, LEFT(f1, 1) AS l_v1, f2 / 100 AS l_v2, COUNT(*) AS c FROM sink_source_comments GROUP BY LEFT(key1, 2), LEFT(f1, 1), f2 / 100

                > COMMENT ON MATERIALIZED VIEW sink_source_comments_view IS 'comment on view sink_source_comments_view'

                > CREATE SINK sink_sink_comments1 FROM sink_source_comments_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-comments1')
                  KEY (l_v2) NOT ENFORCED
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ( NULL DEFAULTS,
                    DOC ON COLUMN sink_source_comments_view.l_v1 = 'doc on l_v1',
                    VALUE DOC ON COLUMN sink_source_comments_view.l_v2 = 'value doc on l_v2',
                    KEY DOC ON COLUMN sink_source_comments_view.l_v2 = 'key doc on l_v2'
                  )
                  ENVELOPE DEBEZIUM
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas_null() + dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON sink_source_comments_view TO materialize
                GRANT USAGE ON CONNECTION kafka_conn TO materialize
                GRANT USAGE ON CONNECTION csr_conn TO materialize

                $ kafka-ingest format=avro key-format=avro topic=sink-source-comments key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I2${kafka-ingest.iteration}"} {"f1": {"string": "B${kafka-ingest.iteration}"}, "f2": null}
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": null, "f2": {"long": ${kafka-ingest.iteration}}}
                {"key1": "D2${kafka-ingest.iteration}"}

                > CREATE SINK sink_sink_comments2 FROM sink_source_comments_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-comments2')
                  KEY (l_v2) NOT ENFORCED
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ( NULL DEFAULTS,
                    DOC ON COLUMN sink_source_comments_view.l_v1 = 'doc on l_v1',
                    VALUE DOC ON COLUMN sink_source_comments_view.l_v2 = 'value doc on l_v2',
                    KEY DOC ON COLUMN sink_source_comments_view.l_v2 = 'key doc on l_v2'
                  )
                  ENVELOPE DEBEZIUM
                """,
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON sink_source_comments_view TO materialize
                GRANT USAGE ON CONNECTION kafka_conn TO materialize
                GRANT USAGE ON CONNECTION csr_conn TO materialize

                $ kafka-ingest format=avro key-format=avro topic=sink-source-comments key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I2${kafka-ingest.iteration}"} {"f1": {"string": "B${kafka-ingest.iteration}"}, "f2": null}
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": null, "f2": {"long": ${kafka-ingest.iteration}}}
                {"key1": "D2${kafka-ingest.iteration}"}

                > CREATE SINK sink_sink_comments3 FROM sink_source_comments_view
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-comments3')
                  KEY (l_v2) NOT ENFORCED
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ( NULL DEFAULTS,
                    DOC ON COLUMN sink_source_comments_view.l_v1 = 'doc on l_v1',
                    VALUE DOC ON COLUMN sink_source_comments_view.l_v2 = 'value doc on l_v2',
                    KEY DOC ON COLUMN sink_source_comments_view.l_v2 = 'key doc on l_v2'
                  )
                  ENVELOPE DEBEZIUM
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                $ schema-registry-verify schema-type=avro subject=sink-sink-comments1-key
                {"type":"record","name":"row","doc":"comment on view sink_source_comments_view","fields":[{"name":"l_v2","type":["null","long"],"default":null,"doc":"key doc on l_v2"}]}

                $ schema-registry-verify schema-type=avro subject=sink-sink-comments2-key
                {"type":"record","name":"row","doc":"comment on view sink_source_comments_view","fields":[{"name":"l_v2","type":["null","long"],"default":null,"doc":"key doc on l_v2"}]}

                $ schema-registry-verify schema-type=avro subject=sink-sink-comments3-key
                {"type":"record","name":"row","doc":"comment on view sink_source_comments_view","fields":[{"name":"l_v2","type":["null","long"],"default":null,"doc":"key doc on l_v2"}]}

                $ schema-registry-verify schema-type=avro subject=sink-sink-comments1-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","doc":"comment on view sink_source_comments_view","fields":[{"name":"l_k","type":"string"},{"name":"l_v1","type":["null","string"],"default":null,"doc":"doc on l_v1"},{"name":"l_v2","type":["null","long"],"default":null,"doc":"value doc on l_v2"},{"name":"c","type":"long"}]}],"default":null},{"name":"after","type":["null","row"],"default":null}]}

                $ schema-registry-verify schema-type=avro subject=sink-sink-comments2-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","doc":"comment on view sink_source_comments_view","fields":[{"name":"l_k","type":"string"},{"name":"l_v1","type":["null","string"],"default":null,"doc":"doc on l_v1"},{"name":"l_v2","type":["null","long"],"default":null,"doc":"value doc on l_v2"},{"name":"c","type":"long"}]}],"default":null},{"name":"after","type":["null","row"],"default":null}]}

                $ schema-registry-verify schema-type=avro subject=sink-sink-comments3-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","doc":"comment on view sink_source_comments_view","fields":[{"name":"l_k","type":"string"},{"name":"l_v1","type":["null","string"],"default":null,"doc":"doc on l_v1"},{"name":"l_v2","type":["null","long"],"default":null,"doc":"value doc on l_v2"},{"name":"c","type":"long"}]}],"default":null},{"name":"after","type":["null","row"],"default":null}]}

                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT SELECT ON sink_source_comments_view TO materialize
                GRANT USAGE ON CONNECTION kafka_conn TO materialize
                GRANT USAGE ON CONNECTION csr_conn TO materialize

                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATECLUSTER ON SYSTEM TO materialize

                > SELECT * FROM sink_source_comments_view;
                D3 <null> 0 100
                D3 <null> 1 100
                D3 <null> 2 100
                D3 <null> 3 100
                D3 <null> 4 100
                D3 <null> 5 100
                D3 <null> 6 100
                D3 <null> 7 100
                D3 <null> 8 100
                D3 <null> 9 100
                I2 B <null> 1000
                U2 <null> 0 100
                U2 <null> 1 100
                U2 <null> 2 100
                U2 <null> 3 100
                U2 <null> 4 100
                U2 <null> 5 100
                U2 <null> 6 100
                U2 <null> 7 100
                U2 <null> 8 100
                U2 <null> 9 100
                U3 A <null> 1000

                # We check the contents of the sink topics by re-ingesting them.

                > CREATE SOURCE sink_view_comments1_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-comments1')
                > CREATE TABLE sink_view_comments1 FROM SOURCE sink_view_comments1_src (REFERENCE "sink-sink-comments1")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE SOURCE sink_view_comments2_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-comments2')
                > CREATE TABLE sink_view_comments2 FROM SOURCE sink_view_comments2_src (REFERENCE "sink-sink-comments2")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE SOURCE sink_view_comments3_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-sink-comments3')
                > CREATE TABLE sink_view_comments3 FROM SOURCE sink_view_comments3_src (REFERENCE "sink-sink-comments3")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                # Validate the sink by aggregating all the 'before' and 'after' records using SQL
                > SELECT l_v1, l_v2, l_k, SUM(c)
                  FROM (
                    SELECT (after).l_v1, (after).l_v2, (after).l_k, (after).c FROM sink_view_comments1
                    UNION ALL
                    SELECT (before).l_v1, (before).l_v2, (before).l_k, -(before).c FROM sink_view_comments1
                  ) GROUP BY l_v1, l_v2, l_k
                  HAVING SUM(c) > 0;
                <null> 0 D3 100
                <null> 0 U2 100
                <null> 1 D3 100
                <null> 1 U2 100
                <null> 2 D3 100
                <null> 2 U2 100
                <null> 3 D3 100
                <null> 3 U2 100
                <null> 4 D3 100
                <null> 4 U2 100
                <null> 5 D3 100
                <null> 5 U2 100
                <null> 6 D3 100
                <null> 6 U2 100
                <null> 7 D3 100
                <null> 7 U2 100
                <null> 8 D3 100
                <null> 8 U2 100
                <null> 9 D3 100
                <null> 9 U2 100
                A <null> U3 1000
                B <null> I2 1000

                > SELECT l_v1, l_v2, l_k, SUM(c)
                  FROM (
                    SELECT (after).l_v1, (after).l_v2, (after).l_k, (after).c FROM sink_view_comments2
                    UNION ALL
                    SELECT (before).l_v1, (before).l_v2, (before).l_k, -(before).c FROM sink_view_comments2
                  ) GROUP BY l_v1, l_v2, l_k
                  HAVING SUM(c) > 0;
                <null> 0 D3 100
                <null> 0 U2 100
                <null> 1 D3 100
                <null> 1 U2 100
                <null> 2 D3 100
                <null> 2 U2 100
                <null> 3 D3 100
                <null> 3 U2 100
                <null> 4 D3 100
                <null> 4 U2 100
                <null> 5 D3 100
                <null> 5 U2 100
                <null> 6 D3 100
                <null> 6 U2 100
                <null> 7 D3 100
                <null> 7 U2 100
                <null> 8 D3 100
                <null> 8 U2 100
                <null> 9 D3 100
                <null> 9 U2 100
                A <null> U3 1000
                B <null> I2 1000

                > SELECT l_v1, l_v2, l_k, SUM(c)
                  FROM (
                    SELECT (after).l_v1, (after).l_v2, (after).l_k, (after).c FROM sink_view_comments3
                    UNION ALL
                    SELECT (before).l_v1, (before).l_v2, (before).l_k, -(before).c FROM sink_view_comments3
                  ) GROUP BY l_v1, l_v2, l_k
                  HAVING SUM(c) > 0;
                <null> 0 D3 100
                <null> 0 U2 100
                <null> 1 D3 100
                <null> 1 U2 100
                <null> 2 D3 100
                <null> 2 U2 100
                <null> 3 D3 100
                <null> 3 U2 100
                <null> 4 D3 100
                <null> 4 U2 100
                <null> 5 D3 100
                <null> 5 U2 100
                <null> 6 D3 100
                <null> 6 U2 100
                <null> 7 D3 100
                <null> 7 U2 100
                <null> 8 D3 100
                <null> 8 U2 100
                <null> 9 D3 100
                <null> 9 U2 100
                A <null> U3 1000
                B <null> I2 1000

                > DROP SOURCE sink_view_comments1_src CASCADE;
                > DROP SOURCE sink_view_comments2_src CASCADE;
                > DROP SOURCE sink_view_comments3_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
class SinkAutoCreatedTopicConfig(Check):
    """Check on a sink with auto-created topic configuration"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas_null()
            + dedent(
                """
                > CREATE TABLE sink_config_table (f1 int)
                > INSERT INTO sink_config_table VALUES (1);

                > CREATE SINK sink_config1 FROM sink_config_table
                  INTO KAFKA CONNECTION kafka_conn (
                    TOPIC 'sink-config1',
                    TOPIC CONFIG MAP['cleanup.policy' => 'compact']
                  )
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas_null() + dedent(s))
            for s in [
                """
                > INSERT INTO sink_config_table VALUES (2);

                > CREATE SINK sink_config2 FROM sink_config_table
                  INTO KAFKA CONNECTION kafka_conn (
                    TOPIC 'sink-config2',
                    TOPIC CONFIG MAP['cleanup.policy' => 'compact']
                  )
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """,
                """
                > INSERT INTO sink_config_table VALUES (3);

                > CREATE SINK sink_config3 FROM sink_config_table
                  INTO KAFKA CONNECTION kafka_conn (
                    TOPIC 'sink-config3',
                    TOPIC CONFIG MAP['cleanup.policy' => 'compact']
                  )
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                $ kafka-verify-topic sink=materialize.public.sink_config1 partition-count=1 topic-config={"cleanup.policy": "compact"}

                $ kafka-verify-topic sink=materialize.public.sink_config2 partition-count=1 topic-config={"cleanup.policy": "compact"}

                $ kafka-verify-topic sink=materialize.public.sink_config3 partition-count=1 topic-config={"cleanup.policy": "compact"}


                # TODO: Reenable this check when kafka-verify-data can deal with validate being run twice
                # $ kafka-verify-data format=avro sink=materialize.public.sink_config1 sort-messages=true
                # {{"before": null, "after": {{"row":{{"f1": 1}}}}}}
                # {{"before": null, "after": {{"row":{{"f1": 2}}}}}}
                # {{"before": null, "after": {{"row":{{"f1": 3}}}}}}

                # $ kafka-verify-data format=avro sink=materialize.public.sink_config2 sort-messages=true
                # {{"before": null, "after": {{"row":{{"f1": 1}}}}}}
                # {{"before": null, "after": {{"row":{{"f1": 2}}}}}}
                # {{"before": null, "after": {{"row":{{"f1": 3}}}}}}

                # $ kafka-verify-data format=avro sink=materialize.public.sink_config3 sort-messages=true
                # {{"before": null, "after": {{"row":{{"f1": 1}}}}}}
                # {{"before": null, "after": {{"row":{{"f1": 2}}}}}}
                # {{"before": null, "after": {{"row":{{"f1": 3}}}}}}
            """
            )
        )


@externally_idempotent(False)
class AlterSink(Check):
    """Check ALTER SINK"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE table_alter1 (x int, y string)
                > CREATE TABLE table_alter2 (x int, y string)
                > CREATE TABLE table_alter3 (x int, y string)
                > CREATE SINK sink_alter FROM table_alter1
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'alter-sink')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                > INSERT INTO table_alter1 VALUES (0, 'a')
                > INSERT INTO table_alter2 VALUES (1, 'b')
                > INSERT INTO table_alter3 VALUES (2, 'c')
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter';

                > ALTER SINK sink_alter SET FROM table_alter2;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter';
                true

                > INSERT INTO table_alter1 VALUES (10, 'aa')
                > INSERT INTO table_alter2 VALUES (11, 'bb')
                > INSERT INTO table_alter3 VALUES (12, 'cc')
                """,
                """
                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter';

                > ALTER SINK sink_alter SET FROM table_alter3;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter';
                true

                > INSERT INTO table_alter1 VALUES (100, 'aaa')
                > INSERT INTO table_alter2 VALUES (101, 'bbb')
                > INSERT INTO table_alter3 VALUES (102, 'ccc')
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                # We check the contents of the sink topics by re-ingesting them.

                > CREATE SOURCE sink_alter_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'alter-sink')
                > CREATE TABLE sink_alter_source FROM SOURCE sink_alter_source_src (REFERENCE "alter-sink")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > SELECT before IS NULL, (after).x, (after).y FROM sink_alter_source
                true 0 a
                true 11 bb
                true 102 ccc

                > DROP SOURCE sink_alter_source_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
class AlterSinkMv(Check):
    """Check ALTER SINK with materialized views"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.134.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE table_alter_mv1 (a INT);
                > INSERT INTO table_alter_mv1 VALUES (0)
                > CREATE MATERIALIZED VIEW mv_alter1 AS SELECT * FROM table_alter_mv1
                > CREATE SINK sink_alter_mv FROM mv_alter1
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-alter-mv')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE table_alter_mv2 (a INT);
                > CREATE MATERIALIZED VIEW mv_alter2 AS SELECT * FROM table_alter_mv2

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_mv';

                > ALTER SINK sink_alter_mv SET FROM mv_alter2;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_mv';
                true

                > INSERT INTO table_alter_mv1 VALUES (10)
                > INSERT INTO table_alter_mv2 VALUES (11)
                """,
                """
                > CREATE TABLE table_alter_mv3 (a INT);
                > CREATE MATERIALIZED VIEW mv_alter3 AS SELECT * FROM table_alter_mv3

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_mv';

                > ALTER SINK sink_alter_mv SET FROM mv_alter3;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_mv';
                true

                > INSERT INTO table_alter_mv1 VALUES (100)
                > INSERT INTO table_alter_mv2 VALUES (101)
                > INSERT INTO table_alter_mv3 VALUES (102)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                > CREATE SOURCE sink_alter_mv_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-alter-mv')
                > CREATE TABLE sink_alter_mv_source FROM SOURCE sink_alter_mv_source_src (REFERENCE "sink-alter-mv")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > SELECT before IS NULL, (after).a FROM sink_alter_mv_source
                true 0
                true 11
                true 102

                > DROP SOURCE sink_alter_mv_source_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
@disabled("due to database-issues#8982")
class AlterSinkLGSource(Check):
    """Check ALTER SINK with a load generator source"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.134.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE SOURCE lg1 FROM LOAD GENERATOR COUNTER (UP TO 2);
                > CREATE SINK sink_alter_lg FROM lg1
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-alter-lg')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE SOURCE lg2 FROM LOAD GENERATOR COUNTER (UP TO 4, TICK INTERVAL '5s');

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_lg';

                > ALTER SINK sink_alter_lg SET FROM lg2;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_lg';
                true
                """,
                """
                > CREATE SOURCE lg3 FROM LOAD GENERATOR COUNTER (UP TO 6, TICK INTERVAL '5s');

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_lg';

                > ALTER SINK sink_alter_lg SET FROM lg3;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_lg';
                true
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                > CREATE SOURCE sink_alter_lg_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-alter-lg')
                > CREATE TABLE sink_alter_lg_source FROM SOURCE sink_alter_lg_source_src (REFERENCE "sink-alter-lg")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > SELECT before IS NULL, (after).counter FROM sink_alter_lg_source
                true 1
                true 2
                true 3
                true 4
                true 5
                true 6

                > DROP SOURCE sink_alter_lg_source_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
@disabled(
    "manual sleep is impossible to get right, this check has to be reworked so as not to flake CI"
)
class AlterSinkPgSource(Check):
    """Check ALTER SINK with a postgres source"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.134.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                CREATE USER postgres3 WITH SUPERUSER PASSWORD 'postgres';
                ALTER USER postgres3 WITH replication;
                DROP PUBLICATION IF EXISTS pg;
                CREATE PUBLICATION pg FOR ALL TABLES;
                DROP TABLE IF EXISTS pg_table1;
                CREATE TABLE pg_table1 (f1 INT);
                ALTER TABLE pg_table1 REPLICA IDENTITY FULL;

                > CREATE SECRET pgpass3 AS 'postgres'
                > CREATE CONNECTION pg_conn1 FOR POSTGRES
                  HOST 'postgres',
                  DATABASE postgres,
                  USER postgres3,
                  PASSWORD SECRET pgpass3
                > CREATE SOURCE pg_source1
                  FROM POSTGRES CONNECTION pg_conn1
                  (PUBLICATION 'pg')
                > CREATE TABLE pg1 FROM SOURCE pg_source1 (REFERENCE pg_table1)
                > CREATE SINK sink_alter_pg FROM pg1
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-alter-pg')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO pg_table1 VALUES (1);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP TABLE IF EXISTS pg_table2;
                CREATE TABLE pg_table2 (f1 INT);
                ALTER TABLE pg_table2 REPLICA IDENTITY FULL;

                > CREATE SOURCE pg_source2
                  FROM POSTGRES CONNECTION pg_conn1
                  (PUBLICATION 'pg')
                > CREATE TABLE pg2 FROM SOURCE pg_source2 (REFERENCE pg_table2)

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_pg';

                > ALTER SINK sink_alter_pg SET FROM pg2;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_pg';
                true

                # Still needs to sleep some before the sink is updated
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="30s"

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO pg_table2 VALUES (2);
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP TABLE IF EXISTS pg_table3;
                CREATE TABLE pg_table3 (f1 INT);
                ALTER TABLE pg_table3 REPLICA IDENTITY FULL;

                > CREATE SOURCE pg_source3
                  FROM POSTGRES CONNECTION pg_conn1
                  (PUBLICATION 'pg')
                > CREATE TABLE pg3 FROM SOURCE pg_source3 (REFERENCE pg_table3)

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_pg';

                > ALTER SINK sink_alter_pg SET FROM pg3;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_pg';
                true

                # Still needs to sleep some before the sink is updated
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="30s"

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO pg_table3 VALUES (3);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                > CREATE SOURCE sink_alter_pg_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-alter-pg')
                > CREATE TABLE sink_alter_pg_source FROM SOURCE sink_alter_pg_source_src (REFERENCE "sink-alter-pg")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > SELECT before IS NULL, (after).f1 FROM sink_alter_pg_source
                true 1
                true 2
                true 3

                > DROP SOURCE sink_alter_pg_source_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
class AlterSinkWebhook(Check):
    """Check ALTER SINK with webhook sources"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.134.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                >[version>=14700] CREATE CLUSTER sink_webhook_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 2;
                >[version<14700] CREATE CLUSTER sink_webhook_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 1;
                > CREATE SOURCE webhook_alter1 IN CLUSTER sink_webhook_cluster FROM WEBHOOK BODY FORMAT TEXT;
                > CREATE SINK sink_alter_wh FROM webhook_alter1
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-alter-wh')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                $ webhook-append database=materialize schema=public name=webhook_alter1
                1
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE SOURCE webhook_alter2 IN CLUSTER sink_webhook_cluster FROM WEBHOOK BODY FORMAT TEXT;

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_wh';

                > ALTER SINK sink_alter_wh SET FROM webhook_alter2;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_wh';
                true

                $ webhook-append database=materialize schema=public name=webhook_alter2
                2
                """,
                """
                > CREATE SOURCE webhook_alter3 IN CLUSTER sink_webhook_cluster FROM WEBHOOK BODY FORMAT TEXT;

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_wh';

                > ALTER SINK sink_alter_wh SET FROM webhook_alter3;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_wh';
                true

                $ webhook-append database=materialize schema=public name=webhook_alter3
                3
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                # Can be slow in 0dt upgrade scenarios
                $ set-sql-timeout duration=480s

                > CREATE SOURCE sink_alter_wh_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink-alter-wh')
                > CREATE TABLE sink_alter_wh_source FROM SOURCE sink_alter_wh_source_src (REFERENCE "sink-alter-wh")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > SELECT before IS NULL, (after).body FROM sink_alter_wh_source
                true 1
                true 2
                true 3

                > DROP SOURCE sink_alter_wh_source_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
class AlterSinkOrder(Check):
    """Check ALTER SINK with a table created after the sink, see incident 131"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE table_alter_order1 (x int, y string)
                > CREATE SINK sink_alter_order FROM table_alter_order1
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'alter-sink-order')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
                > INSERT INTO table_alter_order1 VALUES (0, 'a')
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE table_alter_order2 (x int, y string)

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_order';

                > ALTER SINK sink_alter_order SET FROM table_alter_order2;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_order';
                true

                > INSERT INTO table_alter_order2 VALUES (1, 'b')
                > INSERT INTO table_alter_order1 VALUES (10, 'aa')
                > INSERT INTO table_alter_order2 VALUES (11, 'bb')
                """,
                """
                > CREATE TABLE table_alter_order3 (x int, y string)

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_order';

                > ALTER SINK sink_alter_order SET FROM table_alter_order3;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_order';
                true

                > INSERT INTO table_alter_order3 VALUES (2, 'c')
                > INSERT INTO table_alter_order3 VALUES (12, 'cc')
                > INSERT INTO table_alter_order1 VALUES (100, 'aaa')
                > INSERT INTO table_alter_order2 VALUES (101, 'bbb')
                > INSERT INTO table_alter_order3 VALUES (102, 'ccc')
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                # We check the contents of the sink topics by re-ingesting them.

                > CREATE SOURCE sink_alter_order_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'alter-sink-order')
                > CREATE TABLE sink_alter_order_source FROM SOURCE sink_alter_order_source_src (REFERENCE "alter-sink-order")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > SELECT before IS NULL, (after).x, (after).y FROM sink_alter_order_source
                true 0 a
                true 1 b
                true 2 c
                true 11 bb
                true 12 cc
                true 102 ccc

                > DROP SOURCE sink_alter_order_source_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
class SinkFormat(Check):
    """Check SINK with KEY FORMAT and VALUE FORMAT"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE TABLE sink_format_table (f1 INTEGER, f2 TEXT, f3 INT, PRIMARY KEY (f1));
                > CREATE DEFAULT INDEX ON sink_format_table;
                > INSERT INTO sink_format_table VALUES (1, 'A', 10);
                > CREATE CLUSTER sink_format_sink1_cluster SIZE 'scale=1,workers=1';

                > CREATE SINK sink_format_sink1
                  IN CLUSTER sink_format_sink1_cluster
                  FROM sink_format_table
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-format-sink-${testdrive.seed}')
                  KEY (f1)
                  KEY FORMAT TEXT
                  VALUE FORMAT JSON
                  ENVELOPE UPSERT;
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                > INSERT INTO sink_format_table VALUES (2, 'B', 20);
                """,
                """
                > INSERT INTO sink_format_table VALUES (3, 'C', 30);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=60s
                # We check the contents of the sink topics by re-ingesting them.
                > CREATE SOURCE sink_format_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-format-sink-${testdrive.seed}')
                > CREATE TABLE sink_format_source FROM SOURCE sink_format_source_src (REFERENCE "testdrive-sink-format-sink-${testdrive.seed}")
                  KEY FORMAT TEXT
                  VALUE FORMAT JSON
                  ENVELOPE UPSERT

                > SELECT key, data->>'f2', data->>'f3' FROM sink_format_source
                1 A 10
                2 B 20
                3 C 30

                > DROP SOURCE sink_format_source_src CASCADE;
            """
            )
        )


@externally_idempotent(False)
class SinkPartitionByDebezium(Check):
    """Check SINK with ENVELOPE DEBEZIUM and PARTITION BY"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE TABLE sink_partition_by_debezium_table (f1 INTEGER, f2 TEXT, PRIMARY KEY (f1));
                > CREATE DEFAULT INDEX ON sink_partition_by_debezium_table;

                > INSERT INTO sink_partition_by_debezium_table SELECT generate_series, REPEAT('x', 1024) FROM generate_series(1, 100000);

                > CREATE MATERIALIZED VIEW sink_partition_by_debezium_view AS SELECT f1 - 1 AS f1 , f2 FROM sink_partition_by_debezium_table;

                > CREATE CLUSTER sink_partition_by_debezium_sink1_cluster SIZE 'scale=1,workers=4';

                > CREATE SINK sink_partition_by_debezium_sink1
                  IN CLUSTER sink_partition_by_debezium_sink1_cluster
                  FROM sink_partition_by_debezium_view
                  INTO KAFKA CONNECTION kafka_conn (
                    TOPIC 'testdrive-sink-partition-by-debezium-sink-${testdrive.seed}',
                    TOPIC PARTITION COUNT 4,
                    PARTITION BY f1
                  )
                  KEY (f1) NOT ENFORCED
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM;
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                > UPDATE sink_partition_by_debezium_table SET f2 = REPEAT('y', 1024)
                """,
                """
                > UPDATE sink_partition_by_debezium_table SET f2 = REPEAT('z', 1024)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                # Can be slow in 0dt upgrade scenarios
                $ set-sql-timeout duration=480s

                $ schema-registry-verify schema-type=avro subject=testdrive-sink-partition-by-debezium-sink-${testdrive.seed}-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","fields":[{"name":"f1","type":"int"},{"name":"f2","type":["null","string"]}]}]},{"name":"after","type":["null","row"]}]}

                # We check the contents of the sink topics by re-ingesting them.
                > CREATE SOURCE sink_partition_by_debezium_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-partition-by-debezium-sink-${testdrive.seed}')
                > CREATE TABLE sink_partition_by_debezium_source
                  FROM SOURCE sink_partition_by_debezium_source_src (REFERENCE "testdrive-sink-partition-by-debezium-sink-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE MATERIALIZED VIEW sink_partition_by_debezium_view2
                  AS
                  SELECT COUNT(*) AS c1 , COUNT(f1) AS c2, COUNT(DISTINCT f1) AS c3 , MIN(f1), MAX(f1)
                  FROM (
                    SELECT (before).f1, (before).f2 FROM sink_partition_by_debezium_source
                  )

                > CREATE MATERIALIZED VIEW sink_partition_by_debezium_view3
                  AS
                  SELECT COUNT(*) AS c1 , COUNT(f1) AS c2, COUNT(DISTINCT f1) AS c3 , MIN(f1), MAX(f1)
                  FROM (
                    SELECT (after).f1, (after).f2 FROM sink_partition_by_debezium_source
                  )

                > CREATE MATERIALIZED VIEW sink_partition_by_debezium_view4
                  AS
                  SELECT LEFT(f2, 1), SUM(c)
                  FROM (
                    SELECT (after).f2, COUNT(*) AS c FROM sink_partition_by_debezium_source GROUP BY (after).f2
                    UNION ALL
                    SELECT (before).f2, -COUNT(*) AS c  FROM sink_partition_by_debezium_source GROUP BY (before).f2
                  )
                  GROUP BY f2

                > SELECT * FROM sink_partition_by_debezium_view2
                300000 200000 100000 0 99999

                > SELECT * FROM sink_partition_by_debezium_view3
                300000 300000 100000 0 99999

                > SELECT * FROM sink_partition_by_debezium_view4
                <null> -100000
                x 0
                y 0
                z 100000

                > DROP SOURCE sink_partition_by_debezium_source_src CASCADE;

                # TODO: kafka-verify-data when it can deal with being run twice, to check the actual partitioning
            """
            )
        )
