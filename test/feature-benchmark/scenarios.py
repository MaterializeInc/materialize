# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.feature_benchmark.measurement_source import Lambda, Td
from materialize.feature_benchmark.scenario import Scenario


class Sleep(Scenario):
    """Dummy benchmark that measures the duration of mz_sleep(0.1)"""

    BENCHMARK = Td(
        """
> /* A */ SELECT 1
1

> /* B */ SELECT mz_internal.mz_sleep(0.1);
<null>
"""
    )


class FastPath(Scenario):
    """Feature benchmarks related to the "fast path" in query execution, as described in the
    'Internals of One-off Queries' presentation.
    """


class FastPathFilterNoIndex(FastPath):
    """Measure the time it takes for the fast path to filter our all rows from a materialized view and return"""

    INIT = Td(
        """
> CREATE TABLE ten (f1 INTEGER);
> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

> CREATE MATERIALIZED VIEW v1 (f1, f2) AS
  SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) +
  (a7.f1 * 1000000) AS f1,
  1 AS f2
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6, ten AS a7;

> SELECT COUNT(*) = 10000000 FROM v1;
true
"""
    )

    BENCHMARK = Td(
        """
> /* A */ SELECT 1;
1

> /* B */ SELECT * FROM v1 WHERE f2 < 0;

"""
    )


class FastPathFilterIndex(FastPath):
    """Measure the time it takes for the fast path to filter our all rows from a materialized view using an index and return"""

    INIT = Td(
        """
> CREATE TABLE ten (f1 INTEGER);
> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

> CREATE MATERIALIZED VIEW v1 AS
  SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) AS f1
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> SELECT COUNT(*) = 1000000 FROM v1;
true
"""
    )

    # Since an individual query of this particular type being benchmarked takes 1ms to execute, the results are susceptible
    # to a lot of random noise. As we can not make the query any slower by using e.g. a large dataset,
    # we run the query 100 times in a row and measure the total execution time.

    BENCHMARK = Td(
        """
> BEGIN

> /* A */ SELECT 1;
1
"""
        + "\n".join(
            [
                """
> SELECT * FROM v1 WHERE f1 = 1;
1
1
1
1
1
1
1
1
1
1
"""
                for i in range(0, 100)
            ]
        )
        + """
> /* B */ SELECT 1;
1

"""
    )


class FastPathOrderByLimit(FastPath):
    """Benchmark the case SELECT * FROM materialized_view ORDER BY <key> LIMIT <i>"""

    INIT = Td(
        """
> CREATE TABLE ten (f1 INTEGER);
> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

> CREATE MATERIALIZED VIEW v1 AS
  SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f1
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> SELECT COUNT(*) = 1000000 FROM v1;
true
"""
    )

    BENCHMARK = Td(
        """
> /* A */ SELECT 1;
1

> /* B */ SELECT f1 FROM v1 ORDER BY f1 DESC LIMIT 1000;
"""
        + "\n".join([str(x) for x in range(999000, 1000000)])
    )


class DML(Scenario):
    """Benchmarks around the performance of DML statements"""

    pass


class Insert(DML):
    """Measure the time it takes for an INSERT statement to return."""

    INIT = Td(
        """
> CREATE TABLE ten (f1 INTEGER);
> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
"""
    )

    BENCHMARK = Td(
        """
> DROP TABLE IF EXISTS t1;

> /* A */ CREATE TABLE t1 (f1 INTEGER);
> /* B */ INSERT INTO t1 SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000)
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;
"""
    )


class Update(DML):
    """Measure the time it takes for an UPDATE statement to return to client"""

    INIT = Td(
        """
> CREATE TABLE ten (f1 INTEGER);
> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

> CREATE TABLE t1 (f1 BIGINT);
> INSERT INTO t1 SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000)
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;
"""
    )

    BENCHMARK = Td(
        """
> /* A */ SELECT 1
1

> /* B */ UPDATE t1 SET f1 = f1 + 10000000
"""
    )


class InsertAndSelect(DML):
    """Measure the time it takes for an INSERT statement to return
    AND for a follow-up SELECT to return data, that is, for the
    dataflow to be completely caught up.
    """

    INIT = Td(
        """
> CREATE TABLE ten (f1 INTEGER);
> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
"""
    )

    BENCHMARK = Td(
        """
> DROP TABLE IF EXISTS t1;

> /* A */ CREATE TABLE t1 (f1 INTEGER);

> INSERT INTO t1 SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000)
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> /* B */ SELECT 1 FROM t1 WHERE f1 = 1;
1
"""
    )


class Dataflow(Scenario):
    """Benchmark scenarios around individual dataflow patterns/operators"""

    pass


class OrderBy(Dataflow):
    """Benchmark ORDER BY as executed by the dataflow layer,
    in contrast with an ORDER BY executed using a Finish step in the coordinator"""

    INIT = Td(
        """
> CREATE TABLE ten (f1 INTEGER);

> CREATE MATERIALIZED VIEW v1 AS
  SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f1
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

# Just to spice things up a bit, we perform individual
# inserts here so that the rows are assigned separate timestamps

> INSERT INTO ten VALUES (0);

> INSERT INTO ten VALUES (1);

> INSERT INTO ten VALUES (2);

> INSERT INTO ten VALUES (3);

> INSERT INTO ten VALUES (4);

> INSERT INTO ten VALUES (5);

> INSERT INTO ten VALUES (6);

> INSERT INTO ten VALUES (7);

> INSERT INTO ten VALUES (8);

> INSERT INTO ten VALUES (9);

> SELECT COUNT(*) = 1000000 FROM v1;
true
"""
    )

    BENCHMARK = Td(
        """
> DROP VIEW IF EXISTS v2
  /* A */

# explicit LIMIT is needed for the ORDER BY to not be optimized away
> CREATE MATERIALIZED VIEW v2 AS SELECT * FROM v1 ORDER BY f1 LIMIT 999999999999

> SELECT COUNT(*) FROM v2
  /* B */
1000000
"""
    )


class CountDistinct(Dataflow):
    INIT = Td(
        """
> CREATE VIEW ten (f1) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9));
> CREATE MATERIALIZED VIEW v1 AS
  SELECT
  a1.f1 +
  (a2.f1 * 10) AS f1,
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) +
  (a7.f1 * 1000000) /* +
  (a8.f1 * 10000000) */ AS unique
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6, ten AS a7;


> SELECT COUNT(*) = 10000000 FROM v1;
true
"""
    )

    BENCHMARK = Td(
        """
> /* A */ SELECT 1
1

> /* B */ SELECT COUNT(DISTINCT f1) AS f1 FROM v1;
100
"""
    )


class MinMax(Dataflow):
    INIT = Td(
        """
> CREATE VIEW ten (f1) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9));
> CREATE MATERIALIZED VIEW v1 AS SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f1
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> SELECT COUNT(*) = 1000000 FROM v1;
true
"""
    )

    BENCHMARK = Td(
        """
> /* A */ SELECT 1
1

> /* B */ SELECT MIN(f1), MAX(f1) AS f1 FROM v1;
0 999999
"""
    )


class GroupBy(Dataflow):
    INIT = Td(
        """
> CREATE VIEW ten (f1) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9));
> CREATE MATERIALIZED VIEW v1 AS SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f1,
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f2
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> SELECT COUNT(*) = 1000000 FROM v1
true
"""
    )

    BENCHMARK = Td(
        """
> /* A */ SELECT 1
1

> /* B */ SELECT COUNT(*), MIN(f1_min), MAX(f1_max) FROM (SELECT f2, MIN(f1) AS f1_min, MAX(f1) AS f1_max FROM v1 GROUP BY f2);
1000000 0 999999
"""
    )


class CrossJoin(Dataflow):

    INIT = Td(
        """
> CREATE VIEW ten (f1) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9));
"""
    )

    BENCHMARK = Td(
        """
> DROP VIEW IF EXISTS v1;

> /* A */ CREATE MATERIALIZED VIEW v1 AS
  SELECT a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000)
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6

> /* B */ SELECT COUNT(*) = 1000000 AS f1 FROM v1;
true
"""
    )


class Retraction(Dataflow):
    """Benchmark the time it takes to process a very large retraction"""

    BENCHMARK = Td(
        """
> DROP VIEW IF EXISTS v1;

> DROP TABLE IF EXISTS ten;

> CREATE TABLE ten (f1 INTEGER);

> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

> CREATE MATERIALIZED VIEW v1 AS
  SELECT a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000)
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6

> SELECT COUNT(*) = 1000000 AS f1 FROM v1;
true

> /* A */ SELECT 1;
1

> DELETE FROM ten;

> /* B */ SELECT COUNT(*) FROM v1;
0
"""
    )


class CreateIndex(Dataflow):
    """Measure the time it takes for CREATE INDEX to return *plus* the time
    it takes for a SELECT query that would use the index to return rows.
    """

    INIT = Td(
        """
> CREATE TABLE ten (f1 INTEGER);
> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

> CREATE TABLE t1 (f1 INTEGER, f2 INTEGER);
> INSERT INTO t1 (f1) SELECT a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000)
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

# Make sure the dataflow is fully hydrated
> SELECT 1 FROM t1 WHERE f1 = 0;
1
"""
    )

    BENCHMARK = Td(
        """
> /* A */ DROP INDEX IF EXISTS i1;

> CREATE INDEX i1 ON t1(f1);

> /* B */ SELECT COUNT(*)
  FROM t1 AS a1, t1 AS a2
  WHERE a1.f1 = a2.f1
  AND a1.f1 = 0
  AND a2.f1 = 0;
1
"""
    )


class DeltaJoin(Dataflow):
    INIT = Td(
        """
> CREATE VIEW ten (f1) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9));

> CREATE MATERIALIZED VIEW v1 AS
  SELECT a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f1
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

"""
    )

    BENCHMARK = Td(
        """
> /* A */ SELECT 1;
1


> /* B */ SELECT COUNT(*) FROM v1 AS a1 JOIN v1 AS a2 USING (f1);
1000000
"""
    )


class DifferentialJoin(Dataflow):
    INIT = Td(
        """
> CREATE VIEW ten (f1) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9));

> CREATE MATERIALIZED VIEW v1 AS
  SELECT a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f1,
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f2
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

"""
    )

    BENCHMARK = Td(
        """
> /* A */ SELECT 1;
1


> /* B */ SELECT COUNT(*) FROM v1 AS a1 JOIN v1 AS a2 USING (f1);
1000000
"""
    )


class Finish(Scenario):
    """Benchmarks around te Finish stage of query processing"""


class FinishOrderByLimit(Finish):
    """Benchmark ORDER BY + LIMIT without the benefit of an index"""

    INIT = Td(
        """
> CREATE VIEW ten (f1) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9));
> CREATE MATERIALIZED VIEW v1 AS SELECT
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f1,
  a1.f1 +
  (a2.f1 * 10) +
  (a3.f1 * 100) +
  (a4.f1 * 1000) +
  (a5.f1 * 10000) +
  (a6.f1 * 100000) AS f2
  FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> SELECT COUNT(*) = 1000000 FROM v1;
true
"""
    )

    BENCHMARK = Td(
        """
> /* A */ SELECT 1
1

> /* B */ SELECT f2 FROM v1 ORDER BY 1 DESC LIMIT 1;
999999
"""
    )


class KafkaScenario(Scenario):
    pass


class KafkaRaw(KafkaScenario):
    SHARED = Td(
        """
$ set count=1000000

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"long"}
        ]
    }

$ kafka-create-topic topic=kafka-raw

$ kafka-ingest format=avro topic=kafka-raw schema=${schema} publish=true repeat=${count}
{"f1": 1}
"""
    )
    BENCHMARK = Td(
        """
$ set count=1000000

> DROP SOURCE IF EXISTS s1;

> SELECT COUNT(*) = 0
  FROM mz_kafka_source_statistics
  WHERE CAST(statistics->'topics'->'testdrive-kafka-raw-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT) > 0
true

> /* A */ CREATE MATERIALIZED SOURCE s1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka-raw-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

> /* B */ SELECT SUM(CAST(statistics->'topics'->'testdrive-kafka-raw-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) = ${count}
  FROM mz_kafka_source_statistics;
true
"""
    )


class KafkaUpsert(KafkaScenario):
    SHARED = Td(
        """
$ set count=1000000

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=kafka-upsert

$ kafka-ingest format=avro topic=kafka-upsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=${count}
{"f1": 1} {"f2": ${kafka-ingest.iteration}}

$ kafka-ingest format=avro topic=kafka-upsert key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"f1": 2} {"f2": 2}
"""
    )
    BENCHMARK = Td(
        """

> DROP SOURCE IF EXISTS s1;

> /* A */ CREATE MATERIALIZED SOURCE s1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka-upsert-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT;

> /* B */ SELECT f1 FROM s1;
1
2
"""
    )


class KafkaUpsertUnique(KafkaScenario):
    SHARED = Td(
        """
$ set keyschema={"type": "record", "name": "Key", "fields": [ {"name": "f1", "type": "long"} ] }

$ set schema={"type" : "record", "name" : "test", "fields": [ {"name": "f2", "type": "long"} ] }

$ kafka-create-topic topic=upsert-unique partitions=16

$ kafka-ingest format=avro topic=upsert-unique key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=1000000
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}
"""
    )
    BENCHMARK = Td(
        """
> DROP SOURCE IF EXISTS s1;

> /* A */ CREATE MATERIALIZED SOURCE s1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-upsert-unique-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT;

> /* B */ SELECT COUNT(*) FROM s1;
1000000
"""
    )


class KafkaRecovery(KafkaScenario):
    SHARED = Td(
        """
$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=kafka-recovery partitions=8

$ kafka-ingest format=avro topic=kafka-recovery key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000000
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}
"""
    )

    INIT = Td(
        """
> CREATE MATERIALIZED SOURCE s1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka-recovery-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT;

# Make sure we are fully caught up before continuing
> SELECT COUNT(*) = 10000000 FROM s1;
true
"""
    )

    BEFORE = Lambda(lambda e: e.RestartMz())

    BENCHMARK = Td(
        """
> /* A */ SELECT 1;
1

> /* B */ SELECT COUNT(*) = 10000000 FROM s1;
true
"""
    )


class Sink(Scenario):
    pass


class ExactlyOnce(Sink):
    """Measure the time it takes to emit 1M records to a reuse_topic=true sink. As we have limited
    means to figure out when the complete output has been emited, we have no option of re-ingesting
    the data again to determine completion.
    """

    SHARED = Td(
        """
$ set keyschema={"type": "record", "name": "Key", "fields": [ {"name": "f1", "type": "long"} ] }

$ set schema={"type" : "record", "name" : "test", "fields": [ {"name": "f2", "type": "long"} ] }

$ kafka-create-topic topic=sink-input partitions=16

$ kafka-ingest format=avro topic=sink-input key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=1000000
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}
"""
    )

    INIT = Td(
        """
> CREATE MATERIALIZED SOURCE source1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-sink-input-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT;

> /* B */ SELECT COUNT(*) FROM source1;
1000000
"""
    )

    BENCHMARK = Td(
        """
> DROP SINK IF EXISTS sink1;

> DROP SOURCE IF EXISTS sink1_check CASCADE;
  /* A */

> CREATE SINK sink1 FROM source1
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-sink-output-${testdrive.seed}'
  KEY (f1)
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# Wait until all the records have been emited from the sink, as observed by the sink1_check source

> CREATE SOURCE sink1_check
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-sink-output-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT;

> CREATE MATERIALIZED VIEW sink1_check_v AS SELECT COUNT(*) FROM sink1_check;

> SELECT * FROM sink1_check_v
  /* B */
1000000
"""
    )
