# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from math import ceil, floor
from typing import List

from parameterized import parameterized_class  # type: ignore

from materialize.feature_benchmark.action import Action, Kgen, TdAction
from materialize.feature_benchmark.measurement_source import (
    Lambda,
    MeasurementSource,
    Td,
)
from materialize.feature_benchmark.scenario import (
    BenchmarkingSequence,
    Scenario,
    ScenarioBig,
)


class FastPath(Scenario):
    """Feature benchmarks related to the "fast path" in query execution, as described in the
    'Internals of One-off Queries' presentation.
    """


class FastPathFilterNoIndex(FastPath):
    """Measure the time it takes for the fast path to filter our all rows from a materialized view and return"""

    SCALE = 7

    def init(self) -> List[Action]:
        return [
            self.table_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 (f1, f2) AS SELECT {self.unique_values()} AS f1, 1 AS f2 FROM {self.join()}

> CREATE DEFAULT INDEX ON v1;

> SELECT COUNT(*) = {self.n()} FROM v1;
true
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            """
> /* A */ SELECT 1;
1
> /* B */ SELECT * FROM v1 WHERE f2 < 0;
"""
        )


class MFPPushdown(Scenario):
    """Test MFP pushdown -- WHERE clause with a suitable condition and no index defined."""

    SCALE = 7

    def init(self) -> List[Action]:
        return [
            self.table_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 (f1, f2) AS SELECT {self.unique_values()} AS f1, 1 AS f2 FROM {self.join()}

> SELECT COUNT(*) = {self.n()} FROM v1;
true
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            """
> /* A */ SELECT 1;
1
> /* B */ SELECT * FROM v1 WHERE f2 < 0;
"""
        )


class FastPathFilterIndex(FastPath):
    """Measure the time it takes for the fast path to filter our all rows from a materialized view using an index and return"""

    def init(self) -> List[Action]:
        return [
            self.table_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1 FROM {self.join()}

> CREATE DEFAULT INDEX ON v1;

> SELECT COUNT(*) = {self.n()} FROM v1;
true
"""
            ),
        ]

    # Since an individual query of this particular type being benchmarked takes 1ms to execute, the results are susceptible
    # to a lot of random noise. As we can not make the query any slower by using e.g. a large dataset,
    # we run the query 100 times in a row and measure the total execution time.

    def benchmark(self) -> MeasurementSource:
        hundred_selects = "\n".join(
            "> SELECT * FROM v1 WHERE f1 = 1;\n1\n" for i in range(0, 1000)
        )

        return Td(
            f"""
> BEGIN

> SELECT 1;
  /* A */
1

{hundred_selects}

> SELECT 1
  /* B */
1
"""
        )


class FastPathOrderByLimit(FastPath):
    """Benchmark the case SELECT * FROM materialized_view ORDER BY <key> LIMIT <i>"""

    def init(self) -> List[Action]:
        return [
            self.table_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1 FROM {self.join()};

> CREATE DEFAULT INDEX ON v1;

> SELECT COUNT(*) = {self.n()} FROM v1;
true
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            """
> SELECT 1;
  /* A */
1
> SELECT f1 FROM v1 ORDER BY f1 DESC LIMIT 1000
  /* B */
"""
            + "\n".join([str(x) for x in range(self.n() - 1000, self.n())])
        )


class DML(Scenario):
    """Benchmarks around the performance of DML statements"""

    pass


class Insert(DML):
    """Measure the time it takes for an INSERT statement to return."""

    def init(self) -> Action:
        return self.table_ten()

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> DROP TABLE IF EXISTS t1;

> CREATE TABLE t1 (f1 INTEGER)
  /* A */

> INSERT INTO t1 SELECT {self.unique_values()} FROM {self.join()}
  /* B */
"""
        )


class InsertBatch(DML):
    """Measure the time it takes for a batch of INSERT statements to return."""

    SCALE = 4

    def benchmark(self) -> MeasurementSource:
        inserts = "\n".join(
            f"> INSERT INTO t1 VALUES ({i});" for i in range(0, self.n())
        )

        return Td(
            f"""
> DROP TABLE IF EXISTS t1;

> CREATE TABLE t1 (f1 INTEGER)
  /* A */

> BEGIN

{inserts}

> COMMIT
  /* B */
"""
        )


class InsertMultiRow(DML):
    """Measure the time it takes for a single multi-row INSERT statement to return."""

    SCALE = 4

    def benchmark(self) -> MeasurementSource:
        values = ", ".join(f"({i})" for i in range(0, self.n()))

        return Td(
            f"""
> DROP TABLE IF EXISTS t1;

> CREATE TABLE t1 (f1 INTEGER)
  /* A */

> INSERT INTO t1 VALUES {values}
  /* B */
"""
        )


class Update(DML):
    """Measure the time it takes for an UPDATE statement to return to client"""

    def init(self) -> List[Action]:
        return [
            self.table_ten(),
            TdAction(
                f"""
> CREATE TABLE t1 (f1 BIGINT);

> CREATE DEFAULT INDEX ON t1;

> INSERT INTO t1 SELECT {self.unique_values()} FROM {self.join()}
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> SELECT 1
  /* A */
1

> UPDATE t1 SET f1 = f1 + {self.n()}
  /* B */
"""
        )


class UpdateMultiNoIndex(DML):
    """Measure the time it takes to perform multiple updates over the same records in a non-indexed table. GitHub Issue #11071"""

    def before(self) -> Action:
        # Due to exterme variability in the results, we have no option but to drop and re-create
        # the table prior to each measurement
        return TdAction(
            f"""
> DROP TABLE IF EXISTS t1;

> CREATE TABLE t1 (f1 BIGINT);

> INSERT INTO t1 SELECT * FROM generate_series(0, {self.n()})
"""
        )

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> SELECT 1
  /* A */
1

> UPDATE t1 SET f1 = f1 + {self.n()}

> SELECT COUNT(*) FROM t1 WHERE f1 > {self.n()}
  /* B */
{self.n()}
"""
        )


class InsertAndSelect(DML):
    """Measure the time it takes for an INSERT statement to return
    AND for a follow-up SELECT to return data, that is, for the
    dataflow to be completely caught up.
    """

    def init(self) -> Action:
        return self.table_ten()

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> DROP TABLE IF EXISTS t1;

> CREATE TABLE t1 (f1 INTEGER)
  /* A */

> INSERT INTO t1 SELECT {self.unique_values()} FROM {self.join()};

> SELECT 1 FROM t1 WHERE f1 = 1
  /* B */
1
"""
        )


class Dataflow(Scenario):
    """Benchmark scenarios around individual dataflow patterns/operators"""

    pass


class OrderBy(Dataflow):
    """Benchmark ORDER BY as executed by the dataflow layer,
    in contrast with an ORDER BY executed using a Finish step in the coordinator"""

    def init(self) -> Action:
        # Just to spice things up a bit, we perform individual
        # inserts here so that the rows are assigned separate timestamps
        inserts = "\n\n".join(f"> INSERT INTO ten VALUES ({i})" for i in range(0, 10))

        return TdAction(
            f"""
> CREATE TABLE ten (f1 INTEGER);

> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1 FROM {self.join()};

{inserts}

> SELECT COUNT(*) = {self.n()} FROM v1;
true
"""
        )

    def benchmark(self) -> MeasurementSource:
        # Explicit LIMIT is needed for the ORDER BY to not be optimized away
        return Td(
            f"""
> DROP MATERIALIZED VIEW IF EXISTS v2
  /* A */

> CREATE MATERIALIZED VIEW v2 AS SELECT * FROM v1 ORDER BY f1 LIMIT 999999999999

> SELECT COUNT(*) FROM v2
  /* B */
{self.n()}
"""
        )


class CountDistinct(Dataflow):
    def init(self) -> List[Action]:
        return [
            self.view_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1, {self.unique_values()} AS f2 FROM {self.join()};

> SELECT COUNT(*) = {self.n()} FROM v1;
true
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> SELECT 1
  /* A */
1

> SELECT COUNT(DISTINCT f1) AS f1 FROM v1
  /* B */
{self.n()}
"""
        )


class MinMax(Dataflow):
    def init(self) -> List[Action]:
        return [
            self.view_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1 FROM {self.join()};

> SELECT COUNT(*) = {self.n()} FROM v1;
true
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> SELECT 1
  /* A */
1

> SELECT MIN(f1), MAX(f1) AS f1 FROM v1
  /* B */
0 {self.n()-1}
"""
        )


class GroupBy(Dataflow):
    def init(self) -> List[Action]:
        return [
            self.view_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1, {self.unique_values()} AS f2 FROM {self.join()}

> SELECT COUNT(*) = {self.n()} FROM v1
true
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> SELECT 1
  /* A */
1

> SELECT COUNT(*), MIN(f1_min), MAX(f1_max) FROM (SELECT f2, MIN(f1) AS f1_min, MAX(f1) AS f1_max FROM v1 GROUP BY f2)
  /* B */
{self.n()} 0 {self.n()-1}
"""
        )


class CrossJoin(Dataflow):
    def init(self) -> Action:
        return self.view_ten()

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> DROP MATERIALIZED VIEW IF EXISTS v1;

> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} FROM {self.join()}
  /* A */

> SELECT COUNT(*) = {self.n()} AS f1 FROM v1;
  /* B */
true
"""
        )


class Retraction(Dataflow):
    """Benchmark the time it takes to process a very large retraction"""

    def before(self) -> Action:
        return TdAction(
            f"""
> DROP TABLE IF EXISTS ten CASCADE;

> CREATE TABLE ten (f1 INTEGER);

> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} FROM {self.join()}

> SELECT COUNT(*) = {self.n()} AS f1 FROM v1;
true
"""
        )

    def benchmark(self) -> MeasurementSource:
        return Td(
            """
> SELECT 1
  /* A */
1

> DELETE FROM ten;

> SELECT COUNT(*) FROM v1
  /* B */
0
"""
        )


class CreateIndex(Dataflow):
    """Measure the time it takes for CREATE INDEX to return *plus* the time
    it takes for a SELECT query that would use the index to return rows.
    """

    def init(self) -> List[Action]:
        return [
            self.table_ten(),
            TdAction(
                f"""
> CREATE TABLE t1 (f1 INTEGER, f2 INTEGER);
> INSERT INTO t1 (f1) SELECT {self.unique_values()} FROM {self.join()}

# Make sure the dataflow is fully hydrated
> SELECT 1 FROM t1 WHERE f1 = 0;
1
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            """
> DROP INDEX IF EXISTS i1;
  /* A */

> CREATE INDEX i1 ON t1(f1);

> SELECT COUNT(*)
  FROM t1 AS a1, t1 AS a2
  WHERE a1.f1 = a2.f1
  AND a1.f1 = 0
  AND a2.f1 = 0
  /* B */
1
"""
        )


class DeltaJoin(Dataflow):
    def init(self) -> List[Action]:
        return [
            self.view_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1 FROM {self.join()}
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> SELECT 1;
  /* A */
1

# Delta joins require 2+ tables
> SELECT COUNT(*) FROM v1 AS a1 , v1 AS a2 , v1 AS a3 WHERE a1.f1 = a2.f1 AND a2.f1 = a3.f1
  /* B */
{self.n()}
"""
        )


class DifferentialJoin(Dataflow):
    def init(self) -> List[Action]:
        return [
            self.view_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1, {self.unique_values()} AS f2 FROM {self.join()}
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> SELECT 1;
  /* A */
1


> SELECT COUNT(*) FROM v1 AS a1 JOIN v1 AS a2 USING (f1)
  /* B */
{self.n()}
"""
        )


class FullOuterJoin(Dataflow):
    def benchmark(self) -> BenchmarkingSequence:
        columns_select = ", ".join(
            [f"a{i+1}.f1 AS f{i+1}" for i in range(0, floor(self.scale()))]
        )
        columns_using = ", ".join([f"f{i+1}" for i in range(0, floor(self.scale()))])
        inserts = "\n".join([f"> INSERT INTO ten VALUES ({i+1})" for i in range(0, 10)])

        return [
            Td(
                f"""
> DROP MATERIALIZED VIEW IF EXISTS v2 CASCADE;

> DROP MATERIALIZED VIEW IF EXISTS v1 CASCADE;

> DROP TABLE IF EXISTS ten;

> CREATE TABLE ten (f1 INTEGER);

> CREATE MATERIALIZED VIEW v1 AS SELECT {columns_select} FROM {self.join()}
> SELECT 1;
  /* A */
1

> CREATE MATERIALIZED VIEW v2 AS
  SELECT COUNT(a1.f1) AS c1, COUNT(a2.f1) AS c2
  FROM v1 AS a1
  FULL OUTER JOIN v1 AS a2 USING ({columns_using});

{inserts}

> SELECT * FROM v2;
  /* B */
{self.n()} {self.n()}
"""
            )
        ]


class Finish(Scenario):
    """Benchmarks around te Finish stage of query processing"""


class FinishOrderByLimit(Finish):
    """Benchmark ORDER BY + LIMIT without the benefit of an index"""

    def init(self) -> List[Action]:
        return [
            self.view_ten(),
            TdAction(
                f"""
> CREATE MATERIALIZED VIEW v1 AS SELECT {self.unique_values()} AS f1, {self.unique_values()} AS f2 FROM {self.join()}

> SELECT COUNT(*) = {self.n()} FROM v1;
true
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> SELECT 1
  /* A */
1

> SELECT f2 FROM v1 ORDER BY 1 DESC LIMIT 1
  /* B */
{self.n()-1}
"""
        )


class Kafka(Scenario):
    pass


class KafkaEnvelopeNoneBytes(Kafka):
    def shared(self) -> Action:
        data = "a" * 512
        return TdAction(
            f"""
$ kafka-create-topic topic=kafka-envelope-none-bytes

$ kafka-ingest format=bytes topic=kafka-envelope-none-bytes repeat={self.n()}
{data}
"""
        )

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> DROP CONNECTION IF EXISTS s1_kafka_conn CASCADE

> CREATE CONNECTION s1_kafka_conn
  TO KAFKA (BROKER '${{testdrive.kafka-addr}}')

> CREATE SOURCE s1
  FROM KAFKA CONNECTION s1_kafka_conn (TOPIC 'testdrive-kafka-envelope-none-bytes-${{testdrive.seed}}')
  FORMAT BYTES
  ENVELOPE NONE
  /* A */

> SELECT COUNT(*) = {self.n()} FROM s1
  /* B */
true
"""
        )


class KafkaUpsert(Kafka):
    def shared(self) -> Action:
        return TdAction(
            self.keyschema()
            + self.schema()
            + f"""
$ kafka-create-topic topic=kafka-upsert

$ kafka-ingest format=avro topic=kafka-upsert key-format=avro key-schema=${{keyschema}} schema=${{schema}} repeat={self.n()}
{{"f1": 1}} {{"f2": ${{kafka-ingest.iteration}} }}

$ kafka-ingest format=avro topic=kafka-upsert key-format=avro key-schema=${{keyschema}} schema=${{schema}}
{{"f1": 2}} {{"f2": 2}}
"""
        )

    def benchmark(self) -> MeasurementSource:
        return Td(
            """
> DROP CONNECTION IF EXISTS s1_kafka_conn CASCADE

> CREATE CONNECTION s1_kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}')

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SOURCE s1
  FROM KAFKA CONNECTION s1_kafka_conn (TOPIC 'testdrive-kafka-upsert-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT
  /* A */

> SELECT f1 FROM s1
  /* B */
1
2
"""
        )


class KafkaUpsertUnique(Kafka):
    def shared(self) -> Action:
        return TdAction(
            self.keyschema()
            + self.schema()
            + f"""
$ kafka-create-topic topic=upsert-unique partitions=16

$ kafka-ingest format=avro topic=upsert-unique key-format=avro key-schema=${{keyschema}} schema=${{schema}} repeat={self.n()}
{{"f1": ${{kafka-ingest.iteration}} }} {{"f2": ${{kafka-ingest.iteration}} }}
"""
        )

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> DROP CONNECTION IF EXISTS s1_kafka_conn CASCADE
> DROP CONNECTION IF EXISTS s1_csr_conn CASCADE

> CREATE CONNECTION s1_kafka_conn
  TO KAFKA (BROKER '${{testdrive.kafka-addr}}')

> CREATE CONNECTION IF NOT EXISTS s1_csr_conn
  TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');
  /* A */

> CREATE SOURCE s1
  FROM KAFKA CONNECTION s1_kafka_conn (TOPIC 'testdrive-upsert-unique-${{testdrive.seed}}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION s1_csr_conn
  ENVELOPE UPSERT

> SELECT COUNT(*) FROM s1;
  /* B */
{self.n()}
"""
        )


class KafkaRestart(Scenario):
    def shared(self) -> Action:
        return TdAction(
            self.keyschema()
            + self.schema()
            + f"""
$ kafka-create-topic topic=kafka-recovery partitions=8

$ kafka-ingest format=avro topic=kafka-recovery key-format=avro key-schema=${{keyschema}} schema=${{schema}} repeat={self.n()}
{{"f1": ${{kafka-ingest.iteration}} }} {{"f2": ${{kafka-ingest.iteration}} }}
"""
        )

    def init(self) -> Action:
        return TdAction(
            f"""
> DROP CONNECTION IF EXISTS s1_kafka_conn CASCADE
> DROP CONNECTION IF EXISTS s1_csr_conn CASCADE

> CREATE CONNECTION s1_kafka_conn
  TO KAFKA (BROKER '${{testdrive.kafka-addr}}')

> CREATE CONNECTION IF NOT EXISTS s1_csr_conn
  TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');

> CREATE SOURCE s1
  FROM KAFKA CONNECTION s1_kafka_conn (TOPIC 'testdrive-kafka-recovery-${{testdrive.seed}}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION s1_csr_conn
  ENVELOPE UPSERT;

# Make sure we are fully caught up before continuing
> SELECT COUNT(*) FROM s1;
{self.n()}

# Give time for any background tasks (e.g. compaction) to settle down
> SELECT mz_internal.mz_sleep(10)
<null>
"""
        )

    def benchmark(self) -> BenchmarkingSequence:
        return [
            Lambda(lambda e: e.RestartMz()),
            Td(
                f"""
> SELECT COUNT(*) /* {self.n()} */ FROM s1;
  /* B */
{self.n()}
"""
            ),
        ]


class KafkaRestartBig(ScenarioBig):
    """Ingest 100M records without constructing
    a dataflow that would keep all of them in memory. For the purpose, we
    emit a bunch of "EOF" records after the primary ingestion is complete
    and consider that the source has caught up when all the EOF records have
    been seen.
    """

    SCALE = 8

    def shared(self) -> List[Action]:
        return [
            TdAction("$ kafka-create-topic topic=kafka-recovery-big partitions=8"),
            # Ingest 10 ** SCALE records
            Kgen(
                topic="kafka-recovery-big",
                args=[
                    "--keys=random",
                    f"--num-records={self.n()}",
                    "--values=bytes",
                    "--max-message-size=32",
                    "--min-message-size=32",
                    "--key-min=256",
                    f"--key-max={256+(self.n()**2)}",
                ],
            ),
            # Add 256 EOF markers with key values <= 256.
            # This high number is chosen as to guarantee that there will be an EOF marker
            # in each partition, even if the number of partitions is increased in the future.
            Kgen(
                topic="kafka-recovery-big",
                args=[
                    "--keys=sequential",
                    "--num-records=256",
                    "--values=bytes",
                    "--min-message-size=32",
                    "--max-message-size=32",
                ],
            ),
        ]

    def init(self) -> Action:
        return TdAction(
            """
> CREATE CONNECTION s1_kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}')

> CREATE SOURCE s1
  FROM KAFKA CONNECTION s1_kafka_conn (TOPIC 'testdrive-kafka-recovery-big-${testdrive.seed}')
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
  ENVELOPE UPSERT;

# Confirm that all the EOF markers generated above have been processed
> CREATE MATERIALIZED VIEW s1_is_complete AS SELECT COUNT(*) = 256 FROM s1 WHERE key <= '\\x00000000000000ff'

> SELECT * FROM s1_is_complete;
true
"""
        )

    def benchmark(self) -> BenchmarkingSequence:
        return [
            Lambda(lambda e: e.RestartMz()),
            Td(
                """
> SELECT * FROM s1_is_complete
  /* B */
true
"""
            ),
        ]


@parameterized_class(
    [{"SCALE": i} for i in [5, 6, 7, 8, 9]], class_name_func=Scenario.name_with_scale
)
class KafkaEnvelopeNoneBytesScalability(ScenarioBig):
    """Run the same scenario across different scales. Do not materialize the entire
    source but rather just a non-memory-consuming view on top of it.
    """

    def shared(self) -> List[Action]:
        return [
            TdAction(
                """
$ kafka-create-topic topic=kafka-scalability partitions=8
"""
            ),
            Kgen(
                topic="kafka-scalability",
                args=[
                    "--keys=sequential",
                    f"--num-records={self.n()}",
                    "--values=bytes",
                    "--max-message-size=100",
                    "--min-message-size=100",
                ],
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> DROP CONNECTION IF EXISTS s1_kafka_conn CASCADE

> CREATE CONNECTION s1_kafka_conn
  TO KAFKA (BROKER '${{testdrive.kafka-addr}}')

> CREATE SOURCE s1
  FROM KAFKA CONNECTION s1_kafka_conn (TOPIC 'testdrive-kafka-scalability-${{testdrive.seed}}')
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
  ENVELOPE NONE
  /* A */

> CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM s1;

> SELECT c = {self.n()} FROM v1
  /* B */
true
"""
        )


class Sink(Scenario):
    pass


class ExactlyOnce(Sink):
    """Measure the time it takes to emit 1M records to a reuse_topic=true sink. As we have limited
    means to figure out when the complete output has been emited, we have no option but to re-ingest
    the data to determine completion.
    """

    def shared(self) -> Action:
        return TdAction(
            self.keyschema()
            + self.schema()
            + f"""
$ kafka-create-topic topic=sink-input partitions=16

$ kafka-ingest format=avro topic=sink-input key-format=avro key-schema=${{keyschema}} schema=${{schema}} repeat={self.n()}
{{"f1": ${{kafka-ingest.iteration}} }} {{"f2": ${{kafka-ingest.iteration}} }}
"""
        )

    def init(self) -> Action:
        return TdAction(
            f"""
> CREATE CONNECTION IF NOT EXISTS kafka_conn
  TO KAFKA (BROKER '${{testdrive.kafka-addr}}')

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${{testdrive.schema-registry-url}}';

> CREATE SOURCE source1
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-input-${{testdrive.seed}}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

> SELECT COUNT(*) FROM source1;
{self.n()}
"""
        )

    def benchmark(self) -> MeasurementSource:
        return Td(
            """
> DROP SINK IF EXISTS sink1;
> DROP SOURCE IF EXISTS sink1_check CASCADE;
  /* A */

> CREATE SINK sink1 FROM source1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-output-${testdrive.seed}')
  KEY (f1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# Wait until all the records have been emited from the sink, as observed by the sink1_check source

> CREATE SOURCE sink1_check
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-output-${testdrive.seed}')
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

> CREATE MATERIALIZED VIEW sink1_check_v AS SELECT COUNT(*) FROM sink1_check;

> SELECT * FROM sink1_check_v
  /* B */
"""
            + str(self.n())
        )


class PgCdc(Scenario):
    pass


class PgCdcInitialLoad(PgCdc):
    """Measure the time it takes to read 1M existing records from Postgres
    when creating a materialized source"""

    def shared(self) -> Action:
        return TdAction(
            f"""
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE pk_table (pk BIGINT PRIMARY KEY, f2 BIGINT);
INSERT INTO pk_table SELECT x, x*2 FROM generate_series(1, {self.n()}) as x;
ALTER TABLE pk_table REPLICA IDENTITY FULL;
"""
        )

    def before(self) -> Action:
        return TdAction(
            """
> DROP SOURCE IF EXISTS mz_source_pgcdc;
            """
        )

    def benchmark(self) -> MeasurementSource:
        return Td(
            f"""
> CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'

> CREATE CONNECTION IF NOT EXISTS pg_conn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE SOURCE mz_source_pgcdc
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source')
  FOR TABLES ("pk_table")
  /* A */

> SELECT count(*) FROM pk_table
  /* B */
{self.n()}
            """
        )


class PgCdcStreaming(PgCdc):
    """Measure the time it takes to ingest records from Postgres post-snapshot"""

    SCALE = 5

    def shared(self) -> Action:
        return TdAction(
            """
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS p1;
CREATE PUBLICATION p1 FOR ALL TABLES;
"""
        )

    def before(self) -> Action:
        return TdAction(
            """
> DROP SOURCE IF EXISTS s1;

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (pk SERIAL PRIMARY KEY, f2 BIGINT);
ALTER TABLE t1 REPLICA IDENTITY FULL;

> CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'

> CREATE CONNECTION IF NOT EXISTS pg_conn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE SOURCE s1
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'p1')
  FOR TABLES ("t1")
            """
        )

    def benchmark(self) -> MeasurementSource:
        insertions = "\n".join(
            [
                f"INSERT INTO t1 (f2) SELECT x FROM generate_series(1, {self.n()/1000}) as x;\nCOMMIT;"
                for i in range(0, 1000)
            ]
        )

        return Td(
            f"""
> SELECT 1;
  /* A */
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
{insertions}

> SELECT count(*) FROM t1
  /* B */
{self.n()}
            """
        )


class Coordinator(Scenario):
    """Feature benchmarks pertaining to the coordinator."""


class QueryLatency(Coordinator):
    SCALE = 3
    """Measure the time it takes to run SELECT 1 queries"""

    def benchmark(self) -> MeasurementSource:
        selects = "\n".join("> SELECT 1\n1\n" for i in range(0, self.n()))

        return Td(
            f"""
> BEGIN

> SELECT 1;
  /* A */
1

{selects}

> SELECT 1;
  /* B */
1
"""
        )


class ConnectionLatency(Coordinator):
    """Measure the time it takes to establish connections to Mz"""

    SCALE = 2  # Many connections * many measurements = TCP port exhaustion

    def benchmark(self) -> MeasurementSource:
        connections = "\n".join(
            """
$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
SELECT 1;
"""
            for i in range(0, self.n())
        )

        return Td(
            f"""
> BEGIN

> SELECT 1;
  /* A */
1

{connections}

> SELECT 1;
  /* B */
1
"""
        )


class Startup(Scenario):
    pass


class StartupEmpty(Startup):
    """Measure the time it takes to restart an empty Mz instance."""

    def benchmark(self) -> BenchmarkingSequence:
        return [
            Lambda(lambda e: e.RestartMz()),
            Td(
                """
> SELECT 1;
  /* B */
1
"""
            ),
        ]


class StartupLoaded(Scenario):
    """Measure the time it takes to restart a populated Mz instance and have all the dataflows be ready to return something"""

    SCALE = 1.2  # 25 objects of each kind
    FIXED_SCALE = (
        True  # Can not scale to 100s of objects, so --size=+N will have no effect
    )

    def shared(self) -> Action:
        return TdAction(
            self.schema()
            + """
$ kafka-create-topic topic=startup-time

$ kafka-ingest format=avro topic=startup-time schema=${schema} repeat=1
{"f2": 1}
"""
        )

    def init(self) -> Action:
        create_tables = "\n".join(
            f"> CREATE TABLE t{i} (f1 INTEGER);\n> INSERT INTO t{i} DEFAULT VALUES;"
            for i in range(0, self.n())
        )
        create_sources = "\n".join(
            f"""
> CREATE SOURCE source{i}
  FROM KAFKA CONNECTION s1_kafka_conn (TOPIC 'testdrive-startup-time-${{testdrive.seed}}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION s1_csr_conn
  ENVELOPE NONE
"""
            for i in range(0, self.n())
        )
        join = " ".join(
            f"LEFT JOIN source{i} USING (f2)" for i in range(1, (ceil(self.scale())))
        )

        create_views = "\n".join(
            f"> CREATE MATERIALIZED VIEW v{i} AS SELECT * FROM source{i} AS s {join} LIMIT {i+1}"
            for i in range(0, self.n())
        )

        create_sinks = "\n".join(
            f"""
> CREATE SINK sink{i} FROM source{i}
  INTO KAFKA CONNECTION s1_kafka_conn (TOPIC 'testdrive-sink-output-${{testdrive.seed}}')
  KEY (f2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION s1_csr_conn
  ENVELOPE DEBEZIUM
"""
            for i in range(0, self.n())
        )

        return TdAction(
            f"""
$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_objects_per_schema = {self.n() * 10};
ALTER SYSTEM SET max_materialized_views = {self.n() * 2};
ALTER SYSTEM SET max_sources = {self.n() * 2};
ALTER SYSTEM SET max_sinks = {self.n() * 2};
ALTER SYSTEM SET max_tables = {self.n() * 2};

> CREATE CONNECTION IF NOT EXISTS s1_kafka_conn
  TO KAFKA (BROKER '${{testdrive.kafka-addr}}')

> CREATE CONNECTION IF NOT EXISTS s1_csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${{testdrive.schema-registry-url}}';

{create_tables}
{create_sources}
{create_views}
{create_sinks}
"""
        )

    def benchmark(self) -> BenchmarkingSequence:
        check_tables = "\n".join(
            f"> SELECT COUNT(*) >= 0 FROM t{i}\ntrue" for i in range(0, self.n())
        )
        check_sources = "\n".join(
            f"> SELECT COUNT(*) > 0 FROM source{i}\ntrue" for i in range(0, self.n())
        )
        check_views = "\n".join(
            f"> SELECT COUNT(*) > 0 FROM v{i}\ntrue" for i in range(0, self.n())
        )

        return [
            Lambda(lambda e: e.RestartMz()),
            Td(
                f"""
{check_views}
{check_sources}
{check_tables}
> SELECT 1;
  /* B */
1
"""
            ),
        ]
