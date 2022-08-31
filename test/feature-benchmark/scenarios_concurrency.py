# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.feature_benchmark.action import Action, TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario


class Concurrency(Scenario):
    """Feature benchmarks related to testing concurrency aspects of the system"""


class ParallelIngestion(Concurrency):
    """Measure the time it takes to ingest multiple sources concurrently."""

    SOURCES = 10

    def shared(self) -> Action:
        return TdAction(
            self.schema()
            + self.keyschema()
            + f"""
$ kafka-create-topic topic=kafka-parallel-ingestion partitions=4

$ kafka-ingest format=avro topic=kafka-parallel-ingestion key-format=avro key-schema=${{keyschema}} schema=${{schema}} repeat={self.n()}
{{"f1": ${{kafka-ingest.iteration}} }} {{"f2": ${{kafka-ingest.iteration}} }}
"""
        )

    def benchmark(self) -> MeasurementSource:
        sources = range(1, ParallelIngestion.SOURCES + 1)
        drop_sources = "\n".join(
            [
                f"""
> DROP SOURCE IF EXISTS s{s}
"""
                for s in sources
            ]
        )

        create_sources = "\n".join(
            [
                f"""
> CREATE CONNECTION IF NOT EXISTS csr_conn
FOR CONFLUENT SCHEMA REGISTRY
URL '${{testdrive.schema-registry-url}}';

> CREATE SOURCE s{s}
  FROM KAFKA BROKER '${{testdrive.kafka-addr}}' TOPIC 'testdrive-kafka-parallel-ingestion-${{testdrive.seed}}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn'
"""
                for s in sources
            ]
        )

        create_indexes = "\n".join(
            [
                f"""
> CREATE DEFAULT INDEX ON s{s}
"""
                for s in sources
            ]
        )

        selects = "\n".join(
            [
                f"""
> SELECT * FROM s{s} WHERE f2 = {self.n()-1}
{self.n()-1}
"""
                for s in sources
            ]
        )

        return Td(
            self.schema()
            + f"""
{drop_sources}

{create_sources}

> SELECT 1
  /* A */
1

{create_indexes}

{selects}

> SELECT 1
  /* B */
1
"""
        )


class ParallelDataflows(Concurrency):
    """Measure the time it takes to compute multiple parallel dataflows."""

    SCALE = 4
    VIEWS = 100

    def benchmark(self) -> MeasurementSource:
        views = range(1, ParallelDataflows.VIEWS + 1)

        create_views = "\n".join(
            [
                f"""
> CREATE MATERIALIZED VIEW v{v} AS
  SELECT COUNT(DISTINCT f1) + {v} - {v} AS f1
  FROM t1
"""
                for v in views
            ]
        )

        selects = "\n".join(
            [
                f"""
> SELECT * FROM v{v}
{self.n()}
"""
                for v in views
            ]
        )

        return Td(
            f"""
> DROP TABLE IF EXISTS t1 CASCADE

> CREATE TABLE t1 (f1 INTEGER)

{create_views}

> SELECT 1
  /* A */
1

> INSERT INTO t1 SELECT * FROM generate_series(1,{self.n()})

{selects}

> SELECT 1
  /* B */
1
"""
        )
