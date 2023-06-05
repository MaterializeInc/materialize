# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from textwrap import dedent
from typing import Callable

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized, Redpanda, Testdrive, Toxiproxy

SERVICES = [
    Materialized(options=["--persist-pubsub-url=http://toxiproxy:6879"]),
    Redpanda(),
    Toxiproxy(),
    Testdrive(no_reset=True, seed=1),
]

SCHEMA = dedent(
    """
    $ set keyschema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"long"}
        ]
      }

    $ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"long"}
        ]
      }
    """
)


@dataclass
class Disruption:
    name: str
    breakage: Callable
    fixage: Callable


disruptions = [
    Disruption(
        name="kill-pubsub",
        breakage=lambda c: c.kill("toxiproxy"),
        fixage=lambda c: toxiproxy_start(c),
    ),
    Disruption(
        name="pause-pubsub",
        breakage=lambda c: c.pause("toxiproxy"),
        fixage=lambda c: c.unpause("toxiproxy"),
    ),
]


def workflow_default(c: Composition) -> None:
    """Test that the system is able to make progress in the face of PubSub disruptions."""

    for disruption in disruptions:
        c.down(destroy_volumes=True)
        c.up("redpanda", "materialized")
        c.up("testdrive", persistent=True)

        toxiproxy_start(c)

        c.testdrive(
            input=SCHEMA
            + dedent(
                """
                > CREATE TABLE t1 (f1 INTEGER, f2 INTEGER);
                $ kafka-create-topic topic=pubsub-disruption partitions=4

                > CREATE CONNECTION IF NOT EXISTS csr_conn
                  TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}');

                > CREATE CONNECTION IF NOT EXISTS kafka_conn
                  TO KAFKA (BROKER '${testdrive.kafka-addr}');

                > INSERT INTO t1 SELECT generate_series, 1 FROM generate_series(1,1000000);
                $ kafka-ingest format=avro key-format=avro topic=pubsub-disruption schema=${schema} key-schema=${keyschema} start-iteration=1 repeat=1000000
                {"f1": ${kafka-ingest.iteration}} {"f2": 1}

                > CREATE SOURCE s1
                  FROM KAFKA CONNECTION kafka_conn
                  (TOPIC 'testdrive-pubsub-disruption-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW v1 AS
                  SELECT COUNT(*) AS c1, COUNT(DISTINCT f1) AS c2, COUNT(DISTINCT f2) AS c3,
                         MIN(f1) AS min1, MIN(f2) AS min2, MAX(f1) AS max1, MAX(f2) AS max2
                  FROM t1;

                > CREATE MATERIALIZED VIEW v2 AS
                  SELECT COUNT(*) AS c1, COUNT(DISTINCT f1) AS c2, COUNT(DISTINCT f2) AS c3,
                         MIN(f1) AS min1, MIN(f2) AS min2, MAX(f1) AS max1, MAX(f2) AS max2
                  FROM s1;

                > UPDATE t1 SET f2 = 2;
                $ kafka-ingest format=avro key-format=avro topic=pubsub-disruption schema=${schema} key-schema=${keyschema} start-iteration=1 repeat=1000000
                {"f1": ${kafka-ingest.iteration}} {"f2": 2}
                """
            )
        )

        disruption.breakage(c)

        c.testdrive(
            input=SCHEMA
            + dedent(
                """
                > UPDATE t1 SET f2 = 3;
                $ kafka-ingest format=avro key-format=avro topic=pubsub-disruption schema=${schema} key-schema=${keyschema} start-iteration=1 repeat=1000000
                {"f1": ${kafka-ingest.iteration}} {"f2": 3}

                > SELECT * FROM v1
                1000000 1000000 1 1 3 1000000 3

                > SELECT * FROM v2
                1000000 1000000 1 1 3 1000000 3

                # Create more views during the disruption
                > CREATE MATERIALIZED VIEW v3 AS
                  SELECT COUNT(*) AS c1, COUNT(DISTINCT f1) AS c2, COUNT(DISTINCT f2) AS c3,
                         MIN(f1) AS min1, MIN(f2) AS min2, MAX(f1) AS max1, MAX(f2) AS max2
                  FROM t1;

                > CREATE MATERIALIZED VIEW v4 AS
                  SELECT COUNT(*) AS c1, COUNT(DISTINCT f1) AS c2, COUNT(DISTINCT f2) AS c3,
                         MIN(f1) AS min1, MIN(f2) AS min2, MAX(f1) AS max1, MAX(f2) AS max2
                  FROM s1;

                """
            )
        )

        disruption.fixage(c)

        c.testdrive(
            input=SCHEMA
            + dedent(
                """
                > UPDATE t1 SET f2 = 4;
                $ kafka-ingest format=avro key-format=avro topic=pubsub-disruption schema=${schema} key-schema=${keyschema} start-iteration=1 repeat=1000000
                {"f1": ${kafka-ingest.iteration}} {"f2": 4}

                > SELECT * FROM v1
                1000000 1000000 1 1 4 1000000 4

                > SELECT * FROM v2
                1000000 1000000 1 1 4 1000000 4

                > SELECT * FROM v3
                1000000 1000000 1 1 4 1000000 4

                > SELECT * FROM v4
                1000000 1000000 1 1 4 1000000 4
                """
            )
        )


def toxiproxy_start(c: Composition) -> None:
    c.up("toxiproxy")
    c.testdrive(
        input=dedent(
            """
                $ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
                {
                   "name": "pubsub",
                   "listen": "0.0.0.0:6879",
                   "upstream": "materialized:6879",
                   "enabled": true
                }
                """
        )
    )
