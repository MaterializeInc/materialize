# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from typing import Callable

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Computed,
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Postgres(),
    Materialized(
        options="--persist-consensus-url postgres://postgres:postgres@postgres"
    ),
    Testdrive(),
]


@dataclass
class Disruption:
    name: str
    disruption: Callable


disruptions = [
    Disruption(
        name="pause-one-computed",
        disruption=lambda c: c.pause("computed_1_1"),
    ),
    Disruption(
        name="kill-all-computed",
        disruption=lambda c: c.kill("computed_1_1", "computed_1_2"),
    ),
    Disruption(
        name="pause-in-materialized-view",
        disruption=lambda c: c.testdrive(
            """
> SET cluster=cluster1

> CREATE TABLE sleep_table (sleep INTEGER);

> CREATE MATERIALIZED VIEW sleep_view AS SELECT mz_internal.mz_sleep(sleep) FROM sleep_table;

> INSERT INTO sleep_table SELECT 1200 FROM generate_series(1,32)
""",
        ),
    ),
    Disruption(
        name="drop-cluster",
        disruption=lambda c: c.testdrive(
            """
> DROP CLUSTER cluster1
""",
        ),
    ),
    Disruption(
        name="panic-in-insert-select",
        disruption=lambda c: c.testdrive(
            """
> SET cluster=cluster1
> SET statement_timeout='1s'

> CREATE TABLE panic_table (f1 TEXT);

> INSERT INTO panic_table VALUES ('panic!');

! INSERT INTO panic_table SELECT mz_internal.mz_panic(f1) FROM panic_table;
contains: statement timeout
""",
        ),
    ),
]


def workflow_default(c: Composition) -> None:
    """Test cluster isolation by introducing faults of various kinds in cluster1
    and then making sure that cluster2 continues to operate properly
    """

    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])
    for id, disruption in enumerate(disruptions):
        run_test(c, disruption, id)


def populate(c: Composition) -> None:
    # Create some database objects
    c.testdrive(
        """
> SET cluster=cluster1

> DROP TABLE IF EXISTS t1 CASCADE;

> CREATE TABLE t1 (f1 TEXT);

> INSERT INTO t1 VALUES (1), (2);

> CREATE VIEW v1 AS SELECT COUNT(*) AS c1 FROM t1;

> CREATE DEFAULT INDEX i1 IN CLUSTER cluster2 ON v1;
""",
    )


def validate(c: Composition) -> None:
    # Validate that cluster2 continues to operate
    c.testdrive(
        """
# Dataflows

> SET cluster=cluster2

> SELECT * FROM v1;
2

# Tables

> INSERT INTO t1 VALUES (3);

> SELECT * FROM t1;
1
2
3

# Introspection tables

> SHOW CLUSTERS LIKE 'cluster2'
cluster2

> SELECT name FROM mz_tables WHERE name = 't1';
t1

# DDL statements

> CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) AS c1 FROM t1;

> SELECT * FROM v2;
3

> CREATE INDEX i2 IN CLUSTER cluster2 ON t1 (f1);

> SELECT f1 FROM t1;
1
2
3

# Tables

> CREATE TABLE t2 (f1 INTEGER);

> INSERT INTO t2 VALUES (1);

> INSERT INTO t2 SELECT * FROM t2;

> SELECT * FROM t2;
1
1

# Sources

$ kafka-create-topic topic=source1 partitions=1
$ kafka-ingest format=bytes topic=source1
A

> CREATE MATERIALIZED SOURCE source1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-source1-${testdrive.seed}'
  FORMAT BYTES

> SELECT * FROM source1
A 1

# Sinks

> CREATE SINK sink1 FROM v1
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sink1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.sink1 sort-messages=true
{"before": null, "after": {"row":{"c1": 3}}}
""",
    )


def run_test(c: Composition, disruption: Disruption, id: int) -> None:
    print(f"+++ Running disruption scenario {disruption.name}")

    c.up("testdrive", persistent=True)
    c.up("postgres")
    c.wait_for_postgres()
    c.up("materialized")
    c.wait_for_materialized(service="materialized")

    nodes = [
        Computed(
            name="computed_1_1",
            peers=["computed_1_1", "computed_1_2"],
        ),
        Computed(
            name="computed_1_2",
            peers=["computed_1_1", "computed_1_2"],
        ),
        Computed(
            name="computed_2_1",
            peers=["computed_2_1", "computed_2_2"],
        ),
        Computed(
            name="computed_2_2",
            peers=["computed_2_1", "computed_2_2"],
        ),
    ]

    with c.override(*nodes):
        c.up(*[n.name for n in nodes])

        c.sql(
            """
            DROP CLUSTER IF EXISTS cluster1 CASCADE;
            CREATE CLUSTER cluster1 REPLICAS (replica1 (REMOTE ('computed_1_1:2100', 'computed_1_2:2100')));
            """
        )

        c.sql(
            """
            DROP CLUSTER IF EXISTS cluster2 CASCADE;
            CREATE CLUSTER cluster2 REPLICAS (replica1 (REMOTE ('computed_2_1:2100', 'computed_2_2:2100')));
            """
        )

        with c.override(
            Testdrive(
                validate_data_dir=False,
                no_reset=True,
                materialized_params={"cluster": "cluster2"},
                seed=id,
            )
        ):
            populate(c)

            # Disrupt cluster1 by some means
            disruption.disruption(c)

            validate(c)

        cleanup_list = [
            "materialized",
            "testdrive",
            "postgres",
            *[n.name for n in nodes],
        ]
        c.kill(*cleanup_list)
        c.rm(*cleanup_list, destroy_volumes=True)

    c.rm_volumes("mzdata", "pgdata")
