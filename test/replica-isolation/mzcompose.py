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
from materialize.mzcompose.services import (
    Computed,
    Kafka,
    Localstack,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Materialized(),
    Testdrive(volumes=["mzdata:/mzdata"]),
]


def populate(c: Composition) -> None:
    # Create some database objects
    c.testdrive(
        dedent(
            """
            > CREATE TABLE t1 (f1 INTEGER);
            > INSERT INTO t1 SELECT * FROM generate_series(1, 10);
            > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c1 FROM t1;
            > CREATE TABLE ten (f1 INTEGER);
            > INSERT INTO ten SELECT * FROM generate_series(1, 10);
            > CREATE MATERIALIZED VIEW expensive AS SELECT (a1.f1 * 1) +
              (a2.f1 * 10) +
              (a3.f1 * 100) +
              (a4.f1 * 1000) +
              (a5.f1 * 10000) +
              (a6.f1 * 100000) +
              (a7.f1 * 1000000)
              FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6, ten AS a7;
            $ kafka-create-topic topic=source1
            $ kafka-ingest format=bytes topic=source1 repeat=1000000
            A${kafka-ingest.iteration}
            > CREATE MATERIALIZED SOURCE source1
              FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-source1-${testdrive.seed}'
              FORMAT BYTES
            > CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) FROM source1
            """
        ),
    )


def restart_replica(c: Composition) -> None:
    c.kill("computed_1_1", "computed_1_2")
    c.up("computed_1_1", "computed_1_2")


def drop_create_replica(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
            > DROP CLUSTER REPLICA cluster1.replica1
            > CREATE CLUSTER REPLICA cluster1.replica3 REMOTE ['computed_1_1:2100', 'computed_1_2:2100']
            """
        )
    )


def create_invalid_replica(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
            > CREATE CLUSTER REPLICA cluster1.replica3 REMOTE ['no_such_host:2100']
            """
        )
    )


def validate(c: Composition) -> None:
    # Validate that the cluster continues to operate
    c.testdrive(
        dedent(
            """
            # Dataflows

            > SELECT * FROM v1;
            10

            # Existing sources
            $ kafka-ingest format=bytes topic=source1 repeat=1000000
            B${kafka-ingest.iteration}
            > SELECT * FROM v2;
            2000000

            # Existing tables
            > INSERT INTO t1 VALUES (20);
            > SELECT * FROM v1;
            11

            # New materialized views
            > CREATE MATERIALIZED VIEW v3 AS SELECT COUNT(*) AS c1 FROM t1;
            > SELECT * FROM v3;
            11

            # New tables
            > CREATE TABLE t2 (f1 INTEGER);
            > INSERT INTO t2 SELECT * FROM t1;
            > SELECT COUNT(*) FROM t2;
            11

            # New sources
            > CREATE MATERIALIZED SOURCE source2
              FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-source1-${testdrive.seed}'
              FORMAT BYTES
            > SELECT COUNT(*) FROM source2
            2000000
"""
        ),
    )


@dataclass
class Disruption:
    name: str
    disruption: Callable


disruptions = [
    Disruption(
        name="drop-create-replica",
        disruption=lambda c: drop_create_replica(c),
    ),
    Disruption(
        name="create-invalid-replica",
        disruption=lambda c: create_invalid_replica(c),
    ),
    Disruption(
        name="restart-replica",
        disruption=lambda c: restart_replica(c),
    ),
    Disruption(
        name="pause-one-computed",
        disruption=lambda c: c.pause("computed_1_1"),
    ),
    Disruption(
        name="kill-replica",
        disruption=lambda c: c.kill("computed_1_1", "computed_1_2"),
    ),
    Disruption(
        name="drop-replica",
        disruption=lambda c: c.testdrive("> DROP CLUSTER REPLICA cluster1.replica1"),
    ),
]


def workflow_default(c: Composition) -> None:
    """Test replica isolation by introducing faults of various kinds in replica1
    and then making sure that the cluster continues to operate properly
    """

    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "localstack"]
    )
    for id, disruption in enumerate(disruptions):
        run_test(c, disruption, id)


def run_test(c: Composition, disruption: Disruption, id: int) -> None:
    print(f"+++ Running disruption scenario {disruption.name}")

    c.up("testdrive", persistent=True)

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
        c.up("materialized", *[n.name for n in nodes])
        c.wait_for_materialized()

        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                replica1 (REMOTE ['computed_1_1:2100', 'computed_1_2:2100']),
                replica2 (REMOTE ['computed_2_1:2100', 'computed_2_2:2100'])
            )
            """
        )

        with c.override(
            Testdrive(
                validate_data_dir=False,
                no_reset=True,
                materialize_params={"cluster": "cluster1"},
                seed=id,
            )
        ):
            populate(c)

            # Disrupt replica1 by some means
            disruption.disruption(c)

            validate(c)

        cleanup_list = ["materialized", "testdrive", *[n.name for n in nodes]]
        c.kill(*cleanup_list)
        c.rm(*cleanup_list, destroy_volumes=True)
        c.rm_volumes("mzdata", "pgdata")
