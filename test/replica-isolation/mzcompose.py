# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test replica isolation by introducing faults of various kinds in replica1 and
then making sure that the cluster continues to operate properly
"""


import time
from collections.abc import Callable
from dataclasses import dataclass
from textwrap import dedent
from typing import Any

from psycopg import Cursor

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.localstack import Localstack
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import selected_by_name

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Materialized(
        additional_system_parameter_defaults={
            "log_filter": "mz_cluster::client=debug,info",
        },
        support_external_clusterd=True,
    ),
    Clusterd(name="clusterd_1_1"),
    Clusterd(name="clusterd_1_2"),
    Clusterd(name="clusterd_2_1"),
    Clusterd(name="clusterd_2_2"),
    Testdrive(),
]


class AllowCompactionCheck:
    # replica: a string describing the SQL accessible name of the replica. Example: "cluster1.replica1"
    # host: docker container name from which to check the log. Example: "clusterd_1_1"
    def __init__(self, replica: str, host: str):
        assert "." in replica
        self.replica = replica
        self.host = host
        self.ids: list[str] | None = None
        self.missing_ids: list[str] = []
        self.satisfied = False

    def find_ids(self, c: Composition) -> None:
        raise NotImplementedError

    def print_error(self) -> None:
        raise NotImplementedError

    def check_log(self, c: Composition) -> None:
        self.find_ids(c)
        assert self.ids is not None
        log: str = c.invoke("logs", self.host, capture=True).stdout
        self.missing_ids = []
        self.satisfied = all([self._log_contains_id(log, x) for x in self.ids])

    def replica_id(self, c: Composition) -> str:
        cursor = c.sql_cursor()
        (cluster, replica) = self.replica.split(".")
        cursor.execute(
            f"""
                SELECT mz_cluster_replicas.id FROM mz_clusters, mz_cluster_replicas
                WHERE cluster_id = mz_clusters.id AND mz_clusters.name = '{cluster}'
                AND mz_cluster_replicas.name = '{replica}'""".encode(),
        )
        return str(get_single_value_from_cursor(cursor))

    def cluster_id(self, c: Composition) -> str:
        cursor = c.sql_cursor()
        cluster = self.replica.split(".")[0]
        cursor.execute(
            f"SELECT id FROM mz_clusters WHERE mz_clusters.name = '{cluster}'".encode(),
        )
        return str(get_single_value_from_cursor(cursor))

    def _log_contains_id(self, log: str, the_id: str) -> bool:
        for line in [
            x for x in log.splitlines() if "ClusterClient send=AllowCompaction" in x
        ]:
            if the_id in line:
                return True
        self.missing_ids.append(the_id)
        return False

    @staticmethod
    def _format_id(iid: str) -> str:
        if iid.startswith("si"):
            return "IntrospectionSourceIndex(" + iid[2:] + ")"
        elif iid.startswith("s"):
            return "System(" + iid[1:] + ")"
        elif iid.startswith("u"):
            return "User(" + iid[1:] + ")"
        raise RuntimeError(f"Unexpected iid: {iid}")

    @staticmethod
    def all_checks(replica: str, host: str) -> list["AllowCompactionCheck"]:
        return [
            MaterializedView(replica, host),
            ArrangedIntro(replica, host),
            ArrangedIndex(replica, host),
        ]


class MaterializedView(AllowCompactionCheck):
    """
    Checks that clusterd receives AllowCompaction commands for materialized views.

    For materialized views we hold back compaction until slow replicas have caught
    up. Hence we dont expect these messages if there is another failing replica in
    the cluster.
    """

    def find_ids(self, c: Composition) -> None:
        cursor = c.sql_cursor()
        cursor.execute(
            """
                SELECT id,shard_id from mz_internal.mz_storage_shards, mz_catalog.mz_materialized_views
                WHERE object_id = id AND name = 'v3';
            """
        )
        self.ids = [self._format_id(get_single_value_from_cursor(cursor))]

    def print_error(self) -> None:
        print(
            f"!! AllowCompaction not found for materialized view with ids {self.missing_ids}"
        )


class ArrangedIntro(AllowCompactionCheck):
    """
    Checks that clusterd receives AllowCompaction commands for introspection sources.

    This is purely per replica property. Other failing replicas in the same cluster should
    not influence the result of this test.
    """

    def find_ids(self, c: Composition) -> None:
        cluster_id = self.cluster_id(c)
        cursor = c.sql_cursor()
        cursor.execute(
            f"""
                SELECT idx.id from mz_catalog.mz_sources AS src, mz_catalog.mz_indexes AS idx
                WHERE src.id = idx.on_id AND idx.cluster_id = '{cluster_id}'""".encode()
        )
        self.ids = [self._format_id(x[0]) for x in cursor.fetchall()]

    def print_error(self) -> None:
        print(
            f"!! AllowCompaction not found for introspection with ids {self.missing_ids}"
        )


class ArrangedIndex(AllowCompactionCheck):
    """
    Checks that the arrangement of an index receive AllowCompaction.

    For arrangements, we hold back compaction until all replicas have caught up. Thus, a failing
    replica will not guarantee these messages anymore.
    """

    def find_ids(self, c: Composition) -> None:
        cursor = c.sql_cursor()
        cursor.execute(
            """
                SELECT idx.id FROM mz_catalog.mz_views AS views, mz_catalog.mz_indexes AS idx
                WHERE views.name = 'ct1' AND views.id = idx.on_id
            """
        )
        self.ids = [self._format_id(x[0]) for x in cursor.fetchall()]

    def print_error(self) -> None:
        print(
            f"!! AllowCompaction not found for index arrangement with ids {self.missing_ids}"
        )


def populate(c: Composition) -> None:
    # Create some database objects
    c.testdrive(
        dedent(
            """
            > CREATE TABLE t1 (f1 INTEGER);
            > INSERT INTO t1 SELECT * FROM generate_series(1, 10);
            > CREATE VIEW ct1 AS SELECT COUNT(*) AS c1 FROM t1;
            > CREATE DEFAULT INDEX ON ct1;
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
            > CREATE CLUSTER c SIZE '1';
            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
            > CREATE SOURCE source1
              IN CLUSTER c
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-source1-${testdrive.seed}')
              FORMAT BYTES
            > CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) FROM source1
            """
        ),
    )


def restart_replica(c: Composition) -> None:
    c.kill("clusterd_1_1", "clusterd_1_2")
    c.up("clusterd_1_1", "clusterd_1_2")


def restart_environmentd(c: Composition) -> None:
    c.kill("materialized")
    c.up("materialized")


def drop_create_replica(c: Composition) -> None:

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    c.testdrive(
        dedent(
            """
            > DROP CLUSTER REPLICA cluster1.replica1
            > CREATE CLUSTER REPLICA cluster1.replica3
              STORAGECTL ADDRESSES ['clusterd_1_1:2100', 'clusterd_1_2:2100'],
              STORAGE ADDRESSES ['clusterd_1_1:2103', 'clusterd_1_2:2103'],
              COMPUTECTL ADDRESSES ['clusterd_1_1:2101', 'clusterd_1_2:2101'],
              COMPUTE ADDRESSES ['clusterd_1_1:2102', 'clusterd_1_2:2102']
            """
        )
    )


def create_invalid_replica(c: Composition) -> None:
    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )
    c.testdrive(
        dedent(
            """
            > CREATE CLUSTER REPLICA cluster1.replica3
              STORAGECTL ADDRESSES ['no_such_host:2100'],
              STORAGE ADDRESSES ['no_such_host:2103'],
              COMPUTECTL ADDRESSES ['no_such_host:2101'],
              COMPUTE ADDRESSES ['no_such_host:2102']
            """
        )
    )


def validate(c: Composition) -> None:
    # Validate that the cluster continues to operate
    c.testdrive(
        dedent(
            """
            # Dataflows
            > SELECT * FROM ct1;
            10
            > SELECT * FROM v1;
            10

            # Existing sources
            $ kafka-ingest format=bytes topic=source1 repeat=1000000
            B${kafka-ingest.iteration}
            > SELECT * FROM v2;
            2000000

            # Existing tables
            > INSERT INTO t1 VALUES (20);
            > SELECT * FROM ct1;
            11
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

            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

            # New sources
            > CREATE SOURCE source2
              IN CLUSTER c
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-source1-${testdrive.seed}')
              FORMAT BYTES
            > SELECT COUNT(*) FROM source2
            2000000
"""
        ),
    )


def validate_introspection_compaction(
    c: Composition, checks: list[AllowCompactionCheck]
) -> None:
    # Validate that the AllowCompaction commands arrive at the corresponding replicas.
    # Allow up to 10 seconds for the compaction the command to appear
    start = time.time()
    while time.time() < start + 5:
        for check in checks:
            check.check_log(c)

        if all([check.satisfied for check in checks]):
            return

    for check in checks:
        if not check.satisfied:
            check.print_error()
    assert all([check.satisfied for check in checks])


@dataclass
class Disruption:
    name: str
    disruption: Callable
    compaction_checks: list[AllowCompactionCheck]


disruptions = [
    Disruption(
        name="none",
        disruption=lambda c: None,
        compaction_checks=AllowCompactionCheck.all_checks(
            "cluster1.replica1", "clusterd_1_1"
        )
        + AllowCompactionCheck.all_checks("cluster1.replica2", "clusterd_2_1"),
    ),
    Disruption(
        name="drop-create-replica",
        disruption=lambda c: drop_create_replica(c),
        compaction_checks=[
            ArrangedIntro("cluster1.replica2", "clusterd_2_1"),
        ],
    ),
    Disruption(
        name="create-invalid-replica",
        disruption=lambda c: create_invalid_replica(c),
        compaction_checks=[
            ArrangedIntro("cluster1.replica2", "clusterd_2_1"),
        ],
    ),
    Disruption(
        name="restart-replica",
        disruption=lambda c: restart_replica(c),
        compaction_checks=AllowCompactionCheck.all_checks(
            "cluster1.replica1", "clusterd_1_1"
        )
        + AllowCompactionCheck.all_checks("cluster1.replica2", "clusterd_2_1"),
    ),
    Disruption(
        name="pause-one-clusterd",
        disruption=lambda c: c.pause("clusterd_1_1"),
        compaction_checks=[
            ArrangedIntro("cluster1.replica2", "clusterd_2_1"),
        ],
    ),
    Disruption(
        name="kill-replica",
        disruption=lambda c: c.kill("clusterd_1_1", "clusterd_1_2"),
        compaction_checks=[
            ArrangedIntro("cluster1.replica2", "clusterd_2_1"),
        ],
    ),
    Disruption(
        name="drop-replica",
        disruption=lambda c: c.testdrive("> DROP CLUSTER REPLICA cluster1.replica1"),
        compaction_checks=AllowCompactionCheck.all_checks(
            "cluster1.replica2", "clusterd_2_1"
        ),
    ),
    Disruption(
        name="restart-environmentd",
        disruption=restart_environmentd,
        compaction_checks=AllowCompactionCheck.all_checks(
            "cluster1.replica1", "clusterd_1_1"
        )
        + AllowCompactionCheck.all_checks("cluster1.replica2", "clusterd_2_1"),
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("disruptions", nargs="*", default=[d.name for d in disruptions])

    args = parser.parse_args()

    c.up("zookeeper", "kafka", "schema-registry", "localstack")
    for id, disruption in enumerate(selected_by_name(args.disruptions, disruptions)):
        run_test(c, disruption, id)


def run_test(c: Composition, disruption: Disruption, id: int) -> None:
    # Cleanup here instead of at the end of the test to make sure we keep state
    # and logs in case something goes wrong
    cleanup_list = [
        "materialized",
        "testdrive",
        "clusterd_1_1",
        "clusterd_1_2",
        "clusterd_2_1",
        "clusterd_2_2",
    ]
    c.kill(*cleanup_list)
    c.rm(*cleanup_list, destroy_volumes=True)
    c.rm_volumes("mzdata")
    print(f"+++ Running disruption scenario {disruption.name}")

    with c.override(
        Testdrive(
            no_reset=True,
            materialize_params={"cluster": "cluster1"},
            seed=id,
            default_timeout="300s",
        ),
        Clusterd(
            name="clusterd_1_1",
            process_names=["clusterd_1_1", "clusterd_1_2"],
        ),
        Clusterd(
            name="clusterd_1_2",
            process_names=["clusterd_1_1", "clusterd_1_2"],
        ),
        Clusterd(
            name="clusterd_2_1",
            process_names=["clusterd_2_1", "clusterd_2_2"],
        ),
        Clusterd(
            name="clusterd_2_2",
            process_names=["clusterd_2_1", "clusterd_2_2"],
        ),
    ):
        c.up(
            "materialized",
            "clusterd_1_1",
            "clusterd_1_2",
            "clusterd_2_1",
            "clusterd_2_2",
            Service("testdrive", idle=True),
        )

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

        if any(
            isinstance(check, ArrangedIntro) for check in disruption.compaction_checks
        ):
            # Disable introspection subscribes because they break the
            # `ArrangedIntro` check by disabling compaction of logging indexes
            # on all replicas if one of the replicas is failing. That's because
            # of a defect of replica-targeted subscribes: They get installed on
            # all replicas but only the targeted replica can drive the write
            # frontier forward. If the targeted replica is crashing, the write
            # frontier cannot advance and thus the read frontier cannot either.
            #
            # TODO(database-issues#8091): Fix this by installing targeted subscribes only on the
            #               targeted replica.
            c.sql(
                "ALTER SYSTEM SET enable_introspection_subscribes = false;",
                port=6877,
                user="mz_system",
            )

        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                replica1 (
                    STORAGECTL ADDRESSES ['clusterd_1_1:2100', 'clusterd_1_2:2100'],
                    STORAGE ADDRESSES ['clusterd_1_1:2103', 'clusterd_1_2:2103'],
                    COMPUTECTL ADDRESSES ['clusterd_1_1:2101', 'clusterd_1_2:2101'],
                    COMPUTE ADDRESSES ['clusterd_1_1:2102', 'clusterd_1_2:2102']
                ),
                replica2 (
                    STORAGECTL ADDRESSES ['clusterd_2_1:2100', 'clusterd_2_2:2100'],
                    STORAGE ADDRESSES ['clusterd_2_1:2103', 'clusterd_2_2:2103'],
                    COMPUTECTL ADDRESSES ['clusterd_2_1:2101', 'clusterd_2_2:2101'],
                    COMPUTE ADDRESSES ['clusterd_2_1:2102', 'clusterd_2_2:2102']
                )
            )
            """
        )

        populate(c)

        # Disrupt replica1 by some means
        disruption.disruption(c)

        validate(c)
        validate_introspection_compaction(c, disruption.compaction_checks)


def get_single_value_from_cursor(cursor: Cursor) -> Any:
    result = cursor.fetchone()
    assert result is not None
    return result[0]
