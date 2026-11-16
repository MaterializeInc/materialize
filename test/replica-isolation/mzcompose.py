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

import argparse
import asyncio
import time
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from textwrap import dedent
from typing import Any, LiteralString

from psycopg import AsyncConnection, sql

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.clusterd import Clusterd, native_catalog_options
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.localstack import Localstack
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import METADATA_STORE
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.util import selected_by_name

SERVICES = [
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Materialized(
        support_external_clusterd=True,
    ),
    Clusterd(name="clusterd_1_1"),
    Clusterd(name="clusterd_1_2"),
    Clusterd(name="clusterd_2_1"),
    Clusterd(name="clusterd_2_2"),
    Testdrive(),
]


class CompactionCheck:
    """Observe progress after validation, not commands from an earlier execution."""

    def __init__(self, replica: str):
        self.replica = replica
        self.deadline = 0.0
        self.replica_id: str | None = None
        self.ids: dict[str, str] = {}
        self.baseline: dict[str, int] | None = None
        self.observed: dict[str, int | None] = {}

    def find_ids(self, c: Composition) -> dict[str, str]:
        """Return catalog IDs and their physical global IDs for this check."""
        raise NotImplementedError

    def query(
        self,
        c: Composition,
        query: LiteralString | sql.Composed,
        params: tuple = (),
        *,
        system: bool = False,
    ) -> list[tuple[Any, ...]]:
        port = c.port("materialized", 6877 if system else 6875)

        async def execute():
            remaining = min(10, self.deadline - time.monotonic())
            if remaining <= 0:
                raise TimeoutError(f"Compaction deadline expired for {self.replica}")
            conn = await asyncio.wait_for(
                AsyncConnection.connect(
                    host="127.0.0.1",
                    port=port,
                    dbname="materialize",
                    user="mz_system" if system else "materialize",
                    sslmode="disable",
                    autocommit=True,
                    connect_timeout=10,
                ),
                timeout=remaining,
            )
            task = asyncio.create_task(conn.execute(query, params))
            try:
                done, _ = await asyncio.wait(
                    [task], timeout=max(0, min(10, self.deadline - time.monotonic()))
                )
                if not done:
                    raise TimeoutError(
                        f"Compaction SQL stalled for {self.replica}: {query}"
                    )
                return await task.result().fetchall()
            finally:
                # Close before cancelling: psycopg cancellation otherwise waits
                # for a server reply, which a blocked coordinator cannot send.
                await conn.close()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        return asyncio.run(execute())

    def prepare(self, c: Composition, deadline: float) -> None:
        self.deadline = deadline
        cluster, replica = self.replica.split(".")
        [(self.replica_id,)] = self.query(
            c,
            """SELECT r.id FROM mz_cluster_replicas r
               JOIN mz_clusters c ON c.id = r.cluster_id
               WHERE c.name = %s AND r.name = %s""",
            (cluster, replica),
        )
        self.ids = self.find_ids(c)
        assert self.ids, f"No objects found for {type(self).__name__} on {self.replica}"
        assert all(self.ids.values()), f"Missing global ID mappings: {self.ids}"
        self.baseline = None
        self.observed = {}

    def replica_frontiers(self, c: Composition, frontier: str) -> dict[str, int | None]:
        assert frontier in ("read", "write")
        observed: dict[str, int | None] = dict.fromkeys(self.ids.values())
        # Require all current observations to be known and nonempty. Missing
        # rows and completed frontiers must not masquerade as live progress.
        rows = self.query(
            c,
            sql.SQL("""SELECT object_id,
                   bool_and({known} AND {frontier} IS NOT NULL),
                   min({frontier})
               FROM mz_internal.mz_current_replica_frontiers
               WHERE replica_id = %s AND object_id = ANY(%s::text[])
               GROUP BY object_id""").format(
                known=sql.Identifier(f"{frontier}_known"),
                frontier=sql.Identifier(f"{frontier}_frontier"),
            ),
            (self.replica_id, list(self.ids.values())),
        )
        for object_id, known, value in rows:
            if known:
                observed[object_id] = int(value)
        return observed

    def sample(self, c: Composition) -> dict[str, int | None]:
        return self.replica_frontiers(c, "read")

    def check(self, c: Composition) -> bool:
        self.observed = self.sample(c)
        if self.baseline is None:
            if all(value is not None for value in self.observed.values()):
                self.baseline = {
                    key: value
                    for key, value in self.observed.items()
                    if value is not None
                }
            return False
        return all(
            (observed := self.observed[key]) is not None and observed > value
            for key, value in self.baseline.items()
        )

    @staticmethod
    def all_checks(replica: str) -> list["CompactionCheck"]:
        return [
            MaterializedView(replica),
            ArrangedIntro(replica),
            ArrangedIndex(replica),
        ]


class MaterializedView(CompactionCheck):
    """Check applied shared shard compaction and this replica's MV execution."""

    def find_ids(self, c: Composition) -> dict[str, str]:
        rows = self.query(
            c,
            """
            SELECT m.id, g.global_id FROM mz_materialized_views m
            LEFT JOIN mz_internal.mz_object_global_ids g ON g.id = m.id
            WHERE m.name = 'v3'
        """,
        )
        assert len(rows) == 1, rows
        return dict(rows)

    def sample(self, c: Composition) -> dict[str, int | None]:
        observed = self.replica_frontiers(c, "write")
        for item_id in self.ids:
            [(state,)] = self.query(
                c, sql.SQL("INSPECT SHARD {}").format(sql.Literal(item_id)), system=True
            )
            since = state["since"]
            assert len(since) <= 1, since
            observed[f"{item_id}/persist-since"] = int(since[0]) if since else None
        return observed


class ArrangedIntro(CompactionCheck):
    """Check installed logging trace compaction independently of failing peers."""

    def find_ids(self, c: Composition) -> dict[str, str]:
        cluster = self.replica.split(".")[0]
        return dict(
            self.query(
                c,
                """SELECT idx.id, g.global_id FROM mz_sources src
               JOIN mz_indexes idx ON idx.on_id = src.id
               JOIN mz_clusters c ON c.id = idx.cluster_id
               LEFT JOIN mz_internal.mz_object_global_ids g ON g.id = idx.id
               WHERE c.name = %s""",
                (cluster,),
            )
        )


class ArrangedIndex(CompactionCheck):
    """Check installed user-index trace compaction under committed protection."""

    def find_ids(self, c: Composition) -> dict[str, str]:
        rows = self.query(
            c,
            """
            SELECT idx.id, g.global_id FROM mz_views v
            JOIN mz_indexes idx ON idx.on_id = v.id
            LEFT JOIN mz_internal.mz_object_global_ids g ON g.id = idx.id
            WHERE v.name = 'ct1'
        """,
        )
        assert len(rows) == 1, rows
        return dict(rows)


def populate(c: Composition) -> None:
    # Create some database objects
    c.testdrive(
        dedent("""
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
            > CREATE CLUSTER c SIZE 'scale=1,workers=1';
            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
            > CREATE SOURCE source1
              IN CLUSTER c
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-source1-${testdrive.seed}')
              FORMAT BYTES
            > CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) FROM source1
            """),
    )


def drop_replica(c: Composition, _stack: ExitStack, _unified: bool) -> None:
    c.testdrive("> DROP CLUSTER REPLICA cluster1.replica1")


def restart_replica(c: Composition) -> None:
    c.kill("clusterd_1_1", "clusterd_1_2")
    c.up("clusterd_1_1", "clusterd_1_2")


def restart_environmentd(c: Composition) -> None:
    c.kill("materialized")
    c.up("materialized")


def drop_create_replica(c: Composition, stack: ExitStack, unified: bool) -> None:

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    c.testdrive(dedent("""
            > DROP CLUSTER REPLICA cluster1.replica1
            > CREATE CLUSTER REPLICA cluster1.replica3
              STORAGECTL ADDRESSES ['clusterd_1_1:2100', 'clusterd_1_2:2100'],
              STORAGE ADDRESSES ['clusterd_1_1:2103', 'clusterd_1_2:2103'],
              COMPUTECTL ADDRESSES ['clusterd_1_1:2101', 'clusterd_1_2:2101'],
              COMPUTE ADDRESSES ['clusterd_1_1:2102', 'clusterd_1_2:2102']
            """))

    c.kill("clusterd_1_1", "clusterd_1_2")
    start_replica(c, stack, "replica3", ["clusterd_1_1", "clusterd_1_2"], unified)
    c.testdrive(dedent("""
        > SET cluster_replica = replica3;
        > SELECT * FROM ct1;
        10
        > RESET cluster_replica;
    """))


def create_invalid_replica(c: Composition) -> None:
    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )
    c.testdrive(dedent("""
            > CREATE CLUSTER REPLICA cluster1.replica3
              STORAGECTL ADDRESSES ['no_such_host:2100'],
              STORAGE ADDRESSES ['no_such_host:2103'],
              COMPUTECTL ADDRESSES ['no_such_host:2101'],
              COMPUTE ADDRESSES ['no_such_host:2102']
            """))


def validate(c: Composition) -> None:
    # Validate that the cluster continues to operate
    c.testdrive(
        dedent("""
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
"""),
    )


def validate_introspection_compaction(
    c: Composition, checks: list[CompactionCheck]
) -> None:
    deadline = time.monotonic() + 300
    for check in checks:
        check.prepare(c, deadline)
    # Publication and Persist critical-since application are asynchronous. Keep
    # their production settings, and require advancement beyond a known baseline
    # rather than accepting the initial dataflow as_of as proof of compaction.
    pending = list(checks)
    while pending and time.monotonic() < deadline:
        pending = [check for check in pending if not check.check(c)]
        if pending:
            time.sleep(1)
    assert not pending, [
        (type(check).__name__, check.replica, check.baseline, check.observed)
        for check in pending
    ]


@dataclass
class Disruption:
    name: str
    disruption: Callable[[Composition, ExitStack, bool], None]
    compaction_checks: list[CompactionCheck]


disruptions = [
    Disruption(
        name="none",
        disruption=lambda c, _stack, _unified: None,
        compaction_checks=CompactionCheck.all_checks("cluster1.replica1")
        + CompactionCheck.all_checks("cluster1.replica2"),
    ),
    Disruption(
        name="drop-create-replica",
        disruption=drop_create_replica,
        compaction_checks=[
            ArrangedIntro("cluster1.replica2"),
        ],
    ),
    Disruption(
        name="create-invalid-replica",
        disruption=lambda c, _stack, _unified: create_invalid_replica(c),
        compaction_checks=[
            ArrangedIntro("cluster1.replica2"),
        ],
    ),
    Disruption(
        name="restart-replica",
        disruption=lambda c, _stack, _unified: restart_replica(c),
        compaction_checks=CompactionCheck.all_checks("cluster1.replica1")
        + CompactionCheck.all_checks("cluster1.replica2"),
    ),
    Disruption(
        name="pause-one-clusterd",
        disruption=lambda c, _stack, _unified: c.pause("clusterd_1_1"),
        compaction_checks=[
            ArrangedIntro("cluster1.replica2"),
        ],
    ),
    Disruption(
        name="kill-replica",
        disruption=lambda c, _stack, _unified: c.kill("clusterd_1_1", "clusterd_1_2"),
        compaction_checks=[
            ArrangedIntro("cluster1.replica2"),
        ],
    ),
    Disruption(
        name="drop-replica",
        disruption=drop_replica,
        compaction_checks=CompactionCheck.all_checks("cluster1.replica2"),
    ),
    Disruption(
        name="restart-environmentd",
        disruption=lambda c, _stack, _unified: restart_environmentd(c),
        compaction_checks=CompactionCheck.all_checks("cluster1.replica1")
        + CompactionCheck.all_checks("cluster1.replica2"),
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--unified-cluster", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("disruptions", nargs="*", default=[d.name for d in disruptions])

    args = parser.parse_args()

    c.up("kafka", "schema-registry", "localstack")
    for id, disruption in enumerate(selected_by_name(args.disruptions, disruptions)):
        run_test(c, disruption, id, args.unified_cluster)


def run_test(c: Composition, disruption: Disruption, id: int, unified: bool) -> None:
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
    # Fresh controller state requires fresh metadata too. Remove only the
    # composition-owned store, never a separately supplied database endpoint.
    if METADATA_STORE in c.compose["services"]:
        cleanup_list.append(METADATA_STORE)
    try:
        c.kill(*cleanup_list)
    except:
        pass
    try:
        c.rm(*cleanup_list, destroy_volumes=True)
    except:
        pass
    try:
        c.rm_volumes("mzdata")
    except:
        pass
    print(f"+++ Running disruption scenario {disruption.name}")

    with ExitStack() as stack:
        stack.enter_context(
            c.override(
                Testdrive(
                    no_reset=True,
                    materialize_params={"cluster": "cluster1"},
                    seed=id,
                    default_timeout="300s",
                )
            )
        )
        c.up("materialized", Service("testdrive", idle=True))

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

        if any(
            isinstance(check, ArrangedIntro) for check in disruption.compaction_checks
        ):
            # Replica-targeted introspection readers hold back compaction of
            # logging indexes across the cluster if their target cannot advance.
            # Disable both subscribes and curated metric sinks so ArrangedIntro
            # can check compaction on healthy replicas when another is failing.
            # This must happen before cluster creation, since disabling metric
            # sinks does not remove sinks that are already installed.
            #
            # TODO(database-issues#8091): Isolate introspection read holds per replica.
            c.sql(
                """
                ALTER SYSTEM SET enable_introspection_subscribes = false;
                ALTER SYSTEM SET enable_metric_sink = false;
                """,
                port=6877,
                user="mz_system",
            )

        c.sql("""
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
            """)

        start_replica(c, stack, "replica1", ["clusterd_1_1", "clusterd_1_2"], unified)
        start_replica(c, stack, "replica2", ["clusterd_2_1", "clusterd_2_2"], unified)
        populate(c)

        # Disrupt replica1 by some means
        disruption.disruption(c, stack, unified)

        validate(c)
        validate_introspection_compaction(c, disruption.compaction_checks)


def start_replica(
    c: Composition, stack: ExitStack, replica: str, processes: list[str], unified: bool
) -> None:
    with c.sql_cursor() as cursor:
        cursor.execute(
            """SELECT c.id, r.id FROM mz_clusters c
               JOIN mz_cluster_replicas r ON r.cluster_id = c.id
               WHERE c.name = 'cluster1' AND r.name = %s""",
            (replica,),
        )
        [(cluster_id, replica_id)] = cursor.fetchall()
    options = native_catalog_options(c)
    for process in processes:
        stack.enter_context(
            c.override(
                Clusterd(
                    name=process,
                    process_names=processes,
                    unified_cluster=unified,
                    options=[
                        f"--catalog-cluster-id={cluster_id}",
                        f"--catalog-replica-id={replica_id}",
                        *options,
                    ],
                )
            )
        )
    c.up(*processes)
