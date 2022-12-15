# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
import time
from textwrap import dedent
from typing import Tuple

from pg8000.dbapi import ProgrammingError

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    Cockroach,
    Kafka,
    Localstack,
    Materialized,
    Postgres,
    Redpanda,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Clusterd(name="compute_1"),
    Clusterd(name="compute_2"),
    Clusterd(name="compute_3"),
    Clusterd(name="compute_4"),
    # We use mz_panic() in some test scenarios, so environmentd must stay up.
    Materialized(propagate_crashes=False),
    Redpanda(),
    Testdrive(
        volumes=[
            "mzdata:/mzdata",
            "tmp:/share/tmp",
            ".:/workdir/smoke",
            "../testdrive:/workdir/testdrive",
        ],
        materialize_params={"cluster": "cluster1"},
    ),
    Clusterd(name="storage"),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    for name in [
        "test-smoke",
        "test-github-12251",
        "test-github-15531",
        "test-github-15535",
        "test-github-15799",
        "test-github-15930",
        "test-remote-storage",
        "test-drop-default-cluster",
        "test-upsert",
        "test-resource-limits",
        "test-invalid-compute-reuse",
        "test-builtin-migration",
        "pg-snapshot-resumption",
        "test-system-table-indexes",
    ]:
        with c.test_case(name):
            c.workflow(name)


def workflow_test_smoke(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive in a variety of compute cluster configurations."""

    parser.add_argument(
        "glob",
        nargs="*",
        default=["smoke/*.td"],
        help="run against the specified files",
    )
    args = parser.parse_args()

    c.down(destroy_volumes=True)
    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "localstack"]
    )
    c.up("materialized")
    c.wait_for_materialized()

    # Create a remote cluster and verify that tests pass.
    c.up("compute_1")
    c.up("compute_2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        """CREATE CLUSTER cluster1 REPLICAS (replica1 (
            REMOTE ['compute_1:2101', 'compute_2:2101'],
            COMPUTE ['compute_1:2102', 'compute_2:2102'],
            WORKERS 2
            ));
    """
    )
    c.run("testdrive", *args.glob)

    # Add a replica to that remote cluster and verify that tests still pass.
    c.up("compute_3")
    c.up("compute_4")
    c.sql(
        """CREATE CLUSTER REPLICA cluster1.replica2
            REMOTE ['compute_3:2101', 'compute_4:2101'],
            COMPUTE ['compute_3:2102', 'compute_4:2102'],
            WORKERS 2
    """
    )
    c.run("testdrive", *args.glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.kill("compute_1")
    c.run("testdrive", *args.glob)

    # Leave only replica 2 up and verify that tests still pass.
    c.sql("DROP CLUSTER REPLICA cluster1.replica1")
    c.run("testdrive", *args.glob)


def workflow_test_invalid_compute_reuse(c: Composition) -> None:
    """Ensure clusterds correctly crash if used in unsupported communication config"""
    c.down(destroy_volumes=True)
    c.up("materialized")
    c.wait_for_materialized()

    # Create a remote cluster and verify that tests pass.
    c.up("compute_1")
    c.up("compute_2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        """CREATE CLUSTER cluster1 REPLICAS (replica1 (
            REMOTE ['compute_1:2101', 'compute_2:2101'],
            COMPUTE ['compute_1:2102', 'compute_2:2102'],
            WORKERS 2
            ));
    """
    )
    c.sql("DROP CLUSTER cluster1 CASCADE;")

    # Note the different WORKERS argument
    c.sql(
        """CREATE CLUSTER cluster1 REPLICAS (replica1 (
            REMOTE ['compute_1:2101', 'compute_2:2101'],
            COMPUTE ['compute_1:2102', 'compute_2:2102'],
            WORKERS 1
            ));
    """
    )

    # This should ensure that compute crashed (and does not just hang forever)
    c1 = c.invoke("logs", "compute_1", capture=True)
    assert (
        "halting process: new timely configuration does not match existing timely configuration"
        in c1.stdout
    )


def workflow_test_github_12251(c: Composition) -> None:
    """Test that clients do not wait indefinitely for a crashed resource."""

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.wait_for_materialized()
    c.up("compute_1")
    c.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        CREATE CLUSTER cluster1 REPLICAS (replica1 (REMOTE ['compute_1:2101'], COMPUTE ['compute_1:2102'], WORKERS 2));
        SET cluster = cluster1;
        """
    )
    start_time = time.process_time()
    try:
        c.sql(
            """
        SET statement_timeout = '1 s';
        CREATE TABLE IF NOT EXISTS log_table (f1 TEXT);
        CREATE TABLE IF NOT EXISTS panic_table (f1 TEXT);
        INSERT INTO panic_table VALUES ('panic!');
        -- Crash loop the cluster with the table's index
        INSERT INTO log_table SELECT mz_internal.mz_panic(f1) FROM panic_table;
        """
        )
    except ProgrammingError as e:
        # Ensure we received the correct error message
        assert "statement timeout" in e.args[0]["M"], e
        # Ensure the statemenet_timeout setting is ~honored
        assert (
            time.process_time() - start_time < 2
        ), "idle_in_transaction_session_timeout not respected"
    else:
        assert False, "unexpected success in test_github_12251"

    # Ensure we can select from tables after cancellation.
    c.sql("SELECT * FROM log_table;")


def workflow_test_github_15531(c: Composition) -> None:
    """
    Test that compute command history does not leak peek commands.

    Regression test for https://github.com/MaterializeInc/materialize/issues/15531.

    The test currently only inspects the history on clusterd, and it should be
    extended in the future to also consider the history size in the compute
    controller.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("compute_1")
    c.wait_for_materialized()

    # helper function to get command history metrics for clusterd
    def find_clusterd_command_history_metrics(c: Composition) -> Tuple[int, int]:
        metrics = c.exec(
            "compute_1", "curl", "localhost:6878/metrics", capture=True
        ).stdout

        history_len = None
        dataflow_count = None
        for metric in metrics.splitlines():
            if metric.startswith("mz_compute_comamnd_history_size"):
                history_len = int(metric[len("mz_compute_comamnd_history_size") :])
            elif metric.startswith("mz_compute_dataflow_count_in_history"):
                dataflow_count = int(
                    metric[len("mz_compute_dataflow_count_in_history") :]
                )

        assert (
            history_len is not None
        ), "command history length not found in clusterd metrics"
        assert (
            dataflow_count is not None
        ), "dataflow count in history not found in clusterd metrics"

        return (history_len, dataflow_count)

    # Set up a cluster with an indexed table and an unindexed one.
    c.sql(
        """
        CREATE CLUSTER cluster1 REPLICAS (replica1 (
            REMOTE ['compute_1:2101'],
            COMPUTE ['compute_1:2102'],
            WORKERS 2
        ));
        SET cluster = cluster1;
        -- table for fast-path peeks
        CREATE TABLE t (a int);
        CREATE DEFAULT INDEX ON t;
        INSERT INTO t VALUES (42);
        -- table for slow-path peeks
        CREATE TABLE t2 (a int);
        INSERT INTO t2 VALUES (84);
        """
    )

    # obtain initial history size and dataflow count
    (
        clusterd_history_len,
        clusterd_dataflow_count,
    ) = find_clusterd_command_history_metrics(c)
    assert (
        clusterd_dataflow_count == 1
    ), "more dataflows than expected in clusterd history"
    assert clusterd_history_len > 0, "clusterd history cannot be empty"

    # execute 400 fast- and slow-path peeks
    for i in range(20):
        c.sql(
            """
            SELECT * FROM t;
            SELECT * FROM t2;
            SELECT * FROM t;
            SELECT * FROM t2;
            SELECT * FROM t;
            SELECT * FROM t2;
            SELECT * FROM t;
            SELECT * FROM t2;
            SELECT * FROM t;
            SELECT * FROM t2;
            SELECT * FROM t;
            SELECT * FROM t2;
            SELECT * FROM t;
            SELECT * FROM t2;
            SELECT * FROM t;
            SELECT * FROM t2;
            SELECT * FROM t;
            SELECT * FROM t2;
            SELECT * FROM t;
            SELECT * FROM t2;
            """
        )

    # check that dataflow count is the same and
    # that history size is well-behaved
    (
        clusterd_history_len,
        clusterd_dataflow_count,
    ) = find_clusterd_command_history_metrics(c)
    assert (
        clusterd_dataflow_count == 1
    ), "more dataflows than expected in clusterd history"
    assert (
        clusterd_history_len < 100
    ), "clusterd history grew more than expected after peeks"


def workflow_test_github_15535(c: Composition) -> None:
    """
    Test that compute reconciliation does not produce empty frontiers.

    Regression test for https://github.com/MaterializeInc/materialize/issues/15535.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("compute_1")
    c.wait_for_materialized()

    # Set up a dataflow on clusterd.
    c.sql(
        """
        CREATE CLUSTER cluster1 REPLICAS (replica1 (
            REMOTE ['compute_1:2101'],
            COMPUTE ['compute_1:2102'],
            WORKERS 2
        ));
        SET cluster = cluster1;
        CREATE TABLE t (a int);
        CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;
        -- wait for the dataflow to be ready
        SELECT * FROM mv;
        """
    )

    # Restart environmentd to trigger a reconciliation on clusterd.
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized()

    print("Sleeping to wait for frontier updates")
    time.sleep(10)

    def extract_frontiers(output: str) -> Tuple[str, str]:
        since_re = re.compile("^\s+since:\[(?P<frontier>.*)\]")
        upper_re = re.compile("^\s+upper:\[(?P<frontier>.*)\]")
        since = None
        upper = None
        for line in output.splitlines():
            if match := since_re.match(line):
                since = match.group("frontier").strip()
            elif match := upper_re.match(line):
                upper = match.group("frontier").strip()

        assert since is not None, "since not found in EXPLAIN TIMESTAMP output"
        assert upper is not None, "upper not found in EXPLAIN TIMESTAMP output"
        return (since, upper)

    # Verify that there are no empty frontiers.
    output = c.sql_query("EXPLAIN TIMESTAMP FOR SELECT * FROM mv")
    mv_since, mv_upper = extract_frontiers(output[0][0])
    output = c.sql_query("EXPLAIN TIMESTAMP FOR SELECT * FROM t")
    t_since, t_upper = extract_frontiers(output[0][0])

    assert mv_since, "mv has empty since frontier"
    assert mv_upper, "mv has empty upper frontier"
    assert t_since, "t has empty since frontier"
    assert t_upper, "t has empty upper frontier"


def workflow_test_github_15799(c: Composition) -> None:
    """
    Test that querying arranged introspection sources on a replica does not
    crash other replicas in the same cluster that have introspection disabled.

    Regression test for https://github.com/MaterializeInc/materialize/issues/15799.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("compute_1")
    c.up("compute_2")
    c.wait_for_materialized()

    c.sql(
        """
        CREATE CLUSTER cluster1 REPLICAS (
            logging_on (
                REMOTE ['compute_1:2101'],
                COMPUTE ['compute_1:2102'],
                WORKERS 2
            ),
            logging_off (
                REMOTE ['compute_2:2101'],
                COMPUTE ['compute_2:2102'],
                WORKERS 2,
                INTROSPECTION INTERVAL 0
            )
        );
        SET cluster = cluster1;

        -- query the arranged introspection sources on the replica with logging enabled
        SET cluster_replica = logging_on;
        SELECT * FROM mz_internal.mz_active_peeks, mz_internal.mz_compute_exports;

        -- verify that the other replica has not crashed and still responds
        SET cluster_replica = logging_off;
        SELECT * FROM mz_tables, mz_sources;
        """
    )


def workflow_test_github_15930(c: Composition) -> None:
    """
    Test that triggering reconciliation does not wedge the mz_worker_compute_frontiers
    introspection source.

    Regression test for https://github.com/MaterializeInc/materialize/issues/15930.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("compute_1")
        c.wait_for_materialized()

        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                logging_on (
                    REMOTE ['compute_1:2101'],
                    COMPUTE ['compute_1:2102'],
                    WORKERS 2
                )
            );
            """
        )

        # verify that we can query the introspection source
        c.testdrive(
            input=dedent(
                """
            > SET cluster = cluster1;
            > SELECT 1 FROM mz_internal.mz_worker_compute_frontiers LIMIT 1;
            1
                """
            )
        )

        # Restart environmentd to trigger a reconciliation on clusterd.
        c.kill("materialized")
        c.up("materialized")
        c.wait_for_materialized()

        # verify again that we can query the introspection source
        c.testdrive(
            input=dedent(
                """
            > SET cluster = cluster1;
            > SELECT 1 FROM mz_internal.mz_worker_compute_frontiers LIMIT 1;
            1
                """
            )
        )

        c.sql(
            """
            SET cluster = cluster1;
            -- now let's give it another go with user-defined objects
            CREATE TABLE t (a int);
            CREATE DEFAULT INDEX ON t;
            INSERT INTO t VALUES (42);
            """
        )

        # Restart environmentd to trigger yet another reconciliation on clusterd.
        c.kill("materialized")
        c.up("materialized")
        c.wait_for_materialized()

        # verify yet again that we can query the introspection source and now the table.
        c.testdrive(
            input=dedent(
                """
            > SET cluster = cluster1;
            > SELECT 1 FROM mz_internal.mz_worker_compute_frontiers LIMIT 1;
            1
            > SELECT * FROM t;
            42
                """
            )
        )


def workflow_test_upsert(c: Composition) -> None:
    """Test creating upsert sources and continuing to ingest them after a restart."""
    with c.override(
        Testdrive(default_timeout="30s", no_reset=True, consistent_seed=True),
    ):
        c.down(destroy_volumes=True)
        dependencies = [
            "materialized",
            "zookeeper",
            "kafka",
            "schema-registry",
        ]
        c.start_and_wait_for_tcp(
            services=dependencies,
        )

        c.run("testdrive", "upsert/01-create-sources.td")
        # Sleep to make sure the errors have made it to persist.
        # This isn't necessary for correctness,
        # as we should be able to crash at any point and re-start.
        # But if we don't sleep here, then we might be ingesting the errored
        # records in the new process, and so we won't actually be testing
        # the ability to retract error values that make it to persist.
        print("Sleeping for ten seconds")
        time.sleep(10)
        c.exec("materialized", "bash", "-c", "kill -9 `pidof clusterd`")
        c.run("testdrive", "upsert/02-after-clusterd-restart.td")


def workflow_test_remote_storage(c: Composition) -> None:
    """Test creating sources in a remote clusterd process."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(default_timeout="15s", no_reset=True, consistent_seed=True),
        # Use a separate CockroachDB service for persist rather than the one in
        # the `Materialized` service, so that crashing `environmentd` does not
        # also take down CockroachDB.
        Cockroach(),
        Materialized(options="--persist-consensus-url=postgres://root@cockroach:26257"),
    ):
        dependencies = [
            "materialized",
            "cockroach",
            "storage",
            "zookeeper",
            "kafka",
            "schema-registry",
        ]
        c.start_and_wait_for_tcp(
            services=dependencies,
        )

        c.run("testdrive", "storage/01-create-sources.td")

        c.kill("materialized")
        c.up("materialized")
        c.run("testdrive", "storage/02-after-environmentd-restart.td")

        c.kill("storage")
        c.run("testdrive", "storage/03-while-clusterd-down.td")

        c.up("storage")
        c.run("testdrive", "storage/04-after-clusterd-restart.td")


def workflow_test_drop_default_cluster(c: Composition) -> None:
    """Test that the default cluster can be dropped"""

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.wait_for_materialized()

    c.sql("DROP CLUSTER default CASCADE")
    c.sql("CREATE CLUSTER default REPLICAS (default (SIZE '1'))")


def workflow_test_resource_limits(c: Composition) -> None:
    """Test resource limits in Materialize."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(),
        Postgres(),
        Materialized(),
    ):
        dependencies = [
            "materialized",
            "postgres",
        ]
        c.start_and_wait_for_tcp(
            services=dependencies,
        )

        c.run("testdrive", "resources/resource-limits.td")


def workflow_test_builtin_migration(c: Composition) -> None:
    """Exercise the builtin object migration code by upgrading between two versions
    that will have a migration triggered between them. Create a materialized view
    over the affected builtin object to confirm that the migration was successful
    """

    c.down(destroy_volumes=True)
    with c.override(
        # Random commit before the migrations that we are testing.
        Materialized(
            image="materialize/materialized:devel-aa4128c9c485322f90ab0af2b9cb4d16e1c470c0",
            default_size=1,
        ),
        Testdrive(default_timeout="15s", no_reset=True, consistent_seed=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.wait_for_materialized()

        c.testdrive(
            input=dedent(
                """
        # pg_catalog.pg_proc migration

        # The limit is added to avoid having to update the number every time we add a function.
        > CREATE VIEW v1 AS SELECT COUNT(*) FROM (SELECT * FROM pg_proc ORDER BY oid LIMIT 5);
        > SELECT * FROM v1;
        5
        ! SELECT DISTINCT proowner FROM pg_proc;
        contains:column "proowner" does not exist

        # mz_internal.mz_dataflow_operator_reachability migration

        # Populate mz_dataflow_operator_reachability
        > CREATE TABLE t (a INT);
        > CREATE DEFAULT INDEX ON t;

        > SELECT pg_typeof(address) FROM mz_internal.mz_dataflow_operator_reachability LIMIT 1;
        "bigint list"

        # mz_internal.mz_cluster_replica_statuses migration

        > SELECT pg_typeof(process_id) FROM mz_internal.mz_cluster_replica_statuses LIMIT 1;
        "bigint"

        ! SELECT updated_at FROM mz_internal.mz_cluster_replica_statuses;
        contains:column "updated_at" does not exist

        > SELECT last_update FROM mz_internal.mz_cluster_replica_statuses LIMIT 0;

        # mz_internal.mz_show_cluster_replicas migration

        ! SELECT ready FROM mz_internal.mz_show_cluster_replicas LIMIT 0;
        contains:column "ready" does not exist

        # mz_catalog.mz_sources migration

        > CREATE MATERIALIZED VIEW source_types AS SELECT type FROM mz_catalog.mz_sources WHERE id LIKE 'u%';

        > CREATE SOURCE load_gen_source FROM LOAD GENERATOR COUNTER WITH (SIZE '1');

        > SELECT * FROM source_types
        load-generator
    """
            )
        )

        c.kill("materialized")

    with c.override(
        # This will stop working if we introduce a breaking change.
        Materialized(),
        Testdrive(default_timeout="15s", no_reset=True, consistent_seed=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.wait_for_materialized()

        c.testdrive(
            input=dedent(
                """
        # pg_catalog.pg_proc migration

        > SELECT * FROM v1;
        5
        # This column is new after the migration
        > SELECT DISTINCT proowner FROM pg_proc;
        <null>

        # mz_internal.mz_dataflow_operator_reachability migration

        > SELECT pg_typeof(address) FROM mz_internal.mz_dataflow_operator_reachability LIMIT 1;
        "uint8 list"

        # mz_internal.mz_cluster_replica_statuses migration

        > SELECT pg_typeof(process_id) FROM mz_internal.mz_cluster_replica_statuses LIMIT 1;
        "uint8"

        ! SELECT last_update FROM mz_internal.mz_cluster_replica_statuses;
        contains:column "last_update" does not exist

        > SELECT updated_at FROM mz_internal.mz_cluster_replica_statuses LIMIT 0;

        # mz_internal.mz_show_cluster_replicas migration

        > SELECT ready FROM mz_internal.mz_show_cluster_replicas LIMIT 0;

        # mz_catalog.mz_sources migration

        > SELECT * FROM source_types
        load-generator
    """
            )
        )

    # Restart materialize and test that everything still works to ensure that the migration was persisted correctly.
    with c.override(
        # This will stop working if we introduce a breaking change.
        Materialized(),
        Testdrive(default_timeout="15s", no_reset=True, consistent_seed=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.wait_for_materialized()

        c.testdrive(
            input=dedent(
                """
        # pg_catalog.pg_proc migration

        > SELECT * FROM v1;
        5
        # This column is new after the migration
        > SELECT DISTINCT proowner FROM pg_proc;
        <null>

        # mz_internal.mz_dataflow_operator_reachability migration

        > SELECT pg_typeof(address) FROM mz_internal.mz_dataflow_operator_reachability LIMIT 1;
        "uint8 list"

        # mz_internal.mz_cluster_replica_statuses migration

        > SELECT pg_typeof(process_id) FROM mz_internal.mz_cluster_replica_statuses LIMIT 1;
        "uint8"

        ! SELECT last_update FROM mz_internal.mz_cluster_replica_statuses;
        contains:column "last_update" does not exist

        > SELECT updated_at FROM mz_internal.mz_cluster_replica_statuses LIMIT 0;

        # mz_internal.mz_show_cluster_replicas migration

        > SELECT ready FROM mz_internal.mz_show_cluster_replicas LIMIT 0;

        # mz_catalog.mz_sources migration

        # mz_catalog.mz_sources migration

        > SELECT * FROM source_types
        load-generator
    """
            )
        )


def workflow_pg_snapshot_resumption(c: Composition) -> None:
    """Test PostgreSQL snapshot resumption."""

    c.down(destroy_volumes=True)

    with c.override(
        # Start postgres for the pg source
        Postgres(),
        Testdrive(no_reset=True),
        Clusterd(name="storage", environment=["FAILPOINTS=pg_snapshot_failure=return"]),
    ):
        dependencies = [
            "materialized",
            "postgres",
            "storage",
        ]
        c.start_and_wait_for_tcp(
            services=dependencies,
        )

        c.run("testdrive", "pg-snapshot-resumption/01-configure-postgres.td")
        c.run("testdrive", "pg-snapshot-resumption/02-create-sources.td")

        # Temporarily disabled because it is timing out.
        # https://github.com/MaterializeInc/materialize/issues/14533
        # # clusterd should crash
        # c.run("testdrive", "pg-snapshot-resumption/03-while-clusterd-down.td")

        print("Sleeping to ensure that clusterd crashes")
        time.sleep(10)

        with c.override(
            # turn off the failpoint
            Clusterd(name="storage")
        ):
            c.start_and_wait_for_tcp(
                services=["storage"],
            )
            c.run("testdrive", "pg-snapshot-resumption/04-verify-data.td")


def workflow_test_bootstrap_vars(c: Composition) -> None:
    """Test default system vars values passed with a CLI option."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True),
        Materialized(
            options="--bootstrap-system-parameter=\"allowed_cluster_replica_sizes='1', '2', 'oops'\"",
        ),
    ):
        dependencies = [
            "materialized",
        ]
        c.start_and_wait_for_tcp(
            services=dependencies,
        )

        c.run("testdrive", "resources/bootstrapped-system-vars.td")

    with c.override(
        Testdrive(no_reset=True),
        Materialized(
            environment_extra=[
                """ MZ_BOOTSTRAP_SYSTEM_PARAMETER=allowed_cluster_replica_sizes='1', '2', 'oops'""".strip()
            ],
        ),
    ):
        dependencies = [
            "materialized",
        ]
        c.start_and_wait_for_tcp(
            services=dependencies,
        )

        c.run("testdrive", "resources/bootstrapped-system-vars.td")


def workflow_test_system_table_indexes(c: Composition) -> None:
    """Test system table indexes."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(),
        Materialized(),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.wait_for_materialized()
        c.testdrive(
            input=dedent(
                """
        > CREATE DEFAULT INDEX ON mz_views;
        > SELECT id FROM mz_indexes WHERE id like 'u%';
        u1
    """
            )
        )
        c.kill("materialized")

    with c.override(
        Testdrive(),
        Materialized(),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.wait_for_materialized()
        c.testdrive(
            input=dedent(
                """
        > SELECT id FROM mz_indexes WHERE id like 'u%';
        u1
    """
            )
        )
