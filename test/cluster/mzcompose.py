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
from threading import Thread
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
    Cockroach(setup_materialize=True),
    Clusterd(name="clusterd1"),
    Clusterd(name="clusterd2"),
    Clusterd(name="clusterd3"),
    Clusterd(name="clusterd4"),
    # We use mz_panic() in some test scenarios, so environmentd must stay up.
    Materialized(propagate_crashes=False, external_cockroach=True),
    Redpanda(),
    Testdrive(
        volume_workdir="../testdrive:/workdir/testdrive",
        volumes_extra=[".:/workdir/smoke"],
        materialize_params={"cluster": "cluster1"},
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    for name in [
        "test-smoke",
        "test-github-12251",
        "test-github-15531",
        "test-github-15535",
        "test-github-15799",
        "test-github-15930",
        "test-github-15496",
        "test-remote-storage",
        "test-drop-default-cluster",
        "test-upsert",
        "test-resource-limits",
        "test-invalid-compute-reuse",
        "pg-snapshot-resumption",
        "pg-snapshot-partial-failure",
        "test-system-table-indexes",
        "test-replica-targeted-subscribe-abort",
        "test-compute-reconciliation-reuse",
    ]:
        with c.test_case(name):
            c.workflow(name)


def workflow_test_smoke(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive in a variety of cluster configurations."""

    parser.add_argument(
        "glob",
        nargs="*",
        default=["smoke/*.td"],
        help="run against the specified files",
    )
    args = parser.parse_args()

    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "localstack")
    c.up("materialized")

    # Create a cluster and verify that tests pass.
    c.up("clusterd1")
    c.up("clusterd2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        """CREATE CLUSTER cluster1 REPLICAS (replica1 (
            STORAGECTL ADDRESSES ['clusterd1:2100', 'clusterd2:2100'],
            STORAGE ADDRESSES ['clusterd1:2103', 'clusterd2:2103'],
            COMPUTECTL ADDRESSES ['clusterd1:2101', 'clusterd2:2101'],
            COMPUTE ADDRESSES ['clusterd1:2102', 'clusterd2:2102'],
            WORKERS 2
        ));
    """
    )
    c.run("testdrive", *args.glob)

    # Add a replica to that cluster and verify that tests still pass.
    c.up("clusterd3")
    c.up("clusterd4")
    c.sql(
        """CREATE CLUSTER REPLICA cluster1.replica2
            STORAGECTL ADDRESSES ['clusterd3:2100', 'clusterd4:2100'],
            STORAGE ADDRESSES ['clusterd3:2103', 'clusterd4:2103'],
            COMPUTECTL ADDRESSES ['clusterd3:2101', 'clusterd4:2101'],
            COMPUTE ADDRESSES ['clusterd3:2102', 'clusterd4:2102'],
            WORKERS 2
    """
    )
    c.run("testdrive", *args.glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.kill("clusterd1")
    c.run("testdrive", *args.glob)

    # Leave only replica 2 up and verify that tests still pass.
    c.sql("DROP CLUSTER REPLICA cluster1.replica1")
    c.run("testdrive", *args.glob)


def workflow_test_invalid_compute_reuse(c: Composition) -> None:
    """Ensure clusterds correctly crash if used in unsupported communication config"""
    c.down(destroy_volumes=True)
    c.up("materialized")

    # Create a remote cluster and verify that tests pass.
    c.up("clusterd1")
    c.up("clusterd2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        """CREATE CLUSTER cluster1 REPLICAS (replica1 (
            STORAGECTL ADDRESSES ['clusterd1:2100', 'clusterd2:2100'],
            STORAGE ADDRESSES ['clusterd1:2103', 'clusterd2:2103'],
            COMPUTECTL ADDRESSES ['clusterd1:2101', 'clusterd2:2101'],
            COMPUTE ADDRESSES ['clusterd1:2102', 'clusterd2:2102'],
            WORKERS 2
        ));
    """
    )
    c.sql("DROP CLUSTER cluster1 CASCADE;")

    # Note the different WORKERS argument
    c.sql(
        """CREATE CLUSTER cluster1 REPLICAS (replica1 (
            STORAGECTL ADDRESSES ['clusterd1:2100', 'clusterd2:2100'],
            STORAGE ADDRESSES ['clusterd1:2103', 'clusterd2:2103'],
            COMPUTECTL ADDRESSES ['clusterd1:2101', 'clusterd2:2101'],
            COMPUTE ADDRESSES ['clusterd1:2102', 'clusterd2:2102'],
            WORKERS 1
        ));
    """
    )

    # This should ensure that compute crashed (and does not just hang forever)
    c1 = c.invoke("logs", "clusterd1", capture=True)
    assert (
        "halting process: new timely configuration does not match existing timely configuration"
        in c1.stdout
    )


def workflow_test_github_12251(c: Composition) -> None:
    """Test that clients do not wait indefinitely for a crashed resource."""

    c.down(destroy_volumes=True)
    c.up("materialized")

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
    c.up("clusterd1")

    # helper function to get command history metrics for clusterd
    def find_clusterd_command_history_metrics(c: Composition) -> Tuple[int, int]:
        metrics = c.exec(
            "clusterd1", "curl", "localhost:6878/metrics", capture=True
        ).stdout

        history_len = None
        dataflow_count = None
        for metric in metrics.splitlines():
            if metric.startswith("mz_compute_command_history_size"):
                history_len = int(metric[len("mz_compute_command_history_size") :])
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
            STORAGECTL ADDRESSES ['clusterd1:2100'],
            STORAGE ADDRESSES ['clusterd1:2103'],
            COMPUTECTL ADDRESSES ['clusterd1:2101'],
            COMPUTE ADDRESSES ['clusterd1:2102'],
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
    c.up("clusterd1")

    # Set up a dataflow on clusterd.
    c.sql(
        """
        CREATE CLUSTER cluster1 REPLICAS (replica1 (
            STORAGECTL ADDRESSES ['clusterd1:2100'],
            STORAGE ADDRESSES ['clusterd1:2103'],
            COMPUTECTL ADDRESSES ['clusterd1:2101'],
            COMPUTE ADDRESSES ['clusterd1:2102'],
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
    c.up("clusterd1")
    c.up("clusterd2")

    c.sql(
        """
        CREATE CLUSTER cluster1 REPLICAS (
            logging_on (
                STORAGECTL ADDRESSES ['clusterd1:2100'],
                STORAGE ADDRESSES ['clusterd1:2103'],
                COMPUTECTL ADDRESSES ['clusterd1:2101'],
                COMPUTE ADDRESSES ['clusterd1:2102'],
                WORKERS 2
            ),
            logging_off (
                STORAGECTL ADDRESSES ['clusterd1:2100'],
                STORAGE ADDRESSES ['clusterd1:2103'],
                COMPUTECTL ADDRESSES ['clusterd2:2101'],
                COMPUTE ADDRESSES ['clusterd2:2102'],
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
        c.up("clusterd1")

        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                logging_on (
                    STORAGECTL ADDRESSES ['clusterd1:2100'],
                    STORAGE ADDRESSES ['clusterd1:2103'],
                    COMPUTECTL ADDRESSES ['clusterd1:2101'],
                    COMPUTE ADDRESSES ['clusterd1:2102'],
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

        cursor = c.sql_cursor()
        cursor.execute("SET cluster = cluster1;")
        cursor.execute("BEGIN;")
        cursor.execute("DECLARE c CURSOR FOR SUBSCRIBE t;")
        cursor.execute("FETCH ALL c;")

        # Restart environmentd to trigger yet another reconciliation on clusterd.
        c.kill("materialized")
        c.up("materialized")

        # Verify yet again that we can query the introspection source and now the table.
        # The subscribe should have been dropped during reconciliation, so we expect to not find a
        # frontier entry for it.
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


def workflow_test_github_15496(c: Composition) -> None:
    """
    Test that a reduce collation over a source with an invalid accumulation does not
    panic, but rather logs errors, when soft assertions are turned off.

    Regression test for https://github.com/MaterializeInc/materialize/issues/15496.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Clusterd(
            name="clusterd_nopanic",
            environment_extra=[
                "MZ_SOFT_ASSERTIONS=0",
            ],
        ),
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd_nopanic")

        # set up a test cluster and run a testdrive regression script
        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                r1 (
                    STORAGECTL ADDRESSES ['clusterd_nopanic:2100'],
                    STORAGE ADDRESSES ['clusterd_no_panic:2103'],
                    COMPUTECTL ADDRESSES ['clusterd_nopanic:2101'],
                    COMPUTE ADDRESSES ['clusterd_nopanic:2102'],
                    WORKERS 2
                )
            );
            """
        )
        c.testdrive(
            dedent(
                f"""
            # Set data for test up
            > SET cluster = cluster1;

            > CREATE TABLE base (data bigint, diff bigint);

            > CREATE MATERIALIZED VIEW data AS SELECT data FROM base, repeat_row(diff);

            > INSERT INTO base VALUES (1, 1);

            > INSERT INTO base VALUES (1, -1), (1, -1);

            # Run a query that would generate a panic before the fix. Note that
            # we expect the query to succeed for now, but follow-up work might
            # eventually lead us to favor a SQL-level error for such a query, as
            # tracked by https://github.com/MaterializeInc/materialize/issues/17178
            > SELECT SUM(data), MAX(data) FROM data;
            <null> <null>
            """
            )
        )

        # ensure that an error was put into the logs
        c1 = c.invoke("logs", "clusterd_nopanic", capture=True)
        assert "Mismatched aggregates for key in ReduceCollation" in c1.stdout


def workflow_test_upsert(c: Composition) -> None:
    """Test creating upsert sources and continuing to ingest them after a restart."""
    with c.override(
        Testdrive(default_timeout="30s", no_reset=True, consistent_seed=True),
    ):
        c.down(destroy_volumes=True)
        c.up("materialized", "zookeeper", "kafka", "schema-registry")

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
    ):
        c.up(
            "cockroach",
            "materialized",
            "clusterd1",
            "clusterd2",
            "zookeeper",
            "kafka",
            "schema-registry",
        )

        c.run("testdrive", "storage/01-create-sources.td")

        c.kill("materialized")
        c.up("materialized")
        c.kill("clusterd1")
        c.up("clusterd1")
        c.up("clusterd2")
        c.run("testdrive", "storage/02-after-environmentd-restart.td")

        # just kill one of the clusterd's and make sure we can recover.
        # `clusterd2` will die on its own.
        c.kill("clusterd1")
        c.run("testdrive", "storage/03-while-clusterd-down.td")

        # Bring back both clusterd's
        c.up("clusterd1")
        c.up("clusterd2")
        c.run("testdrive", "storage/04-after-clusterd-restart.td")


def workflow_test_drop_default_cluster(c: Composition) -> None:
    """Test that the default cluster can be dropped"""

    c.down(destroy_volumes=True)
    c.up("materialized")

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
        c.up("materialized", "postgres")

        c.run("testdrive", "resources/resource-limits.td")


def workflow_pg_snapshot_resumption(c: Composition) -> None:
    """Test PostgreSQL snapshot resumption."""

    c.down(destroy_volumes=True)

    with c.override(
        # Start postgres for the pg source
        Postgres(),
        Testdrive(no_reset=True),
        Clusterd(
            name="storage", environment_extra=["FAILPOINTS=pg_snapshot_failure=return"]
        ),
    ):
        c.up("materialized", "postgres", "storage")

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
            c.up("storage")
            c.run("testdrive", "pg-snapshot-resumption/04-verify-data.td")


def workflow_test_bootstrap_vars(c: Composition) -> None:
    """Test default system vars values passed with a CLI option."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True),
        Materialized(
            options=[
                "--bootstrap-system-parameter=allowed_cluster_replica_sizes='1', '2', 'oops'"
            ],
        ),
    ):
        c.up("materialized")

        c.run("testdrive", "resources/bootstrapped-system-vars.td")

    with c.override(
        Testdrive(no_reset=True),
        Materialized(
            environment_extra=[
                """ MZ_BOOTSTRAP_SYSTEM_PARAMETER=allowed_cluster_replica_sizes='1', '2', 'oops'""".strip()
            ],
        ),
    ):
        c.up("materialized")
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
        c.testdrive(
            input=dedent(
                """
        > SELECT id FROM mz_indexes WHERE id like 'u%';
        u1
    """
            )
        )


def workflow_test_replica_targeted_subscribe_abort(c: Composition) -> None:
    """
    Test that a replica-targeted SUBSCRIBE is aborted when the target
    replica disconnects.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")
    c.up("clusterd2")

    c.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        CREATE CLUSTER cluster1 REPLICAS (
            replica1 (
                STORAGECTL ADDRESSES ['clusterd1:2100'],
                STORAGE ADDRESSES ['clusterd1:2103'],
                COMPUTECTL ADDRESSES ['clusterd1:2101'],
                COMPUTE ADDRESSES ['clusterd1:2102'],
                WORKERS 2
            ),
            replica2 (
                STORAGECTL ADDRESSES ['clusterd2:2100'],
                STORAGE ADDRESSES ['clusterd2:2103'],
                COMPUTECTL ADDRESSES ['clusterd2:2101'],
                COMPUTE ADDRESSES ['clusterd2:2102'],
                WORKERS 2
            )
        );
        CREATE TABLE t (a int);
        """
    )

    def drop_replica_with_delay() -> None:
        time.sleep(2)
        c.sql("DROP CLUSTER REPLICA cluster1.replica1;")

    dropper = Thread(target=drop_replica_with_delay)
    dropper.start()

    try:
        c.sql(
            """
            SET cluster = cluster1;
            SET cluster_replica = replica1;
            BEGIN;
            DECLARE c CURSOR FOR SUBSCRIBE t;
            FETCH c WITH (timeout = '5s');
            """
        )
    except ProgrammingError as e:
        assert "target replica failed or was dropped" in e.args[0]["M"], e
    else:
        assert False, "SUBSCRIBE didn't return the expected error"

    dropper.join()

    def kill_replica_with_delay() -> None:
        time.sleep(2)
        c.kill("clusterd2")

    killer = Thread(target=kill_replica_with_delay)
    killer.start()

    try:
        c.sql(
            """
            SET cluster = cluster1;
            SET cluster_replica = replica2;
            BEGIN;
            DECLARE c CURSOR FOR SUBSCRIBE t;
            FETCH c WITH (timeout = '5s');
            """
        )
    except ProgrammingError as e:
        assert "target replica failed or was dropped" in e.args[0]["M"], e
    else:
        assert False, "SUBSCRIBE didn't return the expected error"

    killer.join()


def workflow_pg_snapshot_partial_failure(c: Composition) -> None:
    """Test PostgreSQL snapshot partial failure"""

    c.down(destroy_volumes=True)

    with c.override(
        # Start postgres for the pg source
        Postgres(),
        Testdrive(no_reset=True),
        Clusterd(
            name="storage", environment_extra=["FAILPOINTS=pg_snapshot_pause=return(2)"]
        ),
    ):
        c.up("materialized", "postgres", "storage")

        c.run("testdrive", "pg-snapshot-partial-failure/01-configure-postgres.td")
        c.run("testdrive", "pg-snapshot-partial-failure/02-create-sources.td")

        c.run("testdrive", "pg-snapshot-partial-failure/03-verify-good-sub-source.td")

        c.kill("storage")
        # Restart the storage instance with the failpoint off...
        with c.override(
            # turn off the failpoint
            Clusterd(name="storage")
        ):
            c.run("testdrive", "pg-snapshot-partial-failure/04-add-more-data.td")
            c.up("storage")
            c.run("testdrive", "pg-snapshot-partial-failure/05-verify-data.td")


def workflow_test_compute_reconciliation_reuse(c: Composition) -> None:
    """
    Test that compute reconciliation reuses existing dataflows.

    Note that this is currently not working, due to #17594. This test
    tests the current, undesired behavior and must be adjusted once
    #17594 is fixed.
    """

    c.down(destroy_volumes=True)

    c.up("materialized")
    c.up("clusterd1")

    # Helper function to get reconciliation metrics for clusterd.
    def fetch_reconciliation_metrics() -> Tuple[int, int]:
        metrics = c.exec(
            "clusterd1", "curl", "localhost:6878/metrics", capture=True
        ).stdout

        reused = 0
        replaced = 0
        for metric in metrics.splitlines():
            if metric.startswith("mz_compute_reconciliation_reused_dataflows"):
                reused += int(metric.split()[1])
            elif metric.startswith("mz_compute_reconciliation_replaced_dataflows"):
                replaced += int(metric.split()[1])

        return reused, replaced

    # Set up a cluster and a number of dataflows that can be reconciled.
    c.sql(
        """
        CREATE CLUSTER cluster1 REPLICAS (replica1 (
            STORAGECTL ADDRESSES ['clusterd1:2100'],
            STORAGE ADDRESSES ['clusterd1:2103'],
            COMPUTECTL ADDRESSES ['clusterd1:2101'],
            COMPUTE ADDRESSES ['clusterd1:2102'],
            WORKERS 1
        ));
        SET cluster = cluster1;

        -- index on table
        CREATE TABLE t1 (a int);
        CREATE DEFAULT INDEX on t1;

        -- index on view
        CREATE VIEW v AS SELECT a + 1 FROM t1;
        CREATE DEFAULT INDEX on v;

        -- materialized view on table
        CREATE TABLE t2 (a int);
        CREATE MATERIALIZED VIEW mv1 AS SELECT a + 1 FROM t2;

        -- materialized view on index
        CREATE MATERIALIZED VIEW mv2 AS SELECT a + 1 FROM t1;
        """
    )

    # Give the dataflows some time to make progress and get compacted.
    # This is done to trigger the bug described in #17594.
    time.sleep(10)

    # Restart environmentd to trigger a reconciliation.
    c.kill("materialized")
    c.up("materialized")

    # Perform a query to ensure reconciliation has finished.
    c.sql(
        """
        SET cluster = cluster1;
        SELECT * FROM v;
        """
    )

    reused, replaced = fetch_reconciliation_metrics()

    # TODO(#17594): Flip these once the bug is fixed.
    assert reused == 0
    assert replaced == 4
