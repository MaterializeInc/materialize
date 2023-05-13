# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import time
from textwrap import dedent
from threading import Thread
from typing import Tuple

from pg8000 import Cursor
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
        "test-github-17177",
        "test-github-17510",
        "test-github-17509",
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
        "test-compute-reconciliation-no-errors",
        "test-mz-subscriptions",
        "test-mv-source-sink",
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
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    c.sql(
        """
            CREATE CLUSTER cluster1 REPLICAS (replica1 (
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
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

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
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

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

    c.sql(
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

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

    c.sql(
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

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

    def extract_frontiers(output: str) -> Tuple[int, int]:
        j = json.loads(output)
        (upper,) = j["determination"]["upper"]["elements"]
        (since,) = j["determination"]["since"]["elements"]
        return (upper, since)

    # Verify that there are no empty frontiers.
    output = c.sql_query("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM mv")
    mv_since, mv_upper = extract_frontiers(output[0][0])
    output = c.sql_query("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM t")
    t_since, t_upper = extract_frontiers(output[0][0])

    assert mv_since, "mv has empty since frontier"
    assert mv_upper, "mv has empty upper frontier"
    assert t_since, "t has empty since frontier"
    assert t_upper, "t has empty upper frontier"


def workflow_test_github_15799(c: Composition) -> None:
    """
    Test that querying introspection sources on a replica does not
    crash other replicas in the same cluster that have introspection disabled.

    Regression test for https://github.com/MaterializeInc/materialize/issues/15799.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")
    c.up("clusterd2")

    c.sql(
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

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

        -- query the introspection sources on the replica with logging enabled
        SET cluster_replica = logging_on;
        SELECT * FROM mz_internal.mz_active_peeks, mz_internal.mz_compute_exports;

        -- verify that the other replica has not crashed and still responds
        SET cluster_replica = logging_off;
        SELECT * FROM mz_tables, mz_sources;
        """
    )


def workflow_test_github_15930(c: Composition) -> None:
    """
    Test that triggering reconciliation does not wedge the
    mz_compute_frontiers_per_worker introspection source.

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
            "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

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
            > SELECT 1 FROM mz_internal.mz_compute_frontiers_per_worker LIMIT 1;
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
            > SELECT 1 FROM mz_internal.mz_compute_frontiers_per_worker LIMIT 1;
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
            > SELECT 1 FROM mz_internal.mz_compute_frontiers_per_worker LIMIT 1;
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

        c.sql(
            "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

        c.sql(
            "ALTER SYSTEM SET enable_repeat_row = true;",
            port=6877,
            user="mz_system",
        )

        # set up a test cluster and run a testdrive regression script
        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                r1 (
                    STORAGECTL ADDRESSES ['clusterd_nopanic:2100'],
                    STORAGE ADDRESSES ['clusterd_nopanic:2103'],
                    COMPUTECTL ADDRESSES ['clusterd_nopanic:2101'],
                    COMPUTE ADDRESSES ['clusterd_nopanic:2102'],
                    WORKERS 2
                )
            );
            -- Set data for test up.
            SET cluster = cluster1;
            CREATE TABLE base (data bigint, diff bigint);
            CREATE MATERIALIZED VIEW data AS SELECT data FROM base, repeat_row(diff);
            INSERT INTO base VALUES (1, 1);
            INSERT INTO base VALUES (1, -1), (1, -1);

            -- Create a materialized view to ensure non-monotonic rendering.
            -- Note that we employ below a query hint to hit the case of not yet
            -- generating a SQL-level error, given the partial fix to bucketed
            -- aggregates introduced in PR #17918.
            CREATE MATERIALIZED VIEW sum_and_max AS
            SELECT SUM(data), MAX(data) FROM data OPTIONS (EXPECTED GROUP SIZE = 1);
            """
        )
        c.testdrive(
            dedent(
                """
            > SET cluster = cluster1;

            # Run a query that would generate a panic before the fix.
            ! SELECT * FROM sum_and_max;
            contains:Non-positive accumulation in ReduceMinsMaxes
            """
            )
        )

        # ensure that an error was put into the logs
        c1 = c.invoke("logs", "clusterd_nopanic", capture=True)
        assert "Non-positive accumulation in ReduceMinsMaxes" in c1.stdout


def workflow_test_github_17177(c: Composition) -> None:
    """
    Test that an accumulable reduction over a source with an invalid accumulation not only
    emits errors to the logs when soft assertions are turned off, but also produces a clean
    query-level error.

    Regression test for https://github.com/MaterializeInc/materialize/issues/17177.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")

        c.sql(
            "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

        # set up a test cluster and run a testdrive regression script
        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                r1 (
                    STORAGECTL ADDRESSES ['clusterd1:2100'],
                    STORAGE ADDRESSES ['clusterd1:2103'],
                    COMPUTECTL ADDRESSES ['clusterd1:2101'],
                    COMPUTE ADDRESSES ['clusterd1:2102'],
                    WORKERS 2
                )
            );
            """
        )

        c.testdrive(
            dedent(
                """
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_repeat_row  = true;

            # Set data for test up
            > SET cluster = cluster1;

            > CREATE TABLE base (data float, diff bigint);

            > CREATE MATERIALIZED VIEW data AS SELECT data FROM base, repeat_row(diff);

            > INSERT INTO base VALUES (1.00, 1);

            > INSERT INTO base VALUES (1.01, -1);

            # The query below would not fail previously, but now should produce
            # a SQL-level error that is observable by users.
            ! SELECT SUM(data) FROM data;
            contains:Invalid data in source, saw net-zero records for key

            # It should be possible to fix the data in the source and make the error
            # go away.
            > INSERT INTO base VALUES (1.01, 1);

            > SELECT SUM(data) FROM data;
            1
            """
            )
        )

        # ensure that an error was put into the logs
        c1 = c.invoke("logs", "clusterd1", capture=True)
        assert (
            "Net-zero records with non-zero accumulation in ReduceAccumulable"
            in c1.stdout
        )


def workflow_test_github_17510(c: Composition) -> None:
    """
    Test that sum aggregations over uint2 and uint4 types do not produce a panic
    when soft assertions are turned off, but rather a SQL-level error when faced
    with invalid accumulations due to too many retractions in a source. Additionally,
    we verify that in these cases, an adequate error message is written to the logs.

    Regression test for https://github.com/MaterializeInc/materialize/issues/17510.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")

        c.sql(
            "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

        c.sql(
            "ALTER SYSTEM SET enable_repeat_row = true;",
            port=6877,
            user="mz_system",
        )

        # set up a test cluster and run a testdrive regression script
        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                r1 (
                    STORAGECTL ADDRESSES ['clusterd1:2100'],
                    STORAGE ADDRESSES ['clusterd1:2103'],
                    COMPUTECTL ADDRESSES ['clusterd1:2101'],
                    COMPUTE ADDRESSES ['clusterd1:2102'],
                    WORKERS 2
                )
            );
            -- Set data for test up
            SET cluster = cluster1;
            CREATE TABLE base (data2 uint2, data4 uint4, data8 uint8, diff bigint);
            CREATE MATERIALIZED VIEW data AS
              SELECT data2, data4, data8
              FROM base, repeat_row(diff);
            CREATE MATERIALIZED VIEW sum_types AS
              SELECT SUM(data2) AS sum2, SUM(data4) AS sum4, SUM(data8) AS sum8
              FROM data;
            INSERT INTO base VALUES (1, 1, 1, 1);
            INSERT INTO base VALUES (1, 1, 1, -1), (1, 1, 1, -1);
            CREATE MATERIALIZED VIEW constant_sums AS
              SELECT SUM(data2) AS sum2, SUM(data4) AS sum4, SUM(data8) AS sum8
              FROM (
                  SELECT * FROM (
                      VALUES (1::uint2, 1::uint4, 1::uint8, 1),
                          (1::uint2, 1::uint4, 1::uint8, -1),
                          (1::uint2, 1::uint4, 1::uint8, -1)
                  ) AS base (data2, data4, data8, diff),
                  repeat_row(diff)
              );
              CREATE MATERIALIZED VIEW constant_wrapped_sums AS
              SELECT SUM(data2) AS sum2, SUM(data4) AS sum4, SUM(data8) AS sum8
              FROM (
                  SELECT * FROM (
                      VALUES (2::uint2, 2::uint4, 2::uint8, 9223372036854775807),
                        (1::uint2, 1::uint4, 1::uint8, 1),
                        (1::uint2, 1::uint4, 1::uint8, 1),
                        (1::uint2, 1::uint4, 1::uint8, 1)
                  ) AS base (data2, data4, data8, diff),
                  repeat_row(diff)
              );
            """
        )
        c.testdrive(
            dedent(
                """
            > SET cluster = cluster1;

            # Run a queries that would generate panics before the fix.
            ! SELECT SUM(data2) FROM data;
            contains:Invalid data in source, saw negative accumulation with unsigned type for key

            ! SELECT SUM(data4) FROM data;
            contains:Invalid data in source, saw negative accumulation with unsigned type for key

            ! SELECT * FROM constant_sums;
            contains:constant folding encountered reduce on collection with non-positive multiplicities

            # The following statement verifies that the behavior introduced in PR #16852
            # is now rectified, i.e., instead of wrapping to a negative number, we produce
            # an error upon seeing invalid multiplicities.
            ! SELECT SUM(data8) FROM data;
            contains:Invalid data in source, saw negative accumulation with unsigned type for key

            # Test repairs
            > INSERT INTO base VALUES (1, 1, 1, 1), (1, 1, 1, 1);

            > SELECT SUM(data2) FROM data;
            1

            > SELECT SUM(data4) FROM data;
            1

            > SELECT SUM(data8) FROM data;
            1

            # Ensure that the output types for uint sums are unaffected.
            > SELECT c.name, c.type
              FROM mz_materialized_views mv
                   JOIN mz_columns c USING (id)
              WHERE mv.name = 'sum_types'
              ORDER BY c.type, c.name;
            sum8 numeric
            sum2 uint8
            sum4 uint8

            > SELECT c.name, c.type
              FROM mz_materialized_views mv
                   JOIN mz_columns c USING (id)
              WHERE mv.name = 'constant_sums'
              ORDER BY c.type, c.name;
            sum8 numeric
            sum2 uint8
            sum4 uint8

            # Test wraparound behaviors
            > INSERT INTO base VALUES (1, 1, 1, -1);

            > INSERT INTO base VALUES (2, 2, 2, 9223372036854775807);

            > SELECT sum(data2) FROM data;
            18446744073709551614

            > SELECT sum(data4) FROM data;
            18446744073709551614

            > SELECT sum(data8) FROM data;
            18446744073709551614

            > INSERT INTO base VALUES (1, 1, 1, 1), (1, 1, 1, 1), (1, 1, 1, 1);

            # Constant-folding behavior matches for now the rendered behavior
            # wrt. wraparound; this can be revisited as part of #17758.
            > SELECT * FROM constant_wrapped_sums;
            1 1 18446744073709551617

            > SELECT SUM(data2) FROM data;
            1

            > SELECT SUM(data4) FROM data;
            1

            > SELECT SUM(data8) FROM data;
            18446744073709551617
            """
            )
        )

        # ensure that an error was put into the logs
        c1 = c.invoke("logs", "clusterd1", capture=True)
        assert "Invalid negative unsigned aggregation in ReduceAccumulable" in c1.stdout


def workflow_test_github_17509(c: Composition) -> None:
    """
    Test that a bucketed hierarchical reduction over a source with an invalid accumulation produces
    a clean error when an arrangement hierarchy is built, in addition to logging an error, when soft
    assertions are turned off.

    This is a partial regression test for https://github.com/MaterializeInc/materialize/issues/17509.
    It is still possible to trigger the behavior described in the issue by opting into
    a smaller group size with a query hint (e.g., OPTIONS (EXPECTED GROUP SIZE = 1)).
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

        c.sql(
            "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

        c.sql(
            "ALTER SYSTEM SET enable_repeat_row = true;",
            port=6877,
            user="mz_system",
        )

        # set up a test cluster and run a testdrive regression script
        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (
                r1 (
                    STORAGECTL ADDRESSES ['clusterd_nopanic:2100'],
                    STORAGE ADDRESSES ['clusterd_nopanic:2103'],
                    COMPUTECTL ADDRESSES ['clusterd_nopanic:2101'],
                    COMPUTE ADDRESSES ['clusterd_nopanic:2102'],
                    WORKERS 2
                )
            );
            -- Set data for test up.
            SET cluster = cluster1;
            CREATE TABLE base (data bigint, diff bigint);
            CREATE MATERIALIZED VIEW data AS SELECT data FROM base, repeat_row(diff);
            INSERT INTO base VALUES (1, 1);
            INSERT INTO base VALUES (1, -1), (1, -1);

            -- Create materialized views to ensure non-monotonic rendering.
            CREATE MATERIALIZED VIEW max_data AS
            SELECT MAX(data) FROM data;
            CREATE MATERIALIZED VIEW max_group_by_data AS
            SELECT data, MAX(data) FROM data GROUP BY data;
            """
        )
        c.testdrive(
            dedent(
                """
            > SET cluster = cluster1;

            # The query below would return a null previously, but now fails cleanly.
            ! SELECT * FROM max_data;
            contains:Invalid data in source, saw non-positive accumulation for key

            ! SELECT * FROM max_group_by_data;
            contains:Invalid data in source, saw non-positive accumulation for key

            # Repairing the error must be possible.
            > INSERT INTO base VALUES (1, 2), (2, 1);

            > SELECT * FROM max_data;
            2

            > SELECT * FROM max_group_by_data;
            1 1
            2 2
            """
            )
        )

        # ensure that an error was put into the logs
        c1 = c.invoke("logs", "clusterd_nopanic", capture=True)
        assert "Non-positive accumulation in MinsMaxesHierarchical" in c1.stdout
        assert "Negative accumulation in ReduceMinsMaxes" not in c1.stdout


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
        c.run("testdrive", "pg-snapshot-resumption/03-ensure-source-down.td")

        # Temporarily disabled because it is timing out.
        # https://github.com/MaterializeInc/materialize/issues/14533
        # # clusterd should crash
        # c.run("testdrive", "pg-snapshot-resumption/04-while-clusterd-down.td")

        with c.override(
            # turn off the failpoint
            Clusterd(name="storage")
        ):
            c.up("storage")
            c.run("testdrive", "pg-snapshot-resumption/05-verify-data.td")


def workflow_test_bootstrap_vars(c: Composition) -> None:
    """Test default system vars values passed with a CLI option."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True),
        Materialized(
            options=[
                "--system-var-default=allowed_cluster_replica_sizes='1', '2', 'oops'"
            ],
        ),
    ):
        c.up("materialized")

        c.run("testdrive", "resources/bootstrapped-system-vars.td")

    with c.override(
        Testdrive(no_reset=True),
        Materialized(
            environment_extra=[
                """ MZ_SYSTEM_PARAMETER_DEFAULT=allowed_cluster_replica_sizes='1', '2', 'oops'""".strip()
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
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

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

    c.sql(
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

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


def workflow_test_compute_reconciliation_no_errors(c: Composition) -> None:
    """
    Test that no errors are logged during or after compute
    reconciliation.

    This is generally useful to find unknown issues, and specifically
    to verify that replicas don't send unexpected compute responses
    in the process of reconciliation.
    """

    c.down(destroy_volumes=True)

    c.up("materialized")
    c.up("clusterd1")

    c.sql(
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

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

    # Set up a subscribe dataflow that will be dropped during reconciliation.
    cursor = c.sql_cursor()
    cursor.execute("SET cluster = cluster1")
    cursor.execute("INSERT INTO t1 VALUES (1)")
    cursor.execute("BEGIN")
    cursor.execute("DECLARE c CURSOR FOR SUBSCRIBE t1")
    cursor.execute("FETCH 1 c")

    # Perform a query to ensure dataflows have been installed.
    c.sql(
        """
        SET cluster = cluster1;
        SELECT * FROM t1, v, mv1, mv2;
        """
    )

    # We don't have much control over compute reconciliation from here. We
    # drop a dataflow and immediately kill environmentd, in hopes of maybe
    # provoking an interesting race that way.
    c.sql("DROP MATERIALIZED VIEW mv2")

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

    # Verify the absence of logged errors.
    for service in ("materialized", "clusterd1"):
        p = c.invoke("logs", service, capture=True)
        for line in p.stdout.splitlines():
            assert "ERROR" not in line, f"found ERROR in service {service}: {line}"


def workflow_test_mz_subscriptions(c: Composition) -> None:
    """
    Test that in-progress subscriptions are reflected in
    mz_subscriptions.
    """

    c.down(destroy_volumes=True)
    c.up("materialized", "clusterd1")

    c.sql(
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    c.sql(
        """
        CREATE CLUSTER cluster1 REPLICAS (r (
                STORAGECTL ADDRESSES ['clusterd1:2100'],
                STORAGE ADDRESSES ['clusterd1:2103'],
                COMPUTECTL ADDRESSES ['clusterd1:2101'],
                COMPUTE ADDRESSES ['clusterd1:2102'],
                WORKERS 1
        ));

        CREATE TABLE t1 (a int);
        CREATE TABLE t2 (a int);
        CREATE TABLE t3 (a int);
        INSERT INTO t1 VALUES (1);
        INSERT INTO t2 VALUES (1);
        INSERT INTO t3 VALUES (1);
        """
    )

    def start_subscribe(table: str, cluster: str) -> Cursor:
        """Start a subscribe on the given table and cluster."""
        cursor = c.sql_cursor()
        cursor.execute(f"SET cluster = {cluster}")
        cursor.execute("BEGIN")
        cursor.execute(f"DECLARE c CURSOR FOR SUBSCRIBE {table}")
        cursor.execute("FETCH 1 c")
        return cursor

    def stop_subscribe(cursor: Cursor) -> None:
        """Stop a susbscribe started with `start_subscribe`."""
        cursor.execute("ROLLBACK")

    def check_mz_subscriptions(expected: Tuple) -> None:
        """
        Check that the expected subscribes exist in mz_subscriptions.
        We identify subscribes by user, cluster, and target table only.
        We explicitly don't check the `GlobalId`, as how that is
        allocated is an implementation detail and might change in the
        future.
        """
        output = c.sql_query(
            """
            SELECT s.user, c.name, t.name
            FROM mz_internal.mz_subscriptions s
              JOIN mz_clusters c ON (c.id = s.cluster_id)
              JOIN mz_tables t ON (t.id = s.referenced_object_ids[1])
            ORDER BY s.created_at
            """
        )
        assert output == expected, f"expected: {expected}, got: {output}"

    subscribe1 = start_subscribe("t1", "default")
    check_mz_subscriptions((["materialize", "default", "t1"],))

    subscribe2 = start_subscribe("t2", "cluster1")
    check_mz_subscriptions(
        (
            ["materialize", "default", "t1"],
            ["materialize", "cluster1", "t2"],
        )
    )

    stop_subscribe(subscribe1)
    check_mz_subscriptions((["materialize", "cluster1", "t2"],))

    subscribe3 = start_subscribe("t3", "default")
    check_mz_subscriptions(
        (
            ["materialize", "cluster1", "t2"],
            ["materialize", "default", "t3"],
        )
    )

    stop_subscribe(subscribe3)
    check_mz_subscriptions((["materialize", "cluster1", "t2"],))

    stop_subscribe(subscribe2)
    check_mz_subscriptions(())


def workflow_test_mv_source_sink(c: Composition) -> None:
    """
    Test that compute materialized view's "since" timestamp is at least as large as source table's "since" timestamp.

    Regression test for https://github.com/MaterializeInc/materialize/issues/19151
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")

    c.sql(
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

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
        """
    )

    def extract_since_ts(output: str) -> int:
        j = json.loads(output)
        (since,) = j["determination"]["since"]["elements"]
        return int(since)

    # Verify that there are no empty frontiers.
    output = c.sql_query("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM t")
    t_since = extract_since_ts(output[0][0])
    output = c.sql_query("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM mv")
    mv_since = extract_since_ts(output[0][0])

    assert (
        mv_since >= t_since
    ), f'"since" timestamp of mv ({mv_since}) is less than "since" timestamp of its source table ({t_since})'
