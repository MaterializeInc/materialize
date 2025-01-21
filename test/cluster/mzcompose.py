# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional tests which require separate clusterd containers (instead of the
usual clusterd included in the materialized container).
"""

import json
import random
import re
import time
from collections.abc import Callable
from copy import copy
from datetime import datetime, timedelta
from statistics import quantiles
from textwrap import dedent
from threading import Thread

import psycopg
import requests
from psycopg import Cursor
from psycopg.errors import (
    DatabaseError,
    InternalError_,
    OperationalError,
    QueryCanceled,
)

from materialize import buildkite, ui
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.localstack import Localstack
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.postgres import (
    CockroachOrPostgresMetadata,
    Postgres,
)
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import PropagatingThread

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Clusterd(name="clusterd1"),
    Clusterd(name="clusterd2"),
    Clusterd(name="clusterd3"),
    Clusterd(name="clusterd4"),
    Materialized(
        # We use mz_panic() in some test scenarios, so environmentd must stay up.
        propagate_crashes=False,
        external_metadata_store=True,
        additional_system_parameter_defaults={
            "unsafe_enable_unsafe_functions": "true",
            "unsafe_enable_unorchestrated_cluster_replicas": "true",
        },
    ),
    CockroachOrPostgresMetadata(),
    Postgres(),
    Redpanda(),
    Toxiproxy(),
    Testdrive(
        volume_workdir="../testdrive:/workdir/testdrive",
        volumes_extra=[".:/workdir/smoke"],
        materialize_params={"cluster": "cluster1"},
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    for name in buildkite.shard_list(
        list(c.workflows.keys()), lambda workflow: workflow
    ):
        # incident-70 requires more memory, runs in separate CI step
        # concurrent-connections is too flaky
        if name in (
            "default",
            "test-incident-70",
            "test-concurrent-connections",
        ):
            continue
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

    # Make sure cluster1 is owned by the system so it doesn't get dropped
    # between testdrive runs.
    c.sql(
        """
        ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;

        DROP CLUSTER IF EXISTS cluster1 CASCADE;

        CREATE CLUSTER cluster1 REPLICAS (
            replica1 (
                STORAGECTL ADDRESSES ['clusterd1:2100', 'clusterd2:2100'],
                STORAGE ADDRESSES ['clusterd1:2103', 'clusterd2:2103'],
                COMPUTECTL ADDRESSES ['clusterd1:2101', 'clusterd2:2101'],
                COMPUTE ADDRESSES ['clusterd1:2102', 'clusterd2:2102'],
                WORKERS 2
            )
        );

        GRANT ALL ON CLUSTER cluster1 TO materialize;
        """,
        port=6877,
        user="mz_system",
    )

    c.run_testdrive_files(*args.glob)

    # Add a replica to that cluster and verify that tests still pass.
    c.up("clusterd3")
    c.up("clusterd4")

    c.sql(
        """
        ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;

        CREATE CLUSTER REPLICA cluster1.replica2
            STORAGECTL ADDRESSES ['clusterd3:2100', 'clusterd4:2100'],
            STORAGE ADDRESSES ['clusterd3:2103', 'clusterd4:2103'],
            COMPUTECTL ADDRESSES ['clusterd3:2101', 'clusterd4:2101'],
            COMPUTE ADDRESSES ['clusterd3:2102', 'clusterd4:2102'],
            WORKERS 2;
    """,
        port=6877,
        user="mz_system",
    )
    c.run_testdrive_files(*args.glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.kill("clusterd1")
    c.run_testdrive_files(*args.glob)

    # Leave only replica 2 up and verify that tests still pass.
    c.sql("DROP CLUSTER REPLICA cluster1.replica1", port=6877, user="mz_system")
    c.run_testdrive_files(*args.glob)

    c.sql("DROP CLUSTER cluster1 CASCADE", port=6877, user="mz_system")


def workflow_test_invalid_compute_reuse(c: Composition) -> None:
    """Ensure clusterds correctly crash if used in unsupported communication config"""
    c.down(destroy_volumes=True)
    c.up("materialized")

    # Create a remote cluster and verify that tests pass.
    c.up("clusterd1")
    c.up("clusterd2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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

    for i in range(5):
        # This should ensure that compute crashed (and does not just hang forever)
        c1 = c.invoke("logs", "clusterd1", capture=True)
        if (
            "halting process: new timely configuration does not match existing timely configuration"
            in c1.stdout
        ):
            break
        # Waiting for logs to arrive
        time.sleep(1)


def workflow_test_github_3553(c: Composition) -> None:
    """Test that clients do not wait indefinitely for a crashed resource."""

    c.down(destroy_volumes=True)
    c.up("materialized")

    start_time = time.time()
    try:
        c.sql(
            """
        SET statement_timeout = '1 s';
        CREATE TABLE IF NOT EXISTS log_table (f1 TEXT);
        CREATE TABLE IF NOT EXISTS panic_table (f1 TEXT);
        INSERT INTO panic_table VALUES ('forced panic');
        -- Crash loop the cluster with the table's index
        INSERT INTO log_table SELECT mz_unsafe.mz_panic(f1) FROM panic_table;
        """
        )
    except QueryCanceled as e:
        # Ensure we received the correct error message
        assert "statement timeout" in str(e)
        # Ensure the statement_timeout setting is ~honored
        assert (
            time.time() - start_time < 2
        ), "idle_in_transaction_session_timeout not respected"
    else:
        raise RuntimeError("unexpected success in test_github_3553")

    # Ensure we can select from tables after cancellation.
    c.sql("SELECT * FROM log_table;")


def workflow_test_github_4443(c: Composition) -> None:
    """
    Test that compute command history does not leak peek commands.

    Regression test for https://github.com/MaterializeInc/database-issues/issues/4443.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")

    # helper function to get command history metrics
    def find_command_history_metrics(c: Composition) -> tuple[int, int, int, int]:
        controller_metrics = c.exec(
            "materialized", "curl", "localhost:6878/metrics", capture=True
        ).stdout
        replica_metrics = c.exec(
            "clusterd1", "curl", "localhost:6878/metrics", capture=True
        ).stdout
        metrics = controller_metrics + replica_metrics

        controller_command_count, controller_command_count_found = 0, False
        controller_dataflow_count, controller_dataflow_count_found = 0, False
        replica_command_count, replica_command_count_found = 0, False
        replica_dataflow_count, replica_dataflow_count_found = 0, False
        for metric in metrics.splitlines():
            if (
                metric.startswith("mz_compute_controller_history_command_count")
                and 'instance_id="u2"' in metric
            ):
                controller_command_count += int(metric.split()[1])
                controller_command_count_found = True
            elif (
                metric.startswith("mz_compute_controller_history_dataflow_count")
                and 'instance_id="u2"' in metric
            ):
                controller_dataflow_count += int(metric.split()[1])
                controller_dataflow_count_found = True
            elif metric.startswith("mz_compute_replica_history_command_count"):
                replica_command_count += int(metric.split()[1])
                replica_command_count_found = True
            elif metric.startswith("mz_compute_replica_history_dataflow_count"):
                replica_dataflow_count += int(metric.split()[1])
                replica_dataflow_count_found = True

        assert (
            controller_command_count_found
        ), "command count not found in controller metrics"
        assert (
            controller_dataflow_count_found
        ), "dataflow count not found in controller metrics"
        assert replica_command_count_found, "command count not found in replica metrics"
        assert (
            replica_dataflow_count_found
        ), "dataflow count not found in replica metrics"

        return (
            controller_command_count,
            controller_dataflow_count,
            replica_command_count,
            replica_dataflow_count,
        )

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
            WORKERS 1
        ));
        SET cluster = cluster1;
        -- table for fast-path peeks
        CREATE TABLE t (a int);
        CREATE DEFAULT INDEX ON t;
        INSERT INTO t VALUES (42);
        -- table for slow-path peeks
        CREATE TABLE t2 (a int);
        INSERT INTO t2 VALUES (84);

        -- Wait for the cluster to be ready.
        SELECT * FROM t;
        SELECT * FROM t2;
        """
    )

    # Wait a bit to let the metrics refresh.
    time.sleep(2)

    # Obtain initial history size and dataflow count.
    # Dataflow count can plausibly be more than 1, if compaction is delayed.
    (
        controller_command_count,
        controller_dataflow_count,
        replica_command_count,
        replica_dataflow_count,
    ) = find_command_history_metrics(c)
    assert controller_command_count > 0, "controller history cannot be empty"
    assert (
        controller_dataflow_count > 0
    ), "at least one dataflow expected in controller history"
    assert (
        controller_dataflow_count < 5
    ), "more dataflows than expected in controller history"
    assert replica_command_count > 0, "replica history cannot be empty"
    assert (
        replica_dataflow_count > 0
    ), "at least one dataflow expected in replica history"
    assert replica_dataflow_count < 5, "more dataflows than expected in replica history"

    # execute 400 fast- and slow-path peeks
    for _ in range(20):
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

    # Wait a bit to let the metrics refresh.
    time.sleep(2)

    # Check that history size and dataflow count are well-behaved.
    # Dataflow count can plausibly be more than 1, if compaction is delayed.
    (
        controller_command_count,
        controller_dataflow_count,
        replica_command_count,
        replica_dataflow_count,
    ) = find_command_history_metrics(c)
    assert (
        controller_command_count < 100
    ), "controller history grew more than expected after peeks"
    assert (
        controller_dataflow_count > 0
    ), "at least one dataflow expected in controller history"
    assert (
        controller_dataflow_count < 5
    ), "more dataflows than expected in controller history"
    assert (
        replica_command_count < 100
    ), "replica history grew more than expected after peeks"
    assert (
        replica_dataflow_count > 0
    ), "at least one dataflow expected in replica history"
    assert replica_dataflow_count < 5, "more dataflows than expected in replica history"


def workflow_test_github_4444(c: Composition) -> None:
    """
    Test that compute reconciliation does not produce empty frontiers.

    Regression test for https://github.com/MaterializeInc/database-issues/issues/4444.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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

    def extract_frontiers(output: str) -> tuple[int, int]:
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


def workflow_test_github_4545(c: Composition) -> None:
    """
    Test that querying introspection sources on a replica does not
    crash other replicas in the same cluster that have introspection disabled.

    Regression test for https://github.com/MaterializeInc/database-issues/issues/4545.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")
    c.up("clusterd2")

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
        SELECT * FROM mz_introspection.mz_active_peeks, mz_introspection.mz_compute_exports;

        -- verify that the other replica has not crashed and still responds
        SET cluster_replica = logging_off;
        SELECT * FROM mz_tables, mz_sources;
        """
    )


def workflow_test_github_4587(c: Composition) -> None:
    """
    Test that triggering reconciliation does not wedge the
    mz_compute_frontiers_per_worker introspection source.

    Regression test for https://github.com/MaterializeInc/database-issues/issues/4587.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
            > SELECT 1 FROM mz_introspection.mz_compute_frontiers_per_worker LIMIT 1;
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
            > SELECT 1 FROM mz_introspection.mz_compute_frontiers_per_worker LIMIT 1;
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
            > SELECT 1 FROM mz_introspection.mz_compute_frontiers_per_worker LIMIT 1;
            1
            > SELECT * FROM t;
            42
                """
            )
        )


def workflow_test_github_4433(c: Composition) -> None:
    """
    Test that a reduce collation over a source with an invalid accumulation does not
    panic, but rather logs errors, when soft assertions are turned off.

    Regression test for https://github.com/MaterializeInc/database-issues/issues/4433.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Clusterd(
            name="clusterd1",
            environment_extra=[
                "MZ_SOFT_ASSERTIONS=0",
            ],
        ),
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
            -- Set data for test up.
            SET cluster = cluster1;
            CREATE TABLE base (data bigint, diff bigint);
            CREATE MATERIALIZED VIEW data AS SELECT data FROM base, repeat_row(diff);
            INSERT INTO base VALUES (1, 1);
            INSERT INTO base VALUES (1, -1), (1, -1);

            -- Create a materialized view to ensure non-monotonic rendering.
            -- Note that we employ below a query hint to hit the case of not yet
            -- generating a SQL-level error, given the partial fix to bucketed
            -- aggregates introduced in PR materialize#17918.
            CREATE MATERIALIZED VIEW sum_and_max AS
            SELECT SUM(data), MAX(data) FROM data OPTIONS (AGGREGATE INPUT GROUP SIZE = 1);
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
        c1 = c.invoke("logs", "clusterd1", capture=True)
        assert "Non-positive accumulation in ReduceMinsMaxes" in c1.stdout


def workflow_test_github_4966(c: Composition) -> None:
    """
    Test that an accumulable reduction over a source with an invalid accumulation not only
    emits errors to the logs when soft assertions are turned off, but also produces a clean
    query-level error.

    Regression test for https://github.com/MaterializeInc/database-issues/issues/4966.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
            $[version>=5500] postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
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


def workflow_test_github_5087(c: Composition) -> None:
    """
    Test that sum aggregations over uint2 and uint4 types do not produce a panic
    when soft assertions are turned off, but rather a SQL-level error when faced
    with invalid accumulations due to too many retractions in a source. Additionally,
    we verify that in these cases, an adequate error message is written to the logs.

    Regression test for https://github.com/MaterializeInc/database-issues/issues/5087.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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

            # The following statement verifies that the behavior introduced in PR materialize#6122
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
            # wrt. wraparound; this can be revisited as part of database-issues#5172.
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


def workflow_test_github_5086(c: Composition) -> None:
    """
    Test that a bucketed hierarchical reduction over a source with an invalid accumulation produces
    a clean error when an arrangement hierarchy is built, in addition to logging an error, when soft
    assertions are turned off.

    This is a partial regression test for https://github.com/MaterializeInc/database-issues/issues/5086.
    The checks here are extended by opting into a smaller group size with a query hint (e.g.,
    OPTIONS (AGGREGATE INPUT GROUP SIZE = 1)) in workflow test-github-4433. This scenario was
    initially not covered, but eventually got supported as well.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Clusterd(
            name="clusterd1",
            environment_extra=[
                "MZ_SOFT_ASSERTIONS=0",
            ],
        ),
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
        c1 = c.invoke("logs", "clusterd1", capture=True)
        assert "Non-positive accumulation in MinsMaxesHierarchical" in c1.stdout
        assert "Negative accumulation in ReduceMinsMaxes" not in c1.stdout


def workflow_test_github_5831(c: Composition) -> None:
    """
    Test that a monotonic one-shot SELECT will perform consolidation without error on valid data.
    We introduce data that results in a multiset and compute min/max. In a monotonic one-shot
    evaluation strategy, we must consolidate and subsequently assert monotonicity.

    This is a regression test for https://github.com/MaterializeInc/database-issues/issues/5831, where
    we observed a performance regression caused by a correctness issue. Here, we validate that the
    underlying correctness issue has been fixed.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Clusterd(
            name="clusterd1",
            environment_extra=[
                "MZ_PERSIST_COMPACTION_DISABLED=true",
            ],
        ),
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
                    WORKERS 4
                )
            );
            -- Set data for test up.
            SET cluster = cluster1;
            CREATE TABLE base (data bigint, diff bigint);
            CREATE MATERIALIZED VIEW data AS SELECT data FROM base, repeat_row(diff);
            INSERT INTO base VALUES (1, 6);
            INSERT INTO base VALUES (1, -3), (1, -2);
            INSERT INTO base VALUES (2, 3), (2, 2);
            INSERT INTO base VALUES (2, -1), (2, -1);
            INSERT INTO base VALUES (3, 3), (3, 2);
            INSERT INTO base VALUES (3, -3), (3, -2);
            INSERT INTO base VALUES (4, 1), (4, 2);
            INSERT INTO base VALUES (4, -1), (4, -2);
            INSERT INTO base VALUES (5, 5), (5, 6);
            INSERT INTO base VALUES (5, -5), (5, -6);
            """
        )
        c.testdrive(
            dedent(
                """
            > SET cluster = cluster1;

            # Computing min/max with a monotonic one-shot SELECT requires
            # consolidation. We test here that consolidation works correctly,
            # since we assert monotonicity right after consolidating.
            # Note that we employ a cursor to avoid testdrive retries.
            # Hash functions used for exchanges in consolidation may be
            # nondeterministic and produce the correct output by chance.
            > BEGIN
            > DECLARE cur CURSOR FOR SELECT min(data), max(data) FROM data;
            > FETCH ALL cur;
            1 2
            > COMMIT;

            # To reduce the chance of a (un)lucky strike of the hash function,
            # let's do the same a few times.
            > BEGIN
            > DECLARE cur CURSOR FOR SELECT min(data), max(data) FROM data;
            > FETCH ALL cur;
            1 2
            > COMMIT;

            > BEGIN
            > DECLARE cur CURSOR FOR SELECT min(data), max(data) FROM data;
            > FETCH ALL cur;
            1 2
            > COMMIT;

            > BEGIN
            > DECLARE cur CURSOR FOR SELECT min(data), max(data) FROM data;
            > FETCH ALL cur;
            1 2
            > COMMIT;
            """
            )
        )


def workflow_test_single_time_monotonicity_enforcers(c: Composition) -> None:
    """
    Test that a monotonic one-shot SELECT where a single-time monotonicity enforcer is present
    can process a subsequent computation where consolidation can be turned off without error.
    We introduce data that results in a multiset, process these data with an enforcer, and then
    compute min/max subsequently. In a monotonic one-shot evaluation strategy, we can toggle the
    must_consolidate flag off for min/max due to the enforcer, but still use internally an
    ensure_monotonic operator to subsequently assert monotonicity. Note that Constant is already
    checked as an enforcer in test/transform/relax_must_consolidate.slt, so we focus on TopK,
    Reduce, Get, and Threshold here. This test conservatively employs cursors to avoid testdrive's
    behavior of performing repetitions to see if the output matches.
    """

    c.down(destroy_volumes=True)
    with c.override(
        Clusterd(
            name="clusterd1",
            environment_extra=[
                "MZ_PERSIST_COMPACTION_DISABLED=true",
            ],
        ),
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
                    WORKERS 4
                )
            );
            -- Set data for test up.
            SET cluster = cluster1;
            CREATE TABLE base (data bigint, diff bigint);
            CREATE MATERIALIZED VIEW data AS SELECT data FROM base, repeat_row(diff);
            INSERT INTO base VALUES (1, 6);
            INSERT INTO base VALUES (1, -3), (1, -2);
            INSERT INTO base VALUES (2, 3), (2, 2);
            INSERT INTO base VALUES (2, -1), (2, -1);
            INSERT INTO base VALUES (3, 3), (3, 2);
            INSERT INTO base VALUES (3, -3), (3, -2);
            INSERT INTO base VALUES (4, 1), (4, 2);
            INSERT INTO base VALUES (4, -1), (4, -2);
            INSERT INTO base VALUES (5, 5), (5, 6);
            INSERT INTO base VALUES (5, -5), (5, -6);
            """
        )
        c.testdrive(
            dedent(
                """
            > SET cluster = cluster1;

            # Check TopK as an enforcer
            > BEGIN
            > DECLARE cur CURSOR FOR
                SELECT MIN(data), MAX(data)
                FROM (SELECT data FROM data ORDER BY data LIMIT 5);
            > FETCH ALL cur;
            1 2
            > COMMIT;

            # Check Get and Reduce as enforcers
            > CREATE VIEW reduced_data AS
                SELECT data % 2 AS evenodd, SUM(data) AS data
                FROM data GROUP BY data % 2;

            > BEGIN
            > DECLARE cur CURSOR FOR
                SELECT MIN(data), MAX(data)
                FROM (
                    SELECT * FROM reduced_data WHERE evenodd + 1 = 1
                    UNION ALL
                    SELECT * FROM reduced_data WHERE data + 1 = 2);
            > FETCH ALL cur;
            1 6
            > COMMIT;

            # Check Threshold as enforcer
            > BEGIN
            > DECLARE cur CURSOR FOR
                SELECT MIN(data), MAX(data)
                FROM (
                    SELECT * FROM data WHERE data % 2 = 0
                    EXCEPT ALL
                    SELECT * FROM data WHERE data + 1 = 2);
            > FETCH ALL cur;
            2 2
            > COMMIT;
            """
            )
        )


def workflow_test_github_7645(c: Composition) -> None:
    """Regression test for database-issues#7645"""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True, consistent_seed=True),
    ):
        c.up(
            "materialized",
        )

        c.run_testdrive_files("github-7645/01-create-source.td")

        latency = c.sql_query(
            """
            SELECT
                (u.rehydration_latency)::text
            FROM mz_sources s
            JOIN mz_internal.mz_source_statistics u ON s.id = u.id
            WHERE s.name IN ('count')
            """
        )[0][0]

        c.kill("materialized")
        c.up("materialized")

        c.run_testdrive_files(
            f"--var=rehydration-latency={latency}",
            "github-7645/02-after-environmentd-restart.td",
        )


def workflow_test_upsert(c: Composition) -> None:
    """Test creating upsert sources and continuing to ingest them after a restart."""
    with c.override(
        Testdrive(default_timeout="30s", no_reset=True, consistent_seed=True),
    ):
        c.down(destroy_volumes=True)
        c.up("materialized", "zookeeper", "kafka", "schema-registry")

        c.run_testdrive_files("upsert/01-create-sources.td")
        # Sleep to make sure the errors have made it to persist.
        # This isn't necessary for correctness,
        # as we should be able to crash at any point and re-start.
        # But if we don't sleep here, then we might be ingesting the errored
        # records in the new process, and so we won't actually be testing
        # the ability to retract error values that make it to persist.
        print("Sleeping for ten seconds")
        time.sleep(10)
        c.exec("materialized", "bash", "-c", "kill -9 `pidof clusterd`")
        c.run_testdrive_files("upsert/02-after-clusterd-restart.td")


def workflow_test_remote_storage(c: Composition) -> None:
    """Test creating sources in a remote clusterd process."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True, consistent_seed=True),
    ):
        c.up(
            "materialized",
            "clusterd1",
            "clusterd2",
            "zookeeper",
            "kafka",
            "schema-registry",
        )

        c.run_testdrive_files("storage/01-create-sources.td")

        c.kill("materialized")
        c.up("materialized")
        c.kill("clusterd1")
        c.up("clusterd1")
        c.up("clusterd2")
        c.run_testdrive_files("storage/02-after-environmentd-restart.td")

        # just kill one of the clusterd's and make sure we can recover.
        # `clusterd2` will die on its own.
        c.kill("clusterd1")
        c.run_testdrive_files("storage/03-while-clusterd-down.td")

        # Bring back both clusterd's
        c.up("clusterd1")
        c.up("clusterd2")
        c.run_testdrive_files("storage/04-after-clusterd-restart.td")


def workflow_test_drop_quickstart_cluster(c: Composition) -> None:
    """Test that the quickstart cluster can be dropped"""

    c.down(destroy_volumes=True)
    c.up("materialized")

    c.sql("DROP CLUSTER quickstart CASCADE", user="mz_system", port=6877)
    c.sql(
        "CREATE CLUSTER quickstart REPLICAS (quickstart (SIZE '1'))",
        user="mz_system",
        port=6877,
    )


def workflow_test_resource_limits(c: Composition) -> None:
    """Test resource limits in Materialize."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(),
        Materialized(),
    ):
        c.up("materialized", "postgres")

        c.run_testdrive_files("resources/resource-limits.td")


def workflow_pg_snapshot_resumption(c: Composition) -> None:
    """Test PostgreSQL snapshot resumption."""

    c.down(destroy_volumes=True)

    with c.override(
        # Start postgres for the pg source
        Testdrive(no_reset=True),
        Clusterd(
            name="clusterd1",
            environment_extra=["FAILPOINTS=pg_snapshot_failure=return"],
        ),
    ):
        c.up("materialized", "postgres", "clusterd1")

        c.run_testdrive_files("pg-snapshot-resumption/01-configure-postgres.td")
        c.run_testdrive_files("pg-snapshot-resumption/02-create-sources.td")
        c.run_testdrive_files("pg-snapshot-resumption/03-ensure-source-down.td")

        # Temporarily disabled because it is timing out.
        # https://github.com/MaterializeInc/database-issues/issues/4145
        # # clusterd should crash
        # c.run_testdrive_files("pg-snapshot-resumption/04-while-clusterd-down.td")

        with c.override(
            # turn off the failpoint
            Clusterd(name="clusterd1")
        ):
            c.up("clusterd1")
            c.run_testdrive_files("pg-snapshot-resumption/05-verify-data.td")


def workflow_sink_failure(c: Composition) -> None:
    """Test specific sink failure scenarios"""

    c.down(destroy_volumes=True)

    with c.override(
        # Start postgres for the pg source
        Testdrive(no_reset=True),
        Clusterd(
            name="clusterd1",
            environment_extra=["FAILPOINTS=kafka_sink_creation_error=return"],
        ),
    ):
        c.up("materialized", "zookeeper", "kafka", "schema-registry", "clusterd1")

        c.run_testdrive_files("sink-failure/01-configure-sinks.td")
        c.run_testdrive_files("sink-failure/02-ensure-sink-down.td")

        with c.override(
            # turn off the failpoint
            Clusterd(name="clusterd1")
        ):
            c.up("clusterd1")
            c.run_testdrive_files("sink-failure/03-verify-data.td")


def workflow_test_bootstrap_vars(c: Composition) -> None:
    """Test default system vars values passed with a CLI option."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True),
        Materialized(
            options=[
                "--system-parameter-default=allowed_cluster_replica_sizes='1', '2', 'oops'"
            ],
        ),
    ):
        c.up("materialized")

        c.run_testdrive_files("resources/bootstrapped-system-vars.td")

    with c.override(
        Testdrive(no_reset=True),
        Materialized(
            additional_system_parameter_defaults={
                "allowed_cluster_replica_sizes": "'1', '2', 'oops'"
            },
        ),
    ):
        c.up("materialized")
        c.run_testdrive_files("resources/bootstrapped-system-vars.td")


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
        $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
        SET CLUSTER TO DEFAULT;
        CREATE VIEW v_mz_views AS SELECT \
            id, \
            oid, \
            schema_id, \
            name, \
            definition, \
            owner_id, \
            privileges, \
            create_sql, \
            redacted_create_sql \
        FROM mz_views;
        CREATE DEFAULT INDEX ON v_mz_views;

        > SELECT id FROM mz_indexes WHERE id like 'u%';
        u2
    """
            )
        )
        c.kill("materialized")

    with c.override(
        Testdrive(no_reset=True),
        Materialized(),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.testdrive(
            input=dedent(
                """
        > SELECT id FROM mz_indexes WHERE id like 'u%';
        u2
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
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
    except InternalError_ as e:
        assert (
            e.diag.message_primary
            and "target replica failed or was dropped" in e.diag.message_primary
        ), e
    else:
        raise RuntimeError("SUBSCRIBE didn't return the expected error")

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
    except InternalError_ as e:
        assert (
            e.diag.message_primary
            and "target replica failed or was dropped" in e.diag.message_primary
        ), e
    else:
        raise RuntimeError("SUBSCRIBE didn't return the expected error")

    killer.join()


def workflow_test_replica_targeted_select_abort(c: Composition) -> None:
    """
    Test that a replica-targeted SELECT is aborted when the target
    replica disconnects.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")
    c.up("clusterd2")

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
            SELECT * FROM t AS OF 18446744073709551615;
            """
        )
    except InternalError_ as e:
        assert (
            e.diag.message_primary
            and "target replica failed or was dropped" in e.diag.message_primary
        ), e
    else:
        raise RuntimeError("SELECT didn't return the expected error")

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
            SELECT * FROM t AS OF 18446744073709551615;
            """
        )
    except InternalError_ as e:
        assert (
            e.diag.message_primary
            and "target replica failed or was dropped" in e.diag.message_primary
        ), e
    else:
        raise RuntimeError("SELECT didn't return the expected error")

    killer.join()


def workflow_test_compute_reconciliation_reuse(c: Composition) -> None:
    """
    Test that compute reconciliation reuses existing dataflows.
    """

    c.down(destroy_volumes=True)

    c.up("materialized")
    c.up("clusterd1")
    c.up("clusterd2")

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    # Helper function to get reconciliation metrics for clusterd.
    def fetch_reconciliation_metrics(process: str) -> tuple[int, int]:
        metrics = c.exec(process, "curl", "localhost:6878/metrics", capture=True).stdout

        reused = 0
        replaced = 0
        for metric in metrics.splitlines():
            if metric.startswith(
                "mz_compute_reconciliation_reused_dataflows_count_total"
            ):
                reused += int(metric.split()[1])
            elif metric.startswith(
                "mz_compute_reconciliation_replaced_dataflows_count_total"
            ):
                replaced += int(metric.split()[1])

        return reused, replaced

    # Run a slow-path SELECT to allocate a transient ID. This ensures that
    # after the restart dataflows get different internal transient IDs
    # assigned, which is something we want reconciliation to be able to handle.
    c.sql("SELECT * FROM mz_views JOIN mz_indexes USING (id)")

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
        CREATE DEFAULT INDEX ON t1;

        -- index on index
        CREATE VIEW v1 AS SELECT a + 1 AS a FROM t1;
        CREATE DEFAULT INDEX ON v1;

        -- index on index on index
        CREATE VIEW v2 AS SELECT a + 1 AS a FROM v1;
        CREATE DEFAULT INDEX ON v2;

        -- materialized view on table
        CREATE TABLE t2 (a int);
        CREATE MATERIALIZED VIEW mv1 AS SELECT a + 1 AS a FROM t2;

        -- materialized view on index
        CREATE MATERIALIZED VIEW mv2 AS SELECT a + 1 AS a FROM t1;

        -- materialized view on index on index
        CREATE MATERIALIZED VIEW mv3 AS SELECT a + 1 AS a FROM v1;

        -- materialized view on index on index on index
        CREATE MATERIALIZED VIEW mv4 AS SELECT a + 1 AS a FROM v2;

        -- REFRESH materialized view on table
        CREATE MATERIALIZED VIEW rmv1 WITH (REFRESH EVERY '1m') AS SELECT a + 1 AS a FROM t2;

        -- REFRESH materialized view on index
        CREATE MATERIALIZED VIEW rmv2 WITH (REFRESH EVERY '1m') AS SELECT a + 1 AS a FROM t1;

        -- REFRESH materialized view on index on index
        CREATE MATERIALIZED VIEW rmv3 WITH (REFRESH EVERY '1m') AS SELECT a + 1 AS a FROM v1;

        -- REFRESH materialized view on index on index on index
        CREATE MATERIALIZED VIEW rmv4 WITH (REFRESH EVERY '1m') AS SELECT a + 1 AS a FROM v2;

        -- REFRESH materialized view on materialized view
        CREATE MATERIALIZED VIEW rmv5 WITH (REFRESH EVERY '1m') AS SELECT a + 1 AS a FROM mv1;

        -- REFRESH materialized view on REFRESH materialized view
        CREATE MATERIALIZED VIEW rmv6 WITH (REFRESH EVERY '1m') AS SELECT a + 1 AS a FROM rmv1;

        -- materialized view on REFRESH materialized view
        CREATE MATERIALIZED VIEW mv5 AS SELECT a + 1 AS a FROM rmv1;

        -- index on REFRESH materialized view
        CREATE DEFAULT INDEX ON rmv1;
        """
    )

    # Replace the `mz_catalog_server` replica with an unorchestrated one so we
    # can test reconciliation of system indexes too.
    c.sql(
        """
        ALTER CLUSTER mz_catalog_server SET (MANAGED = false);
        DROP CLUSTER REPLICA mz_catalog_server.r1;
        CREATE CLUSTER REPLICA mz_catalog_server.r1 (
            STORAGECTL ADDRESSES ['clusterd2:2100'],
            STORAGE ADDRESSES ['clusterd2:2103'],
            COMPUTECTL ADDRESSES ['clusterd2:2101'],
            COMPUTE ADDRESSES ['clusterd2:2102'],
            WORKERS 1
        );
        """,
        port=6877,
        user="mz_system",
    )

    # Give the dataflows some time to make progress and get compacted.
    # This is done to trigger the bug described in database-issues#5113.
    time.sleep(10)

    # Restart environmentd to trigger a reconciliation.
    c.kill("materialized")
    c.up("materialized")

    # Perform queries to ensure reconciliation has finished.
    c.sql(
        """
        SET cluster = cluster1;
        SELECT * FROM v1; -- cluster1
        SHOW INDEXES;     -- mz_catalog_server
        """
    )

    reused, replaced = fetch_reconciliation_metrics("clusterd1")

    assert reused == 15
    assert replaced == 0

    reused, replaced = fetch_reconciliation_metrics("clusterd2")

    assert reused > 10
    assert replaced == 0


def workflow_test_compute_reconciliation_replace(c: Composition) -> None:
    """
    Test that compute reconciliation replaces changed dataflows, as well as
    dataflows transitively depending on them.

    Regression test for database-issues#8444.
    """

    c.down(destroy_volumes=True)

    c.up("materialized")
    c.up("clusterd1")

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    # Helper function to get reconciliation metrics for clusterd.
    def fetch_reconciliation_metrics(process: str) -> tuple[int, int]:
        metrics = c.exec(process, "curl", "localhost:6878/metrics", capture=True).stdout

        reused = 0
        replaced = 0
        for metric in metrics.splitlines():
            if metric.startswith(
                "mz_compute_reconciliation_reused_dataflows_count_total"
            ):
                reused += int(metric.split()[1])
            elif metric.startswith(
                "mz_compute_reconciliation_replaced_dataflows_count_total"
            ):
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

        CREATE TABLE t (a int);
        CREATE INDEX idx ON t (a);

        CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;

        CREATE VIEW v1 AS SELECT a + 1 AS b FROM t;
        CREATE INDEX idx1 ON v1 (b);
        CREATE VIEW v2 AS SELECT b + 1 AS c FROM v1;
        CREATE INDEX idx2 ON v2 (c);
        CREATE VIEW v3 AS SELECT c + 1 AS d FROM v2;
        CREATE INDEX idx3 ON v3 (d);

        SELECT * FROM v3;
        """
    )

    # Drop the index on the base table. This will change the plan of `mv1` the
    # next time it is replanned, which should cause reconciliation to replace
    # it, as well as the other dataflows that depend on `mv1`.
    c.sql("DROP INDEX idx")

    # Restart environmentd to trigger a replanning and reconciliation.
    c.kill("materialized")
    c.up("materialized")

    # Perform queries to ensure reconciliation has finished.
    c.sql(
        """
        SET cluster = cluster1;
        SELECT * FROM v3;
        """
    )

    reused, replaced = fetch_reconciliation_metrics("clusterd1")

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
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
            assert " ERROR " not in line, f"found ERROR in service {service}: {line}"


def workflow_test_drop_during_reconciliation(c: Composition) -> None:
    """
    Test that dropping storage and compute objects during reconciliation works.

    Regression test for database-issues#8399.
    """

    c.down(destroy_volumes=True)

    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "unsafe_enable_unsafe_functions": "true",
                "unsafe_enable_unorchestrated_cluster_replicas": "true",
            },
        ),
        Clusterd(
            name="clusterd1",
            environment_extra=[
                # Disable GRPC host checking. We are connecting through a
                # proxy, so the host in the request URI doesn't match
                # clusterd's fqdn.
                "CLUSTERD_GRPC_HOST=",
            ],
        ),
        Testdrive(
            no_reset=True,
            default_timeout="30s",
        ),
    ):
        c.up("materialized", "clusterd1", "toxiproxy")
        c.up("testdrive", persistent=True)

        # Set up toxi-proxies for all clusterd endpoints.
        toxi_url = "http://toxiproxy:8474/proxies"
        for port in range(2100, 2104):
            c.testdrive(
                dedent(
                    f"""
                    $ http-request method=POST url={toxi_url} content-type=application/json
                    {{
                      "name": "clusterd_{port}",
                      "listen": "0.0.0.0:{port}",
                      "upstream": "clusterd1:{port}"
                    }}
                    """
                )
            )

        # Set up a cluster with storage and compute objects that can be dropped
        # during reconciliation.
        c.sql(
            """
            CREATE CLUSTER cluster1 REPLICAS (replica1 (
                STORAGECTL ADDRESSES ['toxiproxy:2100'],
                STORAGE ADDRESSES ['toxiproxy:2103'],
                COMPUTECTL ADDRESSES ['toxiproxy:2101'],
                COMPUTE ADDRESSES ['toxiproxy:2102'],
                WORKERS 1
            ));
            SET cluster = cluster1;

            CREATE SOURCE s FROM LOAD GENERATOR COUNTER;
            CREATE DEFAULT INDEX on s;
            CREATE MATERIALIZED VIEW mv AS SELECT * FROM s;
            """
        )

        # Wait for objects to be installed on the cluster.
        c.sql("SELECT * FROM mv")

        # Sever the connection between envd and clusterd.
        for port in range(2100, 2104):
            c.testdrive(
                dedent(
                    f"""
                    $ http-request method=POST url={toxi_url}/clusterd_{port} content-type=application/json
                    {{"enabled": false}}
                    """
                )
            )

        # Drop all objects installed on the cluster.
        c.sql("DROP SOURCE s CASCADE")

        # Restore the connection between envd and clusterd, causing a
        # reconciliation.
        for port in range(2100, 2104):
            c.testdrive(
                dedent(
                    f"""
                    $ http-request method=POST url={toxi_url}/clusterd_{port} content-type=application/json
                    {{"enabled": true}}
                    """
                )
            )

        # Confirm the cluster is still healthy and the compute objects have
        # been dropped. We can't verify the dropping of storage objects due to
        # the lack of introspection for storage dataflows.
        c.testdrive(
            dedent(
                """
                > SET cluster = cluster1;
                > SELECT * FROM mz_introspection.mz_compute_exports WHERE export_id LIKE 'u%';
                """
            )
        )


def workflow_test_mz_subscriptions(c: Composition) -> None:
    """
    Test that in-progress subscriptions are reflected in
    mz_subscriptions.
    """

    c.down(destroy_volumes=True)
    c.up("materialized", "clusterd1")

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
        cursor.execute(f"SET cluster = {cluster}".encode())
        cursor.execute("BEGIN")
        cursor.execute(f"DECLARE c CURSOR FOR SUBSCRIBE {table}".encode())
        cursor.execute("FETCH 1 c")
        return cursor

    def stop_subscribe(cursor: Cursor) -> None:
        """Stop a susbscribe started with `start_subscribe`."""
        cursor.execute("ROLLBACK")

    def check_mz_subscriptions(expected: list) -> None:
        """
        Check that the expected subscribes exist in mz_subscriptions.
        We identify subscribes by user, cluster, and target table only.
        We explicitly don't check the `GlobalId`, as how that is
        allocated is an implementation detail and might change in the
        future.
        """
        output = c.sql_query(
            """
            SELECT r.name, c.name, t.name
            FROM mz_internal.mz_subscriptions s
              JOIN mz_internal.mz_sessions e ON (e.id = s.session_id)
              JOIN mz_roles r ON (r.id = e.role_id)
              JOIN mz_clusters c ON (c.id = s.cluster_id)
              JOIN mz_tables t ON (t.id = s.referenced_object_ids[1])
            ORDER BY s.created_at
            """
        )
        assert output == expected, f"expected: {expected}, got: {output}"

    subscribe1 = start_subscribe("t1", "quickstart")
    check_mz_subscriptions([("materialize", "quickstart", "t1")])

    subscribe2 = start_subscribe("t2", "cluster1")
    check_mz_subscriptions(
        [
            ("materialize", "quickstart", "t1"),
            ("materialize", "cluster1", "t2"),
        ]
    )

    stop_subscribe(subscribe1)
    check_mz_subscriptions([("materialize", "cluster1", "t2")])

    subscribe3 = start_subscribe("t3", "quickstart")
    check_mz_subscriptions(
        [
            ("materialize", "cluster1", "t2"),
            ("materialize", "quickstart", "t3"),
        ]
    )

    stop_subscribe(subscribe3)
    check_mz_subscriptions([("materialize", "cluster1", "t2")])

    stop_subscribe(subscribe2)
    check_mz_subscriptions([])


def workflow_test_mv_source_sink(c: Composition) -> None:
    """
    Test that compute materialized view's "since" timestamp is at least as large as source table's "since" timestamp.

    Regression test for https://github.com/MaterializeInc/database-issues/issues/5676
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
        """
    )

    def extract_since_ts(output: str) -> int:
        j = json.loads(output)
        (since,) = j["determination"]["since"]["elements"]
        return int(since)

    cursor = c.sql_cursor()
    cursor.execute("CREATE TABLE t (a int)")
    # Verify that there are no empty frontiers.
    cursor.execute("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM t")
    t_since = extract_since_ts(cursor.fetchall()[0][0])

    cursor.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t")
    cursor.execute("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM mv")
    mv_since = extract_since_ts(cursor.fetchall()[0][0])

    assert (
        mv_since >= t_since
    ), f'"since" timestamp of mv ({mv_since}) is less than "since" timestamp of its source table ({t_since})'


def workflow_test_query_without_default_cluster(c: Composition) -> None:
    """Test queries without a default cluster in Materialize."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(),
        Materialized(),
    ):
        c.up("materialized", "postgres")

        c.run_testdrive_files(
            "query-without-default-cluster/query-without-default-cluster.td",
        )


def workflow_test_clusterd_death_detection(c: Composition) -> None:
    """
    Test that environmentd notices when a clusterd becomes disconnected.

    Regression test for https://github.com/MaterializeInc/database-issues/issues/6095
    """

    c.down(destroy_volumes=True)

    with c.override(
        Clusterd(
            name="clusterd1",
            environment_extra=[
                # Disable GRPC host checking. We are connecting through a
                # proxy, so the host in the request URI doesn't match
                # clusterd's fqdn.
                "CLUSTERD_GRPC_HOST=",
            ],
        ),
    ):
        c.up("materialized", "clusterd1", "toxiproxy")
        c.up("testdrive", persistent=True)

        c.testdrive(
            input=dedent(
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

                $ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
                {
                  "name": "clusterd1",
                  "listen": "0.0.0.0:2100",
                  "upstream": "clusterd1:2100",
                  "enabled": true
                }

                $ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
                {
                  "name": "clusterd2",
                  "listen": "0.0.0.0:2101",
                  "upstream": "clusterd1:2101",
                  "enabled": true
                }

                $ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
                {
                  "name": "clusterd3",
                  "listen": "0.0.0.0:2102",
                  "upstream": "clusterd1:2102",
                  "enabled": true
                }

                $ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
                {
                  "name": "clusterd4",
                  "listen": "0.0.0.0:2103",
                  "upstream": "clusterd1:2103",
                  "enabled": true
                }

                > CREATE CLUSTER cluster1 REPLICAS (replica1 (
                    STORAGECTL ADDRESSES ['toxiproxy:2100'],
                    STORAGE ADDRESSES ['toxiproxy:2103'],
                    COMPUTECTL ADDRESSES ['toxiproxy:2101'],
                    COMPUTE ADDRESSES ['toxiproxy:2102'],
                    WORKERS 2));

                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                $ http-request method=POST url=http://toxiproxy:8474/proxies/clusterd1/toxics content-type=application/json
                {
                  "name": "clusterd1",
                  "type": "timeout",
                  "attributes": {"timeout": 0}
                }

                $ http-request method=POST url=http://toxiproxy:8474/proxies/clusterd2/toxics content-type=application/json
                {
                  "name": "clusterd2",
                  "type": "timeout",
                  "attributes": {"timeout": 0}
                }

                $ http-request method=POST url=http://toxiproxy:8474/proxies/clusterd3/toxics content-type=application/json
                {
                  "name": "clusterd3",
                  "type": "timeout",
                  "attributes": {"timeout": 0}
                }

                $ http-request method=POST url=http://toxiproxy:8474/proxies/clusterd4/toxics content-type=application/json
                {
                  "name": "clusterd4",
                  "type": "timeout",
                  "attributes": {"timeout": 0}
                }
            """
            )
        )
        # Should detect broken connection after a few seconds, works with c.kill("clusterd1")
        time.sleep(10)
        envd = c.invoke("logs", "materialized", capture=True)
        assert (
            "error reading a body from connection: stream closed because of a broken pipe"
            in envd.stdout
        )


class Metrics:
    metrics: dict[str, str]

    def __init__(self, raw: str) -> None:
        self.metrics = {}
        for line in raw.splitlines():
            key, value = line.split(maxsplit=1)
            self.metrics[key] = value

    def for_instance(self, id: str) -> "Metrics":
        new = copy(self)
        new.metrics = {
            k: v for k, v in self.metrics.items() if f'instance_id="{id}"' in k
        }
        return new

    def with_name(self, metric_name: str) -> dict[str, float]:
        items = {}
        for key, value in self.metrics.items():
            if key.startswith(metric_name):
                items[key] = float(value)
        return items

    def get_value(self, metric_name: str) -> float:
        metrics = self.with_name(metric_name)
        values = list(metrics.values())
        assert len(values) == 1
        return values[0]

    def get_command_count(self, metric: str, command_type: str) -> float:
        metrics = self.with_name(metric)
        values = [
            v for k, v in metrics.items() if f'command_type="{command_type}"' in k
        ]
        assert len(values) == 1
        return values[0]

    def get_response_count(self, metric: str, response_type: str) -> float:
        metrics = self.with_name(metric)
        values = [
            v for k, v in metrics.items() if f'response_type="{response_type}"' in k
        ]
        assert len(values) == 1
        return values[0]

    def get_replica_history_command_count(self, command_type: str) -> float:
        return self.get_command_count(
            "mz_compute_replica_history_command_count", command_type
        )

    def get_controller_history_command_count(self, command_type: str) -> float:
        return self.get_command_count(
            "mz_compute_controller_history_command_count", command_type
        )

    def get_commands_total(self, command_type: str) -> float:
        return self.get_command_count("mz_compute_commands_total", command_type)

    def get_command_bytes_total(self, command_type: str) -> float:
        return self.get_command_count(
            "mz_compute_command_message_bytes_total", command_type
        )

    def get_responses_total(self, response_type: str) -> float:
        return self.get_response_count("mz_compute_responses_total", response_type)

    def get_response_bytes_total(self, response_type: str) -> float:
        return self.get_response_count(
            "mz_compute_response_message_bytes_total", response_type
        )

    def get_peeks_total(self, result: str) -> float:
        metrics = self.with_name("mz_compute_peeks_total")
        values = [v for k, v in metrics.items() if f'result="{result}"' in k]
        assert len(values) == 1
        return values[0]

    def get_initial_output_duration(self, collection_id: str) -> float | None:
        metrics = self.with_name("mz_dataflow_initial_output_duration_seconds")
        values = [
            v for k, v in metrics.items() if f'collection_id="{collection_id}"' in k
        ]
        assert len(values) <= 1
        return next(iter(values), None)

    def get_e2e_optimization_time(self, object_type: str) -> float:
        metrics = self.with_name("mz_optimizer_e2e_optimization_time_seconds_sum")
        values = [v for k, v in metrics.items() if f'object_type="{object_type}"' in k]
        assert len(values) == 1
        return values[0]

    def get_last_command_received(self, server_name: str) -> float:
        metrics = self.with_name("mz_grpc_server_last_command_received")
        values = [v for k, v in metrics.items() if server_name in k]
        assert len(values) == 1
        return values[0]


def workflow_test_replica_metrics(c: Composition) -> None:
    """Test metrics exposed by replicas."""

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")

    def fetch_metrics() -> Metrics:
        resp = c.exec(
            "clusterd1", "curl", "localhost:6878/metrics", capture=True
        ).stdout
        return Metrics(resp)

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    metrics = fetch_metrics()

    # The cluster should not report the time that the last command was received
    # as 0 until environmentd connects.
    assert metrics.get_last_command_received("compute") == 0
    assert metrics.get_last_command_received("storage") == 0

    before_connection_time = time.time()

    # Set up a cluster with a couple dataflows.
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

        CREATE TABLE t (a int);
        INSERT INTO t SELECT generate_series(1, 10);

        CREATE INDEX idx ON t (a);
        CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;

        SELECT * FROM t;
        SELECT * FROM mv;
        """
    )

    # Can take a few seconds, don't request the metrics too quickly
    time.sleep(2)
    # Check that expected metrics exist and have sensible values.
    metrics = fetch_metrics()

    count = metrics.get_replica_history_command_count("create_timely")
    assert count == 0, f"unexpected create_timely count: {count}"
    count = metrics.get_replica_history_command_count("create_instance")
    assert count == 1, f"unexpected create_instance count: {count}"
    count = metrics.get_replica_history_command_count("allow_compaction")
    assert count > 0, f"unexpected allow_compaction count: {count}"
    count = metrics.get_replica_history_command_count("create_dataflow")
    assert count > 0, f"unexpected create_dataflow count: {count}"
    count = metrics.get_replica_history_command_count("peek")
    assert count <= 2, f"unexpected peek count: {count}"
    count = metrics.get_replica_history_command_count("cancel_peek")
    assert count <= 2, f"unexpected cancel_peek count: {count}"
    count = metrics.get_replica_history_command_count("initialization_complete")
    assert count == 0, f"unexpected initialization_complete count: {count}"
    count = metrics.get_replica_history_command_count("update_configuration")
    assert count == 1, f"unexpected update_configuration count: {count}"

    count = metrics.get_value("mz_compute_replica_history_dataflow_count")
    assert count >= 2, f"unexpected dataflow count: {count}"

    maintenance = metrics.get_value("mz_arrangement_maintenance_seconds_total")
    assert maintenance > 0, f"unexpected arrangement maintanence time: {maintenance}"

    mv_correction_insertions = metrics.get_value(
        "mz_persist_sink_correction_insertions_total"
    )
    assert (
        mv_correction_insertions > 0
    ), f"unexpected persist sink correction insertions: {mv_correction_insertions}"
    mv_correction_cap_increases = metrics.get_value(
        "mz_persist_sink_correction_capacity_increases_total"
    )
    assert (
        mv_correction_cap_increases > 0
    ), f"unexpected persist sink correction capacity increases: {mv_correction_cap_increases}"
    mv_correction_max_len_per_worker = metrics.get_value(
        "mz_persist_sink_correction_max_per_sink_worker_len_updates"
    )
    assert (
        mv_correction_max_len_per_worker > 0
    ), f"unexpected persist max correction len per worker: {mv_correction_max_len_per_worker}"
    mv_correction_max_cap_per_worker = metrics.get_value(
        "mz_persist_sink_correction_max_per_sink_worker_capacity_updates"
    )
    assert (
        mv_correction_max_cap_per_worker > 0
    ), f"unexpected persist sink max correction capacity per worker: {mv_correction_max_cap_per_worker}"

    # Can take a few seconds, don't request the metrics too quickly
    time.sleep(2)
    assert metrics.get_last_command_received("compute") >= before_connection_time


def workflow_test_compute_controller_metrics(c: Composition) -> None:
    """Test metrics exposed by the compute controller."""

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("testdrive", persistent=True)

    def fetch_metrics() -> Metrics:
        resp = c.exec(
            "materialized", "curl", "localhost:6878/metrics", capture=True
        ).stdout
        return Metrics(resp).for_instance("u2")

    # Set up a cluster with a couple dataflows.
    c.sql(
        """
        CREATE CLUSTER test MANAGED, SIZE '1';
        SET cluster = test;

        CREATE TABLE t (a int);
        INSERT INTO t SELECT generate_series(1, 10);

        CREATE INDEX idx ON t (a);
        CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;

        SELECT * FROM t;
        SELECT * FROM mv;
        """
    )

    index_id = c.sql_query("SELECT id FROM mz_indexes WHERE name = 'idx'")[0][0]
    mv_id = c.sql_query("SELECT id FROM mz_materialized_views WHERE name = 'mv'")[0][0]

    # Wait a bit to let the controller refresh its metrics.
    time.sleep(2)

    # Check that expected metrics exist and have sensible values.
    metrics = fetch_metrics()

    # mz_compute_commands_total
    count = metrics.get_commands_total("create_timely")
    assert count == 1, f"got {count}"
    count = metrics.get_commands_total("create_instance")
    assert count == 1, f"got {count}"
    count = metrics.get_commands_total("allow_compaction")
    assert count > 0, f"got {count}"
    count = metrics.get_commands_total("create_dataflow")
    assert count >= 3, f"got {count}"
    count = metrics.get_commands_total("peek")
    assert count == 2, f"got {count}"
    count = metrics.get_commands_total("cancel_peek")
    assert count == 2, f"got {count}"
    count = metrics.get_commands_total("initialization_complete")
    assert count == 1, f"got {count}"
    count = metrics.get_commands_total("update_configuration")
    assert count == 1, f"got {count}"

    # mz_compute_command_message_bytes_total
    count = metrics.get_command_bytes_total("create_timely")
    assert count > 0, f"got {count}"
    count = metrics.get_command_bytes_total("create_instance")
    assert count > 0, f"got {count}"
    count = metrics.get_command_bytes_total("allow_compaction")
    assert count > 0, f"got {count}"
    count = metrics.get_command_bytes_total("create_dataflow")
    assert count > 0, f"got {count}"
    count = metrics.get_command_bytes_total("peek")
    assert count > 0, f"got {count}"
    count = metrics.get_command_bytes_total("cancel_peek")
    assert count > 0, f"got {count}"
    count = metrics.get_command_bytes_total("initialization_complete")
    assert count > 0, f"got {count}"
    count = metrics.get_command_bytes_total("update_configuration")
    assert count > 0, f"got {count}"

    # mz_compute_responses_total
    count = metrics.get_responses_total("frontiers")
    assert count > 0, f"got {count}"
    count = metrics.get_responses_total("peek_response")
    assert count == 2, f"got {count}"
    count = metrics.get_responses_total("subscribe_response")
    assert count > 0, f"got {count}"

    # mz_compute_response_message_bytes_total
    count = metrics.get_response_bytes_total("frontiers")
    assert count > 0, f"got {count}"
    count = metrics.get_response_bytes_total("peek_response")
    assert count > 0, f"got {count}"
    count = metrics.get_response_bytes_total("subscribe_response")
    assert count > 0, f"got {count}"

    count = metrics.get_value("mz_compute_controller_replica_count")
    assert count == 1, f"got {count}"
    count = metrics.get_value("mz_compute_controller_collection_count")
    assert count > 0, f"got {count}"
    count = metrics.get_value("mz_compute_controller_collection_unscheduled_count")
    assert count == 0, f"got {count}"
    count = metrics.get_value("mz_compute_controller_peek_count")
    assert count == 0, f"got {count}"
    count = metrics.get_value("mz_compute_controller_subscribe_count")
    assert count > 0, f"got {count}"
    count = metrics.get_value("mz_compute_controller_command_queue_size")
    assert count < 10, f"got {count}"
    send_count = metrics.get_value("mz_compute_controller_response_send_count")
    assert send_count > 10, f"got {send_count}"
    recv_count = metrics.get_value("mz_compute_controller_response_recv_count")
    assert recv_count > 10, f"got {recv_count}"
    assert send_count - recv_count < 10, f"got {send_count}, {recv_count}"
    count = metrics.get_value("mz_compute_controller_hydration_queue_size")
    assert count == 0, f"got {count}"

    # mz_compute_controller_history_command_count
    count = metrics.get_controller_history_command_count("create_timely")
    assert count == 1, f"got {count}"
    count = metrics.get_controller_history_command_count("create_instance")
    assert count == 1, f"got {count}"
    count = metrics.get_controller_history_command_count("allow_compaction")
    assert count > 0, f"got {count}"
    count = metrics.get_controller_history_command_count("create_dataflow")
    assert count > 0, f"got {count}"
    count = metrics.get_controller_history_command_count("peek")
    assert count <= 2, f"got {count}"
    count = metrics.get_controller_history_command_count("cancel_peek")
    assert count <= 2, f"got {count}"
    count = metrics.get_controller_history_command_count("initialization_complete")
    assert count == 1, f"got {count}"
    count = metrics.get_controller_history_command_count("update_configuration")
    assert count == 1, f"got {count}"

    count = metrics.get_value("mz_compute_controller_history_dataflow_count")
    assert count >= 2, f"got {count}"

    # mz_compute_peeks_total
    count = metrics.get_peeks_total("rows")
    assert count == 2, f"got {count}"
    count = metrics.get_peeks_total("error")
    assert count == 0, f"got {count}"
    count = metrics.get_peeks_total("canceled")
    assert count == 0, f"got {count}"

    # mz_dataflow_initial_output_duration_seconds
    duration = metrics.get_initial_output_duration(index_id)
    assert duration, f"got {duration}"
    duration = metrics.get_initial_output_duration(mv_id)
    assert duration, f"got {duration}"

    # Drop the dataflows.
    c.sql(
        """
        DROP INDEX idx;
        DROP MATERIALIZED VIEW mv;
        """
    )

    # Wait for the controller to asynchronously drop the dataflows and update
    # metrics. We can inspect the controller's view of things in
    # `mz_compute_hydration_statuses`, which is updated at the same time as
    # these metrics are.
    c.testdrive(
        input=dedent(
            """
            > SELECT *
              FROM mz_internal.mz_compute_hydration_statuses
              WHERE object_id LIKE 'u%'
            """
        )
    )

    # Check that the per-collection metrics have been cleaned up.
    metrics = fetch_metrics()
    assert metrics.get_initial_output_duration(index_id) is None
    assert metrics.get_initial_output_duration(mv_id) is None


def workflow_test_optimizer_metrics(c: Composition) -> None:
    """Test metrics exposed by the optimizer."""

    c.down(destroy_volumes=True)
    c.up("materialized")

    def fetch_metrics() -> Metrics:
        resp = c.exec(
            "materialized", "curl", "localhost:6878/metrics", capture=True
        ).stdout
        return Metrics(resp)

    # Run optimizations for different object types.
    c.sql(
        """
        CREATE TABLE t (a int);

        -- view
        CREATE VIEW v AS SELECT a + 1 FROM t;
        -- index
        CREATE INDEX i ON t (a);
        -- materialized view
        CREATE MATERIALIZED VIEW m AS SELECT a + 1 FROM t;
        -- fast-path peek
        SELECT * FROM t;
        -- slow-path peek;
        SELECT count(*) FROM t JOIN v ON (true);
        -- subscribe
        SUBSCRIBE (SELECT 1);
        """
    )

    # Check that expected metrics exist and have sensible values.
    metrics = fetch_metrics()

    # mz_optimizer_e2e_optimization_time_seconds
    time = metrics.get_e2e_optimization_time("view")
    assert 0 < time < 10, f"got {time}"
    time = metrics.get_e2e_optimization_time("index")
    assert 0 < time < 10, f"got {time}"
    time = metrics.get_e2e_optimization_time("materialized_view")
    assert 0 < time < 10, f"got {time}"
    time = metrics.get_e2e_optimization_time("peek:fast_path")
    assert 0 < time < 10, f"got {time}"
    time = metrics.get_e2e_optimization_time("peek:slow_path")
    assert 0 < time < 10, f"got {time}"
    time = metrics.get_e2e_optimization_time("subscribe")
    assert 0 < time < 10, f"got {time}"


def workflow_test_metrics_retention_across_restart(c: Composition) -> None:
    """
    Test that sinces of retained-metrics objects are held back across
    restarts of environmentd.
    """

    # There are three kinds of retained-metrics objects currently:
    #  * tables (like `mz_cluster_replicas`)
    #  * indexes (like `mz_cluster_replicas_ind`)

    # Generally, metrics tables are indexed in `mz_catalog_server` and
    # not indexed in the `default` cluster, so we can use that to
    # collect the `since` frontiers we want.
    def collect_sinces() -> tuple[int, int]:
        with c.sql_cursor() as cur:
            cur.execute("SET cluster = default;")
            cur.execute("EXPLAIN TIMESTAMP FOR SELECT * FROM mz_cluster_replicas;")
            explain = cur.fetchall()[0][0]
        table_since = parse_since_from_explain(explain)

        with c.sql_cursor() as cur:
            cur.execute("SET cluster = mz_catalog_server;")
            cur.execute("EXPLAIN TIMESTAMP FOR SELECT * FROM mz_cluster_replicas;")
            explain = cur.fetchall()[0][0]
        index_since = parse_since_from_explain(explain)

        return table_since, index_since

    def parse_since_from_explain(explain: str) -> int:
        since_line = re.compile(r"\s*read frontier:\[(?P<since>\d+) \(.+\)\]")
        for line in explain.splitlines():
            if match := since_line.match(line):
                return int(match.group("since"))

        raise AssertionError(f"since not found in explain: {explain}")

    def validate_since(since: int, name: str) -> None:
        now = datetime.now()
        dt = datetime.fromtimestamp(since / 1000.0)
        diff = now - dt

        # This env was just created, so the since should be recent.
        assert (
            diff.days < 30
        ), f"{name} greater than expected (since={since}, diff={diff})"

    c.down(destroy_volumes=True)
    c.up("materialized")

    table_since1, index_since1 = collect_sinces()
    validate_since(table_since1, "table_since1")
    validate_since(index_since1, "index_since1")

    # Restart Materialize.
    c.kill("materialized")
    c.up("materialized")

    # The env has been up for less than 30d, so the since should not have
    # changed.
    table_since2, index_since2 = collect_sinces()
    assert (
        table_since1 == table_since2
    ), f"table sinces did not match {table_since1} vs {table_since2})"
    assert (
        index_since1 == index_since2
    ), f"index sinces did not match {index_since1} vs {index_since2})"


RE_WORKLOAD_CLASS_LABEL = re.compile(r'workload_class="(?P<value>\w+)"')


def workflow_test_workload_class_in_metrics(c: Composition) -> None:
    """
    Test that setting the cluster workload class correctly reflects in metrics
    exposed by envd and clusterd.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")

    def check_workload_class(expected: str | None):
        """
        Assert that metrics on both envd and clusterd are labeled with the
        given expected workload class.
        """

        # Sleep a bit to give workload class changes time to propagate.
        time.sleep(1)

        envd_metrics = c.exec(
            "materialized", "curl", "localhost:6878/metrics", capture=True
        ).stdout
        clusterd_metrics = c.exec(
            "clusterd1", "curl", "localhost:6878/metrics", capture=True
        ).stdout

        envd_classes = {
            m.group("value") for m in RE_WORKLOAD_CLASS_LABEL.finditer(envd_metrics)
        }
        clusterd_classes = {
            m.group("value") for m in RE_WORKLOAD_CLASS_LABEL.finditer(clusterd_metrics)
        }

        if expected is None:
            assert (
                not envd_classes
            ), f"envd: expected no workload classes, found {envd_classes}"
            assert (
                not clusterd_classes
            ), f"clusterd: expected no workload classes, found {clusterd_classes}"
        else:
            assert envd_classes == {
                expected
            }, f"envd: expected workload class '{expected}', found {envd_classes}"
            assert clusterd_classes == {
                expected
            }, f"clusterd: expected workload class '{expected}', found {clusterd_classes}"

    c.sql(
        """
        ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;
        CREATE CLUSTER test REPLICAS (r1 (
            STORAGECTL ADDRESSES ['clusterd1:2100'],
            STORAGE ADDRESSES ['clusterd1:2103'],
            COMPUTECTL ADDRESSES ['clusterd1:2101'],
            COMPUTE ADDRESSES ['clusterd1:2102'],
            WORKERS 1
        ));
        """,
        port=6877,
        user="mz_system",
    )

    check_workload_class(None)

    c.sql(
        "ALTER CLUSTER test SET (WORKLOAD CLASS 'production')",
        port=6877,
        user="mz_system",
    )

    check_workload_class("production")

    c.sql(
        "ALTER CLUSTER test SET (WORKLOAD CLASS 'staging')",
        port=6877,
        user="mz_system",
    )

    check_workload_class("staging")

    c.sql(
        "ALTER CLUSTER test RESET (WORKLOAD CLASS)",
        port=6877,
        user="mz_system",
    )

    check_workload_class(None)


def workflow_test_concurrent_connections(c: Composition) -> None:
    """
    Run many concurrent connections, measure their p50 and p99 latency, make
    sure database-issues#6537 does not regress.
    """
    num_conns = 2000
    p50_limit = 10.0
    p99_limit = 20.0

    runtimes: list[float] = [float("inf")] * num_conns

    def worker(c: Composition, i: int) -> None:
        start_time = time.time()
        c.sql("SELECT 1", print_statement=False)
        end_time = time.time()
        runtimes[i] = end_time - start_time

    c.down(destroy_volumes=True)
    c.up("materialized")

    c.sql(
        f"ALTER SYSTEM SET max_connections = {num_conns + 4};",
        port=6877,
        user="mz_system",
    )

    for i in range(3):
        threads = []
        for j in range(num_conns):
            thread = Thread(name=f"worker_{j}", target=worker, args=(c, j))
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        p = quantiles(runtimes, n=100)
        print(
            f"min: {min(runtimes):.2f}s, p50: {p[49]:.2f}s, p99: {p[98]:.2f}s, max: {max(runtimes):.2f}s"
        )

        p50 = p[49]
        p99 = p[98]
        if p50 < p50_limit and p99 < p99_limit:
            return
        if i < 2:
            print("retry...")
            continue
        assert (
            p50 < p50_limit
        ), f"p50 is {p50:.2f}s, should be less than {p50_limit:.2f}s"
        assert (
            p99 < p99_limit
        ), f"p99 is {p99:.2f}s, should be less than {p99_limit:.2f}s"


def workflow_test_profile_fetch(c: Composition) -> None:
    """
    Test fetching memory and CPU profiles via the internal HTTP
    endpoint.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("clusterd1")

    envd_port = c.port("materialized", 6878)
    envd_url = f"http://localhost:{envd_port}/prof/"
    clusterd_port = c.port("clusterd1", 6878)
    clusterd_url = f"http://localhost:{clusterd_port}/"

    def test_post(data: dict[str, str], check: Callable[[int, str], None]) -> None:
        resp = requests.post(envd_url, data=data)
        check(resp.status_code, resp.text)

        resp = requests.post(clusterd_url, data=data)
        check(resp.status_code, resp.text)

    def test_get(path: str, check: Callable[[int, str], None]) -> None:
        resp = requests.get(envd_url + path)
        check(resp.status_code, resp.text)

        resp = requests.get(clusterd_url + path)
        check(resp.status_code, resp.text)

    def make_check(code: int, contents: str) -> Callable[[int, str], None]:
        def check(code_: int, text: str) -> None:
            assert code_ == code, f"expected {code}, got {code_}"
            assert contents in text, f"'{contents}' not found in text: {text}"

        return check

    def make_ok_check(contents: str) -> Callable[[int, str], None]:
        return make_check(200, contents)

    def check_profiling_disabled(code: int, text: str) -> None:
        check = make_check(403, "heap profiling not activated")
        check(code, text)

    # Test fetching heap profiles. Heap profiling should be activated by default.
    test_post({"action": "dump_jeheap"}, make_ok_check("heap_v2/"))
    test_post(
        {"action": "dump_sym_mzfg"}, make_ok_check("mz_fg_version: 1\nAllocated:")
    )
    test_post(
        {"action": "mem_fg"},
        make_ok_check("mz_fg_version: 1\\ndisplay_bytes: 1\\nAllocated:"),
    )
    test_get("heap", make_ok_check(""))

    # Test fetching CPU profiles. This disables memory profiling!
    test_post(
        {"action": "time_fg", "time_secs": "1", "hz": "1"},
        make_ok_check(
            "mz_fg_version: 1\\nSampling time (s): 1\\nSampling frequency (Hz): 1\\n"
        ),
    )

    # Deactivate memory profiling.
    test_post(
        {"action": "deactivate"},
        make_ok_check("Jemalloc profiling enabled but inactive"),
    )

    # Test that fetching heap profiles is forbidden.
    test_post({"action": "dump_jeheap"}, check_profiling_disabled)
    test_post({"action": "dump_sym_mzfg"}, check_profiling_disabled)
    test_post({"action": "mem_fg"}, check_profiling_disabled)
    test_get("heap", check_profiling_disabled)

    # Activate memory profiling again.
    test_post({"action": "activate"}, make_ok_check("Jemalloc profiling active"))

    # Test fetching heap profiles again.
    test_post({"action": "dump_jeheap"}, make_ok_check("heap_v2/"))
    test_post(
        {"action": "dump_sym_mzfg"}, make_ok_check("mz_fg_version: 1\nAllocated:")
    )
    test_post(
        {"action": "mem_fg"},
        make_ok_check("mz_fg_version: 1\\ndisplay_bytes: 1\\nAllocated:"),
    )
    test_get("heap", make_ok_check(""))

    # Test fetching CPU profiles again.
    test_post(
        {"action": "time_fg", "time_secs": "1", "hz": "1"},
        make_ok_check(
            "mz_fg_version: 1\\nSampling time (s): 1\\nSampling frequency (Hz): 1\\n"
        ),
    )


def workflow_test_incident_70(c: Composition) -> None:
    """
    Test incident-70.
    """
    num_conns = 1
    mv_count = 50
    persist_reader_lease_duration_in_sec = 10
    data_scale_factor = 10

    with c.override(
        Materialized(
            external_metadata_store=True,
            external_blob_store=True,
            sanity_restart=False,
        ),
        Minio(setup_materialize=True),
    ):
        c.down(destroy_volumes=True)
        c.up("minio", "materialized")

        c.sql(
            f"ALTER SYSTEM SET max_connections = {num_conns + 5};",
            port=6877,
            user="mz_system",
        )

        c.sql(
            f"ALTER SYSTEM SET persist_reader_lease_duration = '{persist_reader_lease_duration_in_sec}s';",
            port=6877,
            user="mz_system",
        )

        mz_view_create_statements = []

        for i in range(mv_count):
            mz_view_create_statements.append(
                f"CREATE MATERIALIZED VIEW mv_lineitem_count_{i + 1} AS SELECT count(*) FROM lineitem;"
            )

        mz_view_create_statements_sql = "\n".join(mz_view_create_statements)

        c.sql(
            dedent(
                f"""
                CREATE SOURCE gen FROM LOAD GENERATOR TPCH (SCALE FACTOR {data_scale_factor});

                CREATE TABLE customer FROM SOURCE gen (REFERENCE customer);
                CREATE TABLE lineitem FROM SOURCE gen (REFERENCE lineitem);
                CREATE TABLE nation FROM SOURCE gen (REFERENCE nation);
                CREATE TABLE orders FROM SOURCE gen (REFERENCE orders);
                CREATE TABLE part FROM SOURCE gen (REFERENCE part);
                CREATE TABLE partsupp FROM SOURCE gen (REFERENCE partsupp);
                CREATE TABLE region FROM SOURCE gen (REFERENCE region);
                CREATE TABLE supplier FROM SOURCE gen (REFERENCE supplier);

                {mz_view_create_statements_sql}
                """
            )
        )

        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=600)

        def worker(c: Composition, worker_index: int) -> None:
            print(f"Thread {worker_index} tries to acquire a cursor")
            cursor = c.sql_cursor()
            print(f"Thread {worker_index} got a cursor")

            iteration = 1
            while datetime.now() < end_time:
                if iteration % 20 == 0:
                    print(f"Thread {worker_index}, iteration {iteration}")
                cursor.execute("SELECT * FROM mv_lineitem_count_1;")
                iteration += 1
            print(f"Thread {worker_index} terminates before iteration {iteration}")

        threads = []
        for worker_index in range(num_conns):
            thread = Thread(
                name=f"worker_{worker_index}", target=worker, args=(c, worker_index)
            )
            threads.append(thread)

        for thread in threads:
            thread.start()
            # this is because of database-issues#6639
            time.sleep(0.2)

        for thread in threads:
            thread.join()


def workflow_test_github_cloud_7998(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Regression test for MaterializeInc/cloud#7998."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True),
        Clusterd(name="clusterd1"),
        Materialized(),
    ):
        c.up("materialized")
        c.up("clusterd1")

        c.run_testdrive_files("github-cloud-7998/setup.td")

        # Make the compute cluster unavailable.
        c.kill("clusterd1")
        c.run_testdrive_files("github-cloud-7998/check.td")

        # Trigger an environment bootstrap.
        c.kill("materialized")
        c.up("materialized")
        c.run_testdrive_files("github-cloud-7998/check.td")

        # Run a second bootstrap check, just to be sure.
        c.kill("materialized")
        c.up("materialized")
        c.run_testdrive_files("github-cloud-7998/check.td")


def workflow_test_github_7000(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Regression test for database-issues#7000."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")

        # Create an MV reading from an index. Make sure it doesn't produce its
        # snapshot by installing it in a cluster without replicas.
        c.sql(
            """
            CREATE CLUSTER test SIZE '1', REPLICATION FACTOR 0;
            SET cluster = test;

            CREATE TABLE t (a int);
            INSERT INTO t VALUES (1);

            CREATE DEFAULT INDEX ON t;
            CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;
            """
        )

        # Verify that the MV's upper is zero, which is what caused the bug.
        # This ensures that the test doesn't break in the future because we
        # start initializing frontiers differently.
        c.testdrive(
            input=dedent(
                """
                > SELECT write_frontier
                  FROM mz_internal.mz_frontiers
                  JOIN mz_materialized_views ON (object_id = id)
                  WHERE name = 'mv'
                0
                """
            )
        )

        # Trigger an environment bootstrap, and see if envd comes up without
        # panicking.
        c.kill("materialized")
        c.up("materialized")


def workflow_statement_logging(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Statement logging test needs to run with 100% logging of tests (as opposed to the default 1% )"""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True),
        Materialized(),
    ):
        c.up("materialized")

        c.sql(
            """
            ALTER SYSTEM SET statement_logging_max_sample_rate = 1.0;
            ALTER SYSTEM SET statement_logging_default_sample_rate = 1.0;
        """,
            port=6877,
            user="mz_system",
        )

        c.run_testdrive_files("statement-logging/statement-logging.td")


def workflow_blue_green_deployment(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Blue/Green Deployment testing, see https://www.notion.so/materialize/Testing-Plan-Blue-Green-Deployments-01528a1eec3b42c3a25d5faaff7a9bf9#f53b51b110b044859bf954afc771c63a"""
    c.down(destroy_volumes=True)

    running = True

    def selects():
        runtimes = []
        try:
            with c.sql_cursor() as cursor:
                while running:
                    total_runtime = 0
                    queries = [
                        "SELECT * FROM prod.counter_mv",
                        "SET CLUSTER = prod; SELECT max(counter) FROM counter",
                        "SELECT count(*) FROM prod.tpch_mv",
                    ]

                    for i, query in enumerate(queries):
                        start_time = time.time()
                        try:
                            cursor.execute(query.encode())
                        except DatabaseError as e:
                            # Expected
                            if "cached plan must not change result type" in str(e):
                                continue
                            raise e
                        if query.startswith("SET "):
                            cursor.nextset()
                        results = cursor.fetchone()
                        assert results
                        assert int(results[0]) > 0
                        runtime = time.time() - start_time
                        assert (
                            runtime < 5
                        ), f"query: {query}, runtime spiked to {runtime}"
                        total_runtime += runtime
                    runtimes.append(total_runtime)
        finally:
            print(f"Query runtimes: {runtimes}")

    def subscribe():
        cursor = c.sql_cursor()
        while running:
            try:
                cursor.execute("ROLLBACK")
                cursor.execute("BEGIN")
                cursor.execute(
                    "DECLARE subscribe CURSOR FOR SUBSCRIBE (SELECT * FROM prod.counter_mv)"
                )
                cursor.execute("FETCH ALL subscribe WITH (timeout='15s')")
                assert int(cursor.fetchall()[-1][2]) > 0
                cursor.execute("CLOSE subscribe")
            except DatabaseError as e:
                # Expected
                msg = str(e)
                if ("cached plan must not change result type" in msg) or (
                    "subscribe has been terminated because underlying relation" in msg
                ):
                    continue
                raise e

    with c.override(
        Testdrive(
            no_reset=True, default_timeout="300s"
        ),  # pending dataflows can take a while
        Clusterd(name="clusterd1"),
        Clusterd(name="clusterd2"),
        Materialized(
            additional_system_parameter_defaults={
                "unsafe_enable_unsafe_functions": "true",
                "unsafe_enable_unorchestrated_cluster_replicas": "true",
            },
        ),
    ):
        c.up("materialized")
        c.up("clusterd1")
        c.up("clusterd2")
        c.up("clusterd3")
        c.run_testdrive_files("blue-green-deployment/setup.td")

        threads = [PropagatingThread(target=fn) for fn in (selects, subscribe)]
        for thread in threads:
            thread.start()
        time.sleep(10)  # some time to make sure the queries run fine
        try:
            c.run_testdrive_files("blue-green-deployment/deploy.td")
        finally:
            running = False
            for thread in threads:
                thread.join()


def workflow_test_subscribe_hydration_status(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Test that hydration status tracking works for subscribe dataflows."""

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("testdrive", persistent=True)

    # Start a subscribe.
    cursor = c.sql_cursor()
    cursor.execute("BEGIN")
    cursor.execute("DECLARE c CURSOR FOR SUBSCRIBE mz_tables")
    cursor.execute("FETCH 1 c")

    # Verify that the subscribe dataflow eventually shows as hydrated.
    c.testdrive(
        input=dedent(
            """
            > SET cluster = mz_catalog_server
            > SELECT DISTINCT h.time_ns IS NOT NULL
              FROM mz_internal.mz_subscriptions s,
              unnest(s.referenced_object_ids) as sroi(id)
              JOIN mz_introspection.mz_compute_hydration_times_per_worker h ON h.export_id = s.id
              JOIN mz_tables t ON (t.id = sroi.id)
              WHERE t.name = 'mz_tables'
            true
            """
        )
    )

    # Cancel the subscribe.
    cursor.execute("ROLLBACK")

    # Verify that the subscribe's hydration status is removed.
    c.testdrive(
        input=dedent(
            """
            > SET cluster = mz_catalog_server
            > SELECT DISTINCT h.time_ns IS NOT NULL
              FROM mz_internal.mz_subscriptions s,
              unnest(s.referenced_object_ids) as sroi(id)
              JOIN mz_introspection.mz_compute_hydration_times_per_worker h ON h.export_id = s.id
              JOIN mz_tables t ON (t.id = sroi.id)
              WHERE t.name = 'mz_tables'
            """
        )
    )


def workflow_cluster_drop_concurrent(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Test that dropping a cluster will close already running queries against
    that cluster, both SELECTs and SUBSCRIBEs.
    """
    c.down(destroy_volumes=True)

    def select():
        with c.sql_cursor() as cursor:
            # This should hang instantly as the timestamp is far in the future,
            # until the cluster is dropped
            cursor.execute("SELECT * FROM counter AS OF 18446744073709551615")

    def subscribe():
        cursor = c.sql_cursor()
        cursor.execute("BEGIN")
        cursor.execute("DECLARE subscribe CURSOR FOR SUBSCRIBE (SELECT * FROM counter)")
        # This should hang until the cluster is dropped
        cursor.execute("FETCH ALL subscribe")

    with c.override(
        Testdrive(
            no_reset=True,
        ),
        Clusterd(name="clusterd1"),
        Materialized(),
    ):
        c.up("materialized")
        c.up("clusterd1")
        c.run_testdrive_files("cluster-drop-concurrent/setup.td")
        threads = [
            PropagatingThread(target=fn, name=name)
            for fn, name in ((select, "select"), (subscribe, "subscribe"))
        ]

        for thread in threads:
            thread.start()
        time.sleep(2)  # some time to make sure the queries are in progress
        try:
            c.run_testdrive_files("cluster-drop-concurrent/run.td")
        finally:
            for thread in threads:
                try:
                    thread.join(timeout=10)
                except InternalError_ as e:
                    assert 'query could not complete because relation "materialize.public.counter" was dropped' in str(
                        e
                    ) or 'subscribe has been terminated because underlying relation "materialize.public.counter" was dropped' in str(
                        e
                    )
            for thread in threads:
                assert not thread.is_alive(), f"Thread {thread.name} is still running"


def workflow_test_refresh_mv_warmup(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Test REFRESH materialized view warmup behavior after envd restarts:
    1. Regression test for https://github.com/MaterializeInc/database-issues/issues/7574
       If an MV is past its last refresh, it shouldn't get rehydrated after a restart.
    2. Regression test for https://github.com/MaterializeInc/database-issues/issues/7543
       Bootstrapping should select an `as_of` for an MV dataflow in a way that allows it to warm up before its next
       refresh.
    """

    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "enable_refresh_every_mvs": "true",
            },
        ),
        Testdrive(no_reset=True),
    ):
        c.down(destroy_volumes=True)
        c.up("materialized")
        c.up("testdrive", persistent=True)

        c.testdrive(
            input=dedent(
                """
                > CREATE CLUSTER cluster12 SIZE '1';
                > SET cluster = cluster12;

                ## 1. Create a materialized view that has only one refresh, and takes at least a few seconds to hydrate.
                ##    (Currently, it's ~2 seconds on a release build.)
                > CREATE TABLE t1 (a int);
                > INSERT INTO t1 VALUES (10000000);

                > CREATE MATERIALIZED VIEW mv1 WITH (REFRESH AT CREATION) AS
                  SELECT count(*) FROM (SELECT generate_series(1,a) FROM t1);

                # Let's wait for its initial refresh to complete.
                > SELECT * FROM mv1;
                10000000

                # This INSERT shouldn't be visible in mv1, because we are past its only refresh.
                > INSERT INTO t1 VALUES (10000001);

                ## 2. Create an materialized view that will have its first refresh immediately, but it's next refresh is
                ##    a long time away.
                > CREATE TABLE t2 (a int);
                > INSERT INTO t2 VALUES (100);

                > CREATE MATERIALIZED VIEW mv2 WITH (REFRESH EVERY '1 day') AS
                  SELECT count(*) FROM (SELECT generate_series(1,a) FROM t2);

                > SELECT * FROM mv2;
                100

                > INSERT INTO t2 VALUES (1000);
                """
            )
        )

        # Restart environmentd
        c.kill("materialized")
        c.up("materialized")

        c.testdrive(
            input=dedent(
                """
                ## 1. We shouldn't have a dataflow for mv1.
                > SELECT * FROM mz_introspection.mz_dataflows WHERE name = 'mv1';
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="500ms"
                > SELECT * FROM mz_introspection.mz_dataflows WHERE name = 'mv1';
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="500ms"
                > SELECT * FROM mz_introspection.mz_dataflows WHERE name = 'mv1';
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="500ms"
                > SELECT * FROM mz_introspection.mz_dataflows WHERE name = 'mv1';
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="500ms"
                > SELECT * FROM mz_introspection.mz_dataflows WHERE name = 'mv1';
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="500ms"
                > SELECT * FROM mz_introspection.mz_dataflows WHERE name = 'mv1';

                > SELECT * FROM mv1;
                10000000

                ## 2. Check that mv2's dataflow hydrates, even though we are a long time away from the next refresh.
                > SELECT hydrated
                  FROM mz_internal.mz_compute_hydration_statuses h JOIN mz_objects o ON (h.object_id = o.id)
                  WHERE name = 'mv2';
                true

                # Check that the next refresh hasn't happened yet.
                > INSERT INTO t2 VALUES (10000);
                > SELECT * FROM mv2;
                100
                """
            )
        )


def check_read_frontier_not_stuck(c: Composition, object_name: str):
    query = f"""
        SELECT f.read_frontier
        FROM mz_internal.mz_frontiers f
        JOIN mz_objects o ON o.id = f.object_id
        WHERE o.name = '{object_name}';
        """

    # Because `mz_frontiers` isn't a linearizable relation it's possible that
    # we need to wait a bit for the object's frontier to show up.
    result = c.sql_query(query)
    if not result:
        time.sleep(2)
        result = c.sql_query(query)

    before = int(result[0][0])
    time.sleep(2)
    after = int(c.sql_query(query)[0][0])
    assert before < after, f"read frontier of {object_name} is stuck"


def workflow_test_refresh_mv_restart(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Test REFRESH materialized views with envd restarts:

    1a. Restart just after a refresh, and then check after the next refresh that things still work.
    1b. Same as above, but turn off the replica before the restart, and then turn it back on after the restart.
    1c. Same as 1a, but with the MV reading from an index.
    1i. Same as 1a, but on an auto-scheduled cluster.
    1j. Same as 1a, but there is an index on the MV, and the refresh interval is large, so we are still before the next
        refresh after the restart.

    2a. Sleep through a refresh time after killing envd, then bring up envd and check that we recover.
    2b. Same as 2a, but manipulate replicas as in 1b.
    2c. Same as 2a, but with the MV reading from an index.
    2i. Same as 2a, but on an auto-scheduled cluster.
    2j. Same as 1j, but with the long sleep of 2a.

    3d. No replica while creating the MV, restart envd, and then create a replica
    3e. Same as 3d, but with an MV reading from an index.
    3f. Same as 3d, but with an MV that has a last refresh.
    3g. Same as 3e, but with an MV that has a last refresh.
    3h. Same as 3d, but with an MV that has a short refresh interval, so we miss several refreshes by the time we get a
        replica.

    When querying MVs after the restarts, perform the same queries with all combinations of
     - SERIALIZABLE / STRICT SERIALIZABLE,
     - with / without indexes on MVs.

    After each of 1., 2., and 3., check that the input table's read frontier keeps advancing.

    Also do some sanity checks on introspection objects related to REFRESH MVs.

    Other tests involving REFRESH MVs and envd restarts:
      * workflow_test_github_8734
      * workflow_test_refresh_mv_warmup
    """

    def check_introspection():
        c.testdrive(
            input=dedent(
                """
                # Wait for introspection objects to be populated.
                > SELECT count(*) > 0 FROM mz_catalog.mz_materialized_views;
                true
                > SELECT count(*) > 0 FROM mz_internal.mz_materialized_view_refresh_strategies;
                true
                > SELECT count(*) > 0 FROM mz_internal.mz_materialized_view_refreshes;
                true

                # Check that no MV is missing from any of the introspection objects.
                # Note that only REFRESH MVs show up in `mz_materialized_view_refreshes`, which is ok, because we are
                # creating only REFRESH MVs in these tests.
                > SELECT *
                  FROM
                    mz_catalog.mz_materialized_views mv
                    FULL OUTER JOIN mz_internal.mz_materialized_view_refreshes mvr ON (mv.id = mvr.materialized_view_id)
                    FULL OUTER JOIN mz_internal.mz_materialized_view_refresh_strategies mvrs ON (mv.id = mvrs.materialized_view_id)
                  WHERE
                    mv.id IS NULL OR
                    mvr.materialized_view_id IS NULL OR
                    mvrs.materialized_view_id IS NULL;
                """
            )
        )

    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "enable_refresh_every_mvs": "true",
                "enable_cluster_schedule_refresh": "true",
            },
        ),
        Testdrive(no_reset=True),
    ):
        # We'll issue the same SQL commands in 1. and 2. (the only difference is we make the restart slow with a sleep),
        # so save the SQL commands in `before_restart` and `after_restart`.
        before_restart = dedent(
            """
            > CREATE TABLE t (x int);
            > INSERT INTO t VALUES (100);

            > CREATE CLUSTER cluster_acj SIZE '1';
            > CREATE CLUSTER cluster_b SIZE '1';
            > CREATE CLUSTER cluster_auto_scheduled (SIZE '1', SCHEDULE = ON REFRESH);

            > CREATE MATERIALIZED VIEW mv_a
              IN CLUSTER cluster_acj
              WITH (REFRESH EVERY '20 sec' ALIGNED TO mz_now()::text::int8 + 2000) AS
              SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

            > CREATE MATERIALIZED VIEW mv_b
              IN CLUSTER cluster_b
              WITH (REFRESH EVERY '20 sec' ALIGNED TO mz_now()::text::int8 + 2000) AS
              SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

            > CREATE DEFAULT INDEX
              IN CLUSTER cluster_acj
              ON t;
            > CREATE MATERIALIZED VIEW mv_c
              IN CLUSTER cluster_acj
              WITH (REFRESH EVERY '20 sec' ALIGNED TO mz_now()::text::int8 + 2000) AS
              SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

            > CREATE MATERIALIZED VIEW mv_i
              IN CLUSTER cluster_auto_scheduled
              WITH (REFRESH EVERY '20 sec' ALIGNED TO mz_now()::text::int8 + 2000) AS
              SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

            > CREATE MATERIALIZED VIEW mv_j
              IN CLUSTER cluster_acj
              WITH (REFRESH EVERY '1000000 sec' ALIGNED TO mz_now()::text::int8 + 2000) AS
              SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

            > CREATE CLUSTER serving SIZE '1';
            > CREATE CLUSTER serving_indexed SIZE '1';

            > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_a;
            > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_b;
            > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_c;
            > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_i;
            > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_j;

            # Let's wait for the MVs' initial refresh to complete.

            > SET cluster = 'serving';
            > SELECT * FROM mv_a;
            100
            > SELECT * FROM mv_b;
            100
            > SELECT * FROM mv_c;
            100
            > SELECT * FROM mv_i;
            100
            > SELECT * FROM mv_j;
            100
            > (SELECT * FROM mv_j)
              UNION
              (SELECT x + 1 FROM t);
            100
            101

            > SET cluster = 'serving_indexed';
            > SELECT * FROM mv_a;
            100
            > SELECT * FROM mv_b;
            100
            > SELECT * FROM mv_c;
            100
            > SELECT * FROM mv_i;
            100
            > SELECT * FROM mv_j;
            100
            > (SELECT * FROM mv_j)
              UNION
              (SELECT x + 1 FROM t);
            100
            101

            > INSERT INTO t VALUES (1000);

            > ALTER CLUSTER cluster_b SET (REPLICATION FACTOR 0);
            """
        )

        after_restart = dedent(
            """
            > ALTER CLUSTER cluster_b SET (REPLICATION FACTOR 1);

            > SET TRANSACTION_ISOLATION TO 'STRICT SERIALIZABLE';

            > SET cluster = 'serving';

            > SELECT * FROM mv_a;
            1100
            > SELECT * FROM mv_b;
            1100
            > SELECT * FROM mv_c;
            1100
            > SELECT * FROM mv_i;
            1100
            > SELECT * FROM mv_j;
            100
            > (SELECT * FROM mv_j)
              UNION
              (SELECT x + 1 FROM t);
            100
            101
            1001

            > SET cluster = 'serving_indexed';

            > SELECT * FROM mv_a;
            1100
            > SELECT * FROM mv_b;
            1100
            > SELECT * FROM mv_c;
            1100
            > SELECT * FROM mv_i;
            1100
            > SELECT * FROM mv_j;
            100
            > (SELECT * FROM mv_j)
              UNION
              (SELECT x + 1 FROM t);
            100
            101
            1001

            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';

            > SET cluster = 'serving';

            > SELECT * FROM mv_a;
            1100
            > SELECT * FROM mv_b;
            1100
            > SELECT * FROM mv_c;
            1100
            > SELECT * FROM mv_i;
            1100
            > SELECT * FROM mv_j;
            100
            > (SELECT * FROM mv_j)
              UNION
              (SELECT x + 1 FROM t);
            100
            101
            1001

            > SET cluster = 'serving_indexed';

            > SELECT * FROM mv_a;
            1100
            > SELECT * FROM mv_b;
            1100
            > SELECT * FROM mv_c;
            1100
            > SELECT * FROM mv_i;
            1100
            > SELECT * FROM mv_j;
            100
            > (SELECT * FROM mv_j)
              UNION
              (SELECT x + 1 FROM t);
            100
            101
            1001
            """
        )

        c.down(destroy_volumes=True)
        c.up("materialized")
        c.up("testdrive", persistent=True)

        # 1. (quick restart)
        c.testdrive(input=before_restart)
        check_introspection()
        c.kill("materialized")
        c.up("materialized")
        check_introspection()
        c.testdrive(input=after_restart)
        check_read_frontier_not_stuck(c, "t")
        check_introspection()

        # Reset the testing context.
        c.down(destroy_volumes=True)
        c.up("materialized")
        c.up("testdrive", persistent=True)

        # 2. (slow restart)
        c.testdrive(input=before_restart)
        check_introspection()
        c.kill("materialized")
        time.sleep(20)  # Sleep through the refresh interval of the above MVs
        c.up("materialized")
        check_introspection()
        c.testdrive(input=after_restart)
        check_read_frontier_not_stuck(c, "t")
        check_introspection()

        # Reset the testing context.
        c.down(destroy_volumes=True)
        c.up("materialized")
        c.up("testdrive", persistent=True)

        # 3.
        c.testdrive(
            input=dedent(
                """
                > CREATE TABLE t (x int);
                > INSERT INTO t VALUES (100);

                > CREATE CLUSTER cluster_defgh (SIZE '1', REPLICATION FACTOR 0);

                > CREATE MATERIALIZED VIEW mv_3h
                  IN CLUSTER cluster_defgh
                  WITH (REFRESH EVERY '1600 ms') AS
                  SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

                > CREATE MATERIALIZED VIEW mv_3d
                  IN CLUSTER cluster_defgh
                  WITH (REFRESH EVERY '100000 sec') AS
                  SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

                > CREATE MATERIALIZED VIEW mv_3f
                  IN CLUSTER cluster_defgh
                  WITH (REFRESH AT CREATION) AS
                  SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

                > CREATE DEFAULT INDEX
                  IN CLUSTER cluster_defgh
                  ON t;

                > CREATE MATERIALIZED VIEW mv_3e
                  IN CLUSTER cluster_defgh
                  WITH (REFRESH EVERY '100000 sec') AS
                  SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

                > CREATE MATERIALIZED VIEW mv_3g
                  IN CLUSTER cluster_defgh
                  WITH (REFRESH AT CREATION) AS
                  SELECT count(*) FROM (SELECT generate_series(1,x) FROM t);

                > CREATE CLUSTER serving_indexed SIZE '1';
                > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_3d;
                > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_3e;
                > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_3f;
                > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_3g;
                > CREATE DEFAULT INDEX IN CLUSTER serving_indexed ON mv_3h;

                > INSERT INTO t VALUES (1000);
                """
            )
        )

        check_introspection()
        c.kill("materialized")
        c.up("materialized")
        check_introspection()
        c.testdrive(
            input=dedent(
                """
                > ALTER CLUSTER cluster_defgh SET (REPLICATION FACTOR 1);

                > CREATE CLUSTER serving SIZE '1';

                > SET TRANSACTION_ISOLATION TO 'STRICT SERIALIZABLE';

                > SET cluster = 'serving';

                > SELECT * FROM mv_3d
                100
                > SELECT * FROM mv_3e
                100
                > SELECT * FROM mv_3f
                100
                > SELECT * FROM mv_3g
                100

                > SELECT * FROM mv_3h;
                1100

                > INSERT INTO t VALUES (10000);

                > SELECT * FROM mv_3h;
                11100

                > SET cluster = 'serving_indexed';

                > SELECT * FROM mv_3d
                100
                > SELECT * FROM mv_3e
                100
                > SELECT * FROM mv_3f
                100
                > SELECT * FROM mv_3g
                100

                > SELECT * FROM mv_3h;
                11100

                > INSERT INTO t VALUES (10000);

                > SELECT * FROM mv_3h;
                21100

                > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';

                > SET cluster = 'serving';

                > SELECT * FROM mv_3d
                100
                > SELECT * FROM mv_3e
                100
                > SELECT * FROM mv_3f
                100
                > SELECT * FROM mv_3g
                100

                > SELECT * FROM mv_3h;
                21100

                > INSERT INTO t VALUES (10000);

                > SELECT * FROM mv_3h;
                31100

                > SET cluster = 'serving_indexed';

                > SELECT * FROM mv_3d
                100
                > SELECT * FROM mv_3e
                100
                > SELECT * FROM mv_3f
                100
                > SELECT * FROM mv_3g
                100

                > SELECT * FROM mv_3h;
                31100

                > INSERT INTO t VALUES (10000);

                > SELECT * FROM mv_3h;
                41100
                """
            )
        )
        check_read_frontier_not_stuck(c, "t")
        check_introspection()

        # Drop some MVs and check that this is reflected in the introspection objects.
        c.testdrive(
            input=dedent(
                """
                > DROP MATERIALIZED VIEW mv_3h;
                > DROP MATERIALIZED VIEW mv_3d;
                > SELECT * FROM mz_catalog.mz_materialized_views
                  WHERE name = 'mv_3h' OR name = 'mv_3d';
                """
            )
        )
        check_introspection()


def workflow_test_github_8734(c: Composition) -> None:
    """
    Tests that REFRESH MVs on paused clusters don't unnecessarily hold back
    compaction of their inputs after an envd restart.

    Regression test for database-issues#8734.
    """

    c.down(destroy_volumes=True)

    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "enable_refresh_every_mvs": "true",
            },
        ),
        Testdrive(no_reset=True),
    ):
        c.up("materialized")
        c.up("testdrive", persistent=True)

        # Create a REFRESH MV and wait for it to refresh once, then take down
        # its cluster.
        c.sql(
            """
            CREATE TABLE t (a int);

            CREATE CLUSTER test SIZE '1';
            CREATE MATERIALIZED VIEW mv
                IN CLUSTER test
                WITH (REFRESH EVERY '60m')
                AS SELECT * FROM t;
            SELECT * FROM mv;

            ALTER CLUSTER test SET (REPLICATION FACTOR 0);
            """
        )

        check_read_frontier_not_stuck(c, "t")

        # Restart envd, then verify that the table's frontier still advances.
        c.kill("materialized")
        c.up("materialized")

        c.sql("SELECT * FROM mv")

        check_read_frontier_not_stuck(c, "t")


def workflow_test_github_7798(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Regression test for database-issues#7798."""

    c.down(destroy_volumes=True)

    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "unsafe_enable_unsafe_functions": "true",
                "unsafe_enable_unorchestrated_cluster_replicas": "true",
            },
        ),
        Testdrive(
            no_reset=True,
            default_timeout="10s",
        ),
    ):

        def check_frontiers_advance():
            c.testdrive(
                dedent(
                    r"""
                $ set-regex match=0|\d{13,20} replacement=<TIMESTAMP>

                -- Run the frontier query once to make `tokio-postgres` collect type information.
                -- If we don't do this it will inject SELECTs into the transaction, making it fail.
                > SELECT write_frontier FROM mz_internal.mz_frontiers WHERE false

                > BEGIN
                > DECLARE c CURSOR FOR SUBSCRIBE (
                    SELECT write_frontier
                    FROM mz_internal.mz_frontiers
                    JOIN mz_tables ON (id = object_id)
                    WHERE name = 'lineitem'
                  )

                > FETCH 1 c
                <TIMESTAMP>  1 <TIMESTAMP>

                > FETCH 2 c
                <TIMESTAMP>  1 <TIMESTAMP>
                <TIMESTAMP> -1 <TIMESTAMP>

                > FETCH 2 c
                <TIMESTAMP>  1 <TIMESTAMP>
                <TIMESTAMP> -1 <TIMESTAMP>

                > ROLLBACK
                """
                )
            )

        c.up("materialized")
        c.up("clusterd1")
        c.up("testdrive", persistent=True)

        # Create an unmanaged cluster that isn't restarted together with materialized,
        # and therein a source with subsources.
        c.sql(
            """
            CREATE CLUSTER source REPLICAS (
                replica1 (
                    STORAGECTL ADDRESSES ['clusterd1:2100'],
                    STORAGE ADDRESSES ['clusterd1:2103'],
                    COMPUTECTL ADDRESSES ['clusterd1:2101'],
                    COMPUTE ADDRESSES ['clusterd1:2102'],
                    WORKERS 1
                )
            );

            CREATE SOURCE lgtpch
            IN CLUSTER source
            FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.001, TICK INTERVAL '1s');

            CREATE TABLE customer FROM SOURCE lgtpch (REFERENCE customer);
            CREATE TABLE lineitem FROM SOURCE lgtpch (REFERENCE lineitem);
            CREATE TABLE nation FROM SOURCE lgtpch (REFERENCE nation);
            CREATE TABLE orders FROM SOURCE lgtpch (REFERENCE orders);
            CREATE TABLE part FROM SOURCE lgtpch (REFERENCE part);
            CREATE TABLE partsupp FROM SOURCE lgtpch (REFERENCE partsupp);
            CREATE TABLE region FROM SOURCE lgtpch (REFERENCE region);
            CREATE TABLE supplier FROM SOURCE lgtpch (REFERENCE supplier);
            """,
        )

        check_frontiers_advance()

        # Restart envd to force a storage reconciliation.
        c.kill("materialized")
        c.up("materialized")

        check_frontiers_advance()


def workflow_test_http_race_condition(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    c.down(destroy_volumes=True)
    c.up("materialized")

    def worker() -> None:
        end_time = time.time() + 60
        while time.time() < end_time:
            timeout = random.uniform(0.01, 1.0)
            rows = random.uniform(1, 10000)
            envd_port = c.port("materialized", 6876)
            try:
                result = requests.post(
                    f"http://localhost:{envd_port}/api/sql",
                    data=json.dumps(
                        {"query": f"select generate_series(1, {rows}::int8)"}
                    ),
                    headers={"content-type": "application/json"},
                    timeout=timeout,
                )
            except requests.exceptions.ReadTimeout:
                continue
            assert result.status_code == 200, result

    threads = []
    for j in range(100):
        thread = Thread(name=f"worker_{j}", target=worker)
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    cleanup_seconds = 120 if ui.env_is_truthy("CI_COVERAGE_ENABLED") else 30
    stopping_time = datetime.now() + timedelta(seconds=cleanup_seconds)
    while datetime.now() < stopping_time:
        result = c.sql_query(
            "SELECT * FROM mz_internal.mz_sessions WHERE connection_id <> pg_backend_pid()"
        )
        if not result:
            break
        print(
            f"There are supposed to be no sessions remaining, but there are:\n{result}"
        )
    else:
        raise RuntimeError(f"Sessions did not clean up after {cleanup_seconds}s")


def workflow_test_read_frontier_advancement(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Tests that in a set of dependent healthy compute collections all read
    frontiers keep advancing continually. This is to protect against
    regressions in downgrading read holds.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")

    # Create various dataflows on a cluster with multiple replicas, to also
    # test tracking of per-replica read holds.
    c.sql(
        """
        CREATE CLUSTER test SIZE '2-2', REPLICATION FACTOR 4;
        SET cluster = test;

        CREATE TABLE t1 (x int);
        CREATE TABLE t2 (y int);

        CREATE VIEW v1 AS SELECT * FROM t1 JOIN t2 ON (x = y);
        CREATE DEFAULT INDEX ON v1;

        CREATE VIEW v2 AS SELECT x + 1 AS a, y + 2 AS b FROM v1;
        CREATE DEFAULT INDEX ON v2;

        CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM v2;
        CREATE DEFAULT INDEX ON mv1;

        CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM mv1, t1;

        -- wait for dataflows to hydrate
        SELECT 1 FROM v1, v2, mv1, mv2;
        """,
    )

    # Run a subscribe in a different thread.
    def subscribe():
        cursor = c.sql_cursor()
        cursor.execute("SET cluster = test")
        cursor.execute("BEGIN")
        cursor.execute("DECLARE sub CURSOR FOR SUBSCRIBE (SELECT * FROM mv2)")
        try:
            # This should hang until the cluster is dropped. The timeout is
            # only a failsafe.
            cursor.execute("FETCH ALL sub WITH (timeout = '30s')")
        except DatabaseError as exc:
            assert (
                exc.diag.message_primary
                == 'subscribe has been terminated because underlying relation "materialize.public.mv2" was dropped'
            )

    subscribe_thread = Thread(target=subscribe)
    subscribe_thread.start()

    # Wait for the subscribe to start and frontier introspection to be refreshed.
    time.sleep(3)

    # Check that read frontiers advance.
    def collect_read_frontiers() -> dict[str, int]:
        output = c.sql_query(
            """
            SELECT object_id, read_frontier
            FROM mz_internal.mz_frontiers
            WHERE object_id LIKE 'u%'
            """
        )
        frontiers = {}
        for row in output:
            name, frontier = row
            frontiers[name] = int(frontier)
        return frontiers

    frontiers1 = collect_read_frontiers()
    time.sleep(3)
    frontiers2 = collect_read_frontiers()

    for id_ in frontiers1:
        a, b = frontiers1[id_], frontiers2[id_]
        assert a < b, f"read frontier of {id_} has not advanced: {a} -> {b}"

    # Drop the cluster to cancel the subscribe.
    c.sql("DROP CLUSTER test CASCADE")
    subscribe_thread.join()


def workflow_test_adhoc_system_indexes(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Tests that the system user can create ad-hoc system indexes and that they
    are handled normally by the system.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")

    # The system user should be able to create a new index on a catalog object
    # in the mz_catalog_server cluster.
    c.sql(
        """
        SET cluster = mz_catalog_server;
        CREATE INDEX mz_test_idx1 ON mz_tables (char_length(name));
        """,
        port=6877,
        user="mz_system",
    )

    output = c.sql_query(
        """
        SELECT i.id, o.name, c.name
        FROM mz_indexes i
        JOIN mz_objects o ON (i.on_id = o.id)
        JOIN mz_clusters c ON (i.cluster_id = c.id)
        WHERE i.name = 'mz_test_idx1'
        """
    )
    assert output[0] == ("u1", "mz_tables", "mz_catalog_server"), output
    output = c.sql_query("EXPLAIN SELECT * FROM mz_tables WHERE char_length(name) = 9")
    assert "mz_test_idx1" in output[0][0], output
    output = c.sql_query("SELECT * FROM mz_tables WHERE char_length(name) = 9")
    assert len(output) > 0

    # The system user should be able to create a new index on an unstable
    # catalog object in the mz_catalog_server cluster if
    # `unsafe_enable_unstable_dependencies` is set.
    c.sql(
        """
        ALTER SYSTEM SET unsafe_enable_unstable_dependencies = on;
        SET cluster = mz_catalog_server;
        CREATE INDEX mz_test_idx2 ON mz_internal.mz_hydration_statuses (hydrated);
        ALTER SYSTEM SET unsafe_enable_unstable_dependencies = off;
        """,
        port=6877,
        user="mz_system",
    )

    output = c.sql_query(
        """
        SELECT i.id, o.name, c.name
        FROM mz_indexes i
        JOIN mz_objects o ON (i.on_id = o.id)
        JOIN mz_clusters c ON (i.cluster_id = c.id)
        WHERE i.name = 'mz_test_idx2'
        """
    )
    assert output[0] == ("u2", "mz_hydration_statuses", "mz_catalog_server"), output
    output = c.sql_query(
        "EXPLAIN SELECT * FROM mz_internal.mz_hydration_statuses WHERE hydrated"
    )
    assert "mz_test_idx2" in output[0][0]
    output = c.sql_query(
        "SELECT * FROM mz_internal.mz_hydration_statuses WHERE hydrated"
    )
    assert len(output) > 0

    # Make sure everything the new indexes survive a restart.

    c.kill("materialized")
    c.up("materialized")

    output = c.sql_query(
        """
        SELECT i.id, o.name, c.name
        FROM mz_indexes i
        JOIN mz_objects o ON (i.on_id = o.id)
        JOIN mz_clusters c ON (i.cluster_id = c.id)
        WHERE i.name LIKE 'mz_test_idx%'
        ORDER BY id
        """
    )
    assert output[0] == ("u1", "mz_tables", "mz_catalog_server"), output
    assert output[1] == ("u2", "mz_hydration_statuses", "mz_catalog_server"), output

    # Make sure the new indexes can be dropped again.
    c.sql(
        """
        DROP INDEX mz_test_idx1;
        DROP INDEX mz_internal.mz_test_idx2;
        """,
        port=6877,
        user="mz_system",
    )

    output = c.sql_query(
        """
        SELECT i.id, o.name, c.name
        FROM mz_indexes i
        JOIN mz_objects o ON (i.on_id = o.id)
        JOIN mz_clusters c ON (i.cluster_id = c.id)
        WHERE i.name LIKE 'mz_test_idx%'
        ORDER BY id
        """
    )
    assert not output, output


def workflow_test_mz_introspection_cluster_compat(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Tests that usages of the `mz_introspection` cluster and the
    `auto_route_introspection_queries` variable, which both have been renamed,
    are automatically translated to the new names.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")

    with c.override(
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")

        # Setting variables through `SET <variable>`.
        c.testdrive(
            dedent(
                """
                > SHOW cluster
                quickstart

                > SET cluster = mz_introspection
                > SHOW cluster
                mz_catalog_server

                > SHOW auto_route_catalog_queries
                on

                > SET auto_route_introspection_queries = off
                > SHOW auto_route_catalog_queries
                off

                > RESET cluster
                > SHOW cluster
                quickstart

                > RESET auto_route_introspection_queries
                > SHOW auto_route_catalog_queries
                on
                """
            )
        )

        # Setting variables through `ALTER ROLE`.
        c.sql(
            """
            ALTER ROLE materialize SET cluster = mz_introspection;
            ALTER ROLE materialize SET auto_route_introspection_queries = off;
            """
        )
        c.testdrive(
            dedent(
                """
                > SHOW cluster
                mz_catalog_server
                > SHOW auto_route_catalog_queries
                off
                """
            )
        )
        c.sql(
            """
            ALTER ROLE materialize RESET cluster;
            ALTER ROLE materialize RESET auto_route_introspection_queries;
            """
        )
        c.testdrive(
            dedent(
                """
                > SHOW cluster
                quickstart
                > SHOW auto_route_catalog_queries
                on
                """
            )
        )

        # Setting variables through the connection string.
        port = c.default_port("materialized")
        url = (
            f"postgres://materialize@localhost:{port}?options="
            "--cluster%3Dmz_introspection%20"
            "--auto_route_introspection_queries%3Doff"
        )
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW cluster")
                row = cur.fetchone()
                assert row == ("mz_catalog_server",), row

                cur.execute("SHOW auto_route_catalog_queries")
                row = cur.fetchone()
                assert row == ("off",), row


def workflow_test_unified_introspection_during_replica_disconnect(c: Composition):
    """
    Test that unified introspection data collected for a replica remains
    available after the replica disconnects, until it sends updated
    introspection data.
    """

    c.down(destroy_volumes=True)

    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "unsafe_enable_unsafe_functions": "true",
                "unsafe_enable_unorchestrated_cluster_replicas": "true",
            },
        ),
        Testdrive(
            no_reset=True,
            default_timeout="10s",
        ),
    ):
        c.up("materialized")
        c.up("clusterd1")
        c.up("testdrive", persistent=True)

        # Set up an unorchestrated replica with a couple dataflows.
        c.sql(
            """
            CREATE CLUSTER test REPLICAS (
                test (
                    STORAGECTL ADDRESSES ['clusterd1:2100'],
                    STORAGE ADDRESSES ['clusterd1:2103'],
                    COMPUTECTL ADDRESSES ['clusterd1:2101'],
                    COMPUTE ADDRESSES ['clusterd1:2102'],
                    WORKERS 1
                )
            );
            SET cluster = test;

            CREATE TABLE t (a int);
            CREATE INDEX idx ON t (a);
            CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;
            """
        )

        output = c.sql_query("SELECT id FROM mz_cluster_replicas WHERE name = 'test'")
        replica_id = output[0][0]

        # Wait for the dataflows to be reported as hydrated.
        c.testdrive(
            dedent(
                f"""
                > SELECT o.name, h.time_ns IS NOT NULL
                  FROM mz_internal.mz_compute_hydration_times h
                  JOIN mz_objects o ON o.id = h.object_id
                  WHERE
                      h.replica_id = '{replica_id}' AND
                      h.object_id LIKE 'u%'
                idx true
                mv  true
                """
            )
        )

        output = c.sql_query(
            f"""
            SELECT sum(time_ns)
            FROM mz_internal.mz_compute_hydration_times
            WHERE replica_id = '{replica_id}'
            """
        )
        previous_times = output[0][0]

        # Kill the replica, wait for a bit for envd to notice, then restart it.
        c.kill("clusterd1")
        time.sleep(5)

        # Verify that the hydration times are still queryable.
        c.testdrive(
            dedent(
                f"""
                > SELECT o.name, h.time_ns IS NOT NULL
                  FROM mz_internal.mz_compute_hydration_times h
                  JOIN mz_objects o ON o.id = h.object_id
                  WHERE
                      h.replica_id = '{replica_id}' AND
                      h.object_id LIKE 'u%'
                idx true
                mv  true
                """
            )
        )

        # Restart the replica, wait for it to report a new set of hydration times.
        c.up("clusterd1")

        c.testdrive(
            dedent(
                f"""
                > SELECT sum(time_ns) != {previous_times}
                  FROM mz_internal.mz_compute_hydration_times
                  WHERE replica_id = '{replica_id}'
                true
                """
            )
        )


def workflow_test_graceful_reconfigure(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Tests gracefully reconfiguring a managed cluster
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
            ALTER SYSTEM SET enable_graceful_cluster_reconfiguration = true;

            DROP CLUSTER IF EXISTS cluster1 CASCADE;
            DROP TABLE IF EXISTS t CASCADE;

            CREATE CLUSTER cluster1 ( SIZE = '1');

            SET CLUSTER = cluster1;

            -- now let's give it another go with user-defined objects
            CREATE TABLE t (a int);
            CREATE DEFAULT INDEX ON t;
            INSERT INTO t VALUES (42);
            GRANT ALL ON CLUSTER cluster1 TO materialize;
            """,
            port=6877,
            user="mz_system",
        )
        replicas = c.sql_query(
            """
            SELECT mz_cluster_replicas.name
            FROM mz_cluster_replicas, mz_clusters WHERE
            mz_cluster_replicas.cluster_id = mz_clusters.id AND mz_clusters.name='cluster1';
            """
        )
        assert replicas == [
            ("r1",)
        ], f"Cluster should only have one replica prior to alter, found {replicas}"

        replicas = c.sql_query(
            """
            SELECT cr.name
            FROM mz_internal.mz_pending_cluster_replicas ur
            INNER join mz_cluster_replicas cr ON cr.id=ur.id
            INNER join mz_clusters c ON c.id=cr.cluster_id
            WHERE c.name = 'cluster1';
            """
        )
        assert (
            len(replicas) == 0
        ), f"Cluster should only have no pending replica prior to alter, found {replicas}"

        def gracefully_alter():
            try:
                c.sql(
                    """
                    ALTER CLUSTER cluster1 SET (SIZE = '2') WITH ( WAIT FOR '10s')
                    """,
                    port=6877,
                    user="mz_system",
                )
            except OperationalError:
                # We expect the network to drop during this
                pass

        # Run a reconfigure
        thread = Thread(target=gracefully_alter)
        thread.start()
        time.sleep(3)

        # Validate that there is a pending replica
        replicas = c.sql_query(
            """
            SELECT mz_cluster_replicas.name
            FROM mz_cluster_replicas, mz_clusters WHERE
            mz_cluster_replicas.cluster_id = mz_clusters.id AND mz_clusters.name='cluster1';
            """
        )
        assert replicas == [("r1",), ("r1-pending",)], replicas
        replicas = c.sql_query(
            """
            SELECT cr.name
            FROM mz_internal.mz_pending_cluster_replicas ur
            INNER join mz_cluster_replicas cr ON cr.id=ur.id
            INNER join mz_clusters c ON c.id=cr.cluster_id
            WHERE c.name = 'cluster1';
            """
        )
        assert (
            len(replicas) == 1
        ), "pending replica should be in mz_pending_cluster_replicas"

        # Restart environmentd
        c.kill("materialized")
        c.up("materialized")

        # Ensure there is no pending replica
        replicas = c.sql_query(
            """
            SELECT mz_cluster_replicas.name
            FROM mz_cluster_replicas, mz_clusters
            WHERE mz_cluster_replicas.cluster_id = mz_clusters.id
            AND mz_clusters.name='cluster1';
            """
        )
        assert replicas == [
            ("r1",)
        ], f"Expected one non pending replica, found {replicas}"

        # Ensure the cluster config did not change
        assert (
            c.sql_query(
                """
            SELECT size FROM mz_clusters WHERE name='cluster1';
            """
            )
            == [("1",)]
        )
        c.sql(
            """
            ALTER SYSTEM RESET enable_graceful_cluster_reconfiguration;
            """,
            port=6877,
            user="mz_system",
        )


def workflow_crash_on_replica_expiration_mv(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Tests that clusterd crashes when a replica is set to expire
    """
    c.down(destroy_volumes=True)
    with c.override(
        Clusterd(name="clusterd1", restart="on-failure"),
    ):
        offset = 20

        c.up("materialized")
        c.up("clusterd1")
        c.sql(
            f"""
            ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = 'true';
            ALTER SYSTEM SET compute_replica_expiration_offset = '{offset}s';

            DROP CLUSTER IF EXISTS test CASCADE;
            DROP TABLE IF EXISTS t CASCADE;

            CREATE CLUSTER test REPLICAS (
                test (
                    STORAGECTL ADDRESSES ['clusterd1:2100'],
                    STORAGE ADDRESSES ['clusterd1:2103'],
                    COMPUTECTL ADDRESSES ['clusterd1:2101'],
                    COMPUTE ADDRESSES ['clusterd1:2102'],
                    WORKERS 1
                )
            );
            SET CLUSTER TO test;

            CREATE TABLE t (x int);
            INSERT INTO t VALUES (42);
            CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE x < 84;
            """,
            port=6877,
            user="mz_system",
        )

        c.sleep(offset + 10)

        results = c.sql_query(
            """
            SELECT * from mv;
            """,
            port=6877,
            user="mz_system",
        )
        assert results == [(42,)], f"Results mismatch: expected [42], found {results}"

        c1 = c.invoke("logs", "clusterd1", capture=True)
        assert (
            "replica expired" in c1.stdout
        ), "unexpected success in crash-on-replica-expiration"


def workflow_crash_on_replica_expiration_index(
    c: Composition, parser: WorkflowArgumentParser
) -> None:

    def fetch_metrics() -> Metrics:
        resp = c.exec(
            "clusterd1", "curl", "localhost:6878/metrics", capture=True
        ).stdout
        return Metrics(resp)

    """
    Tests that clusterd crashes when a replica is set to expire
    """
    c.down(destroy_volumes=True)
    with c.override(
        Clusterd(name="clusterd1", restart="on-failure"),
    ):
        offset = 20

        c.up("materialized")
        c.up("clusterd1")
        c.sql(
            f"""
            ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = 'true';
            ALTER SYSTEM SET compute_replica_expiration_offset = '{offset}s';

            DROP CLUSTER IF EXISTS test CASCADE;
            DROP TABLE IF EXISTS t CASCADE;

            CREATE CLUSTER test REPLICAS (
                test (
                    STORAGECTL ADDRESSES ['clusterd1:2100'],
                    STORAGE ADDRESSES ['clusterd1:2103'],
                    COMPUTECTL ADDRESSES ['clusterd1:2101'],
                    COMPUTE ADDRESSES ['clusterd1:2102'],
                    WORKERS 1
                )
            );
            SET CLUSTER TO test;

            CREATE TABLE t (x int);
            INSERT INTO t VALUES (42);
            CREATE VIEW mv AS SELECT * FROM t WHERE x < 84;
            CREATE DEFAULT INDEX ON mv;
            """,
            port=6877,
            user="mz_system",
        )

        c.sleep(offset + 10)

        results = c.sql_query(
            """
            SELECT * from mv;
            """,
            port=6877,
            user="mz_system",
        )
        assert results == [(42,)], f"Results mismatch: expected [42], found {results}"

        c1 = c.invoke("logs", "clusterd1", capture=True)
        assert (
            "replica expired" in c1.stdout
        ), "unexpected success in crash-on-replica-expiration"

        # Wait a bit to let the controller refresh its metrics.
        time.sleep(2)

        # Check that expected metrics exist and have sensible values.
        metrics = fetch_metrics()

        expected_expiration_timestamp_sec = int(time.time())

        expiration_timestamp_sec = (
            metrics.get_value("mz_dataflow_replica_expiration_timestamp_seconds") / 1000
        )
        # Just ensure the expiration_timestamp is within a reasonable range of now().
        assert (
            (expected_expiration_timestamp_sec - offset)
            < expiration_timestamp_sec
            < (expected_expiration_timestamp_sec + offset)
        ), f"expiration_timestamp: expected={expected_expiration_timestamp_sec}[{datetime.fromtimestamp(expected_expiration_timestamp_sec)}], got={expiration_timestamp_sec}[{[{datetime.fromtimestamp(expiration_timestamp_sec)}]}]"

        expiration_remaining = metrics.get_value(
            "mz_dataflow_replica_expiration_remaining_seconds"
        )
        # Ensure the expiration_remaining is within the configured offset.
        offset = float(offset)
        assert (
            expiration_remaining < offset
        ), f"expiration_remaining: expected < 10s, got={expiration_remaining}"


def workflow_replica_expiration_creates_retraction_diffs_after_panic(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Test that retraction diffs within the expiration time are generated after the replica expires and panics
    """
    c.down(destroy_volumes=True)
    with c.override(
        Testdrive(no_reset=True),
        Clusterd(name="clusterd1", restart="on-failure"),
    ):

        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.up("clusterd1")
        c.testdrive(
            dedent(
                """
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = 'true';
            ALTER SYSTEM SET compute_replica_expiration_offset = '50s';

            > DROP CLUSTER IF EXISTS test CASCADE;
            > DROP TABLE IF EXISTS events CASCADE;

            > CREATE CLUSTER test REPLICAS (
                test (
                    STORAGECTL ADDRESSES ['clusterd1:2100'],
                    STORAGE ADDRESSES ['clusterd1:2103'],
                    COMPUTECTL ADDRESSES ['clusterd1:2101'],
                    COMPUTE ADDRESSES ['clusterd1:2102'],
                    WORKERS 1
                )
              );
            > SET CLUSTER TO test;

            > CREATE TABLE events (
              content TEXT,
              event_ts TIMESTAMP
              );

            > CREATE VIEW events_view AS
              SELECT event_ts, content
              FROM events
              WHERE mz_now() <= event_ts + INTERVAL '80s';

            > CREATE DEFAULT INDEX ON events_view;

            > INSERT INTO events SELECT x::text, now() FROM generate_series(1, 1000) AS x;

            # Retraction diffs are not generated
            > SELECT records FROM mz_introspection.mz_dataflow_arrangement_sizes
              WHERE name LIKE '%events_view_primary_idx';
            1000
            # Sleep until the replica expires
            $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="60s"
            # Retraction diffs are now within the expiration time and should be generated
            > SELECT records FROM mz_introspection.mz_dataflow_arrangement_sizes
              WHERE name LIKE '%events_view_primary_idx';
            2000
            > DROP TABLE events CASCADE;
            > DROP CLUSTER test CASCADE;
            """
            )
        )


def workflow_test_constant_sink(c: Composition) -> None:
    """
    Test how we handle constant sinks.

    This test reflects the current behavior, though not the desired behavior,
    as described in database-issues#8842. Once we fix that issue, this can
    become a regression test.
    """

    c.down(destroy_volumes=True)

    with c.override(Testdrive(no_reset=True)):
        c.up("testdrive", persistent=True)
        c.up("materialized", "zookeeper", "kafka", "schema-registry")

        c.testdrive(
            dedent(
                """
                > CREATE CLUSTER test SIZE '1';

                > CREATE MATERIALIZED VIEW const IN CLUSTER test AS SELECT 1

                > CREATE CONNECTION kafka_conn
                  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

                > CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
                    URL '${testdrive.schema-registry-url}'
                  )

                > CREATE SINK snk
                  IN CLUSTER test
                  FROM const
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-snk-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM

                > SELECT write_frontier
                  FROM mz_internal.mz_frontiers
                  JOIN mz_sinks ON id = object_id
                  WHERE name = 'snk'

                > SELECT status
                  FROM mz_internal.mz_sink_statuses
                  WHERE name = 'snk'
                dropped
                """
            )
        )

        c.kill("materialized")
        c.up("materialized")

        c.testdrive(
            dedent(
                """
                > SELECT write_frontier
                  FROM mz_internal.mz_frontiers
                  JOIN mz_sinks ON id = object_id
                  WHERE name = 'snk'

                > SELECT status
                  FROM mz_internal.mz_sink_statuses
                  WHERE name = 'snk'
                dropped
                """
            )
        )
