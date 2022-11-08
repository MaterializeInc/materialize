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
    Computed,
    Kafka,
    Localstack,
    Materialized,
    Postgres,
    Redpanda,
    SchemaRegistry,
    Storaged,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Computed(name="computed_1"),
    Computed(name="computed_2"),
    Computed(name="computed_3"),
    Computed(name="computed_4"),
    Materialized(),
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
    Storaged(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    for name in [
        "test-cluster",
        "test-github-12251",
        "test-github-15535",
        "test-github-15799",
        "test-remote-storaged",
        "test-drop-default-cluster",
        "test-upsert",
        "test-resource-limits",
        "test-invalid-computed-reuse",
        "test-builtin-migration",
        "pg-snapshot-resumption",
    ]:
        with c.test_case(name):
            c.workflow(name)


def workflow_test_cluster(c: Composition, parser: WorkflowArgumentParser) -> None:
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
    c.up("computed_1")
    c.up("computed_2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        """CREATE CLUSTER cluster1 REPLICAS (replica1 (
            REMOTE ['computed_1:2100', 'computed_2:2100'],
            COMPUTE ['computed_1:2102', 'computed_2:2102'],
            WORKERS 2
            ));
    """
    )
    c.run("testdrive", *args.glob)

    # Add a replica to that remote cluster and verify that tests still pass.
    c.up("computed_3")
    c.up("computed_4")
    c.sql(
        """CREATE CLUSTER REPLICA cluster1.replica2
            REMOTE ['computed_3:2100', 'computed_4:2100'],
            COMPUTE ['computed_3:2102', 'computed_4:2102'],
            WORKERS 2
    """
    )
    c.run("testdrive", *args.glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.kill("computed_1")
    c.run("testdrive", *args.glob)

    # Leave only replica 2 up and verify that tests still pass.
    c.sql("DROP CLUSTER REPLICA cluster1.replica1")
    c.run("testdrive", *args.glob)


def workflow_test_invalid_computed_reuse(c: Composition) -> None:
    """Ensure computeds correctly crash if used in unsupported communication config"""
    c.down(destroy_volumes=True)
    c.up("materialized")
    c.wait_for_materialized()

    # Create a remote cluster and verify that tests pass.
    c.up("computed_1")
    c.up("computed_2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        """CREATE CLUSTER cluster1 REPLICAS (replica1 (
            REMOTE ['computed_1:2100', 'computed_2:2100'],
            COMPUTE ['computed_1:2102', 'computed_2:2102'],
            WORKERS 2
            ));
    """
    )
    c.sql("DROP CLUSTER cluster1 CASCADE;")

    # Note the different WORKERS argument
    c.sql(
        """CREATE CLUSTER cluster1 REPLICAS (replica1 (
            REMOTE ['computed_1:2100', 'computed_2:2100'],
            COMPUTE ['computed_1:2102', 'computed_2:2102'],
            WORKERS 1
            ));
    """
    )

    # This should ensure that computed crashed (and does not just hang forever)
    c1 = c.invoke("logs", "computed_1", capture=True)
    assert "panicked" in c1.stdout


def workflow_test_github_12251(c: Composition) -> None:
    """Test that clients do not wait indefinitely for a crashed resource."""

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.wait_for_materialized()
    c.up("computed_1")
    c.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        CREATE CLUSTER cluster1 REPLICAS (replica1 (REMOTE ['computed_1:2100'], COMPUTE ['computed_1:2102'], WORKERS 2));
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


def workflow_test_github_15535(c: Composition) -> None:
    """
    Test that compute reconciliation does not produce empty frontiers.

    Regression test for https://github.com/MaterializeInc/materialize/issues/15535.
    """

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("computed_1")
    c.wait_for_materialized()

    # Set up a dataflow on computed.
    c.sql(
        """
        CREATE CLUSTER cluster1 REPLICAS (replica1 (
            REMOTE ['computed_1:2100'],
            COMPUTE ['computed_1:2102'],
            WORKERS 2
        ));
        SET cluster = cluster1;
        CREATE TABLE t (a int);
        CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;
        -- wait for the dataflow to be ready
        SELECT * FROM mv;
        """
    )

    # Restart environmentd to trigger a reconciliation on computed.
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
    c.up("computed_1")
    c.up("computed_2")
    c.wait_for_materialized()

    c.sql(
        """
        CREATE CLUSTER cluster1 REPLICAS (
            logging_on (
                REMOTE ['computed_1:2100'],
                COMPUTE ['computed_1:2102'],
                WORKERS 2
            ),
            logging_off (
                REMOTE ['computed_2:2100'],
                COMPUTE ['computed_2:2102'],
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
        c.exec("materialized", "bash", "-c", "kill -9 `pidof storaged`")
        c.run("testdrive", "upsert/02-after-storaged-restart.td")


def workflow_test_remote_storaged(c: Composition) -> None:
    """Test creating sources in a remote storaged process."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(default_timeout="15s", no_reset=True, consistent_seed=True),
        # Use a separate PostgreSQL service for persist rather than the one in
        # the `Materialized` service, so that crashing `environmentd` does not
        # also take down PostgreSQL.
        Postgres(),
        Materialized(
            options="--persist-consensus-url=postgres://postgres:postgres@postgres"
        ),
    ):
        dependencies = [
            "materialized",
            "postgres",
            "storaged",
            "zookeeper",
            "kafka",
            "schema-registry",
        ]
        c.start_and_wait_for_tcp(
            services=dependencies,
        )

        c.run("testdrive", "storaged/01-create-sources.td")

        c.kill("materialized")
        c.up("materialized")
        c.run("testdrive", "storaged/02-after-environmentd-restart.td")

        c.kill("storaged")
        c.run("testdrive", "storaged/03-while-storaged-down.td")

        c.up("storaged")
        c.run("testdrive", "storaged/04-after-storaged-restart.td")


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


# TODO: Would be nice to update this test to use a builtin table that can be materialized.
#  pg_proc, and most postgres catalog views, cannot be materialized because they use
#  pg_catalog.current_database(). So we can't test making indexes and materialized views.
def workflow_test_builtin_migration(c: Composition) -> None:
    """Exercise the builtin object migration code by upgrading between two versions
    that will have a migration triggered between them. Create a materialized view
    over the affected builtin object to confirm that the migration was successful
    """

    c.down(destroy_volumes=True)
    with c.override(
        # Random commit before pg_proc was updated.
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
        # The limit is added to avoid having to update the number every time we add a function.
        > CREATE VIEW v1 AS SELECT COUNT(*) FROM (SELECT * FROM pg_proc ORDER BY oid LIMIT 5);
        > SELECT * FROM v1;
        5
        ! SELECT DISTINCT proowner FROM pg_proc;
        contains:column "proowner" does not exist
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
       > SELECT * FROM v1;
       5
       # This column is new after the migration
       > SELECT DISTINCT proowner FROM pg_proc;
       <null>
    """
            )
        )


def workflow_pg_snapshot_resumption(c: Composition) -> None:
    """Test creating sources in a remote storaged process."""

    c.down(destroy_volumes=True)

    with c.override(
        # Start postgres for the pg source
        Postgres(),
        Testdrive(no_reset=True),
        Storaged(environment=["FAILPOINTS=pg_snapshot_failure=return"]),
    ):
        dependencies = [
            "materialized",
            "postgres",
            "storaged",
        ]
        c.start_and_wait_for_tcp(
            services=dependencies,
        )

        c.run("testdrive", "pg-snapshot-resumption/01-configure-postgres.td")
        c.run("testdrive", "pg-snapshot-resumption/02-create-sources.td")

        # Temporarily disabled because it is timing out.
        # https://github.com/MaterializeInc/materialize/issues/14533
        # # storaged should crash
        # c.run("testdrive", "pg-snapshot-resumption/03-while-storaged-down.td")

        print("Sleeping to ensure that storaged crashes")
        time.sleep(10)

        with c.override(
            # turn off the failpoint
            Storaged()
        ):
            c.start_and_wait_for_tcp(
                services=["storaged"],
            )
            c.run("testdrive", "pg-snapshot-resumption/04-verify-data.td")


def workflow_test_bootstrap_vars(c: Composition) -> None:
    """Test default system vars values passed with a CLI option."""

    c.down(destroy_volumes=True)

    with c.override(
        Testdrive(no_reset=True),
        Materialized(
            options="--bootstrap-system-vars=\"allowed_cluster_replica_sizes='1', '2', 'oops'\"",
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
                """ MZ_BOOTSTRAP_SYSTEM_VARS=allowed_cluster_replica_sizes='1', '2', 'oops'""".strip()
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
