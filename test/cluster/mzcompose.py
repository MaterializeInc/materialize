# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from textwrap import dedent

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
    Computed(
        name="computed_1",
        options="--workers 2 --process 0 computed_1:2102 computed_2:2102 ",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_2",
        options="--workers 2 --process 1 computed_1:2102 computed_2:2102",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_3",
        options="--workers 2 --process 0 computed_3:2102 computed_4:2102",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_4",
        options="--workers 2 --process 1 computed_3:2102 computed_4:2102",
        ports=[2100, 2102],
    ),
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
        "test-github-13603",
        "test-remote-storaged",
        "test-drop-default-cluster",
        "test-upsert",
        "test-resource-limits",
        # Disabled to permit a breaking change.
        # See: https://materializeinc.slack.com/archives/C02FWJ94HME/p1661288774456699?thread_ts=1661288684.301649&cid=C02FWJ94HME
        # "test-builtin-migration",
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
        "CREATE CLUSTER cluster1 REPLICAS (replica1 (REMOTE ['computed_1:2100', 'computed_2:2100']));"
    )
    c.run("testdrive", *args.glob)

    # Add a replica to that remote cluster and verify that tests still pass.
    c.up("computed_3")
    c.up("computed_4")
    c.sql(
        "CREATE CLUSTER REPLICA cluster1.replica2 REMOTE ['computed_3:2100', 'computed_4:2100']"
    )
    c.run("testdrive", *args.glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.kill("computed_1")
    c.run("testdrive", *args.glob)

    # Leave only replica 2 up and verify that tests still pass.
    c.sql("DROP CLUSTER REPLICA cluster1.replica1")
    c.run("testdrive", *args.glob)


def workflow_test_github_13603(c: Composition) -> None:
    """Test that multi woker replicas terminate eagerly upon rehydration"""
    c.down(destroy_volumes=True)
    c.up("materialized")
    c.wait_for_materialized()

    c.up("computed_1")
    c.up("computed_2")
    c.sql(
        "CREATE CLUSTER cluster1 REPLICAS (replica1 (REMOTE ['computed_1:2100', 'computed_2:2100']));"
    )

    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized()

    # Ensure the computeds have crashed
    c1 = c.invoke("logs", "computed_1", capture=True)
    assert "panicked" in c1.stdout
    c2 = c.invoke("logs", "computed_2", capture=True)
    assert "panicked" in c2.stdout


def workflow_test_github_12251(c: Composition) -> None:
    """Test that clients do not wait indefinitely for a crashed resource."""

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.wait_for_materialized()
    c.up("computed_1")
    c.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        CREATE CLUSTER cluster1 REPLICAS (replica1 (REMOTE ['computed_1:2100']));
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
            "redpanda",
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
        Materialized(),
    ):
        c.up("materialized")
        c.wait_for_materialized()

        c.run("testdrive", "resources/resource-limits.td")


# TODO: Would be nice to update this test to use a builtin table that can be materialized.
#  pg_roles, and most postgres catalog views, cannot be materialized because they use
#  pg_catalog.current_database(). So we can't test making indexes and materialized views.
def workflow_test_builtin_migration(c: Composition) -> None:
    """Exercise the builtin object migration code by upgrading between two versions
    that will have a migration triggered between them. Create a materialized view
    over the affected builtin object to confirm that the migration was successful
    """

    c.down(destroy_volumes=True)
    with c.override(
        # Random commit before pg_roles was updated.
        Materialized(
            image="materialize/materialized:devel-9efd269199b1510b3e8f90196cb4fa3072a548a1",
        ),
        Testdrive(default_timeout="15s", no_reset=True, consistent_seed=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.wait_for_materialized()

        c.testdrive(
            input=dedent(
                """
        > CREATE VIEW v1 AS SELECT COUNT(*) FROM pg_roles;
        > SELECT * FROM v1;
        2
        ! SELECT DISTINCT rolconnlimit FROM pg_roles;
        contains:column "rolconnlimit" does not exist
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
       2
       # This column is new after the migration
       > SELECT DISTINCT rolconnlimit FROM pg_roles;
       -1
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

        # storaged should crash
        c.run("testdrive", "pg-snapshot-resumption/03-while-storaged-down.td")

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
