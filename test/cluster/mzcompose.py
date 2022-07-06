# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from pathlib import Path

from pg8000.dbapi import ProgrammingError

from materialize import spawn
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
        options="--workers 2 --process 0 computed_1:2102 computed_2:2102",
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
    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )
    args = parser.parse_args()

    # remote storaged tests
    test_remote_storaged(c, args.redpanda)
    c.down(destroy_volumes=True)

    # remote cluster tests
    test_cluster(c, "smoke/*.td")
    test_github_12251(c)


def workflow_nightly(c: Composition) -> None:
    """Run cluster testdrive"""
    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "localstack"]
    )
    # Skip tests that use features that are not supported yet.
    files = spawn.capture(
        [
            "sh",
            "-c",
            "grep -rLE 'mz_catalog|mz_records_' testdrive/*.td",
        ],
        cwd=Path(__file__).parent.parent,
    ).split()
    test_cluster(c, *files)


def test_cluster(c: Composition, *glob: str) -> None:
    c.up("materialized")
    c.wait_for_materialized()

    # Create a remote cluster and verify that tests pass.
    c.up("computed_1")
    c.up("computed_2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        "CREATE CLUSTER cluster1 REPLICAS (replica1 (REMOTE ['computed_1:2100', 'computed_2:2100']));"
    )
    c.run("testdrive", *glob)

    # Add a replica to that remote cluster and verify that tests still pass.
    c.up("computed_3")
    c.up("computed_4")
    c.sql(
        "CREATE CLUSTER REPLICA cluster1.replica2 REMOTE ['computed_3:2100', 'computed_4:2100']"
    )
    c.run("testdrive", *glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.kill("computed_1")
    c.run("testdrive", *glob)

    # Leave only replica 2 up and verify that tests still pass.
    c.sql("DROP CLUSTER REPLICA cluster1.replica1")
    c.run("testdrive", *glob)


# This tests that the client does not wait indefinitely on a resource that crashed
def test_github_12251(c: Composition) -> None:
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


def test_remote_storaged(c: Composition, redpanda: bool) -> None:
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
        dependencies = ["materialized", "postgres", "storaged"]
        if redpanda:
            dependencies += ["redpanda"]
        else:
            dependencies += ["zookeeper", "kafka", "schema-registry"]
        c.start_and_wait_for_tcp(
            services=dependencies,
        )
        c.run("testdrive", "storaged/01-create-sources.td")
        c.kill("materialized")
        c.up("materialized")
        c.run("testdrive", "storaged/02-after-environmentd-restart.td")
