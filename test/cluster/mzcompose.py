# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from materialize import spawn
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
    Computed(
        name="computed_1",
        options="--workers 2 --processes 2 --process 0 computed_1:2102 computed_2:2102 --storage-addr materialized:2101",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_2",
        options="--workers 2 --processes 2 --process 1 computed_1:2102 computed_2:2102 --storage-addr materialized:2101",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_3",
        options="--workers 2 --processes 2 --process 0 computed_3:2102 computed_4:2102 --storage-addr materialized:2101 --linger --reconcile",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_4",
        options="--workers 2 --processes 2 --process 1 computed_3:2102 computed_4:2102 --storage-addr materialized:2101 --linger --reconcile",
        ports=[2100, 2102],
    ),
    Materialized(extra_ports=[2101]),
    Testdrive(
        volumes=[
            "mzdata:/share/mzdata",
            "tmp:/share/tmp",
            ".:/workdir/smoke",
            "../testdrive:/workdir/testdrive",
        ],
        materialized_params={"cluster": "cluster1"},
    ),
]


def workflow_default(c: Composition) -> None:
    test_cluster(c, "smoke/*.td")


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
    c.wait_for_materialized(service="materialized")

    # Create a remote cluster and verify that tests pass.
    c.up("computed_1")
    c.up("computed_2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        "CREATE CLUSTER cluster1 REPLICA replica1 (REMOTE ('computed_1:2100', 'computed_2:2100'));"
    )
    c.run("testdrive", *glob)

    # Add a replica to that remote cluster and verify that tests still pass.
    c.up("computed_3")
    c.up("computed_4")
    c.sql(
        "CREATE CLUSTER REPLICA cluster1.replica2 REMOTE ('computed_3:2100', 'computed_4:2100')"
    )
    c.run("testdrive", *glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.kill("computed_1")
    c.run("testdrive", *glob)

    # Leave only replica 2 up and verify that tests still pass.
    c.sql("DROP CLUSTER REPLICA cluster1.replica1")
    c.run("testdrive", *glob)
