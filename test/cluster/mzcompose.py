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
    Dataflowd,
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Dataflowd(
        name="dataflowd_compute_1",
        options="--workers 2 --processes 2 --process 0 dataflowd_compute_1:2102 dataflowd_compute_2:2102 --storage-addr dataflowd_storage:2101 --runtime compute",
        ports=[2100, 2102],
    ),
    Dataflowd(
        name="dataflowd_compute_2",
        options="--workers 2 --processes 2 --process 1 dataflowd_compute_1:2102 dataflowd_compute_2:2102 --storage-addr dataflowd_storage:2101 --runtime compute",
        ports=[2100, 2102],
    ),
    Dataflowd(
        name="dataflowd_compute_3",
        options="--workers 2 --processes 2 --process 0 dataflowd_compute_3:2102 dataflowd_compute_4:2102 --storage-addr dataflowd_storage:2101 --runtime compute --linger --reconcile",
        ports=[2100, 2102],
    ),
    Dataflowd(
        name="dataflowd_compute_4",
        options="--workers 2 --processes 2 --process 1 dataflowd_compute_3:2102 dataflowd_compute_4:2102 --storage-addr dataflowd_storage:2101 --runtime compute --linger --reconcile",
        ports=[2100, 2102],
    ),
    Dataflowd(
        name="dataflowd_storage",
        options="--workers 2 --storage-addr dataflowd_storage:2101 --runtime storage",
        ports=[2100, 2101],
    ),
    Materialized(
        options="--storage-compute-addr=dataflowd_storage:2101 --storage-controller-addr=dataflowd_storage:2100",
    ),
    Testdrive(
        volumes=[
            "mzdata:/share/mzdata",
            "tmp:/share/tmp",
            ".:/workdir/smoke",
            "../testdrive:/workdir/testdrive",
        ],
        materialized_params={"cluster": "c"},
    ),
]


def workflow_default(c: Composition) -> None:
    test_cluster(c, "smoke/*.td")


def workflow_nightly(c: Composition) -> None:
    """Run cluster testdrive"""
    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])
    # Skip tests that use features that are not supported yet.
    files = spawn.capture(
        [
            "sh",
            "-c",
            "grep -rLE 'mz_catalog|mz_kafka_|mz_records_|mz_metrics' testdrive/*.td",
        ],
        cwd=Path(__file__).parent.parent,
    ).split()
    test_cluster(c, *files)


def test_cluster(c: Composition, *glob: str) -> None:
    c.up("dataflowd_storage")
    c.up("materialized")
    c.wait_for_materialized(service="materialized")

    # Create a remote cluster and verify that tests pass.
    c.up("dataflowd_compute_1")
    c.up("dataflowd_compute_2")
    c.sql(
        "CREATE CLUSTER c REMOTE replica1 ('dataflowd_compute_1:2100', 'dataflowd_compute_2:2100');"
    )
    c.run("testdrive", *glob)

    # Add a replica to that remote cluster and verify that tests still pass.
    c.up("dataflowd_compute_3")
    c.up("dataflowd_compute_4")
    c.sql(
        """
        ALTER CLUSTER c
            REMOTE replica1 ('dataflowd_compute_1:2100', 'dataflowd_compute_2:2100'),
            REMOTE replica2 ('dataflowd_compute_3:2100', 'dataflowd_compute_4:2100')
        """
    )
    c.run("testdrive", *glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.kill("dataflowd_compute_1")
    c.run("testdrive", *glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.sql(
        """
        ALTER CLUSTER c
            REMOTE replica1 ('dataflowd_compute_1:2100', 'dataflowd_compute_2:2100')
        """
    )
    c.sql(
        """
        ALTER CLUSTER c
            REMOTE replica2 ('dataflowd_compute_3:2100', 'dataflowd_compute_4:2100')
        """
    )
    c.run("testdrive", "smoke/insert-select.td")
