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
    Coordd,
    Dataflowd,
    Kafka,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Dataflowd(
        name="dataflowd_1",
        options="--workers 2 --processes 2 --process 0 dataflowd_1:2101 dataflowd_2:2101",
        hostname="dataflowd_1",
        ports=[6876, 2101],
    ),
    Dataflowd(
        name="dataflowd_2",
        options="--workers 2 --processes 2 --process 1 dataflowd_1:2101 dataflowd_2:2101",
        hostname="dataflowd_2",
        ports=[6876, 2101],
    ),
    Coordd(
        name="materialized",
        options="--workers 4 dataflowd_1:6876 dataflowd_2:6876",
    ),
    Dataflowd(
        name="dataflowd_compute_1",
        options="--workers 2 --storage-workers 2 --processes 2 --process 0 dataflowd_compute_1:2101 dataflowd_compute_2:2101 --storage-addr dataflowd_storage:2102 --runtime compute",
        # options="--workers 2 --storage-workers 2 --storage-addr dataflowd_storage:2102 --runtime compute",
        hostname="dataflowd_compute_1",
        ports=[6876, 2101],
    ),
    Dataflowd(
        name="dataflowd_compute_2",
        options="--workers 2 --storage-workers 2 --processes 2 --process 1 dataflowd_compute_1:2101 dataflowd_compute_2:2101 --storage-addr dataflowd_storage:2102 --runtime compute",
        # options="--workers 2 --storage-workers 2 --storage-addr dataflowd_storage:2102 --runtime compute",
        hostname="dataflowd_compute_2",
        ports=[6876, 2101],
    ),
    Dataflowd(
        name="dataflowd_storage",
        options="--workers 2 --storage-addr dataflowd_storage:2102 --runtime storage",
        hostname="dataflowd_storage",
        ports=[6876, 2102],
    ),
    Coordd(
        name="materialized_compute_storage",
        options="--workers 4 dataflowd_compute_1:6876 dataflowd_compute_2:6876 --storaged-addr dataflowd_storage:6876",
    ),
    Testdrive(
        volumes=[
            "mzdata:/share/mzdata",
            "tmp:/share/tmp",
            ".:/workdir/smoke",
            "../testdrive:/workdir/testdrive",
        ],
    ),
    Testdrive(
        name="testdrive-svc-compute-storage",
        materialized_url="postgres://materialize@materialized_compute_storage:6875",
        volumes=[
            "mzdata:/share/mzdata",
            "tmp:/share/tmp",
            ".:/workdir/smoke",
            "../testdrive:/workdir/testdrive",
        ],
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
    c.up("dataflowd_1", "dataflowd_2")
    c.up("materialized")
    c.wait_for_materialized()
    c.run("testdrive-svc", *glob)


def workflow_compute_storage_smoke(c: Composition) -> None:
    test_cluster_compute_storage(c, "smoke/*.td")


def workflow_nightly_compute_storage(c: Composition) -> None:
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
    test_cluster_compute_storage(c, *files)


def test_cluster_compute_storage(c: Composition, *glob: str) -> None:
    c.up("dataflowd_storage")
    c.up("dataflowd_compute_1")
    c.up("dataflowd_compute_2")
    c.up("materialized_compute_storage")
    c.wait_for_materialized(service="materialized_compute_storage")
    c.run("testdrive-svc-compute-storage", *glob)
