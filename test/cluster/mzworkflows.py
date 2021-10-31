# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import (
    Coordd,
    Dataflowd,
    Kafka,
    SchemaRegistry,
    Testdrive,
    Workflow,
    Zookeeper,
)

prerequisites = [Zookeeper(), Kafka(), SchemaRegistry()]

mz_daemons = [
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
]

services = [
    *prerequisites,
    *mz_daemons,
    Testdrive(
        shell_eval=True,
        volumes=[
            "mzdata:/share/mzdata",
            "tmp:/share/tmp",
            ".:/workdir/smoke",
            "../testdrive:/workdir/testdrive",
        ],
    ),
]


def workflow_cluster_smoke(w: Workflow):
    test_cluster(w, "ls -1 smoke/*.td")


def workflow_cluster_testdrive(w: Workflow):
    w.start_and_wait_for_tcp(services=prerequisites)
    # Skip tests that use features that are not supported yet
    test_cluster(
        w, "grep -L -E 'mz_catalog|mz_kafka_|mz_records_|mz_metrics' testdrive/*.td"
    )


def test_cluster(w: Workflow, glob: str):
    w.start_services(services=["dataflowd_1", "dataflowd_2"])
    w.start_services(services=["materialized"])
    w.wait_for_mz()
    w.run_service(service="testdrive-svc", command=glob)
