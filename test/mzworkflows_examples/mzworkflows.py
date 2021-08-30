# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Workflow,
    Zookeeper,
)

confluents = [Zookeeper(), Kafka(), SchemaRegistry()]

versioned_mz = [
    Materialized(
        name=f"materialized_{version}",
        image=f"materialize/materialized:{version}",
        hostname="materialized",
    )
    for version in ["v0.7.0", "v0.8.0"]
]

multiple_mz = [
    Materialized(
        name=f"materialized{i}", data_directory=f"/share/materialized{i}", port=6875 + i
    )
    for i in [1, 2]
]

mz_with_options = [
    Materialized(name="mz_2_workers", hostname="materialized", options="--workers 2"),
    Materialized(name="mz_4_workers", hostname="materialized", options="--workers 4"),
]

services = [
    *confluents,
    *versioned_mz,
    *multiple_mz,
    *mz_with_options,
    Testdrive(),
]


def workflow_start_confluents(w: Workflow):
    w.start_and_wait_for_tcp(services=confluents)


def workflow_versioned_mz(w: Workflow):
    for mz in versioned_mz:
        w.start_services(services=[mz.name])

        w.wait_for_mz(service=mz.name)

        w.run_service(service="testdrive-svc", command="test*.td")

        w.kill_services(services=[mz.name])


def workflow_two_mz(w: Workflow):
    for mz in multiple_mz:
        w.start_services(services=[mz.name])
        w.wait_for_mz(service=mz.name)


def workflow_mz_with_options(w: Workflow):
    w.start_services(services=["mz_2_workers"])
    w.wait_for_mz(service="mz_2_workers")
    w.kill_services(services=["mz_2_workers"])

    w.start_services(services=["mz_4_workers"])
    w.wait_for_mz(service="mz_4_workers")
    w.kill_services(services=["mz_4_workers"])
