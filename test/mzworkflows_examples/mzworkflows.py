# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

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
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    *versioned_mz,
    *multiple_mz,
    *mz_with_options,
    Testdrive(),
]


def workflow_start_confluents(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])


def workflow_versioned_mz(c: Composition) -> None:
    for mz in versioned_mz:
        c.start_services(services=[mz.name])

        c.wait_for_mz(service=mz.name)

        c.run_service(service="testdrive-svc", command="test*.td")

        c.kill_services(services=[mz.name])


def workflow_two_mz(c: Composition) -> None:
    for mz in multiple_mz:
        c.start_services(services=[mz.name])
        c.wait_for_mz(service=mz.name)


def workflow_mz_with_options(c: Composition) -> None:
    c.start_services(services=["mz_2_workers"])
    c.wait_for_mz(service="mz_2_workers")
    c.kill_services(services=["mz_2_workers"])

    c.start_services(services=["mz_4_workers"])
    c.wait_for_mz(service="mz_4_workers")
    c.kill_services(services=["mz_4_workers"])
