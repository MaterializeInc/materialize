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

mz_with_options = [
    Materialized(name="mz_2_workers", hostname="materialized", options="--workers 2"),
    Materialized(name="mz_4_workers", hostname="materialized", options="--workers 4"),
]

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    *versioned_mz,
    *mz_with_options,
    Testdrive(),
]


def workflow_default(c: Composition) -> None:
    """All mzcompose files should contain a default workflow

    This workflow just runs all the other ones
    """
    c.workflow("start-confluents")
    c.workflow("versioned-mz")
    c.workflow("two-mz")
    c.workflow("mz-with-options")


def workflow_start_confluents(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])


def workflow_versioned_mz(c: Composition) -> None:
    for mz in versioned_mz:
        c.up(mz.name)

        c.wait_for_materialized(mz.name)

        c.run("testdrive", "test*.td")

        c.kill(mz.name)


def workflow_mz_with_options(c: Composition) -> None:
    c.up("mz_2_workers")
    c.wait_for_materialized("mz_2_workers")
    c.kill("mz_2_workers")

    c.up("mz_4_workers")
    c.wait_for_materialized("mz_4_workers")
    c.kill("mz_4_workers")
