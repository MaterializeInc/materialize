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

mz_disable_user_indexes = Materialized(
    name="mz_disable_user_indexes",
    hostname="materialized",
    options="--disable-user-indexes",
)

testdrive_no_reset = Testdrive(name="testdrive_no_reset", no_reset=True)

services = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Materialized(),
    mz_disable_user_indexes,
    Testdrive(),
    testdrive_no_reset,
]


def workflow_disable_user_indexes(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])

    # Create catalog with vanilla MZ
    c.start_services(services=["materialized"])
    c.wait_for_mz(service="materialized")
    c.run_service(service="testdrive-svc", command="user-indexes-enabled.td")
    c.kill_services(services=["materialized"])

    # Test semantics of disabling user indexes
    c.start_services(services=["mz_disable_user_indexes"])
    c.wait_for_mz(service="mz_disable_user_indexes")
    c.run_service(service="testdrive_no_reset", command="user-indexes-disabled.td")
    c.kill_services(services=["mz_disable_user_indexes"])


def workflow_github_8021(c: Composition) -> None:
    c.start_services(services=["materialized"])
    c.wait_for_mz(service="materialized")
    c.run_service(service="testdrive-svc", command="github-8021.td")

    # Ensure MZ can boot
    c.kill_services(services=["materialized"])
    c.start_services(services=["materialized"])
    c.wait_for_mz(service="materialized")
    c.kill_services(services=["materialized"])


def workflow_all_restart(c: Composition) -> None:
    workflow_disable_user_indexes(c)
    workflow_github_8021(c)
