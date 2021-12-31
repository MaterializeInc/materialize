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

confluent = []

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


def workflow_disable_user_indexes(w: Workflow):
    w.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])

    # Create catalog with vanilla MZ
    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")
    w.run_service(service="testdrive-svc", command="user-indexes-enabled.td")
    w.kill_services(services=["materialized"])

    # Test semantics of disabling user indexes
    w.start_services(services=["mz_disable_user_indexes"])
    w.wait_for_mz(service="mz_disable_user_indexes")
    w.run_service(service="testdrive_no_reset", command="user-indexes-disabled.td")
    w.kill_services(services=["mz_disable_user_indexes"])


def workflow_github_8021(w: Workflow) -> None:
    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")
    w.run_service(service="testdrive-svc", command="github-8021.td")

    # Ensure MZ can boot
    w.kill_services(services=["materialized"])
    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")
    w.kill_services(services=["materialized"])


def workflow_all_restart(w: Workflow) -> None:
    workflow_disable_user_indexes(w)
    workflow_github_8021(w)
