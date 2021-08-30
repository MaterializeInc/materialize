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

prerequisites = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
]

mz_standard = Materialized(
    name="mz_standard",
    hostname="materialized",
)

mz_disable_user_indexes = Materialized(
    name="mz_disable_user_indexes",
    hostname="materialized",
    options="--disable-user-indexes",
)

services = [
    *prerequisites,
    mz_standard,
    mz_disable_user_indexes,
    Testdrive(no_reset=True),
]


def workflow_disable_user_indexes(w: Workflow):
    w.start_and_wait_for_tcp(services=prerequisites)

    # Create catalog with vanilla MZ
    w.start_services(services=["mz_standard"])

    w.wait_for_mz(service="mz_standard")

    w.run_service(service="testdrive-svc", command="user-indexes-enabled.td")

    w.kill_services(services=["mz_standard"])

    # Test semantics of disabling user indexes
    w.start_services(services=["mz_disable_user_indexes"])

    w.wait_for_mz(service="mz_disable_user_indexes")

    w.run_service(service="testdrive-svc", command="user-indexes-disabled.td")

    w.kill_services(services=["mz_disable_user_indexes"])
