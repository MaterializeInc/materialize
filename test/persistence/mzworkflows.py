# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

materialized = Materialized(
    options="--persistent-user-tables --persistent-kafka-upsert-source --disable-persistent-system-tables-test"
)

mz_disable_user_indexes = Materialized(
    name="mz_disable_user_indexes",
    hostname="materialized",
    options="--persistent-user-tables --persistent-kafka-upsert-source --disable-persistent-system-tables-test --disable-user-indexes",
)

# This instance of Mz is used for failpoint testing. By using --disable-persistent-system-tables-test
# we ensure that only testdrive-initiated actions cause I/O. The --workers 1 is used due to #8739

mz_without_system_tables = Materialized(
    name="mz_without_system_tables",
    hostname="materialized",
    options="--persistent-user-tables --disable-persistent-system-tables-test --workers 1",
)

prerequisites = ["zookeeper", "kafka", "schema-registry"]

services = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    materialized,
    mz_disable_user_indexes,
    mz_without_system_tables,
    Testdrive(no_reset=True, consistent_seed=True),
]

td_test = os.environ.pop("TD_TEST", "*")


def workflow_persistence(c: Composition) -> None:
    workflow_kafka_sources(c)
    workflow_user_tables(c)
    workflow_failpoints(c)
    workflow_disable_user_indexes(c)


def workflow_kafka_sources(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)

    c.start_services(services=["materialized"])
    c.wait_for_mz(service="materialized")

    c.run_service(
        service="testdrive-svc",
        command=f"kafka-sources/*{td_test}*-before.td",
    )

    c.kill_services(services=["materialized"], signal="SIGKILL")
    c.start_services(services=["materialized"])
    c.wait_for_mz(service="materialized")

    # And restart again, for extra stress
    c.kill_services(services=["materialized"], signal="SIGKILL")
    c.start_services(services=["materialized"])
    c.wait_for_mz(service="materialized")

    c.run_service(
        service="testdrive-svc",
        command=f"kafka-sources/*{td_test}*-after.td",
    )

    # Do one more restart, just in case and just confirm that Mz is able to come up
    c.kill_services(services=["materialized"], signal="SIGKILL")
    c.start_services(services=["materialized"])
    c.wait_for_mz(service="materialized")

    c.kill_services(services=["materialized"], signal="SIGKILL")
    c.remove_services(services=["materialized", "testdrive-svc"], destroy_volumes=True)
    c.remove_volumes(volumes=["mzdata"])


def workflow_user_tables(c: Composition) -> None:
    c.start_services(services=["materialized"])
    c.wait_for_mz(service="materialized")

    c.run_service(
        service="testdrive-svc",
        command=f"user-tables/table-persistence-before-{td_test}.td",
    )

    c.kill_services(services=["materialized"], signal="SIGKILL")
    c.start_services(services=["materialized"])

    c.run_service(
        service="testdrive-svc",
        command=f"user-tables/table-persistence-after-{td_test}.td",
    )

    c.kill_services(services=["materialized"], signal="SIGKILL")
    c.remove_services(services=["materialized", "testdrive-svc"], destroy_volumes=True)
    c.remove_volumes(volumes=["mzdata"])


def workflow_failpoints(c: Composition) -> None:
    c.start_services(services=["mz_without_system_tables"])
    c.wait_for_mz(service="mz_without_system_tables")

    c.run_service(service="testdrive-svc", command=f"failpoints/{td_test}.td")

    c.kill_services(services=["mz_without_system_tables"], signal="SIGKILL")
    c.remove_services(
        services=["mz_without_system_tables", "testdrive-svc"], destroy_volumes=True
    )
    c.remove_volumes(volumes=["mzdata"])


def workflow_disable_user_indexes(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)

    c.start_services(services=["materialized"])
    c.wait_for_mz(service="materialized")

    c.run_service(
        service="testdrive-svc",
        command="disable-user-indexes/before.td",
    )

    c.kill_services(services=["materialized"], signal="SIGKILL")
    c.start_services(services=["mz_disable_user_indexes"])
    c.wait_for_mz(service="mz_disable_user_indexes")

    c.run_service(
        service="testdrive-svc",
        command="disable-user-indexes/after.td",
    )

    c.kill_services(services=["mz_disable_user_indexes"], signal="SIGKILL")
    c.remove_services(
        services=["materialized", "mz_disable_user_indexes", "testdrive-svc"],
        destroy_volumes=True,
    )
    c.remove_volumes(volumes=["mzdata"])
