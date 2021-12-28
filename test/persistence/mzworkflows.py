# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize.mzcompose import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Workflow,
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

prerequisites = [Zookeeper(), Kafka(), SchemaRegistry()]
services = [
    *prerequisites,
    materialized,
    mz_disable_user_indexes,
    mz_without_system_tables,
    Testdrive(no_reset=True, seed=1),
]

td_test = os.environ.pop("TD_TEST", "*")


def workflow_persistence(w: Workflow):
    workflow_kafka_sources(w)
    workflow_user_tables(w)
    workflow_failpoints(w)
    workflow_disable_user_indexes(w)


def workflow_kafka_sources(w: Workflow):
    w.start_and_wait_for_tcp(services=prerequisites, timeout_secs=240)

    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")

    w.run_service(
        service="testdrive-svc",
        command=f"kafka-sources/*{td_test}*-before.td",
    )

    w.kill_services(services=["materialized"], signal="SIGKILL")
    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")

    # And restart again, for extra stress
    w.kill_services(services=["materialized"], signal="SIGKILL")
    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")

    w.run_service(
        service="testdrive-svc",
        command=f"kafka-sources/*{td_test}*-after.td",
    )

    # Do one more restart, just in case and just confirm that Mz is able to come up
    w.kill_services(services=["materialized"], signal="SIGKILL")
    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")

    w.kill_services(services=["materialized"], signal="SIGKILL")
    w.remove_services(services=["materialized", "testdrive-svc"], destroy_volumes=True)
    w.remove_volumes(volumes=["mzdata"])


def workflow_user_tables(w: Workflow):
    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")

    w.run_service(
        service="testdrive-svc",
        command=f"user-tables/table-persistence-before-{td_test}.td",
    )

    w.kill_services(services=["materialized"], signal="SIGKILL")
    w.start_services(services=["materialized"])

    w.run_service(
        service="testdrive-svc",
        command=f"user-tables/table-persistence-after-{td_test}.td",
    )

    w.kill_services(services=["materialized"], signal="SIGKILL")
    w.remove_services(services=["materialized", "testdrive-svc"], destroy_volumes=True)
    w.remove_volumes(volumes=["mzdata"])


def workflow_failpoints(w: Workflow):
    w.start_services(services=["mz_without_system_tables"])
    w.wait_for_mz(service="mz_without_system_tables")

    w.run_service(service="testdrive-svc", command=f"failpoints/{td_test}.td")

    w.kill_services(services=["mz_without_system_tables"], signal="SIGKILL")
    w.remove_services(
        services=["mz_without_system_tables", "testdrive-svc"], destroy_volumes=True
    )
    w.remove_volumes(volumes=["mzdata"])


def workflow_disable_user_indexes(w: Workflow):
    w.start_and_wait_for_tcp(services=prerequisites)

    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")

    w.run_service(
        service="testdrive-svc",
        command="disable-user-indexes/before.td",
    )

    w.kill_services(services=["materialized"], signal="SIGKILL")
    w.start_services(services=["mz_disable_user_indexes"])
    w.wait_for_mz(service="mz_disable_user_indexes")

    w.run_service(
        service="testdrive-svc",
        command="disable-user-indexes/after.td",
    )

    w.kill_services(services=["mz_disable_user_indexes"], signal="SIGKILL")
    w.remove_services(
        services=["materialized", "mz_disable_user_indexes", "testdrive-svc"],
        destroy_volumes=True,
    )
    w.remove_volumes(volumes=["mzdata"])
