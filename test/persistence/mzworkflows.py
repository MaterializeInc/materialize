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

materialized = Materialized(options="--persistent-user-tables")

# This instance of Mz is used for failpoint testing. By using --disable-persistent-system-tables-test
# we ensure that only testdrive-initiated actions cause I/O. The --workers 1 is used due to #8739

mz_without_system_tables = Materialized(
    name="mz_without_system_tables",
    hostname="materialized",
    options="--persistent-user-tables --disable-persistent-system-tables-test --workers 1",
)

services = [materialized, mz_without_system_tables, Testdrive(no_reset=True)]


def workflow_persistence(w: Workflow):
    workflow_user_tables(w)
    workflow_failpoints(w)


def workflow_user_tables(w: Workflow):
    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")

    w.run_service(
        service="testdrive-svc",
        command="user-tables/table-persistence-before-*.td",
    )

    w.kill_services(services=["materialized"], signal="SIGKILL")
    w.start_services(services=["materialized"])

    w.run_service(
        service="testdrive-svc",
        command="user-tables/table-persistence-after-*.td",
    )

    w.kill_services(services=["materialized"], signal="SIGKILL")


def workflow_failpoints(w: Workflow):
    w.start_services(services=["mz_without_system_tables"])
    w.wait_for_mz(service="mz_without_system_tables")

    w.run_service(service="testdrive-svc", command="failpoints/*.td")

    w.kill_services(services=["mz_without_system_tables"], signal="SIGKILL")
