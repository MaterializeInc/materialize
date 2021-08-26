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

services = [Materialized(options="--persistent-user-tables"), Testdrive(no_reset=True)]


def workflow_persistent_tables(w: Workflow):
    w.start_services(services=["materialized"])
    w.wait_for_mz(service="materialized")

    w.run_service(service="testdrive-svc", command="table-persistence-before-*.td")

    w.kill_services(services=["materialized"], signal="SIGKILL")
    w.start_services(services=["materialized"])

    w.run_service(service="testdrive-svc", command="table-persistence-after-*.td")
