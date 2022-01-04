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
    options="--log-filter=info,dataflow::source=trace,dataflow::server=trace --disable-persistent-system-tables-test"
)
# materialized = Materialized(
#         options="--log-filter=info,dataflow::source=trace,dataflow::server=trace --persistent-user-tables --persistent-kafka-upsert-source --disable-persistent-system-tables-test"
# )

prerequisites = ["zookeeper", "kafka", "schema-registry"]

services = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    materialized,
    Testdrive(no_reset=True, consistent_seed=True),
]

td_test = os.environ.pop("TD_TEST", "*")


def workflow_persistence(w: Workflow):
    workflow_kafka_sources(w)


def workflow_kafka_sources(w: Workflow):
    w.start_and_wait_for_tcp(services=prerequisites)

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
