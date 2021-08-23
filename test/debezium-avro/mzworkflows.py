# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import (
    Debezium,
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    Testdrive,
    Workflow,
    Zookeeper,
)

prerequisites = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Debezium(),
    Postgres(),
    Materialized(),
]
services = prerequisites + [Testdrive(no_reset=True, default_timeout=300)]


def workflow_debezium_avro(w: Workflow):
    w.start_and_wait_for_tcp(services=prerequisites)

    w.wait_for_postgres(service="postgres")
    w.wait_for_mz(service="materialized")

    w.run_service(service="testdrive-svc", command="debezium-postgres.td.initialize")
    w.run_service(service="testdrive-svc", command="*.td")
