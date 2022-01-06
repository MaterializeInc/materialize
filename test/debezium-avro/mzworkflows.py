# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import random
import string
from unittest.mock import patch

from materialize.mzcompose import Workflow
from materialize.mzcompose.services import (
    Debezium,
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    SqlServer,
    Testdrive,
    Zookeeper,
)

prerequisites = ["zookeeper", "kafka", "schema-registry", "debezium", "materialized"]

sa_password = "AAbb!@" + "".join(
    random.choices(string.ascii_uppercase + string.digits, k=10)
)

services = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Debezium(),
    Materialized(),
    Postgres(),
    SqlServer(sa_password=sa_password),
    Testdrive(no_reset=True, default_timeout="300s"),
]


def workflow_debezium_avro(w: Workflow) -> None:
    w.start_and_wait_for_tcp(services=prerequisites)
    w.start_and_wait_for_tcp(services=["postgres"])

    w.wait_for_postgres(service="postgres")
    w.wait_for_mz(service="materialized")

    w.run_service(service="testdrive-svc", command="debezium-postgres.td.initialize")
    w.run_service(service="testdrive-svc", command="*.td")


@patch.dict(os.environ, {"SA_PASSWORD": sa_password})
def workflow_debezium_sql_server(w: Workflow) -> None:
    w.start_and_wait_for_tcp(services=prerequisites)
    w.start_and_wait_for_tcp(services=["sql-server"])

    w.run_service(service="testdrive-svc", command="sql-server/*.td")
