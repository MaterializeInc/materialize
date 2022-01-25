# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import string

from materialize.mzcompose import Composition
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

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Debezium(),
    Materialized(),
    Postgres(),
    SqlServer(sa_password=sa_password),
    Testdrive(no_reset=True, default_timeout="300s"),
]


def workflow_default(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)
    c.start_and_wait_for_tcp(services=["postgres"])

    c.wait_for_postgres(service="postgres")
    c.wait_for_materialized("materialized")

    c.run("testdrive-svc", "debezium-postgres.td.initialize")
    c.run("testdrive-svc", "*.td")


def workflow_debezium_sql_server(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)
    c.start_and_wait_for_tcp(services=["sql-server"])

    c.run("testdrive-svc", f"--var=sa-password={sa_password}", "sql-server/*.td")
