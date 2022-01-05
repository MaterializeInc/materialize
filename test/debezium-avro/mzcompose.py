# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

sa_password = "P@ssw0rd!"

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Debezium(),
    Materialized(),
    Postgres(),
    SqlServer(sa_password=sa_password),
    Testdrive(
        no_reset=True,
        default_timeout="300s",
        environment=[f"SA_PASSWORD={sa_password}"],
        depends_on=["kafka", "schema-registry", "materialized", "debezium"],
    ),
]


def workflow_debezium_avro(c: Composition) -> None:
    c.up("postgres")
    c.run("testdrive-svc", "debezium-postgres.td.initialize")
    c.run("testdrive-svc", "*.td")


def workflow_debezium_sql_server(c: Composition) -> None:
    c.up("sql-server")
    c.run("testdrive-svc", "sql-server/*.td")
