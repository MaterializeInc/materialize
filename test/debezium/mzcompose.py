# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.debezium import Debezium
from materialize.mzcompose.kafka import Kafka
from materialize.mzcompose.materialized import Materialized
from materialize.mzcompose.mysql import MySql
from materialize.mzcompose.postgres import Postgres
from materialize.mzcompose.schema_registry import SchemaRegistry
from materialize.mzcompose.sql_server import SqlServer
from materialize.mzcompose.testdrive import Testdrive
from materialize.mzcompose.zookeeper import Zookeeper

prerequisites = ["zookeeper", "kafka", "schema-registry", "debezium", "materialized"]

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Debezium(),
    Materialized(),
    Postgres(),
    SqlServer(),
    MySql(),
    Testdrive(no_reset=True, default_timeout="300s"),
]


def workflow_postgres(c: Composition) -> None:
    c.up(*prerequisites, "postgres")

    c.run("testdrive", "postgres/debezium-postgres.td.initialize")
    c.run("testdrive", "postgres/*.td")


def workflow_sql_server(c: Composition) -> None:
    c.up(*prerequisites, "sql-server")

    c.run(
        "testdrive",
        f"--var=sa-password={SqlServer.DEFAULT_SA_PASSWORD}",
        "sql-server/*.td",
    )


def workflow_mysql(c: Composition) -> None:
    c.up(*prerequisites, "mysql")

    c.run(
        "testdrive",
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "mysql/*.td",
    )
