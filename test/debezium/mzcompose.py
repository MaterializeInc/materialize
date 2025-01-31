# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Connect Postgres/SQL Server/MySQL to Materialize using Kafka+Debezium
"""

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.debezium import Debezium
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

prerequisites = ["zookeeper", "kafka", "schema-registry", "debezium", "materialized"]

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Debezium(),
    Mz(app_password=""),
    Materialized(),
    Postgres(),
    SqlServer(),
    MySql(),
    Testdrive(no_reset=True, default_timeout="300s"),
]


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return
        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_postgres(c: Composition) -> None:
    c.up(*prerequisites, "postgres")

    c.run_testdrive_files("postgres/debezium-postgres.td.initialize")
    c.run_testdrive_files("postgres/*.td")


def workflow_sql_server(c: Composition) -> None:
    c.up(*prerequisites, "sql-server")

    c.run_testdrive_files(
        f"--var=sa-password={SqlServer.DEFAULT_SA_PASSWORD}",
        "sql-server/*.td",
    )


def workflow_mysql(c: Composition) -> None:
    c.up(*prerequisites, "mysql")

    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "mysql/*.td",
    )
