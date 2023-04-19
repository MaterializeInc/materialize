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
    MySql,
    Postgres,
    SchemaRegistry,
    SqlServer,
    Testdrive,
    Zookeeper,
)

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

    c.sql(
        "ALTER SYSTEM SET max_compute_credits_per_hour TO 1000;",
        port=6877,
        user="mz_system"
    )
    c.run("testdrive", "postgres/debezium-postgres.td.initialize")
    c.run("testdrive", "postgres/*.td")


def workflow_sql_server(c: Composition) -> None:
    c.up(*prerequisites, "sql-server")

    c.sql(
        "ALTER SYSTEM SET max_compute_credits_per_hour TO 1000;",
        port=6877,
        user="mz_system"
    )
    c.run(
        "testdrive",
        f"--var=sa-password={SqlServer.DEFAULT_SA_PASSWORD}",
        "sql-server/*.td",
    )


def workflow_mysql(c: Composition) -> None:
    c.up(*prerequisites, "mysql")

    c.sql(
        "ALTER SYSTEM SET max_compute_credits_per_hour TO 1000;",
        port=6877,
        user="mz_system"
    )
    c.run(
        "testdrive",
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "mysql/*.td",
    )
