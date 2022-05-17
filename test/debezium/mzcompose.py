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
    MySql,
    Postgres,
    SchemaRegistry,
    SqlServer,
    Testdrive,
    Zookeeper,
)

prerequisites = ["zookeeper", "kafka", "schema-registry", "debezium", "materialized"]

password = "AAbb!@" + "".join(
    random.choices(string.ascii_uppercase + string.digits, k=10)
)

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Debezium(),
    # Currently gets hosed and takes much longer if the default frequency of 100ms is used
    Materialized(timestamp_frequency="1s"),
    Postgres(),
    SqlServer(sa_password=password),
    MySql(mysql_root_password=password),
    Testdrive(no_reset=True, default_timeout="300s"),
]


def workflow_postgres(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)
    c.start_and_wait_for_tcp(services=["postgres"])

    c.wait_for_postgres(service="postgres")
    c.wait_for_materialized("materialized")

    c.run("testdrive", "postgres/debezium-postgres.td.initialize")
    c.run("testdrive", "postgres/*.td")


def workflow_sql_server(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)
    c.start_and_wait_for_tcp(services=["sql-server"])

    c.run("testdrive", f"--var=sa-password={password}", "sql-server/*.td")


def workflow_mysql(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)
    c.start_and_wait_for_tcp(services=["mysql"])

    c.run("testdrive", f"--var=mysql-root-password={password}", "mysql/*.td")
