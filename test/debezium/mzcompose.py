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

import time

import requests

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

prerequisites = ["kafka", "schema-registry", "debezium", "materialized"]

SERVICES = [
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


def wait_for_debezium_connector(c: Composition, connector: str) -> None:
    """Wait for the connector's tasks, restarting ones that failed to start.

    Debezium 3.2 (DBZ-9070) checks the schema history settings twice while
    starting a task, which can lose a race that kills it (DBZ-3096, closed in
    2021 with a guard that is itself racy). Kafka Connect never restarts a
    failed task, so the 40-check-*.td files would wait 300s for nothing.
    """
    settle_polls = 10
    url = f"http://localhost:{c.default_port('debezium')}/connectors/{connector}"
    settled = 0
    restarted: set[int] = set()

    for _ in range(120):
        tasks = requests.get(f"{url}/status").json()["tasks"]
        if tasks and all(task["state"] == "RUNNING" for task in tasks):
            settled += 1
            if settled >= settle_polls:
                return
        else:
            settled = 0

        just_restarted: set[int] = set()
        for task in tasks:
            if task["state"] == "FAILED" and task["id"] not in restarted:
                print(f"Restarting failed task {task['id']}:\n{task.get('trace', '')}")
                requests.post(f"{url}/tasks/{task['id']}/restart").raise_for_status()
                just_restarted.add(task["id"])
        restarted = just_restarted

        time.sleep(1)

    raise AssertionError(f"Debezium connector {connector} did not start up: {tasks}")


def workflow_sql_server(c: Composition) -> None:
    c.up(*prerequisites, "sql-server")

    c.run_testdrive_files(
        f"--var=sa-password={SqlServer.DEFAULT_SA_PASSWORD}",
        "sql-server/[0-3]*.td",
    )
    wait_for_debezium_connector(c, "sql-server-connector")
    c.run_testdrive_files(
        f"--var=sa-password={SqlServer.DEFAULT_SA_PASSWORD}",
        "sql-server/[4-9]*.td",
    )


def workflow_mysql(c: Composition) -> None:
    c.up(*prerequisites, "mysql")

    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "mysql/*.td",
    )
