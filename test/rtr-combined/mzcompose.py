# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test that real-time recency works w/ slow ingest of upstream data from Kafka+MySQL+Postgres
"""

import random
import threading
import time

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import PropagatingThread

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    MySql(),
    Postgres(),
    Materialized(),
    Testdrive(
        entrypoint_extra=[
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        ],
    ),
]


def workflow_default(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "postgres", "mysql", "materialized")
    seed = random.getrandbits(16)
    c.run_testdrive_files(
        "--no-reset",
        "--max-errors=1",
        f"--seed={seed}",
        "rtr/mz-setup.td",
    )

    running = True

    def query():
        with c.sql_cursor() as cursor:
            cursor.execute("SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE'")
            cursor.execute("SET REAL_TIME_RECENCY TO TRUE")
            queries = [
                """SELECT sum(count) FROM (
                SELECT count(*) FROM table_mysql
                UNION ALL SELECT count(*) FROM table_pg
                UNION ALL SELECT count(*) FROM input_kafka)""",
                "SELECT sum FROM sum",
            ]
            thread_name = threading.current_thread().getName()
            while running:
                for query in queries:
                    start_time = time.time()
                    cursor.execute(query.encode())
                    results = cursor.fetchone()
                    assert results
                    runtime = time.time() - start_time
                    print(f"{thread_name}: {results[0]} ({runtime} s)")

    threads = [PropagatingThread(target=query, name=f"verify{i}") for i in range(10)]
    for thread in threads:
        thread.start()

    end_time = time.time() + 150
    while time.time() < end_time:
        # Only reaches one execution every 4 seconds currently, instead of the targetted 1 second, so probably need to parallelize this
        start_time = time.time()
        c.run_testdrive_files(
            "--no-reset",
            "--max-errors=1",
            f"--seed={seed}",
            "rtr/ingest.td",
        )
        runtime = time.time() - start_time
        print(f"ingest: {runtime} s")

    running = False
    for thread in threads:
        thread.join()
