# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional test for Kafka with real-time recency enabled. Queries should block
until results are available instead of returning out of date results.
"""

import random
import threading
import time
from textwrap import dedent

from psycopg import Cursor

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import PropagatingThread

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Mz(app_password=""),
    Materialized(),
    Toxiproxy(),
    Testdrive(no_reset=True, seed=1),
]


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return
        # TODO: Reenable when database-issues#8657 is fixed
        if name == "multithreaded":
            return
        with c.test_case(name):
            c.workflow(name)

    workflows = buildkite.shard_list(list(c.workflows), lambda w: w)
    c.test_parts(workflows, process)


#
# Test that real-time recency works w/ slow ingest of upstream data.
#
def workflow_simple(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "materialized", "toxiproxy")

    seed = random.getrandbits(16)
    c.run_testdrive_files(
        "--max-errors=1",
        f"--seed={seed}",
        f"--temp-dir=/share/tmp/kafka-resumption-{seed}",
        "simple/toxiproxy-setup.td",
        "simple/mz-setup.td",
        "simple/verify-rtr.td",
    )


def workflow_resumption(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "materialized", "toxiproxy")

    priv_cursor = c.sql_cursor(service="materialized", user="mz_system", port=6877)
    priv_cursor.execute("ALTER SYSTEM SET allow_real_time_recency = true;")

    def run_verification_query() -> Cursor:
        cursor = c.sql_cursor()
        cursor.execute("SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE'")
        cursor.execute("SET REAL_TIME_RECENCY TO TRUE")
        cursor.execute("SET statement_timeout = '600s'")
        cursor.execute(
            """
            SELECT sum(count)
              FROM (
                  SELECT count(*) FROM input_1_tbl
                  UNION ALL SELECT count(*) FROM input_2_tbl
                  UNION ALL SELECT count(*) FROM t
              ) AS x;"""
        )
        return cursor

    def verify_ok():
        cursor = run_verification_query()
        result = cursor.fetchall()
        assert result[0][0] == 2000204, f"Unexpected sum: {result[0][0]}"

    def verify_broken():
        try:
            run_verification_query()
        except Exception as e:
            assert (
                "timed out before ingesting the source's visible frontier when real-time-recency query issued"
                in str(e)
            )

    seed = random.getrandbits(16)
    for i, failure_mode in enumerate(
        [
            "toxiproxy-close-connection.td",
            "toxiproxy-limit-connection.td",
            "toxiproxy-timeout.td",
            "toxiproxy-timeout-hold.td",
        ]
    ):
        print(f"Running failure mode {failure_mode}...")

        c.run_testdrive_files(
            f"--seed={seed}{i}",
            f"--temp-dir=/share/tmp/kafka-resumption-{seed}",
            "resumption/toxiproxy-setup.td",  # without toxify
            "resumption/mz-setup.td",
            f"resumption/{failure_mode}",
            "resumption/ingest-data.td",
        )
        t1 = PropagatingThread(target=verify_broken)
        t1.start()
        time.sleep(10)
        c.run_testdrive_files(
            "resumption/toxiproxy-restore-connection.td",
        )
        t1.join()

        t2 = PropagatingThread(target=verify_ok)
        t2.start()
        time.sleep(10)
        t2.join()

        # reset toxiproxy
        c.kill("toxiproxy")
        c.up("toxiproxy")

        c.run_testdrive_files(
            "resumption/mz-reset.td",
        )


def workflow_multithreaded(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "materialized")
    c.up("testdrive", persistent=True)

    value = [201]
    lock = threading.Lock()
    running = True

    def run(value):
        cursor = c.sql_cursor()
        cursor.execute("SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE'")
        cursor.execute("SET REAL_TIME_RECENCY TO TRUE")
        repeat = 1
        while running:
            with lock:
                c.testdrive(
                    dedent(
                        f"""
                    $ kafka-ingest topic=input_1 format=bytes repeat={repeat}
                    A,B,0
                    $ kafka-ingest topic=input_2 format=bytes repeat={repeat}
                    A,B,0
                """
                    )
                )
                value[0] += repeat * 2
                expected = value[0]
                repeat *= 2
                cursor.execute("BEGIN")
            cursor.execute(
                """
                SELECT sum(count)
                  FROM (
                      SELECT count(*) FROM input_1_tbl
                      UNION ALL SELECT count(*) FROM input_2_tbl
                      UNION ALL SELECT count(*) FROM t
                  ) AS x;"""
            )
            result = cursor.fetchall()
            assert result[0][0] >= expected, f"Expected {expected}, got {result[0][0]}"
            cursor.execute("COMMIT")

    c.testdrive(
        dedent(
            """
        $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
        ALTER SYSTEM SET allow_real_time_recency = true

        $ kafka-create-topic topic=input_1

        $ kafka-ingest topic=input_1 format=bytes repeat=100
        A,B,0

        $ kafka-create-topic topic=input_2

        $ kafka-ingest topic=input_2 format=bytes repeat=100
        A,B,0

        > CREATE CONNECTION IF NOT EXISTS kafka_conn_1 TO KAFKA (BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT);
        > CREATE CONNECTION IF NOT EXISTS kafka_conn_2 TO KAFKA (BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT);

        > CREATE SOURCE input_1
          FROM KAFKA CONNECTION kafka_conn_1 (TOPIC 'testdrive-input_1-${testdrive.seed}')

        > CREATE TABLE input_1_tbl (city, state, zip) FROM SOURCE input_1 (REFERENCE "testdrive-input_1-${testdrive.seed}")
          FORMAT CSV WITH 3 COLUMNS

        > CREATE SOURCE input_2
          FROM KAFKA CONNECTION kafka_conn_2 (TOPIC 'testdrive-input_2-${testdrive.seed}')

        > CREATE TABLE input_2_tbl (city, state, zip) FROM SOURCE input_2 (REFERENCE "testdrive-input_2-${testdrive.seed}")
          FORMAT CSV WITH 3 COLUMNS

        > CREATE TABLE t (a int);
        > INSERT INTO t VALUES (1);

        > CREATE MATERIALIZED VIEW sum AS
          SELECT sum(count)
          FROM (
              SELECT count(*) FROM input_1_tbl
              UNION ALL SELECT count(*) FROM input_2_tbl
              UNION ALL SELECT count(*) FROM t
          ) AS x;
    """
        )
    )
    threads = [PropagatingThread(target=run, args=(value,)) for i in range(10)]
    for thread in threads:
        thread.start()
    time.sleep(600)
    running = False
    for thread in threads:
        thread.join()
