# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import threading
import time
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import PropagatingThread

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Toxiproxy(),
    Testdrive(no_reset=True, seed=1),
]


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue

        with c.test_case(name):
            c.workflow(name)


# TODO(@def): investigate getting `workflow_resumption` to work
# https://github.com/MaterializeInc/materialize/pull/25566/commits/d65517d793f00fd1f520e2879c30e658538b0b8d#diff-f39755ef069f5edce6c30efcd534ae3788edb95ea7e6f69d8ba247b4e8b98846R63-R111
#
# The issue with the linked-to approach is that RTR uses the same connection as
# the source, so injecting failures also prevents envd from determining the
# upstream timestamp. We would need to manufacture an error only _after_ envd
# determined the timestamp.

#
# Test that real-time recency works w/ slow ingest of upstream data.
#
def workflow_simple(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "materialized", "toxiproxy")

    seed = random.getrandbits(16)
    c.run_testdrive_files(
        "--no-reset",
        "--max-errors=1",
        f"--seed={seed}",
        f"--temp-dir=/share/tmp/kafka-resumption-{seed}",
        "simple/toxiproxy-setup.td",
        "simple/mz-setup.td",
        "simple/verify-rtr.td",
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
                      SELECT count(*) FROM input_1
                      UNION ALL SELECT count(*) FROM input_2
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

        > CREATE SOURCE input_1 (city, state, zip)
          FROM KAFKA CONNECTION kafka_conn_1 (TOPIC 'testdrive-input_1-${testdrive.seed}')
          FORMAT CSV WITH 3 COLUMNS

        > CREATE SOURCE input_2 (city, state, zip)
          FROM KAFKA CONNECTION kafka_conn_2 (TOPIC 'testdrive-input_2-${testdrive.seed}')
          FORMAT CSV WITH 3 COLUMNS

        > CREATE TABLE t (a int);
        > INSERT INTO t VALUES (1);

        > CREATE MATERIALIZED VIEW sum AS
          SELECT sum(count)
          FROM (
              SELECT count(*) FROM input_1
              UNION ALL SELECT count(*) FROM input_2
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
