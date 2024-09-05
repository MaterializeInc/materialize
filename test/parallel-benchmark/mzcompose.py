# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Benchmark with scenarios combining closed and open loops, can run multiple
actions concurrently, measures various kinds of statistics.
"""

import collections
import gc
import queue
import random
import threading
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent

import matplotlib.pyplot as plt
import numpy
from matplotlib.markers import MarkerStyle

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka as KafkaService
from materialize.mzcompose.services.kgen import Kgen as KgenService
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)
from materialize.util import PgConnInfo, all_subclasses, parse_pg_conn_string

SERVICES = [
    Zookeeper(),
    KafkaService(),
    SchemaRegistry(),
    Redpanda(),
    Cockroach(setup_materialize=True),
    Minio(setup_materialize=True),
    KgenService(),
    Postgres(),
    MySql(),
    Balancerd(),
    # Overridden below
    Materialized(),
    Clusterd(),
    Testdrive(no_reset=True, seed=1),
]


class Measurement:
    duration: float
    timestamp: float

    def __init__(self, duration: float, timestamp: float):
        self.duration = duration
        self.timestamp = timestamp

    def __str__(self) -> str:
        return f"{self.timestamp} {self.duration}"


@dataclass
class State:
    measurements: collections.defaultdict[str, list[Measurement]]
    load_phase_duration: int | None
    periodic_dists: dict[str, int]


class Action:
    def run(
        self,
        start_time: float,
        conns: queue.Queue,
        state: State,
    ):
        self._run(conns)
        duration = time.time() - start_time
        state.measurements[str(self)].append(Measurement(duration, start_time))

    def _run(self, conns: queue.Queue):
        raise NotImplementedError


class TdAction(Action):
    def __init__(self, td: str, c: Composition):
        self.td = dedent(td)
        self.c = c

    def _run(self, conns: queue.Queue):
        self.c.testdrive(self.td, quiet=True)

    def __str__(self) -> str:
        return "testdrive"


class StandaloneQuery(Action):
    def __init__(
        self,
        query: str,
        conn_info: PgConnInfo,
        strict_serializable: bool = True,
    ):
        self.query = query
        self.conn_info = conn_info
        self.strict_serializable = strict_serializable

    def _run(self, conns: queue.Queue):
        conn = self.conn_info.connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            if not self.strict_serializable:
                cur.execute("SET TRANSACTION_ISOLATION TO 'SERIALIZABLE'")
            cur.execute(self.query)
        conn.close()

    def __str__(self) -> str:
        return f"{self.query} (standalone)"


class ReuseConnQuery(Action):
    def __init__(
        self,
        query: str,
        conn_info: PgConnInfo,
        strict_serializable: bool = True,
    ):
        self.query = query
        self.conn = conn_info.connect()
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.cur.execute(
            f"SET TRANSACTION_ISOLATION TO '{'STRICT SERIALIZABLE' if strict_serializable else 'SERIALIZABLE'}'"
        )

    def _run(self, conns: queue.Queue):
        self.cur.execute(self.query)

    def __str__(self) -> str:
        return f"{self.query} (reuse connection)"


class PooledQuery(Action):
    def __init__(self, query: str, conn_info: PgConnInfo):
        self.query = query
        self.conn_info = conn_info

    def _run(self, conns: queue.Queue):
        conn = conns.get()
        with conn.cursor() as cur:
            cur.execute(self.query)
        conns.task_done()
        conns.put(conn)

    def __str__(self) -> str:
        return f"{self.query} (pooled)"


def sleep_until(timestamp: float) -> None:
    time_to_sleep = timestamp - time.time()
    if time_to_sleep > 0:
        time.sleep(time_to_sleep)


class Distribution:
    def generate(
        self, duration: int, action_name: str, state: State
    ) -> Iterator[float]:
        raise NotImplementedError


class Periodic(Distribution):
    """Run the action in each thread in one second, spread apart by the 1/per_second"""

    def __init__(self, per_second: float):
        self.per_second = per_second

    def generate(
        self, duration: int, action_name: str, state: State
    ) -> Iterator[float]:
        per_second = state.periodic_dists.get(action_name) or self.per_second
        next_time = time.time()
        for i in range(int(duration * per_second)):
            yield next_time
            next_time += 1 / per_second
            sleep_until(next_time)


class Gaussian(Distribution):
    """Run the action with a sleep time between actions drawn from a Gaussian distribution"""

    def __init__(self, mean: float, stddev: float):
        self.mean = mean
        self.stddev = stddev

    def generate(
        self, duration: int, action_name: str, state: State
    ) -> Iterator[float]:
        end_time = time.time() + duration
        next_time = time.time()
        while time.time() < end_time:
            yield next_time
            next_time += max(0, random.gauss(self.mean, self.stddev))
            sleep_until(next_time)


class PhaseAction:
    def run(
        self,
        duration: int,
        jobs: queue.Queue,
        conns: queue.Queue,
        state: State,
    ) -> None:
        raise NotImplementedError


class OpenLoop(PhaseAction):
    def __init__(self, action: Action, dist: Distribution):
        self.action = action
        self.dist = dist

    def run(
        self,
        duration: int,
        jobs: queue.Queue,
        conns: queue.Queue,
        state: State,
    ) -> None:
        for start_time in self.dist.generate(duration, str(self.action), state):
            jobs.put(lambda: self.action.run(start_time, conns, state))


class ClosedLoop(PhaseAction):
    def __init__(self, action: Action):
        self.action = action

    def run(
        self,
        duration: int,
        jobs: queue.Queue,
        conns: queue.Queue,
        state: State,
    ) -> None:
        end_time = time.time() + duration
        while time.time() < end_time:
            self.action.run(time.time(), conns, state)


class Phase:
    def run(
        self,
        c: Composition,
        jobs: queue.Queue,
        conns: queue.Queue,
        state: State,
    ) -> None:
        raise NotImplementedError


class TdPhase(Phase):
    def __init__(self, td: str):
        self.td = dedent(td)

    def run(
        self,
        c: Composition,
        jobs: queue.Queue,
        conns: queue.Queue,
        state: State,
    ) -> None:
        c.testdrive(self.td, quiet=True)


class LoadPhase(Phase):
    duration: int
    phase_actions: Sequence[PhaseAction]

    def __init__(self, duration: int, actions: Sequence[PhaseAction]):
        self.duration = duration
        self.phase_actions = actions

    def run(
        self,
        c: Composition,
        jobs: queue.Queue,
        conns: queue.Queue,
        state: State,
    ) -> None:
        duration = state.load_phase_duration or self.duration
        print(f"Load phase for {duration}s")
        threads = [
            threading.Thread(
                target=phase_action.run,
                args=(duration, jobs, conns, state),
            )
            for phase_action in self.phase_actions
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


def run_job(jobs: queue.Queue) -> None:
    while True:
        job = jobs.get()
        try:
            if not job:
                return
            job()
        finally:
            jobs.task_done()


class Scenario:
    phases: list[Phase]
    thread_pool_size: int
    conn_pool_size: int
    guarantees: dict[str, dict[str, float]]
    jobs: queue.Queue
    conns: queue.Queue
    thread_pool: list[threading.Thread]

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        raise NotImplementedError

    def init(
        self,
        phases: list[Phase],
        thread_pool_size: int = 5000,
        conn_pool_size: int = 0,
        guarantees: dict[str, dict[str, float]] = {},
    ):
        self.phases = phases
        self.thread_pool_size = thread_pool_size
        self.conn_pool_size = conn_pool_size
        self.guarantees = guarantees
        self.jobs = queue.Queue()
        self.conns = queue.Queue()

    def setup(self, c: Composition, conn_infos: dict[str, PgConnInfo]) -> None:
        conn_info = conn_infos["materialized"]
        self.thread_pool = [
            threading.Thread(target=run_job, args=(self.jobs,))
            for i in range(self.thread_pool_size)
        ]
        for thread in self.thread_pool:
            thread.start()
        # Start threads and have them wait for work from a queue
        for i in range(self.conn_pool_size):
            self.conns.put(conn_info.connect())

    def run(
        self,
        c: Composition,
        state: State,
    ) -> None:
        for phase in self.phases:
            phase.run(c, self.jobs, self.conns, state)

    def teardown(self) -> None:
        while not self.conns.empty():
            conn = self.conns.get()
            conn.close()
            self.conns.task_done()
        for i in range(len(self.thread_pool)):
            # Indicate to every thread to stop working
            self.jobs.put(None)
        for thread in self.thread_pool:
            thread.join()
        self.jobs.join()


class Kafka(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    $ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }
                    $ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

                    $ kafka-create-topic topic=kafka

                    $ kafka-ingest format=avro topic=kafka key-format=avro key-schema=${keyschema} schema=${schema} repeat=10
                    {"f1": 1} {"f2": ${kafka-ingest.iteration} }

                    > CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

                    > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                      URL '${testdrive.schema-registry-url}');

                    > CREATE SOURCE kafka
                      FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-${testdrive.seed}')
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE UPSERT;

                    > CREATE MATERIALIZED VIEW kafka_mv AS SELECT * FROM kafka;

                    > CREATE DEFAULT INDEX ON kafka_mv;
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=TdAction(
                                """
                                $ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }
                                $ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }
                                $ kafka-ingest format=avro topic=kafka key-format=avro key-schema=${keyschema} schema=${schema} repeat=10
                                {"f1": 1} {"f2": ${kafka-ingest.iteration} }
                                """,
                                c,
                            ),
                            dist=Periodic(per_second=1),
                        )
                    ]
                    + [
                        ClosedLoop(
                            action=StandaloneQuery(
                                "SELECT * FROM kafka_mv",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                "SELECT * FROM kafka_mv (standalone)": {"qps": 15, "p99": 400},
            },
        )


class PgReadReplica(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    > DROP SECRET IF EXISTS pgpass CASCADE
                    > CREATE SECRET pgpass AS 'postgres'
                    > CREATE CONNECTION pg TO POSTGRES (
                        HOST postgres,
                        DATABASE postgres,
                        USER postgres,
                        PASSWORD SECRET pgpass
                      )

                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t1 (f1 INTEGER);
                    ALTER TABLE t1 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source2 FOR ALL TABLES;

                    > CREATE SOURCE mz_source2
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source2')
                      FOR ALL TABLES;

                    > CREATE MATERIALIZED VIEW mv_sum AS
                      SELECT COUNT(*) FROM t1;

                    > CREATE DEFAULT INDEX ON mv_sum;
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "INSERT INTO t1 VALUES (1)",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=100),
                        )
                    ]
                    + [
                        ClosedLoop(
                            action=StandaloneQuery(
                                "SELECT * FROM mv_sum",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                "SELECT * FROM mv_sum (standalone)": {"qps": 15, "p99": 400},
            },
        )


class PgReadReplicaRTR(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    > DROP SECRET IF EXISTS pgpass CASCADE
                    > CREATE SECRET pgpass AS 'postgres'
                    > CREATE CONNECTION pg TO POSTGRES (
                        HOST postgres,
                        DATABASE postgres,
                        USER postgres,
                        PASSWORD SECRET pgpass
                      )

                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t2 (f1 INTEGER);
                    ALTER TABLE t2 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source3 FOR ALL TABLES;

                    > CREATE SOURCE mz_source3
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source3')
                      FOR ALL TABLES;

                    > CREATE MATERIALIZED VIEW mv_sum AS
                      SELECT COUNT(*) FROM t2;

                    > CREATE DEFAULT INDEX ON mv_sum;
                    """
                ),
                LoadPhase(
                    duration=360,
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "INSERT INTO t2 VALUES (1)",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=100),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "SET REAL_TIME_RECENCY TO TRUE; SELECT * FROM mv_sum",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=125),
                        ),
                    ],
                ),
            ],
            guarantees={
                # TODO(def-): Lower max when RTR becomes more performant
                "SET REAL_TIME_RECENCY TO TRUE; SELECT * FROM mv_sum (standalone)": {
                    "qps": 50,
                    "p99": 5000,
                },
            },
        )


class MySQLReadReplica(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    f"""
                    > DROP SECRET IF EXISTS mysqlpass CASCADE
                    > CREATE SECRET mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'
                    > CREATE CONNECTION IF NOT EXISTS mysql_conn TO MYSQL (HOST mysql, USER root, PASSWORD SECRET mysqlpass)

                    $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                    $ mysql-execute name=mysql
                    DROP DATABASE IF EXISTS public;
                    CREATE DATABASE public;
                    USE public;
                    CREATE TABLE t3 (f1 INTEGER);

                    > CREATE SOURCE mysql_source
                      FROM MYSQL CONNECTION mysql_conn
                      FOR TABLES (public.t3);

                    > CREATE MATERIALIZED VIEW mv_sum_mysql AS
                      SELECT COUNT(*) FROM t3;

                    > CREATE DEFAULT INDEX ON mv_sum_mysql;
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=TdAction(
                                f"""
                                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                                $ mysql-execute name=mysql
                                USE public;
                                {"INSERT INTO t3 VALUES (1); " * 100}
                                """,
                                c,
                            ),
                            dist=Periodic(per_second=1),
                        )
                    ]
                    + [
                        ClosedLoop(
                            action=StandaloneQuery(
                                "SELECT * FROM mv_sum_mysql",
                                conn_info=conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                "SELECT * FROM mv_sum_mysql (standalone)": {"qps": 15, "p99": 400},
            },
        )


class OpenIndexedSelects(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    > CREATE TABLE t4 (f1 TEXT, f2 INTEGER);
                    > CREATE DEFAULT INDEX ON t4;
                    > INSERT INTO t4 VALUES ('A', 1);
                    > INSERT INTO t4 VALUES ('B', 2);
                    > INSERT INTO t4 VALUES ('C', 3);
                    > INSERT INTO t4 VALUES ('D', 4);
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=PooledQuery(
                                "SELECT * FROM t4", conn_info=conn_infos["materialized"]
                            ),
                            dist=Periodic(per_second=400),
                        ),
                    ],
                ),
            ],
            conn_pool_size=100,
            guarantees={
                "SELECT * FROM t4 (pooled)": {"qps": 200, "p99": 100},
            },
        )


class ConnectRead(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                LoadPhase(
                    duration=120,
                    actions=[
                        ClosedLoop(
                            action=StandaloneQuery(
                                "SELECT 1",
                                conn_info=conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                "SELECT * FROM t4 (pooled)": {"qps": 35, "max": 700},
            },
        )


class FlagUpdate(Scenario):
    """Reproduces #29235"""

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=ReuseConnQuery(
                                "ALTER SYSTEM SET enable_table_keys = true",
                                conn_info=conn_infos["mz_system"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                    ]
                    + [
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT 1",
                                conn_info=conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                # TODO(def-): Lower when #29235 is fixed to prevent regressions
                "SELECT 1 (reuse connection)": {"avg": 5, "max": 500, "slope": 0.1},
            },
        )


class Read(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                LoadPhase(
                    duration=120,
                    actions=[
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT 1",
                                conn_info=conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        )
                        for i in range(10)
                    ],
                ),
            ],
            guarantees={
                "SELECT 1 (reuse connection)": {"qps": 2000, "max": 100, "slope": 0.1},
            },
        )


class PoolRead(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=PooledQuery(
                                "SELECT 1", conn_info=conn_infos["materialized"]
                            ),
                            dist=Periodic(per_second=100),
                            # dist=Gaussian(mean=0.01, stddev=0.05),
                        ),
                    ],
                ),
            ],
            conn_pool_size=100,
            guarantees={
                "SELECT 1 (pooled)": {"avg": 5, "max": 200, "slope": 0.1},
            },
        )


class StatementLogging(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    ALTER SYSTEM SET statement_logging_max_sample_rate = 1.0;
                    ALTER SYSTEM SET statement_logging_default_sample_rate = 1.0;
                    ALTER SYSTEM SET enable_statement_lifecycle_logging = true;
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=PooledQuery(
                                "SELECT 1", conn_info=conn_infos["materialized"]
                            ),
                            dist=Periodic(per_second=100),
                            # dist=Gaussian(mean=0.01, stddev=0.05),
                        ),
                    ],
                ),
                TdPhase(
                    """
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    ALTER SYSTEM SET statement_logging_default_sample_rate = 0;
                    ALTER SYSTEM SET statement_logging_max_sample_rate = 0;
                    ALTER SYSTEM SET enable_statement_lifecycle_logging = false;
                    """
                ),
            ],
            conn_pool_size=100,
            guarantees={
                "SELECT 1 (pooled)": {"avg": 5, "max": 200, "slope": 0.1},
            },
        )


class InsertWhereNotExists(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    > CREATE TABLE insert_table (a int, b text);
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=ReuseConnQuery(
                                "INSERT INTO insert_table SELECT 1, '1' WHERE NOT EXISTS (SELECT 1 FROM insert_table WHERE a = 100)",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=5),
                        )
                    ],
                ),
            ],
            conn_pool_size=100,
            # TODO(def-): Bump per_second and add guarantees when #29371 is fixed
        )


class InsertsSelects(Scenario):
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    > CREATE TABLE insert_select_table (a int, b text);
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=ReuseConnQuery(
                                "INSERT INTO insert_select_table VALUES (1, '1')",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT min(a) FROM insert_select_table",
                                conn_infos["materialized"],
                                strict_serializable=False,
                            ),
                        ),
                    ],
                ),
            ],
            conn_pool_size=100,
            guarantees={
                "SELECT min(a) FROM insert_select_table (reuse connection)": {
                    "qps": 10,
                    "p99": 350,
                },
            },
        )


# class ReadReplicaBenchmark(Scenario):
#     """https://www.notion.so/materialize/Read-Replica-Benchmark-90b60e455ba648c3a8c9a53297d09492"""
#
#     def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
#         self.init(
#             [
#                 TdPhase(
#                     """
#                     > DROP SECRET IF EXISTS pgpass CASCADE
#                     > CREATE SECRET pgpass AS 'postgres'
#                     > CREATE CONNECTION pg TO POSTGRES (
#                         HOST postgres,
#                         DATABASE postgres,
#                         USER postgres,
#                         PASSWORD SECRET pgpass
#                       )
#
#                     $ postgres-execute connection=postgres://postgres:postgres@postgres
#                     ALTER USER postgres WITH replication;
#                     CREATE TABLE customers (customer_id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, address VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
#                     ALTER TABLE customers REPLICA IDENTITY FULL;
#                     CREATE TABLE accounts (account_id SERIAL PRIMARY KEY, customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE, account_type VARCHAR(50) NOT NULL, balance DECIMAL(18, 2) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
#                     ALTER TABLE accounts REPLICA IDENTITY FULL;
#                     CREATE TABLE securities (security_id SERIAL PRIMARY KEY, ticker VARCHAR(10) NOT NULL UNIQUE, name VARCHAR(255), sector VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
#                     ALTER TABLE securities REPLICA IDENTITY FULL;
#                     CREATE TABLE trades (trade_id SERIAL PRIMARY KEY, account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE, security_id INT REFERENCES securities(security_id) ON DELETE CASCADE, trade_type VARCHAR(10) NOT NULL CHECK (trade_type IN ('buy', 'sell')), quantity INT NOT NULL, price DECIMAL(18, 4) NOT NULL, trade_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
#                     ALTER TABLE trades REPLICA IDENTITY FULL;
#                     CREATE TABLE orders (order_id SERIAL PRIMARY KEY, account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE, security_id INT REFERENCES securities(security_id) ON DELETE CASCADE, order_type VARCHAR(10) NOT NULL CHECK (order_type IN ('buy', 'sell')), quantity INT NOT NULL, limit_price DECIMAL(18, 4), status VARCHAR(10) NOT NULL CHECK (status IN ('pending', 'completed', 'canceled')), order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
#                     ALTER TABLE orders REPLICA IDENTITY FULL;
#                     CREATE TABLE market_data (market_data_id SERIAL PRIMARY KEY, security_id INT REFERENCES securities(security_id) ON DELETE CASCADE, price DECIMAL(18, 4) NOT NULL, volume INT NOT NULL, market_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
#                     ALTER TABLE market_data REPLICA IDENTITY FULL;
#                     CREATE PUBLICATION mz_source FOR ALL TABLES;
#
#                     > CREATE SOURCE mz_source
#                       FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
#                       FOR ALL TABLES;
#
#                     > CREATE VIEW customer_portfolio AS
#                       SELECT c.customer_id, c.name, a.account_id, s.ticker, s.name AS security_name,
#                              SUM(t.quantity * t.price) AS total_value
#                       FROM customers c
#                       JOIN accounts a ON c.customer_id = a.customer_id
#                       JOIN trades t ON a.account_id = t.account_id
#                       JOIN securities s ON t.security_id = s.security_id
#                       GROUP BY c.customer_id, c.name, a.account_id, s.ticker, s.name;
#
#                     > CREATE VIEW top_performers AS
#                       WITH ranked_performers AS (
#                           SELECT s.ticker, s.name, SUM(t.quantity) AS total_traded_volume,
#                                  ROW_NUMBER() OVER (ORDER BY SUM(t.quantity) DESC) AS rank
#                           FROM trades t
#                           JOIN securities s ON t.security_id = s.security_id
#                           GROUP BY s.ticker, s.name
#                       )
#                       SELECT ticker, name, total_traded_volume, rank
#                       FROM ranked_performers
#                       WHERE rank <= 10;
#
#                     > CREATE VIEW market_overview AS
#                       SELECT s.sector, AVG(md.price) AS avg_price, SUM(md.volume) AS total_volume,
#                              MAX(md.market_date) AS last_update
#                       FROM securities s
#                       LEFT JOIN market_data md ON s.security_id = md.security_id
#                       GROUP BY s.sector
#                       HAVING MAX(md.market_date) > NOW() - INTERVAL '5 minutes';
#
#                     > CREATE VIEW recent_large_trades AS
#                       SELECT t.trade_id, a.account_id, s.ticker, t.quantity, t.price, t.trade_date
#                       FROM trades t
#                       JOIN accounts a ON t.account_id = a.account_id
#                       JOIN securities s ON t.security_id = s.security_id
#                       WHERE t.quantity > (SELECT AVG(quantity) FROM trades) * 5
#                       AND t.trade_date > NOW() - INTERVAL '1 hour';
#
#                     > CREATE VIEW customer_order_book AS
#                       SELECT c.customer_id, c.name, COUNT(o.order_id) AS open_orders,
#                              SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS completed_orders
#                       FROM customers c
#                       JOIN accounts a ON c.customer_id = a.customer_id
#                       JOIN orders o ON a.account_id = o.account_id
#                       GROUP BY c.customer_id, c.name;
#
#                     > CREATE VIEW sector_performance AS
#                       SELECT s.sector, AVG(t.price) AS avg_trade_price, COUNT(t.trade_id) AS trade_count,
#                              SUM(t.quantity) AS total_volume
#                       FROM trades t
#                       JOIN securities s ON t.security_id = s.security_id
#                       GROUP BY s.sector;
#
#                     > CREATE VIEW account_activity_summary AS
#                       SELECT a.account_id, COUNT(t.trade_id) AS trade_count,
#                              SUM(t.quantity * t.price) AS total_trade_value,
#                              MAX(t.trade_date) AS last_trade_date
#                       FROM accounts a
#                       LEFT JOIN trades t ON a.account_id = t.account_id
#                       GROUP BY a.account_id;
#
#                     > CREATE VIEW daily_market_movements AS
#                       SELECT md.security_id, s.ticker, s.name,
#                              md.price AS current_price,
#                              LAG(md.price) OVER (PARTITION BY md.security_id ORDER BY md.market_date) AS previous_price,
#                              (md.price - LAG(md.price) OVER (PARTITION BY md.security_id ORDER BY md.market_date)) AS price_change,
#                              md.market_date
#                       FROM market_data md
#                       JOIN securities s ON md.security_id = s.security_id
#                       WHERE md.market_date > NOW() - INTERVAL '1 day';
#
#                     > CREATE VIEW high_value_customers AS
#                       SELECT c.customer_id, c.name, SUM(a.balance) AS total_balance
#                       FROM customers c
#                       JOIN accounts a ON c.customer_id = a.customer_id
#                       GROUP BY c.customer_id, c.name
#                       HAVING SUM(a.balance) > 1000000;
#
#                     > CREATE VIEW pending_orders_summary AS
#                       SELECT s.ticker, s.name, COUNT(o.order_id) AS pending_order_count,
#                              SUM(o.quantity) AS pending_volume,
#                              AVG(o.limit_price) AS avg_limit_price
#                       FROM orders o
#                       JOIN securities s ON o.security_id = s.security_id
#                       WHERE o.status = 'pending'
#                       GROUP BY s.ticker, s.name;
#
#                     > CREATE VIEW trade_volume_by_hour AS
#                       SELECT EXTRACT(HOUR FROM t.trade_date) AS trade_hour,
#                              COUNT(t.trade_id) AS trade_count,
#                              SUM(t.quantity) AS total_quantity
#                       FROM trades t
#                       GROUP BY EXTRACT(HOUR FROM t.trade_date);
#
#                     > CREATE VIEW top_securities_by_sector AS
#                       WITH ranked_securities AS (
#                           SELECT s.sector, s.ticker, s.name,
#                                  SUM(t.quantity) AS total_volume,
#                                  ROW_NUMBER() OVER (PARTITION BY s.sector ORDER BY SUM(t.quantity) DESC) AS sector_rank
#                           FROM trades t
#                           JOIN securities s ON t.security_id = s.security_id
#                           GROUP BY s.sector, s.ticker, s.name
#                       )
#                       SELECT sector, ticker, name, total_volume, sector_rank
#                       FROM ranked_securities
#                       WHERE sector_rank <= 5;
#
#                     > CREATE VIEW recent_trades_by_account AS
#                       SELECT a.account_id, s.ticker, t.quantity, t.price, t.trade_date
#                       FROM trades t
#                       JOIN accounts a ON t.account_id = a.account_id
#                       JOIN securities s ON t.security_id = s.security_id
#                       WHERE t.trade_date > NOW() - INTERVAL '1 day';
#
#                     > CREATE VIEW order_fulfillment_rates AS
#                       SELECT c.customer_id, c.name,
#                              COUNT(o.order_id) AS total_orders,
#                              SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS fulfilled_orders,
#                              (SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id)) AS fulfillment_rate
#                       FROM customers c
#                       JOIN accounts a ON c.customer_id = a.customer_id
#                       JOIN orders o ON a.account_id = o.account_id
#                       GROUP BY c.customer_id, c.name;
#
#                     > CREATE VIEW sector_order_activity AS
#                       SELECT s.sector, COUNT(o.order_id) AS order_count,
#                              SUM(o.quantity) AS total_quantity,
#                              AVG(o.limit_price) AS avg_limit_price
#                       FROM orders o
#                       JOIN securities s ON o.security_id = s.security_id
#                       GROUP BY s.sector;
#                     """
#                 ),
#                 # TODO: Inserts, Selects
#                 LoadPhase(
#                     duration=120,
#                     actions=[],
#                 ),
#             ]
#         )


class Statistics:
    def __init__(self, times: list[float], durations: list[float]):
        assert len(times) == len(durations)
        self.queries: int = len(times)
        self.qps: float = len(times) / max(times)
        self.max: float = max(durations)
        self.min: float = min(durations)
        self.avg = numpy.mean(durations)
        self.p50 = numpy.median(durations)
        self.p95 = numpy.percentile(durations, 95)
        self.p99 = numpy.percentile(durations, 99)
        self.std = numpy.std(durations, ddof=1)
        self.slope = numpy.polyfit(times, durations, 1)[0]

    def __str__(self) -> str:
        return f"""  queries: {self.queries:>5}
  qps: {self.qps:>7.2f}
  max: {self.max:>7.2f}ms
  min: {self.min:>7.2f}ms
  avg: {self.avg:>7.2f}ms
  p50: {self.p50:>7.2f}ms
  p95: {self.p95:>7.2f}ms
  p99: {self.p99:>7.2f}ms
  std: {self.std:>7.2f}ms
  slope: {self.slope:>7.2f}"""


def report(
    mz_string: str,
    scenario: Scenario,
    measurements: collections.defaultdict[str, list[Measurement]],
    start_time: float,
    guarantees: bool,
) -> list[TestFailureDetails]:
    scenario_name = type(scenario).__name__
    failures = []
    plt.figure(figsize=(10, 6))
    for key, m in measurements.items():
        times: list[float] = [x.timestamp - start_time for x in m]
        durations: list[float] = [x.duration * 1000 for x in m]
        stats = Statistics(times, durations)
        plt.scatter(times, durations, label=key, marker=MarkerStyle("+"))
        print(f"Statistics for {key}:\n{stats}")
        if key in scenario.guarantees and guarantees:
            for stat, guarantee in scenario.guarantees[key].items():
                duration = getattr(stats, stat)
                less_than = stat == "qps"
                if duration < guarantee if less_than else duration > guarantee:
                    failure = f"Scenario {scenario_name} failed: {key}: {stat}: {duration:.2f} {'<' if less_than else '>'} {guarantee:.2f}"
                    print(failure)
                    failures.append(
                        TestFailureDetails(
                            message=failure,
                            details=str(stats),
                            test_class_name_override=scenario_name,
                        )
                    )
                else:
                    print(
                        f"Scenario {scenario_name} succeeded: {key}: {stat}: {duration:.2f} {'>=' if less_than else '<='} {guarantee:.2f}"
                    )

    plt.xlabel("time [s]")
    plt.ylabel("latency [ms]")
    plt.title(f"{scenario_name} against {mz_string}")
    plt.legend(loc="best")
    plt.grid(True)
    plt.ylim(bottom=0)
    plot_path = f"plots/{scenario_name}.png"
    plt.savefig(MZ_ROOT / plot_path, dpi=300)
    if buildkite.is_in_buildkite():
        buildkite.upload_artifact(plot_path, cwd=MZ_ROOT)
        print(f"+++ Plot for {scenario_name}")
        print(
            buildkite.inline_image(
                f"artifact://{plot_path}", f"Plot for {scenario_name}"
            )
        )
    else:
        print(f"Saving plot to {plot_path}")

    return failures


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.silent = True

    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )

    parser.add_argument(
        "--guarantees",
        action="store_true",
        default=True,
        help="Check guarantees defined by test scenarios",
    )

    parser.add_argument(
        "--size",
        metavar="N",
        type=int,
        default=1,
        help="default SIZE",
    )

    parser.add_argument(
        "--scenario",
        metavar="SCENARIO",
        action="append",
        type=str,
        help="Scenario to run",
    )

    parser.add_argument(
        "--load-phase-duration",
        type=int,
        help="Override durations of LoadPhases",
    )

    parser.add_argument(
        "--periodic-dist",
        nargs=2,
        metavar=("action", "per_second"),
        action="append",
        help="Override periodic distribution for an action with specified name",
    )

    parser.add_argument("--mz-url", type=str, help="Remote Mz instance to run against")

    args = parser.parse_args()

    if args.scenario:
        for scenario in args.scenario:
            assert scenario in globals(), f"scenario {scenario} does not exist"
        scenarios = [globals()[scenario] for scenario in args.scenario]
    else:
        scenarios = all_subclasses(Scenario)

    service_names = ["materialized", "postgres", "mysql"] + (
        ["redpanda"] if args.redpanda else ["zookeeper", "kafka", "schema-registry"]
    )

    failures: list[TestFailureDetails] = []

    if args.mz_url:
        with c.override(
            Testdrive(
                no_reset=True,
                materialize_url=args.mz_url,
                seed=1,
                no_consistency_checks=True,
            )
        ):
            target = parse_pg_conn_string(args.mz_url)
            c.up("testdrive", persistent=True)
            conn_infos = {"materialized": target}
            conn = target.connect()
            with conn.cursor() as cur:
                cur.execute("SELECT mz_version()")
                mz_version = cur.fetchall()[0][0]
            conn.close()
            mz_string = f"{mz_version} ({target.host})"
    else:
        with c.override(
            Materialized(
                default_size=args.size,
                soft_assertions=False,
                external_cockroach=True,
                external_minio=True,
                sanity_restart=False,
                additional_system_parameter_defaults={
                    "enable_statement_lifecycle_logging": "false",
                    "statement_logging_default_sample_rate": "0",
                    "statement_logging_max_sample_rate": "0",
                },
            )
        ):
            c.up(*service_names)
            c.up("testdrive", persistent=True)
            c.sql(
                "ALTER SYSTEM SET max_connections = 1000000",
                user="mz_system",
                port=6877,
            )

            mz_version = c.query_mz_version()
            mz_string = f"{mz_version} (docker)"
            conn_infos = {
                "materialized": PgConnInfo(
                    user="materialize",
                    database="materialize",
                    host="127.0.0.1",
                    port=c.default_port("materialized"),
                ),
                "mz_system": PgConnInfo(
                    user="mz_system",
                    database="materialize",
                    host="127.0.0.1",
                    port=c.port("materialized", 6877),
                ),
                "postgres": PgConnInfo(
                    user="postgres",
                    password="postgres",
                    database="postgres",
                    host="127.0.0.1",
                    port=c.default_port("postgres"),
                ),
            }

    c.silent = True

    for scenario_class in scenarios:
        scenario_name = scenario_class.__name__
        print(f"--- Running scenario {scenario_name}")
        state = State(
            measurements=collections.defaultdict(list),
            load_phase_duration=args.load_phase_duration,
            periodic_dists={pd[0]: int(pd[1]) for pd in args.periodic_dist or []},
        )
        scenario = scenario_class(c, conn_infos)
        scenario.setup(c, conn_infos)
        start_time = time.time()
        Path(MZ_ROOT / "plots").mkdir(parents=True, exist_ok=True)
        try:
            # Don't let the garbage collector interfere with our measurements
            gc.disable()
            scenario.run(c, state)
            scenario.teardown()
            gc.collect()
            gc.enable()
        finally:
            failures.extend(
                report(
                    mz_string, scenario, state.measurements, start_time, args.guarantees
                )
            )

    if failures:
        raise FailedTestExecutionError(errors=failures)

    # TODO: Choose an existing cluster name (for remote mz)
    # TODO: For CI start comparing against older version
