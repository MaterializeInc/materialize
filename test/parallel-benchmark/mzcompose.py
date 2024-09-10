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

import gc
import os
import queue
import random
import threading
import time
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent

import matplotlib.pyplot as plt
import numpy
import pg8000
from matplotlib.markers import MarkerStyle

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka as KafkaService
from materialize.mzcompose.services.kgen import Kgen as KgenService
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config,
)
from materialize.test_analytics.data.parallel_benchmark import (
    parallel_benchmark_result_storage,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb
from materialize.util import PgConnInfo, all_subclasses, parse_pg_conn_string
from materialize.version_list import resolve_ancestor_image_tag

PARALLEL_BENCHMARK_FRAMEWORK_VERSION = "1.1.0"


def known_regression(scenario: str, other_tag: str) -> bool:
    return False


REGRESSION_THRESHOLDS = {
    "queries": None,
    "qps": 1.2,
    "max": None,
    "min": None,
    "avg": 1.2,
    "p50": 1.2,
    "p95": 1.3,
    "p99": None,
    "p99_9": None,
    "p99_99": None,
    "p99_999": None,
    "p99_9999": None,
    "p99_99999": None,
    "p99_999999": None,
    "std": None,
    "slope": None,
}

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
    Testdrive(no_reset=True, seed=1),
    Mz(app_password=""),
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
    measurements: defaultdict[str, list[Measurement]]
    load_phase_duration: int | None
    periodic_dists: dict[str, int]


def execute_query(cur: pg8000.Cursor, query: str) -> None:
    while True:
        try:
            cur.execute(query)
            break
        except Exception as e:
            if "deadlock detected" in str(e):
                print(f"Deadlock detected, retrying: {query}")
            elif (
                "timed out before ingesting the source's visible frontier when real-time-recency query issued"
                in str(e)
            ):
                print("RTR timeout, ignoring")
                break
            else:
                raise


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
            execute_query(cur, self.query)
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
        self.conn_info = conn_info
        self.strict_serializable = strict_serializable
        self._reconnect()

    def _reconnect(self) -> None:
        self.conn = self.conn_info.connect()
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.cur.execute(
            f"SET TRANSACTION_ISOLATION TO '{'STRICT SERIALIZABLE' if self.strict_serializable else 'SERIALIZABLE'}'"
        )

    def _run(self, conns: queue.Queue):
        execute_query(self.cur, self.query)

    def __str__(self) -> str:
        return f"{self.query} (reuse connection)"


class PooledQuery(Action):
    def __init__(self, query: str, conn_info: PgConnInfo):
        self.query = query
        self.conn_info = conn_info

    def _run(self, conns: queue.Queue):
        conn = conns.get()
        with conn.cursor() as cur:
            execute_query(cur, self.query)
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
    report_regressions: bool
    action: Action

    def run(
        self,
        duration: int,
        jobs: queue.Queue,
        conns: queue.Queue,
        state: State,
    ) -> None:
        raise NotImplementedError


class OpenLoop(PhaseAction):
    def __init__(
        self, action: Action, dist: Distribution, report_regressions: bool = True
    ):
        self.action = action
        self.dist = dist
        self.report_regressions = report_regressions

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
    def __init__(self, action: Action, report_regressions: bool = True):
        self.action = action
        self.report_regressions = report_regressions

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
    regression_thresholds: dict[str, dict[str, float]]
    jobs: queue.Queue
    conns: queue.Queue
    thread_pool: list[threading.Thread]
    version: str = "1.0.0"

    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        raise NotImplementedError

    @classmethod
    def name(cls) -> str:
        return cls.__name__

    def init(
        self,
        phases: list[Phase],
        thread_pool_size: int = 5000,
        conn_pool_size: int = 0,
        guarantees: dict[str, dict[str, float]] = {},
        regression_thresholds: dict[str, dict[str, float]] = {},
    ):
        self.phases = phases
        self.thread_pool_size = thread_pool_size
        self.conn_pool_size = conn_pool_size
        self.guarantees = guarantees
        self.regression_thresholds = regression_thresholds
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

                    > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

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
                    DROP PUBLICATION IF EXISTS mz_source;
                    DROP TABLE IF EXISTS t1 CASCADE;
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t1 (f1 INTEGER);
                    ALTER TABLE t1 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source FOR ALL TABLES;

                    > CREATE SOURCE mz_source
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
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
                    DROP PUBLICATION IF EXISTS mz_source2;
                    DROP TABLE IF EXISTS t2 CASCADE;
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t2 (f1 INTEGER);
                    ALTER TABLE t2 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source2 FOR ALL TABLES;

                    > CREATE SOURCE mz_source2
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source2')
                      FOR ALL TABLES;

                    > CREATE MATERIALIZED VIEW mv_sum AS
                      SELECT COUNT(*) FROM t2;

                    > CREATE DEFAULT INDEX ON mv_sum;
                    """
                ),
                LoadPhase(
                    duration=120,
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
                            report_regressions=False,
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


# TODO Try these scenarios' scaling behavior against cc sizes (locally and remote)


class CommandQueryResponsibilitySegregation(Scenario):
    # TODO: Have one Postgres source with many inserts/updates/deletes and multiple complex materialized view on top of it, read from Mz
    # This should be blocked by materialized view performance
    # We probably need strict serializable to make sure results stay up to date
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
                    DROP PUBLICATION IF EXISTS mz_cqrs_source;
                    DROP TABLE IF EXISTS t1 CASCADE;
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t1 (id INTEGER, name TEXT, date TIMESTAMPTZ);
                    ALTER TABLE t1 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_cqrs_source FOR ALL TABLES;

                    > CREATE SOURCE mz_cqrs_source
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_cqrs_source')
                      FOR ALL TABLES;

                    > CREATE MATERIALIZED VIEW mv_cqrs AS
                      SELECT t1.date, SUM(t1.id) FROM t1 JOIN t1 AS t2 ON true JOIN t1 AS t3 ON true JOIN t1 AS t4 ON true GROUP BY t1.date;
                    > CREATE DEFAULT INDEX ON mv_cqrs;
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "INSERT INTO t1 VALUES (1, '1', now())",
                                # "INSERT INTO t1 (id, name, date) SELECT i, i::text, now() FROM generate_series(1, 1000) AS s(i);",
                                conn_infos["postgres"],
                                strict_serializable=False,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "UPDATE t1 SET id = id + 1",
                                conn_infos["postgres"],
                                strict_serializable=False,
                            ),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "DELETE FROM t1 WHERE date < now() - INTERVAL '10 seconds'",
                                conn_infos["postgres"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=0.1),
                        ),
                    ]
                    + [
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM mv_cqrs",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        )
                    ],
                ),
            ],
        )


class OperationalDataStore(Scenario):
    # TODO: Get data from multiple sources with high volume (webhook source, Kafka, Postgres, MySQL), export to Kafka Sink and Subscribes
    # This should be blocked by read/write performance
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
                    DROP PUBLICATION IF EXISTS mz_source;
                    DROP TABLE IF EXISTS t1 CASCADE;
                    ALTER USER postgres WITH replication;
                    CREATE TABLE t1 (f1 INTEGER);
                    ALTER TABLE t1 REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source FOR ALL TABLES;

                    > CREATE SOURCE mz_source
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
                      FOR ALL TABLES;

                    > CREATE MATERIALIZED VIEW mv_sum AS
                      SELECT COUNT(*) FROM t1;

                    > CREATE DEFAULT INDEX ON mv_sum;

                    # TODO: Other sources
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "INSERT INTO t1 (f1) SELECT i FROM generate_series(1, 50000) AS s(i);",
                                conn_infos["postgres"],
                                strict_serializable=False,
                            ),
                            dist=Periodic(per_second=10),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SET REAL_TIME_RECENCY TO TRUE; SELECT * FROM mv_sum",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                    ],
                ),
            ],
        )


class OperationalDataMesh(Scenario):
    # TODO: One Kafka source/sink, one data source, many materialized views, all exported to Kafka
    # This should be blocked by the number of source/sink combinations
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    $ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }
                    $ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

                    $ kafka-create-topic topic=kafka-mesh

                    $ kafka-ingest format=avro topic=kafka-mesh key-format=avro key-schema=${keyschema} schema=${schema} repeat=10
                    {"f1": 1} {"f2": ${kafka-ingest.iteration} }

                    > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

                    > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                      URL '${testdrive.schema-registry-url}');

                    > CREATE SOURCE kafka_mesh
                      FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-mesh-${testdrive.seed}')
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE UPSERT;

                    > CREATE MATERIALIZED VIEW kafka_mesh_mv AS SELECT * FROM kafka_mesh;

                    > CREATE DEFAULT INDEX ON kafka_mesh_mv;

                    > CREATE SINK sink FROM kafka_mesh_mv
                      INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink')
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE DEBEZIUM;

                    $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="10s"

                    #$ kafka-verify-topic sink=sink

                    > CREATE SOURCE sink_source
                      FROM KAFKA CONNECTION kafka_conn (TOPIC 'sink')
                      FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                      ENVELOPE NONE;
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
                                $ kafka-ingest format=avro topic=kafka-mesh key-format=avro key-schema=${keyschema} schema=${schema} repeat=100000
                                {"f1": 1} {"f2": ${kafka-ingest.iteration} }
                                """,
                                c,
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        ClosedLoop(
                            action=StandaloneQuery(
                                # TODO: This doesn't actually measure rtr all the way
                                "SET REAL_TIME_RECENCY TO TRUE;  SELECT * FROM sink_source",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                    ],
                ),
            ],
        )


class ReadReplicaBenchmark(Scenario):
    # We might want to run a full version of rr-bench instead, this is not a
    # very realistic representation of it but might already help us catch some
    # regressions: https://github.com/MaterializeIncLabs/rr-bench
    def __init__(self, c: Composition, conn_infos: dict[str, PgConnInfo]):
        self.init(
            [
                TdPhase(
                    """
                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    DROP TABLE IF EXISTS customers CASCADE;
                    DROP TABLE IF EXISTS accounts CASCADE;
                    DROP TABLE IF EXISTS securities CASCADE;
                    DROP TABLE IF EXISTS trades CASCADE;
                    DROP TABLE IF EXISTS orders CASCADE;
                    DROP TABLE IF EXISTS market_data CASCADE;
                    CREATE TABLE customers (customer_id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, address VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE accounts (account_id SERIAL PRIMARY KEY, customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE, account_type VARCHAR(50) NOT NULL, balance DECIMAL(18, 2) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE securities (security_id SERIAL PRIMARY KEY, ticker VARCHAR(10) NOT NULL, name VARCHAR(255), sector VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE trades (trade_id SERIAL PRIMARY KEY, account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE, security_id INT REFERENCES securities(security_id) ON DELETE CASCADE, trade_type VARCHAR(10) NOT NULL CHECK (trade_type IN ('buy', 'sell')), quantity INT NOT NULL, price DECIMAL(18, 4) NOT NULL, trade_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE orders (order_id SERIAL PRIMARY KEY, account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE, security_id INT REFERENCES securities(security_id) ON DELETE CASCADE, order_type VARCHAR(10) NOT NULL CHECK (order_type IN ('buy', 'sell')), quantity INT NOT NULL, limit_price DECIMAL(18, 4), status VARCHAR(10) NOT NULL CHECK (status IN ('pending', 'completed', 'canceled')), order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE market_data (market_data_id SERIAL PRIMARY KEY, security_id INT REFERENCES securities(security_id) ON DELETE CASCADE, price DECIMAL(18, 4) NOT NULL, volume INT NOT NULL, market_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    DROP PUBLICATION IF EXISTS mz_source3;
                    ALTER USER postgres WITH replication;
                    ALTER TABLE customers REPLICA IDENTITY FULL;
                    ALTER TABLE accounts REPLICA IDENTITY FULL;
                    ALTER TABLE securities REPLICA IDENTITY FULL;
                    ALTER TABLE trades REPLICA IDENTITY FULL;
                    ALTER TABLE orders REPLICA IDENTITY FULL;
                    ALTER TABLE market_data REPLICA IDENTITY FULL;
                    CREATE PUBLICATION mz_source3 FOR ALL TABLES;
                    INSERT INTO customers (customer_id, name, address, created_at) VALUES (1, 'Elizabeth Ebert', 'Raleigh Motorway', '2024-09-11 15:27:44'), (2, 'Kelley Kuhlman', 'Marvin Circle', '2024-09-11 15:27:44'), (3, 'Frieda Waters', 'Jessy Roads', '2024-09-11 15:27:44'), (4, 'Ian Thiel', 'Rodriguez Squares', '2024-09-11 15:27:44'), (5, 'Clementine Hauck', 'Allen Junction', '2024-09-11 15:27:44'), (6, 'Caesar White', 'Cheyenne Green', '2024-09-11 15:27:44'), (7, 'Hudson Wintheiser', 'Wiza Plain', '2024-09-11 15:27:44'), (8, 'Kendall Marks', 'Kuhn Ports', '2024-09-11 15:27:44'), (9, 'Haley Schneider', 'Erwin Cliffs', '2024-09-11 15:27:44');
                    INSERT INTO accounts (account_id, customer_id, account_type, balance, created_at) VALUES (1, 1, 'Brokerage', 796.9554824679382, '2024-09-11 15:27:44'), (2, 2, 'Checking', 7808.991622105239, '2024-09-11 15:27:44'), (3, 3, 'Checking', 4540.988288421537, '2024-09-11 15:27:44'), (4, 4, 'Brokerage', 4607.257663873947, '2024-09-11 15:27:44'), (5, 5, 'Savings', 9105.123905180497, '2024-09-11 15:27:44'), (6, 6, 'Brokerage', 6072.871742690154, '2024-09-11 15:27:44'), (7, 7, 'Savings', 7374.831288928072, '2024-09-11 15:27:44'), (8, 8, 'Brokerage', 6554.8717824477, '2024-09-11 15:27:44'), (9, 9, 'Checking', 2629.393130856843, '2024-09-11 15:27:44');
                    INSERT INTO securities (security_id, ticker, name, sector, created_at) VALUES (1, 'Y1Fu', 'Goldner and Bechtelar LLC', 'Printing', '2024-09-11 15:27:44'), (2, 'MOF5', 'Adams and Homenick Inc', 'Market Research', '2024-09-11 15:27:44'), (3, 'Oo09', 'Tillman and Wilkinson Inc', 'Apparel & Fashion', '2024-09-11 15:27:44'), (4, 'zmAy', 'Toy and Williamson LLC', 'International Affairs', '2024-09-11 15:27:44'), (5, 'ORyo', 'Olson and Prohaska and Sons', 'Textiles', '2024-09-11 15:27:44'), (6, 'Fpn2', 'Gusikowski and Schinner Inc', 'Think Tanks', '2024-09-11 15:27:44'), (7, 'gTv2', 'Davis and Sons', 'Package / Freight Delivery', '2024-09-11 15:27:44'), (8, '38RH', 'Johns and Braun Group', 'Public Safety', '2024-09-11 15:27:44'), (9, 'Ym5u', 'Goyette Group', 'Cosmetics', '2024-09-11 15:27:44');
                    INSERT INTO trades (trade_id, account_id, security_id, trade_type, quantity, price, trade_date) VALUES (1, 1, 1, 'buy', 337, 464.45448203724607, '2024-09-11 15:27:44'), (2, 2, 2, 'buy', 312, 299.91031464748926, '2024-09-11 15:27:44'), (3, 3, 3, 'buy', 874, 338.5711431239059, '2024-09-11 15:27:44'), (4, 4, 4, 'buy', 523, 356.4236193709552, '2024-09-11 15:27:44'), (5, 5, 5, 'sell', 251, 354.6345239481285, '2024-09-11 15:27:44'), (6, 6, 6, 'buy', 810, 437.6742610108604, '2024-09-11 15:27:44'), (7, 7, 7, 'sell', 271, 116.70199857394587, '2024-09-11 15:27:44'), (8, 8, 8, 'buy', 84, 415.0658279744514, '2024-09-11 15:27:44'), (9, 9, 9, 'sell', 763, 312.3375311232852, '2024-09-11 15:27:44');
                    INSERT INTO orders (order_id, account_id, security_id, order_type, quantity, limit_price, status, order_date) VALUES (1, 1, 1, 'buy', 207, 456.0, 'completed', '2024-09-11 15:27:44'), (2, 2, 2, 'buy', 697, 515.0, 'canceled', '2024-09-11 15:27:44'), (3, 3, 3, 'buy', 789, 198.0, 'completed', '2024-09-11 15:27:44'), (4, 4, 4, 'sell', 280, 505.0, 'completed', '2024-09-11 15:27:44'), (5, 5, 5, 'buy', 368, 966.0, 'pending', '2024-09-11 15:27:44'), (6, 6, 6, 'buy', 439, 7.0, 'completed', '2024-09-11 15:27:44'), (7, 7, 7, 'sell', 345, 972.0, 'completed', '2024-09-11 15:27:44'), (8, 8, 8, 'sell', 867, 968.0, 'completed', '2024-09-11 15:27:44'), (9, 9, 9, 'sell', 472, 534.0, 'completed', '2024-09-11 15:27:44');
                    INSERT INTO market_data (market_data_id, security_id, price, volume, market_date) VALUES (1, 1, 134.07573356469547, 17326, '2024-09-11 15:27:44'), (2, 2, 107.2440801092168, 63229, '2024-09-11 15:27:44'), (3, 3, 498.13544872323644, 69305, '2024-09-11 15:27:44'), (4, 4, 194.24235075387645, 45224, '2024-09-11 15:27:44'), (5, 5, 352.2334739296001, 79796, '2024-09-11 15:27:44'), (6, 6, 241.83322476711587, 44295, '2024-09-11 15:27:44'), (7, 7, 226.93537920792713, 23212, '2024-09-11 15:27:44'), (8, 8, 169.2983285300141, 96883, '2024-09-11 15:27:44'), (9, 9, 331.36982054471935, 5651, '2024-09-11 15:27:44');

                    > DROP SECRET IF EXISTS pgpass CASCADE
                    > CREATE SECRET pgpass AS 'postgres'
                    > CREATE CONNECTION pg TO POSTGRES (
                        HOST postgres,
                        DATABASE postgres,
                        USER postgres,
                        PASSWORD SECRET pgpass
                      )
                    > CREATE SOURCE mz_source3
                      FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source3')
                      FOR ALL TABLES;

                    > CREATE VIEW customer_portfolio AS
                      SELECT c.customer_id, c.name, a.account_id, s.ticker, s.name AS security_name,
                           SUM(t.quantity * t.price) AS total_value
                      FROM customers c
                      JOIN accounts a ON c.customer_id = a.customer_id
                      JOIN trades t ON a.account_id = t.account_id
                      JOIN securities s ON t.security_id = s.security_id
                      GROUP BY c.customer_id, c.name, a.account_id, s.ticker, s.name;

                    > CREATE VIEW top_performers AS
                      WITH trade_volume AS (
                          SELECT security_id, SUM(quantity) AS total_traded_volume
                          FROM trades
                          GROUP BY security_id
                          ORDER BY SUM(quantity) DESC
                          LIMIT 10
                      )
                      SELECT s.ticker, s.name, t.total_traded_volume
                      FROM trade_volume t
                      JOIN securities s USING (security_id);

                    > CREATE VIEW market_overview AS
                      SELECT s.sector, AVG(md.price) AS avg_price, SUM(md.volume) AS total_volume,
                             MAX(md.market_date) AS last_update
                      FROM securities s
                      LEFT JOIN market_data md ON s.security_id = md.security_id
                      GROUP BY s.sector
                      HAVING MAX(md.market_date) + INTERVAL '5 minutes' > mz_now() ;

                    > CREATE VIEW recent_large_trades AS
                      SELECT t.trade_id, a.account_id, s.ticker, t.quantity, t.price, t.trade_date
                      FROM trades t
                      JOIN accounts a ON t.account_id = a.account_id
                      JOIN securities s ON t.security_id = s.security_id
                      WHERE t.quantity > (SELECT AVG(quantity) FROM trades) * 5
                        AND t.trade_date + INTERVAL '1 hour' > mz_now();


                    > CREATE VIEW customer_order_book AS
                      SELECT c.customer_id, c.name, COUNT(o.order_id) AS open_orders,
                             SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS completed_orders
                      FROM customers c
                      JOIN accounts a ON c.customer_id = a.customer_id
                      JOIN orders o ON a.account_id = o.account_id
                      GROUP BY c.customer_id, c.name;

                    > CREATE VIEW sector_performance AS
                      SELECT s.sector, AVG(t.price) AS avg_trade_price, COUNT(t.trade_id) AS trade_count,
                             SUM(t.quantity) AS total_volume
                      FROM trades t
                      JOIN securities s ON t.security_id = s.security_id
                      GROUP BY s.sector;

                    > CREATE VIEW account_activity_summary AS
                      SELECT a.account_id, COUNT(t.trade_id) AS trade_count,
                             SUM(t.quantity * t.price) AS total_trade_value,
                             MAX(t.trade_date) AS last_trade_date
                      FROM accounts a
                      LEFT JOIN trades t ON a.account_id = t.account_id
                      GROUP BY a.account_id;

                    > CREATE VIEW daily_market_movements AS
                      WITH last_two_days AS (
                          SELECT grp.security_id, price, market_date
                          FROM (SELECT DISTINCT security_id FROM market_data) grp,
                          LATERAL (
                              SELECT md.security_id, md.price, md.market_date
                              FROM market_data md
                              WHERE md.security_id = grp.security_id AND md.market_date + INTERVAL '1 day' > mz_now()
                              ORDER BY md.market_date DESC
                              LIMIT 2
                          )
                      ),
                      stg AS (
                          SELECT security_id, today.price AS current_price, yesterday.price AS previous_price, today.market_date
                          FROM last_two_days today
                          LEFT JOIN last_two_days yesterday USING (security_id)
                          WHERE today.market_date > yesterday.market_date
                      )
                      SELECT
                          security_id,
                          ticker,
                          name,
                          current_price,
                          previous_price,
                          current_price - previous_price AS price_change,
                          market_date
                      FROM stg
                      JOIN securities USING (security_id);

                    > CREATE VIEW high_value_customers AS
                      SELECT c.customer_id, c.name, SUM(a.balance) AS total_balance
                      FROM customers c
                      JOIN accounts a ON c.customer_id = a.customer_id
                      GROUP BY c.customer_id, c.name
                      HAVING SUM(a.balance) > 1000000;

                    > CREATE VIEW pending_orders_summary AS
                      SELECT s.ticker, s.name, COUNT(o.order_id) AS pending_order_count,
                             SUM(o.quantity) AS pending_volume,
                             AVG(o.limit_price) AS avg_limit_price
                      FROM orders o
                      JOIN securities s ON o.security_id = s.security_id
                      WHERE o.status = 'pending'
                      GROUP BY s.ticker, s.name;

                    > CREATE VIEW trade_volume_by_hour AS
                      SELECT EXTRACT(HOUR FROM t.trade_date) AS trade_hour,
                             COUNT(t.trade_id) AS trade_count,
                             SUM(t.quantity) AS total_quantity
                      FROM trades t
                      GROUP BY EXTRACT(HOUR FROM t.trade_date);

                    > CREATE VIEW top_securities_by_sector AS
                      SELECT grp.sector, ticker, name, total_volume
                      FROM (SELECT DISTINCT sector FROM securities) grp,
                      LATERAL (
                          SELECT s.sector, s.ticker, s.name, SUM(t.quantity) AS total_volume
                          FROM trades t
                          JOIN securities s ON t.security_id = s.security_id
                          WHERE s.sector = grp.sector
                          GROUP BY s.sector, s.ticker, s.name
                          ORDER BY total_volume DESC
                          LIMIT 5
                      );


                    > CREATE VIEW recent_trades_by_account AS
                      SELECT a.account_id, s.ticker, t.quantity, t.price, t.trade_date
                      FROM trades t
                      JOIN accounts a ON t.account_id = a.account_id
                      JOIN securities s ON t.security_id = s.security_id
                      WHERE t.trade_date + INTERVAL '1 day'> mz_now();

                    > CREATE VIEW order_fulfillment_rates AS
                      SELECT c.customer_id, c.name,
                             COUNT(o.order_id) AS total_orders,
                             SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS fulfilled_orders,
                             (SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id)) AS fulfillment_rate
                      FROM customers c
                      JOIN accounts a ON c.customer_id = a.customer_id
                      JOIN orders o ON a.account_id = o.account_id
                      GROUP BY c.customer_id, c.name;

                    > CREATE VIEW sector_order_activity AS
                      SELECT s.sector, COUNT(o.order_id) AS order_count,
                             SUM(o.quantity) AS total_quantity,
                             AVG(o.limit_price) AS avg_limit_price
                      FROM orders o
                      JOIN securities s ON o.security_id = s.security_id
                      GROUP BY s.sector;

                    > CREATE INDEX ON securities (security_id);
                    > CREATE INDEX ON accounts (account_id);
                    > CREATE INDEX ON customers (customer_id);
                    > CREATE INDEX ON customer_portfolio (customer_id);
                    > CREATE INDEX ON top_performers (ticker);
                    > CREATE INDEX ON market_overview (sector);
                    > CREATE INDEX ON recent_large_trades (trade_id);
                    > CREATE INDEX ON customer_order_book (customer_id);
                    > CREATE INDEX ON account_activity_summary (account_id);
                    > CREATE INDEX ON daily_market_movements (security_id);
                    > CREATE INDEX ON high_value_customers (customer_id);
                    > CREATE INDEX ON pending_orders_summary (ticker);
                    > CREATE INDEX ON trade_volume_by_hour (trade_hour);
                    > CREATE INDEX ON top_securities_by_sector (sector);
                    > CREATE INDEX ON recent_trades_by_account (account_id);
                    > CREATE INDEX ON order_fulfillment_rates (customer_id);
                    > CREATE INDEX ON sector_order_activity (sector);
                    > CREATE INDEX ON sector_performance (sector);
                    """
                ),
                LoadPhase(
                    duration=120,
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE customers SET address = 'foo' WHERE customer_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE accounts SET balance = balance + 1 WHERE customer_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE trades SET price = price + 1 WHERE trade_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE orders SET status = 'pending', limit_price = limit_price + 1 WHERE order_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        OpenLoop(
                            action=StandaloneQuery(
                                "UPDATE market_data SET price = price + 1, volume = volume + 1, market_date = CURRENT_TIMESTAMP WHERE market_data_id = 1",
                                conn_infos["postgres"],
                            ),
                            dist=Periodic(per_second=1),
                        ),
                        # TODO deletes
                        # DELETE FROM accounts WHERE account_id = $1
                        # DELETE FROM securities WHERE security_id = $1
                        # DELETE FROM trades WHERE trade_id = $1
                        # DELETE FROM orders WHERE order_id = $1
                        # DELETE FROM market_data WHERE market_data_id = $1
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM customer_portfolio WHERE customer_id = 1",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM top_performers",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM market_overview WHERE sector = 'Printing'",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM recent_large_trades WHERE account_id = 1",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM customer_order_book WHERE customer_id = 1",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "SELECT * FROM sector_performance WHERE sector = 'Printing'",
                                conn_infos["materialized"],
                                strict_serializable=True,
                            ),
                        ),
                        # TODO: More selects
                        # SELECT * FROM account_activity_summary WHERE account_id = $1
                        # SELECT * FROM daily_market_movements WHERE security_id = $1
                        # SELECT * FROM high_value_customers
                        # SELECT * FROM pending_orders_summary WHERE ticker = $1
                        # SELECT * FROM trade_volume_by_hour
                        # SELECT * FROM top_securities_by_sector WHERE sector = $1
                        # SELECT * FROM recent_trades_by_account WHERE account_id = $1
                        # SELECT * FROM order_fulfillment_rates WHERE customer_id = $1
                        # SELECT * FROM sector_order_activity WHERE sector = $1
                        # SELECT * FROM cascading_order_cancellation_alert
                    ],
                ),
            ]
        )


class Statistics:
    def __init__(self, times: list[float], durations: list[float]):
        assert len(times) == len(durations)
        self.queries: int = len(times)
        self.qps: float = len(times) / max(times)
        self.max: float = max(durations)
        self.min: float = min(durations)
        self.avg: float = float(numpy.mean(durations))
        self.p50: float = float(numpy.median(durations))
        self.p95: float = float(numpy.percentile(durations, 95))
        self.p99: float = float(numpy.percentile(durations, 99))
        self.p99_9: float = float(numpy.percentile(durations, 99.9))
        self.p99_99: float = float(numpy.percentile(durations, 99.99))
        self.p99_999: float = float(numpy.percentile(durations, 99.999))
        self.p99_9999: float = float(numpy.percentile(durations, 99.9999))
        self.p99_99999: float = float(numpy.percentile(durations, 99.99999))
        self.p99_999999: float = float(numpy.percentile(durations, 99.999999))
        self.std: float = float(numpy.std(durations, ddof=1))
        self.slope: float = float(numpy.polyfit(times, durations, 1)[0])

    def __str__(self) -> str:
        return f"""  queries: {self.queries:>5}
  qps: {self.qps:>7.2f}
  min: {self.min:>7.2f}ms
  avg: {self.avg:>7.2f}ms
  p50: {self.p50:>7.2f}ms
  p95: {self.p95:>7.2f}ms
  p99: {self.p99:>7.2f}ms
  max: {self.max:>7.2f}ms
  std: {self.std:>7.2f}ms
  slope: {self.slope:>5.4f}"""

    def __dir__(self) -> list[str]:
        return [
            "queries",
            "qps",
            "max",
            "min",
            "avg",
            "p50",
            "p95",
            "p99",
            "p99_9",
            "p99_99",
            "p99_999",
            "p99_9999",
            "p99_99999",
            "p99_999999",
            "std",
            "slope",
        ]


def report(
    mz_string: str,
    scenario: Scenario,
    measurements: dict[str, list[Measurement]],
    start_time: float,
    guarantees: bool,
    suffix: str,
) -> tuple[dict[str, Statistics], list[TestFailureDetails]]:
    scenario_name = type(scenario).name()
    stats: dict[str, Statistics] = {}
    failures: list[TestFailureDetails] = []
    plt.figure(figsize=(10, 6))
    for key, m in measurements.items():
        times: list[float] = [x.timestamp - start_time for x in m]
        durations: list[float] = [x.duration * 1000 for x in m]
        stats[key] = Statistics(times, durations)
        plt.scatter(times, durations, label=key, marker=MarkerStyle("+"))
        print(f"Statistics for {key}:\n{stats[key]}")
        if key in scenario.guarantees and guarantees:
            for stat, guarantee in scenario.guarantees[key].items():
                duration = getattr(stats[key], stat)
                less_than = less_than_is_regression(stat)
                if duration < guarantee if less_than else duration > guarantee:
                    failure = f"Scenario {scenario_name} failed: {key}: {stat}: {duration:.2f} {'<' if less_than else '>'} {guarantee:.2f}"
                    print(failure)
                    failures.append(
                        TestFailureDetails(
                            message=failure,
                            details=str(stats[key]),
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
    plot_path = f"plots/{scenario_name}_{suffix}.png"
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

    return stats, failures


def run_once(
    c: Composition,
    scenarios: list[type[Scenario]],
    service_names: list[str],
    tag: str | None,
    params: str | None,
    args,
    suffix: str,
) -> tuple[dict[Scenario, dict[str, Statistics]], list[TestFailureDetails]]:
    stats: dict[Scenario, dict[str, Statistics]] = {}
    failures: list[TestFailureDetails] = []

    overrides = []

    if args.mz_url:
        overrides = [
            Testdrive(
                no_reset=True,
                materialize_url=args.mz_url,
                no_consistency_checks=True,
            )
        ]
    else:
        mz_image = f"materialize/materialized:{tag}" if tag else None
        overrides = [
            Materialized(
                image=mz_image,
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
        ]

    c.silent = True

    with c.override(*overrides):
        for scenario_class in scenarios:
            scenario_name = scenario_class.name()
            print(f"--- Running scenario {scenario_name}")

            if args.mz_url:
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

            state = State(
                measurements=defaultdict(list),
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
                new_stats, new_failures = report(
                    mz_string,
                    scenario,
                    state.measurements,
                    start_time,
                    args.guarantees,
                    suffix,
                )
                failures.extend(new_failures)
                stats[scenario] = new_stats

            if not args.mz_url:
                c.kill("cockroach", "materialized", "testdrive")
                c.rm("cockroach", "materialized", "testdrive")
                c.rm_volumes("mzdata")

    return stats, failures


def less_than_is_regression(stat: str) -> bool:
    return stat == "qps"


def check_regressions(
    this_stats: dict[Scenario, dict[str, Statistics]],
    other_stats: dict[Scenario, dict[str, Statistics]],
    other_tag: str,
) -> list[TestFailureDetails]:
    failures: list[TestFailureDetails] = []

    assert len(this_stats) == len(other_stats)

    for scenario, other_scenario in zip(this_stats.keys(), other_stats.keys()):
        scenario_name = type(scenario).name()
        assert type(other_scenario).name() == scenario_name
        has_failed = False
        print(f"Comparing scenario {scenario_name}")
        output_lines = [
            f"{'QUERY':<40} | {'STAT':<7} | {'THIS':^12} | {'OTHER':^12} | {'CHANGE':^9} | {'THRESHOLD':^9} | {'REGRESSION?':^12}",
            "-" * 118,
        ]

        ignored_queries = set()
        for phase in scenario.phases:
            # We only care about LoadPhases, and only they have report_regressions
            if not isinstance(phase, LoadPhase):
                continue
            for phase_action in phase.phase_actions:
                if not phase_action.report_regressions:
                    ignored_queries.add(str(phase_action.action))

        for query in this_stats[scenario].keys():
            for stat in dir(this_stats[scenario][query]):
                this_value = getattr(this_stats[scenario][query], stat)
                other_value = getattr(other_stats[other_scenario][query], stat)
                less_than = less_than_is_regression(stat)
                percentage = f"{(this_value / other_value - 1) * 100:.2f}%"
                threshold = (
                    None
                    if query in ignored_queries
                    else (
                        scenario.regression_thresholds.get(query, {}).get(stat)
                        or REGRESSION_THRESHOLDS[stat]
                    )
                )
                if threshold is None:
                    regression = ""
                elif (
                    this_value < other_value / threshold
                    if less_than
                    else this_value > other_value * threshold
                ):
                    regression = "!!YES!!"
                    if not known_regression(scenario_name, other_tag):
                        has_failed = True
                else:
                    regression = "no"
                threshold_text = (
                    f"{((threshold - 1) * 100):.0f}%" if threshold is not None else ""
                )
                output_lines.append(
                    f"{query[:40]:<40} | {stat:<7} | {this_value:>12.2f} | {other_value:>12.2f} | {percentage:>9} | {threshold_text:>9} | {regression:^12}"
                )

        print("\n".join(output_lines))
        if has_failed:
            failures.append(
                TestFailureDetails(
                    message=f"Scenario {scenario_name} regressed",
                    details="\n".join(output_lines),
                    test_class_name_override=scenario_name,
                )
            )

    return failures


def resolve_tag(tag: str) -> str:
    if tag == "common-ancestor":
        # TODO: We probably will need overrides too
        return resolve_ancestor_image_tag({})
    return tag


def upload_results_to_test_analytics(
    c: Composition,
    load_phase_duration: int | None,
    stats: dict[Scenario, dict[str, Statistics]],
    was_successful: bool,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    test_analytics = TestAnalyticsDb(create_test_analytics_config(c))
    test_analytics.builds.add_build_job(was_successful=was_successful)

    result_entries = []

    for scenario in stats.keys():
        scenario_name = type(scenario).name()
        scenario_version = scenario.version

        for query in stats[scenario].keys():
            result_entries.append(
                parallel_benchmark_result_storage.ParallelBenchmarkResultEntry(
                    scenario_name=scenario_name,
                    scenario_version=str(scenario_version),
                    query=query,
                    load_phase_duration=load_phase_duration,
                    queries=stats[scenario][query].queries,
                    qps=stats[scenario][query].qps,
                    min=stats[scenario][query].min,
                    max=stats[scenario][query].max,
                    avg=stats[scenario][query].avg,
                    p50=stats[scenario][query].p50,
                    p95=stats[scenario][query].p95,
                    p99=stats[scenario][query].p99,
                    p99_9=stats[scenario][query].p99_9,
                    p99_99=stats[scenario][query].p99_99,
                    p99_999=stats[scenario][query].p99_999,
                    p99_9999=stats[scenario][query].p99_9999,
                    p99_99999=stats[scenario][query].p99_99999,
                    p99_999999=stats[scenario][query].p99_999999,
                    std=stats[scenario][query].std,
                    slope=stats[scenario][query].slope,
                )
            )

    test_analytics.parallel_benchmark_results.add_result(
        framework_version=PARALLEL_BENCHMARK_FRAMEWORK_VERSION,
        results=result_entries,
    )

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


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
        metavar="N-N",
        type=str,
        default="1",
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

    parser.add_argument(
        "--this-params",
        metavar="PARAMS",
        type=str,
        default=os.getenv("THIS_PARAMS", None),
        help="Semicolon-separated list of parameter=value pairs to apply to the 'THIS' Mz instance",
    )

    parser.add_argument(
        "--other-tag",
        metavar="TAG",
        type=str,
        default=None,
        help="'Other' Materialize container tag to benchmark. If not provided, the last released Mz version will be used.",
    )

    parser.add_argument(
        "--other-params",
        metavar="PARAMS",
        type=str,
        default=os.getenv("OTHER_PARAMS", None),
        help="Semicolon-separated list of parameter=value pairs to apply to the 'OTHER' Mz instance",
    )

    parser.add_argument("--mz-url", type=str, help="Remote Mz instance to run against")

    args = parser.parse_args()

    if args.scenario:
        for scenario in args.scenario:
            assert scenario in globals(), f"scenario {scenario} does not exist"
        scenarios: list[type[Scenario]] = [
            globals()[scenario] for scenario in args.scenario
        ]
    else:
        scenarios = list(all_subclasses(Scenario))

    sharded_scenarios = buildkite.shard_list(scenarios, lambda s: s.name())

    service_names = ["materialized", "postgres", "mysql"] + (
        ["redpanda"] if args.redpanda else ["zookeeper", "kafka", "schema-registry"]
    )

    this_stats, failures = run_once(
        c,
        sharded_scenarios,
        service_names,
        tag=None,
        params=args.this_params,
        args=args,
        suffix="this",
    )
    if args.other_tag:
        assert not args.mz_url, "Can't set both --mz-url and --other-tag"
        tag = resolve_tag(args.other_tag)
        print(f"--- Running against other tag for comparison: {tag}")
        args.guarantees = False
        other_stats, other_failures = run_once(
            c,
            sharded_scenarios,
            service_names,
            tag=tag,
            params=args.other_params,
            args=args,
            suffix="other",
        )
        failures.extend(other_failures)
        failures.extend(check_regressions(this_stats, other_stats, tag))

    upload_results_to_test_analytics(
        c, args.load_phase_duration, this_stats, not failures
    )

    if failures:
        raise FailedTestExecutionError(errors=failures)


# TODO: 24 hour runs (also against real staging with sources, similar to QA canary)
#       Set up remote sources, share with QA canary pipeline, use concurrency group, start first 24 hour runs
#       Maybe also set up the real rr-bench there as a separate step
# TODO: Choose an existing cluster name (for remote mz)
# TODO: Measure Memory?
