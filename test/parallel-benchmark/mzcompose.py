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
from materialize.version_list import resolve_ancestor_image_tag


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
    measurements: defaultdict[str, list[Measurement]]
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
    scenario_name = type(scenario).__name__
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
) -> tuple[dict[str, dict[str, Statistics]], list[TestFailureDetails]]:
    stats: dict[str, dict[str, Statistics]] = {}
    failures: list[TestFailureDetails] = []

    if args.mz_url:
        with c.override(
            Testdrive(
                no_reset=True,
                materialize_url=args.mz_url,
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
        mz_image = f"materialize/materialized:{tag}" if tag else None
        with c.override(
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
            stats[scenario_name] = new_stats

    if not args.mz_url:
        c.kill("cockroach", "materialized", "testdrive")
        c.rm("cockroach", "materialized", "testdrive")
        c.rm_volumes("mzdata")

    return stats, failures


def less_than_is_regression(stat: str) -> bool:
    return stat == "qps"


def check_regressions(
    this_stats: dict[str, dict[str, Statistics]],
    other_stats: dict[str, dict[str, Statistics]],
    other_tag: str,
) -> list[TestFailureDetails]:
    failures: list[TestFailureDetails] = []

    assert len(this_stats) == len(other_stats)

    for scenario in this_stats.keys():
        has_failed = False
        print(f"Comparing scenario {scenario}")
        output_lines = [
            f"{'QUERY':<40} | {'STAT':<7} | {'THIS':^12} | {'OTHER':^12} | {'CHANGE':^9} | {'THRESHOLD':^9} | {'REGRESSION?':^12}",
            "-" * 118,
        ]

        for query in this_stats[scenario].keys():
            for stat in dir(this_stats[scenario][query]):
                this_value = getattr(this_stats[scenario][query], stat)
                other_value = getattr(other_stats[scenario][query], stat)
                less_than = less_than_is_regression(stat)
                percentage = f"{(this_value / other_value - 1) * 100:.2f}%"
                if REGRESSION_THRESHOLDS[stat] is None:
                    regression = ""
                elif (
                    this_value < other_value / REGRESSION_THRESHOLDS[stat]
                    if less_than
                    else this_value > other_value * REGRESSION_THRESHOLDS[stat]
                ):
                    regression = "!!YES!!"
                    if not known_regression(scenario, other_tag):
                        has_failed = True
                else:
                    regression = "no"
                threshold = (
                    f"{((REGRESSION_THRESHOLDS[stat] - 1) * 100):.0f}%"
                    if REGRESSION_THRESHOLDS[stat] is not None
                    else ""
                )
                output_lines.append(
                    f"{query[:40]:<40} | {stat:<7} | {this_value:>12.2f} | {other_value:>12.2f} | {percentage:>9} | {threshold:>9} | {regression:^12}"
                )

        print("\n".join(output_lines))
        if has_failed:
            failures.append(
                TestFailureDetails(
                    message=f"Scenario {scenario} regressed",
                    details="\n".join(output_lines),
                    test_class_name_override=scenario,
                )
            )

    return failures


def resolve_tag(tag: str) -> str:
    if tag == "common-ancestor":
        # TODO: We probably will need overrides too
        return resolve_ancestor_image_tag({})
    return tag


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

    service_names = ["materialized", "postgres", "mysql"] + (
        ["redpanda"] if args.redpanda else ["zookeeper", "kafka", "schema-registry"]
    )

    this_stats, failures = run_once(
        c,
        scenarios,
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
            scenarios,
            service_names,
            tag=tag,
            params=args.other_params,
            args=args,
            suffix="other",
        )
        failures.extend(other_failures)
        failures.extend(check_regressions(this_stats, other_stats, tag))

    if failures:
        raise FailedTestExecutionError(errors=failures)


# TODO: Choose an existing cluster name (for remote mz)
# TODO: Measure Memory?
