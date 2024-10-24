# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import queue
import random
import sqlite3
import threading
import time
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from textwrap import dedent

import psycopg

from materialize.mzcompose.composition import Composition
from materialize.util import PgConnInfo

DB_FILE = "parallel-benchmark.db"
assert (
    sqlite3.threadsafety == 3
), f"Thread safety level 3 (serialized) required, but is: {sqlite3.threadsafety}"


class Measurement:
    duration: float
    timestamp: float

    def __init__(self, duration: float, timestamp: float):
        self.duration = duration
        self.timestamp = timestamp

    def __str__(self) -> str:
        return f"{self.timestamp} {self.duration}"


class MeasurementsStore:
    def add(self, action: str, measurement: Measurement) -> None:
        raise NotImplementedError

    def actions(self) -> list[str]:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def get_data(
        self, action: str, start_time: float, end_time: float
    ) -> tuple[list[float], list[float]]:
        raise NotImplementedError


class MemoryStore(MeasurementsStore):
    def __init__(self):
        self.data: defaultdict[str, list[Measurement]] = defaultdict(list)

    def add(self, action: str, measurement: Measurement) -> None:
        self.data[action].append(measurement)

    def actions(self) -> list[str]:
        return list(self.data.keys())

    def close(self) -> None:
        pass

    def get_data(
        self, action: str, start_time: float, end_time: float
    ) -> tuple[list[float], list[float]]:
        times: list[float] = [x.timestamp - start_time for x in self.data[action]]
        durations: list[float] = [x.duration * 1000 for x in self.data[action]]
        return (times, durations)


class SQLiteStore(MeasurementsStore):
    def __init__(self, scenario: str):
        self.scenario = scenario
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(
            DB_FILE, check_same_thread=False, isolation_level=None
        )
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=OFF;")
        cursor.execute("PRAGMA cache_size=-64000;")  # 64 MB
        cursor.execute("PRAGMA locking_mode=EXCLUSIVE;")
        self.conn.commit()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS measurements (scenario TEXT NOT NULL, action TEXT NOT NULL, duration FLOAT NOT NULL, timestamp FLOAT NOT NULL);"
        )
        cursor.execute("DELETE FROM measurements WHERE scenario = ?", (self.scenario,))
        cursor.close()

    def add(self, action: str, measurement: Measurement) -> None:
        with self.lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO measurements VALUES (?, ?, ?, ?)",
                    (
                        self.scenario,
                        action,
                        measurement.duration * 1000,
                        measurement.timestamp,
                    ),
                )
            except Exception as e:
                print(
                    f"Caught exception {str(e)} with values: {self.scenario}, {action}, {measurement.duration}, {measurement.timestamp}"
                )
                raise
            cursor.close()

    def actions(self) -> list[str]:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT DISTINCT action FROM measurements WHERE scenario = ?",
                (self.scenario,),
            )
            result = [row[0] for row in cursor.fetchall()]
            cursor.close()
        return result

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    def get_data(
        self, action: str, start_time: float, end_time: float
    ) -> tuple[list[float], list[float]]:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT duration, timestamp FROM measurements WHERE scenario = ? AND action = ? AND timestamp BETWEEN ? AND ?",
                (self.scenario, action, start_time, end_time),
            )
            times: list[float] = []
            durations: list[float] = []
            for row in cursor:
                durations.append(row[0])
                times.append(row[1] - start_time)
            return (times, durations)


@dataclass
class State:
    measurements: MeasurementsStore
    load_phase_duration: int | None
    periodic_dists: dict[str, int]


def execute_query(cur: psycopg.Cursor, query: str) -> None:
    while True:
        try:
            cur.execute(query.encode())
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
        state.measurements.add(str(self), Measurement(duration, start_time))

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
            try:
                execute_query(cur, self.query)
            except psycopg.OperationalError as e:
                print(f"Connection failed on query '{self.query}', reconnecting: {e}")
                conn.close()
                conn = self.conn_info.connect()
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
    # Has to be set for the class already, not just in the constructor, so that
    # we can change the value for the entire class in the decorator
    enabled: bool = True
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
            conn = conn_info.connect()
            conn.autocommit = True
            self.conns.put(conn)

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


def disabled(ignore_reason: str):
    def decorator(cls):
        cls.enabled = False
        return cls

    return decorator
