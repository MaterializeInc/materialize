# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Core types of the invariants framework.

The correctness model: every write action is atomic on the system-under-test
side, so scenario invariants hold whether an individual action applied or
not. Where per-op bookkeeping is needed, each op resolves to one of three
outcomes. Only COMMITTED ops create lower bounds ("must be visible"), UNKNOWN
ops join every upper bound ("may be visible"), FAILED ops must have no
visible effect. Wrong data is always fatal, while errors and hangs during a
disruption are tolerated (liveness is verified after healing).
"""

import random
import threading
import time
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum

SEED_RANGE = 2**31

# Bounded wait for the system to recover and all data paths to converge after
# all disruptions have been healed. Exceeding it is a liveness failure.
CONVERGE_TIMEOUT = 300


class Outcome(Enum):
    COMMITTED = "committed"
    FAILED = "failed"
    UNKNOWN = "unknown"


class InvariantViolation(AssertionError):
    """Wrong data or lost/duplicated writes. Always fails the run."""


class TransientError(Exception):
    """Connection loss, timeout, or cancellation, expected during disruptions.

    Loops that hit this skip the current round and retry, they never fail the
    run. Liveness is enforced separately by the converge phase.
    """


@dataclass(frozen=True)
class Complexity:
    name: str
    workers: int
    accounts: int
    # Delay range between two actions of one worker, seconds.
    op_delay: tuple[float, float]
    # Calm window between disruption cycles / duration of one disruption.
    disruption_interval: tuple[float, float]
    disruption_duration: tuple[float, float]
    # How many legs one disruption cycle may hit at once.
    concurrent_disruptions: int
    # Client-side watchdog for single queries and writes. Not a server
    # setting: statement_timeout does not bound plain SELECTs, so checkers
    # cancel hung queries themselves.
    query_timeout: float
    # Delay range between agitator ticks (random query cancellations and
    # system-flag flips), None to disable.
    agitation_interval: tuple[float, float] | None


COMPLEXITIES = {
    "low": Complexity(
        name="low",
        workers=2,
        accounts=8,
        op_delay=(0.01, 0.05),
        disruption_interval=(20.0, 40.0),
        disruption_duration=(5.0, 15.0),
        concurrent_disruptions=1,
        query_timeout=30.0,
        agitation_interval=(5.0, 15.0),
    ),
    "medium": Complexity(
        name="medium",
        workers=4,
        accounts=16,
        op_delay=(0.005, 0.02),
        disruption_interval=(15.0, 30.0),
        disruption_duration=(10.0, 45.0),
        concurrent_disruptions=1,
        query_timeout=60.0,
        agitation_interval=(3.0, 10.0),
    ),
    "high": Complexity(
        name="high",
        workers=8,
        accounts=32,
        op_delay=(0.001, 0.01),
        disruption_interval=(10.0, 20.0),
        disruption_duration=(15.0, 90.0),
        concurrent_disruptions=2,
        query_timeout=90.0,
        agitation_interval=(1.0, 5.0),
    ),
    # Like high, but with a large account table: persist batches stop being
    # trivial, so compaction, spills, and the AS OF probes do real work.
    "large": Complexity(
        name="large",
        workers=8,
        accounts=200_000,
        op_delay=(0.001, 0.01),
        disruption_interval=(10.0, 20.0),
        disruption_duration=(15.0, 90.0),
        concurrent_disruptions=2,
        query_timeout=90.0,
        agitation_interval=(1.0, 5.0),
    ),
}


class EventLog:
    """Thread-safe event log, written to a file and selectively echoed.

    High-volume events (individual ops) are sampled, everything else (checker
    findings, disruptions, phase changes) is logged in full so a failure can
    be correlated with the active disruptions without rerunning.
    """

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._file = open(path, "w")
        self._start = time.monotonic()

    def log(self, kind: str, message: str, echo: bool = True) -> None:
        elapsed = time.monotonic() - self._start
        thread = threading.current_thread().name
        line = f"{elapsed:9.3f} [{thread}] {kind}: {message}"
        with self._lock:
            try:
                self._file.write(line + "\n")
                self._file.flush()
            except ValueError:
                # A thread that outlived its join deadline may log after
                # close(). Its message still matters, never raise from here.
                pass
            if echo:
                print(line, flush=True)

    def close(self) -> None:
        with self._lock:
            self._file.close()


class Watermark:
    """Thread-safe monotonically increasing value.

    Sampling rule for sound monotonicity checks under concurrency: `get()`
    BEFORE issuing a read, compare the read's result against that snapshot,
    `advance()` with the result AFTER the read completed. Strict
    serializability only orders transactions that do not overlap in real
    time, so comparing a read against a watermark advanced by a concurrent
    read would false-positive.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value = 0

    def get(self) -> int:
        with self._lock:
            return self._value

    def advance(self, value: int) -> None:
        with self._lock:
            if value > self._value:
                self._value = value


class OpLog:
    """Ledger of per-op outcomes, keyed by the unique id (worker, seq).

    Sampling rule for sound bounds checks: `committed_count()` (lower bound)
    must be sampled BEFORE a read is issued, `attempted_count()` (upper
    bound) AFTER the read result is received.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._outcomes: dict[tuple[int, int], Outcome] = {}
        self._counts: Counter[Outcome] = Counter()

    def record(self, worker: int, seq: int, outcome: Outcome) -> None:
        with self._lock:
            previous = self._outcomes.get((worker, seq))
            if previous is not None:
                self._counts[previous] -= 1
            self._outcomes[(worker, seq)] = outcome
            self._counts[outcome] += 1

    def committed_count(self) -> int:
        with self._lock:
            return self._counts[Outcome.COMMITTED]

    def attempted_count(self) -> int:
        with self._lock:
            return self._counts[Outcome.COMMITTED] + self._counts[Outcome.UNKNOWN]

    def seqs(self, worker: int, outcome: Outcome) -> set[int]:
        with self._lock:
            return {
                s
                for (w, s), o in self._outcomes.items()
                if w == worker and o == outcome
            }

    def counts(self) -> dict[Outcome, int]:
        with self._lock:
            return dict(self._counts)


class Action(ABC):
    """One kind of write op, executed repeatedly by a worker thread.

    Implementations hold their own resources (connections, producers) and
    must be safe to call from exactly one thread. `run` reports per-op
    outcomes to the scenario's OpLog where bookkeeping is needed and raises
    TransientError for expected connection trouble. Any other exception fails
    the run.
    """

    name: str

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    @abstractmethod
    def run(self) -> Outcome | None:
        """Execute one op. Returns the outcome, or None if not applicable."""

    def close(self) -> None:
        pass


@dataclass
class WorkerBundle:
    """The weighted actions of one worker thread plus shared resources."""

    actions: list[Action]
    weights: list[int]

    def close(self) -> None:
        for action in self.actions:
            action.close()


class Checker(ABC):
    """Continuously verifies one invariant on its own thread.

    `check_once` raises InvariantViolation on wrong data (fatal) and
    TransientError when the system is unreachable (skipped round). The
    executor calls `open` lazily and retries it after transient errors, so
    checkers keep working across disruptions by reconnecting.
    """

    name: str
    # Delay range between two check rounds, seconds.
    pause: tuple[float, float] = (0.5, 2.0)

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.rounds = 0
        self.skipped = 0
        # How often an invariant was actually verified. Rounds alone can be
        # vacuous, e.g. a SUBSCRIBE session that dies before its first
        # validatable boundary still completes rounds.
        self.validations = 0
        # The subset of validations that completed while a disruption was
        # actively applied.
        self.validations_during = 0

    def open(self) -> None:
        pass

    @abstractmethod
    def check_once(self) -> None:
        pass

    def close(self) -> None:
        pass


@dataclass(frozen=True)
class Endpoints:
    """Host-side connection info for the system under test and its oracles.

    The harness always connects directly (host-mapped ports), bypassing
    toxiproxy: disruptions target Materialize's connections, never the
    oracle's view of the external systems.
    """

    mz_host: str
    mz_port: int
    mz_system_port: int
    mz_http_port: int | None = None
    minio_port: int | None = None
    pg_port: int | None = None
    mysql_port: int | None = None
    sqlserver_port: int | None = None
    kafka_bootstrap: str | None = None


@dataclass
class ScenarioContext:
    endpoints: Endpoints
    complexity: Complexity
    rng: random.Random
    log: EventLog
    seed: str
    stop: threading.Event = field(default_factory=threading.Event)
    # All MzClients ever created, so the shutdown ladder can unblock
    # stragglers by cancelling/closing their connections cross-thread.
    clients: list = field(default_factory=list)


class Scenario(ABC):
    """A self-contained workload with invariants that hold under disruption.

    Lifecycle: setup() -> worker/checker/disruptor threads run for the chaos
    phase -> all disruptions healed, workers stopped -> converge() (bounded
    liveness check) -> final_check() (strong assertions using the OpLog).
    """

    name: str
    # Extra mzcompose services to bring up, e.g. ["postgres"].
    services: list[str] = []
    # Toxiproxy legs this scenario disrupts, by leg name.
    legs: list[str] = []

    def __init__(self, ctx: ScenarioContext) -> None:
        self.ctx = ctx

    @abstractmethod
    def setup(self) -> None:
        pass

    @abstractmethod
    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        pass

    @abstractmethod
    def checkers(self) -> list[Checker]:
        pass

    @abstractmethod
    def converge(self) -> None:
        """Wait (bounded) until all data paths caught up after healing."""

    @abstractmethod
    def final_check(self) -> None:
        pass

    def diagnostics(self) -> None:
        """Best-effort post-failure state dump, called after healing."""

    def teardown(self) -> None:
        pass


def wait_until(
    condition, timeout: float, description: str, interval: float = 1.0
) -> None:
    """Poll `condition` until it returns True, tolerating TransientErrors.

    Raises InvariantViolation on timeout: after healing, the system is
    expected to recover, so failing to converge is a liveness bug.
    """
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if condition():
                return
        except TransientError as e:
            last_error = e
        time.sleep(interval)
    raise InvariantViolation(
        f"liveness: {description} did not converge within {timeout}s"
        + (f" (last transient error: {last_error})" if last_error else "")
    )
