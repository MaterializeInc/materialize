# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The hegel state machine that runs every component's rules.

Each rule draws its inputs from the rule's test case, which Antithesis
controls, hands one operation to a session and returns within about a
second. Swarm testing disables a random subset of the rules for stretches of
a timeline, and per-timeline parameters are drawn when the machine starts.

A rule never raises: hegel would end the run on the first exception. Driver
bugs are reported through `client.report_driver_error`, product violations
through the components' assertions.

The eventually rule stops faults for good, waits for the system to recover,
runs every component's post-fault checks and ends the timeline by exiting.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from typing import TypeVar

import hegel  # pyright: ignore[reportMissingModuleSource]
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
)
from hegel import generators as gs  # pyright: ignore[reportMissingModuleSource]

from materialize.antithesis.ivm_model import SCHEMA as IVM_SCHEMA
from materialize.antithesis_driver import atomic, health
from materialize.antithesis_driver.client import Status, report_driver_error
from materialize.antithesis_driver.context import Config, Context, log
from materialize.antithesis_driver.counters import SCHEMA as COUNTERS_SCHEMA
from materialize.antithesis_driver.counters import Counters
from materialize.antithesis_driver.disrupt import Disruptions
from materialize.antithesis_driver.history import SCHEMA as HISTORY_SCHEMA
from materialize.antithesis_driver.history import History, create_statements
from materialize.antithesis_driver.views import Views
from materialize.antithesis_driver.views import setup_statements as view_statements

T = TypeVar("T")

MAX_CONCURRENCY = 6
EVENTUALLY_PROBABILITY = 0.002
"""Per call of the eventually rule, so eventuallys start at varied depths of
a timeline, from right after setup to deep into it."""
EVENTUALLY_SECONDS = 45 * 60
"""How long the post-fault checks may take in total. Rollouts after faults
stop can take tens of minutes on the simulated core."""
STOP_FAULTS_SECONDS = 24 * 60 * 60
PROGRESS_SECONDS = 60.0
IDLE_SECONDS = 0.1
"""A rule that sent nothing sleeps this long, so a stretch in which swarm
testing enables only such rules does not spin."""


class HegelDraw:
    def __init__(self, tc: hegel.TestCase) -> None:
        self.tc = tc

    def integer(self, lo: int, hi: int) -> int:
        return self.tc.draw_silent(gs.integers(min_value=lo, max_value=hi))

    def boolean(self, p: float = 0.5) -> bool:
        return self.tc.draw_silent(gs.weighted_booleans(p))

    def choice(self, options: Sequence[T]) -> T:
        return self.tc.draw_silent(gs.sampled_from(list(options)))


def setup_statements() -> list[str]:
    """DDL that every timeline starts from, run before setup completes.

    Dropping the schemas first lets a local loop rerun against one
    environment. In Antithesis every run starts from an empty one.
    """
    drops = [
        f"DROP SCHEMA IF EXISTS {schema} CASCADE"
        for schema in (HISTORY_SCHEMA, COUNTERS_SCHEMA, IVM_SCHEMA, atomic.SCHEMA)
    ]
    return drops + create_statements(0) + view_statements() + atomic.setup_statements()


class Components:
    def __init__(self, config: Config) -> None:
        self.ctx = Context.create(config)
        self.history = History(self.ctx)
        # Epoch 0 exists since setup.
        self.history.start(0)
        self.counters = Counters(self.ctx)
        self.views = Views(self.ctx)
        self.atomic = atomic.Atomic(self.ctx)
        self.disruptions = Disruptions(self.ctx)
        self.last_progress = time.monotonic()

    def progress(self) -> str:
        epochs = self.history.epochs
        g = self.counters.current
        return (
            f"progress: history epoch {self.history.current}"
            f" with {len(epochs[self.history.current].records) if self.history.current in epochs else 0} ops,"
            f" {len(epochs)} live epochs; counters generation"
            f" {g.number if g else None} with {g.ops if g else 0} ops;"
            f" ivm writes {len(self.views.writes)};"
            f" busy sessions {len(self.ctx.sessions.busy())}"
        )


_components: Components | None = None
_eventually_lock = threading.Lock()


def components() -> Components:
    assert _components is not None
    return _components


def guarded(name: str, rule: Callable[[HegelDraw], Status], tc: hegel.TestCase) -> None:
    c = components()
    if c.ctx.quiesced.is_set():
        time.sleep(IDLE_SECONDS)
        return
    try:
        status = rule(HegelDraw(tc))
    except (hegel.HegelError, hegel.StopTest, hegel.AssumeFailed):
        raise
    except Exception as e:
        report_driver_error(name, e)
        status = Status.NOT_SENT
    if status == Status.NOT_SENT:
        time.sleep(IDLE_SECONDS)


def stop_faults() -> None:
    binary = os.environ.get("ANTITHESIS_STOP_FAULTS")
    if binary:
        subprocess.run([binary, str(STOP_FAULTS_SECONDS)], check=False)
    else:
        log("eventually: ANTITHESIS_STOP_FAULTS is not set, as in the local loop")


def eventually() -> None:
    """Stop faults, check what must hold once they stop, end the timeline."""
    if not _eventually_lock.acquire(blocking=False):
        return
    try:
        _eventually(components())
    except Exception as e:
        report_driver_error("eventually", e)
    log("eventually: assertions done, ending the timeline")
    os._exit(0)


def _eventually(c: Components) -> None:
    c.ctx.quiesced.set()
    log("eventually: stopping faults")
    stop_faults()
    deadline = time.monotonic() + EVENTUALLY_SECONDS

    def step(name: str, check: Callable[[], T]) -> T | None:
        # A driver bug in one check must not skip the others, nor escape
        # into hegel, which would end the run without ending the timeline.
        try:
            return check()
        except Exception as e:
            report_driver_error(f"eventually: {name}", e)
            return None

    step("disruptions", lambda: c.disruptions.eventually(deadline))
    serving = step("health", lambda: health.eventually(c.ctx, deadline))
    busy = c.ctx.sessions.wait_idle(min(deadline, time.monotonic() + 300))
    always(
        not busy,
        "every SQL session's pending operation completes once faults stop",
        {"busy": busy},
    )
    if serving:
        step("history", lambda: c.history.eventually(deadline))
        step("counters", lambda: c.counters.eventually(deadline))
        step("views", lambda: c.views.eventually(deadline))
        step("atomic", lambda: c.atomic.eventually(deadline))
    kube = c.disruptions.kube
    if kube is not None:
        step("crash loops", lambda: health.no_crash_loops(kube, deadline))


class Driver(hegel.StateMachine):
    steps = 2**63 - 1
    max_concurrency = MAX_CONCURRENCY

    def __init__(self, tc: hegel.TestCase) -> None:
        c = components()
        d = HegelDraw(tc)
        c.history.configure(d)
        c.counters.configure(d)
        c.views.configure(d)
        c.atomic.configure(d)
        c.disruptions.configure(d)
        log(
            "timeline parameters:"
            f" history isolations {c.history.isolation_weights},"
            f" rmw {c.history.rmw_share}, epoch {c.history.epoch_size};"
            f" counters keys {c.counters.keys};"
            f" disruption cooldown {c.disruptions.cooldown}s,"
            f" rollout kill probability {c.disruptions.kill_probability}"
        )

    @hegel.rule(weight=6)
    def history_write(self, tc: hegel.TestCase) -> None:
        guarded("history write", components().history.write, tc)

    @hegel.rule(weight=6)
    def history_read(self, tc: hegel.TestCase) -> None:
        guarded("history read", components().history.read, tc)

    @hegel.rule()
    def history_rotate(self, tc: hegel.TestCase) -> None:
        guarded("history rotate", components().history.rotate, tc)

    @hegel.rule()
    def history_retire(self, tc: hegel.TestCase) -> None:
        guarded("history retire", components().history.retire, tc)

    @hegel.rule(weight=4)
    def counters_increment(self, tc: hegel.TestCase) -> None:
        guarded("counters increment", components().counters.increment, tc)

    @hegel.rule(weight=2)
    def counters_copy(self, tc: hegel.TestCase) -> None:
        guarded("counters copy", components().counters.copy, tc)

    @hegel.rule(weight=3)
    def counters_read(self, tc: hegel.TestCase) -> None:
        guarded("counters read", components().counters.read, tc)

    @hegel.rule()
    def counters_read_log(self, tc: hegel.TestCase) -> None:
        guarded("counters read log", components().counters.read_log, tc)

    @hegel.rule()
    def counters_rotate(self, tc: hegel.TestCase) -> None:
        guarded("counters rotate", components().counters.rotate, tc)

    @hegel.rule(weight=5)
    def ivm_write(self, tc: hegel.TestCase) -> None:
        guarded("ivm write", components().views.write, tc)

    @hegel.rule(weight=2)
    def ivm_check(self, tc: hegel.TestCase) -> None:
        guarded("ivm check", components().views.check, tc)

    @hegel.rule(weight=3)
    def atomic_write(self, tc: hegel.TestCase) -> None:
        guarded("atomic write", components().atomic.write, tc)

    @hegel.rule(weight=2)
    def atomic_read(self, tc: hegel.TestCase) -> None:
        guarded("atomic read", components().atomic.read, tc)

    @hegel.rule()
    def atomic_prune(self, tc: hegel.TestCase) -> None:
        guarded("atomic prune", components().atomic.prune, tc)

    @hegel.rule()
    def replication(self, tc: hegel.TestCase) -> None:
        guarded("replication", components().disruptions.replication, tc)

    @hegel.rule()
    def resize(self, tc: hegel.TestCase) -> None:
        guarded("resize", components().disruptions.resize, tc)

    @hegel.rule()
    def kill_pod(self, tc: hegel.TestCase) -> None:
        guarded("kill pod", components().disruptions.kill_pod, tc)

    @hegel.rule()
    def start_rollout(self, tc: hegel.TestCase) -> None:
        guarded("start rollout", components().disruptions.start_rollout, tc)

    @hegel.rule(weight=2)
    def advance_rollout(self, tc: hegel.TestCase) -> None:
        guarded("advance rollout", components().disruptions.advance_rollout, tc)

    @hegel.rule()
    def eventually(self, tc: hegel.TestCase) -> None:
        config = components().ctx.config
        if config.eventually_disabled or config.eventually_after_seconds is not None:
            time.sleep(IDLE_SECONDS)
            return
        if tc.draw_silent(gs.weighted_booleans(EVENTUALLY_PROBABILITY)):
            eventually()
        else:
            time.sleep(IDLE_SECONDS)

    @hegel.invariant(always_run=True)
    def checks(self, tc: hegel.TestCase) -> None:
        c = components()
        try:
            c.history.check()
            c.disruptions.restore_if_overdue()
        except Exception as e:
            report_driver_error("periodic checks", e)
        now = time.monotonic()
        if now - c.last_progress >= PROGRESS_SECONDS:
            c.last_progress = now
            log(c.progress())
        after = c.ctx.config.eventually_after_seconds
        if after is not None and time.monotonic() - c.ctx.started >= after:
            eventually()


# hegel reports the run's verdict as the property
# "::materialize_driver passes properties", after this function's name.
@hegel.main(phases=[hegel.Phase.generate], database=False)
def materialize_driver(tc: hegel.TestCase) -> None:
    # Even with `steps` at its maximum the engine ends a machine after each
    # round with probability 2**-32, so start it again.
    while True:
        hegel.run_state_machine(tc, Driver)
        log("the state machine ended, starting it again")


def run(config: Config, argv: list[str]) -> None:
    """Run the state machine. `argv` are hegel's own command-line options,
    such as `--backend default --seed N` for a reproducible local run."""
    global _components
    _components = Components(config)
    materialize_driver(argv)
