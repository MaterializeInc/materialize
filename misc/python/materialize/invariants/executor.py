# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Thread orchestration and the phases of one scenario run.

Phases: chaos (workers + checkers + disruptor race for --runtime seconds) ->
heal (disruptor stops and heals all legs) -> shutdown ladder (stop threads,
unblock stragglers by cancelling their connections) -> converge (bounded
liveness check) -> final check (strong assertions) -> vacuity check.

Failure model: the first thread to hit an InvariantViolation or unexpected
error records it and stops the run. Threads stuck after healing are
themselves a liveness failure, never silently abandoned.
"""

import faulthandler
import random
import threading
import time
from collections import Counter

from materialize.invariants.framework import (
    SEED_RANGE,
    Checker,
    InvariantViolation,
    Outcome,
    Scenario,
    TransientError,
    WorkerBundle,
)
from materialize.invariants.mz import MzClient
from materialize.invariants.toxiproxy import (
    Disruptor,
    Leg,
    ProcessTarget,
    ToxiproxyApi,
)

REPORT_INTERVAL = 10.0

# Behavior-preserving system flags the agitator flips mid-run, a subset of
# parallel-workload's FlipFlagsAction catalog. Plan- and performance-affecting
# flags are fair game, results-changing ones are not: every invariant must
# hold under any combination of these.
AGITATOR_FLAGS: dict[str, list[str]] = {
    "persist_blob_target_size": ["1048576", "16777216", "134217728"],
    "persist_batch_max_run_len": ["2", "4", "16", "1000"],
    "persist_compaction_memory_bound_bytes": [
        "67108864",
        "134217728",
        "1073741824",
    ],
    "persist_part_decode_format": ["row_with_validate", "arrow"],
    "persist_encoding_enable_dictionary": ["true", "false"],
    "persist_enable_incremental_compaction": ["true", "false"],
    "persist_claim_unclaimed_compactions": ["true", "false"],
    "persist_optimize_ignored_data_fetch": ["true", "false"],
    "enable_variadic_left_join_lowering": ["true", "false"],
    "enable_eager_delta_joins": ["true", "false"],
    "persist_use_critical_since_catalog": ["true", "false"],
    "persist_use_critical_since_source": ["true", "false"],
    "persist_use_critical_since_snapshot": ["true", "false"],
    "persist_use_critical_since_txn": ["true", "false"],
    "persist_batch_structured_key_lower_len": ["0", "1", "512", "50000"],
}


class Agitator(threading.Thread):
    """Random session cancellations and system-flag flips during chaos.

    Cancellations exercise the UNKNOWN outcome paths: a cancelled write must
    never half-apply, which the scenario invariants then verify. Flag flips
    reconfigure plan and persist behavior mid-run, and results must stay
    exact under every combination. All flipped flags are reset at the end so
    converge and the final check run under defaults.
    """

    def __init__(self, ctx, rng: random.Random, on_error) -> None:
        super().__init__(name="agitator")
        self.ctx = ctx
        self.rng = rng
        self.on_error = on_error
        # ALTER SYSTEM needs mz_system, while pg_cancel_backend requires
        # membership of the target session's role, so cancellations go
        # through a regular materialize connection.
        self.system_client = MzClient(
            ctx, "agitator-system", user="mz_system", port=ctx.endpoints.mz_system_port
        )
        self.cancel_client = MzClient(ctx, "agitator-cancel")
        self.cancels = 0
        self.flips = 0
        self.flipped: set[str] = set()

    def run(self) -> None:
        ctx = self.ctx
        assert ctx.complexity.agitation_interval is not None
        try:
            while not ctx.stop.wait(
                self.rng.uniform(*ctx.complexity.agitation_interval)
            ):
                try:
                    if self.rng.random() < 0.7:
                        self._cancel_random_session()
                    else:
                        self._flip_random_flag()
                except TransientError as e:
                    ctx.log.log("agitate", f"skipped: {e}", echo=False)
        except BaseException as e:
            self.on_error(e)
        finally:
            self._reset_flags()
            self.system_client.reset()
            self.cancel_client.reset()

    def _cancel_random_session(self) -> None:
        rows = self.cancel_client.query(
            "SELECT s.connection_id FROM mz_internal.mz_sessions s"
            " JOIN mz_roles r ON s.role_id = r.id WHERE r.name = 'materialize'"
        )
        if not rows:
            return
        # Sorted before choosing: the server returns rows in arbitrary order.
        target = self.rng.choice(sorted(int(row[0]) for row in rows))
        self.cancel_client.query(f"SELECT pg_cancel_backend({target})")
        self.cancels += 1
        self.ctx.log.log("agitate", f"cancelled connection {target}", echo=False)

    def _flip_random_flag(self) -> None:
        flag = self.rng.choice(sorted(AGITATOR_FLAGS))
        value = self.rng.choice(AGITATOR_FLAGS[flag])
        self.system_client.query(f"ALTER SYSTEM SET {flag} = '{value}'")
        self.flipped.add(flag)
        self.flips += 1
        self.ctx.log.log("agitate", f"set {flag} = {value}")

    def _reset_flags(self) -> None:
        # Best-effort with one shared budget: this runs during shutdown, and
        # against a stalled envd every RESET would otherwise burn its full
        # watchdog timeout, blowing the join ladder. The flags are
        # results-preserving, so leaving some flipped is acceptable.
        deadline = time.monotonic() + 20
        for flag in sorted(self.flipped):
            if time.monotonic() > deadline:
                self.ctx.log.log(
                    "agitate", f"flag reset budget exhausted, {flag}+ left flipped"
                )
                return
            try:
                self.system_client.query(f"ALTER SYSTEM RESET {flag}", timeout=10)
            except Exception:
                pass


class Runner:
    def __init__(
        self,
        scenario: Scenario,
        runtime: float,
        toxiproxy: ToxiproxyApi,
        legs: list[Leg],
        processes: list[ProcessTarget] | None = None,
        midrun_event=None,
    ) -> None:
        self.scenario = scenario
        self.ctx = scenario.ctx
        self.runtime = runtime
        self.toxiproxy = toxiproxy
        self.legs = legs
        self.processes = processes or []
        self.midrun_event = midrun_event
        self.failure: BaseException | None = None
        self._failure_lock = threading.Lock()
        self.worker_stats: list[Counter[tuple[str, str]]] = []
        self.checkers: list[Checker] = []
        self.disruptor: Disruptor | None = None
        self.agitator: Agitator | None = None
        # Checker validations and committed ops at the chaos midpoint, for
        # the progress assertions.
        self._half_marks: tuple[list[int], int] | None = None

    def fail(self, exc: BaseException) -> None:
        with self._failure_lock:
            if self.failure is None:
                self.failure = exc
        # Stop before logging so threads halt even if logging itself fails.
        self.ctx.stop.set()
        self.ctx.log.log("FAILURE", f"{type(exc).__name__}: {exc}")

    def run(self) -> None:
        ctx = self.ctx
        threads: list[threading.Thread] = []

        # Draw all child seeds in a fixed order so a given --seed yields the
        # same per-thread action/disruption sequences across runs.
        worker_rngs = [
            random.Random(ctx.rng.randrange(SEED_RANGE))
            for _ in range(ctx.complexity.workers)
        ]
        self.checkers = self.scenario.checkers()
        checker_rngs = [
            random.Random(ctx.rng.randrange(SEED_RANGE)) for _ in self.checkers
        ]
        disruptor_rng = random.Random(ctx.rng.randrange(SEED_RANGE))
        agitator_rng = random.Random(ctx.rng.randrange(SEED_RANGE))

        for i, rng in enumerate(worker_rngs):
            bundle = self.scenario.make_worker(i, rng)
            stats: Counter[tuple[str, str]] = Counter()
            self.worker_stats.append(stats)
            thread = threading.Thread(
                name=f"worker-{i}", target=self._worker_loop, args=(bundle, rng, stats)
            )
            threads.append(thread)
        for checker, rng in zip(self.checkers, checker_rngs):
            thread = threading.Thread(
                name=f"checker-{checker.name}",
                target=self._checker_loop,
                args=(checker, rng),
            )
            threads.append(thread)
        if self.legs:
            self.disruptor = Disruptor(
                api=self.toxiproxy,
                legs=self.legs,
                rng=disruptor_rng,
                log=ctx.log,
                interval=ctx.complexity.disruption_interval,
                duration=ctx.complexity.disruption_duration,
                concurrent=ctx.complexity.concurrent_disruptions,
                on_error=self.fail,
                processes=self.processes,
            )
            if ctx.complexity.agitation_interval is not None:
                self.agitator = Agitator(ctx, agitator_rng, self.fail)
                threads.append(self.agitator)

        ctx.log.log("phase", f"chaos: running for {self.runtime}s")
        # The finally clause guarantees heal/stop/join even when the monitor
        # itself fails, so no scenario ever leaves disruptions applied or
        # threads running.
        try:
            for thread in threads:
                thread.start()
            if self.disruptor:
                self.disruptor.start()
            self._monitor()
        finally:
            ctx.log.log("phase", "heal: stopping disruptions")
            if self.disruptor is not None and self.disruptor.ident is not None:
                self.disruptor.stop_and_heal()
            ctx.stop.set()
            self._join_threads([t for t in threads if t.ident is not None])

        if self.failure is not None:
            self._dump_diagnostics()
            raise self.failure

        ctx.log.log("phase", "converge: waiting for all data paths to catch up")
        try:
            self.scenario.converge()
            ctx.log.log("phase", "final check")
            self.scenario.final_check()
            self._check_vacuity()
        except BaseException as e:
            self.fail(e)
            self._dump_diagnostics()
            raise

        self._report(final=True)
        ctx.log.log("phase", "success")

    def _monitor(self) -> None:
        end = time.monotonic() + self.runtime
        half = end - self.runtime / 2
        last_report = time.monotonic()
        while time.monotonic() < end and not self.ctx.stop.is_set():
            time.sleep(1)
            if self._half_marks is None and time.monotonic() >= half:
                self._half_marks = (
                    [checker.validations for checker in self.checkers],
                    self._committed_total(),
                )
                if self.midrun_event is not None:
                    self.ctx.log.log("phase", "midrun event: upgrade swap")
                    try:
                        self.midrun_event()
                    except Exception as e:
                        self.fail(e)
            if time.monotonic() - last_report >= REPORT_INTERVAL:
                last_report = time.monotonic()
                self._report(final=False)

    def _join_threads(self, threads: list[threading.Thread]) -> None:
        deadline = time.monotonic() + 60
        for thread in threads:
            thread.join(timeout=max(0.1, deadline - time.monotonic()))
        stragglers = [t for t in threads if t.is_alive()]
        if stragglers:
            self.ctx.log.log(
                "shutdown",
                f"cancelling connections of stuck threads: {[t.name for t in stragglers]}",
            )
            for client in self.ctx.clients:
                try:
                    client.hard_close()
                except Exception:
                    pass
            deadline = time.monotonic() + 30
            for thread in stragglers:
                thread.join(timeout=max(0.1, deadline - time.monotonic()))
            stragglers = [t for t in stragglers if t.is_alive()]
        if stragglers:
            # A thread that is still stuck after all disruptions were healed
            # and its connection was torn down is a liveness bug.
            faulthandler.dump_traceback()
            self.fail(
                InvariantViolation(
                    f"threads stuck after healing: {[t.name for t in stragglers]}"
                )
            )

    def _worker_loop(
        self, bundle: WorkerBundle, rng: random.Random, stats: Counter
    ) -> None:
        ctx = self.ctx
        try:
            while not ctx.stop.is_set():
                action = rng.choices(bundle.actions, bundle.weights)[0]
                try:
                    outcome = action.run()
                    key = outcome.value if outcome is not None else "ok"
                    stats[(action.name, key)] += 1
                    if outcome in (Outcome.UNKNOWN, Outcome.FAILED):
                        ctx.log.log("op", f"{action.name}: {outcome.value}", echo=False)
                except TransientError as e:
                    stats[(action.name, "transient")] += 1
                    ctx.log.log("op", f"{action.name} transient: {e}", echo=False)
                if ctx.stop.wait(rng.uniform(*ctx.complexity.op_delay)):
                    break
        except BaseException as e:
            self.fail(e)
        finally:
            try:
                bundle.close()
            except Exception:
                pass

    def _checker_loop(self, checker: Checker, rng: random.Random) -> None:
        ctx = self.ctx
        opened = False
        try:
            while not ctx.stop.is_set():
                try:
                    if not opened:
                        checker.open()
                        opened = True
                    before = checker.validations
                    checker.check_once()
                    checker.rounds += 1
                    if (
                        checker.validations > before
                        and self.disruptor is not None
                        and self.disruptor.active.is_set()
                    ):
                        checker.validations_during += 1
                except TransientError as e:
                    checker.skipped += 1
                    ctx.log.log("check", f"{checker.name} skipped: {e}", echo=False)
                if ctx.stop.wait(rng.uniform(*checker.pause)):
                    break
        except BaseException as e:
            self.fail(e)
        finally:
            try:
                checker.close()
            except Exception:
                pass

    def _report(self, final: bool) -> None:
        ops: Counter[str] = Counter()
        for stats in self.worker_stats:
            # dict(stats) snapshots in one C-level step: the owning worker
            # thread concurrently inserts new keys, and iterating the live
            # Counter would racily raise "dictionary changed size".
            for (_, key), count in dict(stats).items():
                ops[key] += count
        checks = ", ".join(
            f"{c.name}: {c.rounds} rounds/{c.validations} checks/{c.skipped} skipped"
            for c in self.checkers
        )
        cycles = self.disruptor.cycles if self.disruptor else 0
        agitation = (
            f" cancels={self.agitator.cancels} flag_flips={self.agitator.flips}"
            if self.agitator
            else ""
        )
        self.ctx.log.log(
            "stats",
            f"ops={dict(ops)} checkers=[{checks}]"
            f" disruption_cycles={cycles}{agitation}",
        )
        if final:
            for i, stats in enumerate(self.worker_stats):
                self.ctx.log.log("stats", f"worker-{i}: {dict(stats)}", echo=False)
            if self.disruptor is not None:
                coverage = ", ".join(
                    f"{target}/{kind}: {count}"
                    for (target, kind), count in sorted(self.disruptor.coverage.items())
                )
                self.ctx.log.log("stats", f"disruption coverage: [{coverage}]")
                during = ", ".join(
                    f"{c2.name}: {c2.validations_during}" for c2 in self.checkers
                )
                self.ctx.log.log("stats", f"validations during disruptions: [{during}]")

    def _committed_total(self) -> int:
        return sum(
            count
            for stats in self.worker_stats
            for (_, key), count in dict(stats).items()
            if key in ("ok", Outcome.COMMITTED.value)
        )

    def _check_vacuity(self) -> None:
        """A run that stopped verifying anything must not pass silently.

        Beyond "verified at least once", every checker and the workers must
        have made progress in BOTH halves of the chaos phase: a thread that
        wedges midway would otherwise still pass, and disruptions are capped
        well below a half, so a healthy run always progresses in each.
        """
        committed = self._committed_total()
        if committed == 0:
            raise InvariantViolation("vacuous run: no worker op ever committed")
        for checker in self.checkers:
            if checker.validations == 0:
                raise InvariantViolation(
                    f"vacuous run: checker {checker.name} never verified its"
                    " invariant"
                )
        # Only meaningful at CI-scale runtimes: in short local runs a single
        # watchdog-bounded hung peek can legitimately eat most of a half.
        if self._half_marks is not None and self.runtime >= 600:
            half_validations, half_committed = self._half_marks
            for checker, at_half in zip(self.checkers, half_validations):
                if at_half == 0 or checker.validations <= at_half:
                    raise InvariantViolation(
                        f"stalled run: checker {checker.name} verified"
                        f" {at_half} times in the first half but"
                        f" {checker.validations - at_half} in the second"
                    )
            if half_committed == 0 or committed <= half_committed:
                raise InvariantViolation(
                    f"stalled run: {half_committed} ops committed in the first"
                    f" half, {committed - half_committed} in the second"
                )
        if self.disruptor is not None and self.disruptor.cycles < 2:
            raise InvariantViolation(
                f"vacuous run: only {self.disruptor.cycles} disruption cycles"
            )
        if (
            self.agitator is not None
            and self.agitator.cancels + self.agitator.flips == 0
        ):
            raise InvariantViolation("vacuous run: the agitator never acted")

    def _dump_diagnostics(self) -> None:
        log = self.ctx.log
        log.log("phase", "diagnostics")
        if self.disruptor:
            for line in self.disruptor.history:
                log.log("diag", f"disruption: {line}")
        try:
            for name, proxy in self.toxiproxy.proxies().items():
                log.log(
                    "diag",
                    f"proxy {name}: enabled={proxy['enabled']}"
                    f" toxics={proxy['toxics']}",
                )
        except Exception as e:
            log.log("diag", f"toxiproxy state unavailable: {e}")
        client = MzClient(self.ctx, "diagnostics")
        for what, sql in [
            (
                "source statuses",
                "SELECT name, status, error FROM mz_internal.mz_source_statuses",
            ),
            (
                "sink statuses",
                "SELECT name, status, error FROM mz_internal.mz_sink_statuses",
            ),
        ]:
            try:
                for row in client.query(sql, timeout=30):
                    log.log("diag", f"{what}: {row}")
            except Exception as e:
                log.log("diag", f"{what} unavailable: {e}")
        try:
            self.scenario.diagnostics()
        except Exception as e:
            log.log("diag", f"scenario diagnostics failed: {e}")
        client.reset()
        self._report(final=True)
