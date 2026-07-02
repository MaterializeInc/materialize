# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Reusable checker building blocks: bounded peeks and SUBSCRIBE tailing."""

import random
from abc import abstractmethod
from collections import Counter

from materialize.invariants.framework import (
    Checker,
    InvariantViolation,
    ScenarioContext,
    TransientError,
)
from materialize.invariants.mz import MzClient, UnexpectedQueryError


class PeekChecker(Checker):
    """Base for checkers that verify invariants with one-shot SELECTs.

    Peeks round-robin over the given clusters so that a disrupted cluster is
    probed (it must serve correct data or nothing) while an undisrupted
    cluster keeps providing coverage. Reads of objects maintained on a
    disrupted cluster legitimately hang on every cluster (the persist upper
    stalls), which surfaces as a skipped round via the client watchdog.
    """

    def __init__(
        self, rng: random.Random, ctx: ScenarioContext, name: str, clusters: list[str]
    ) -> None:
        super().__init__(rng)
        self.name = name
        self.ctx = ctx
        self.clusters = clusters
        self._round = 0
        self.client = MzClient(ctx, name)

    def peek(self, sql: str) -> list[tuple]:
        cluster = self.clusters[self._round % len(self.clusters)]
        self._round += 1
        self.client.query(f"SET cluster = {cluster}")
        return self.client.query(sql)

    def close(self) -> None:
        self.client.reset()


class SubscribeChecker(Checker):
    """Base for checkers that tail a query via SUBSCRIBE ... WITH (PROGRESS).

    Maintains the result multiset from the diff stream and calls
    `validate_state` at progress boundaries, where the accumulated state is a
    transactionally consistent snapshot. Timestamps must be non-decreasing
    within one SUBSCRIBE session. After any transient error the subscription
    restarts from a fresh snapshot and all session state is reset, since no
    cross-session continuity is guaranteed.

    Updates at time t may arrive interleaved around a progress row at p <= t,
    so updates are buffered per timestamp and only folded into the state once
    a progress row proves their timestamp complete.
    """

    pause = (0.0, 0.2)

    def __init__(
        self,
        rng: random.Random,
        ctx: ScenarioContext,
        name: str,
        inner_query: str,
        cluster: str = "quickstart",
        durable: bool = False,
    ) -> None:
        super().__init__(rng)
        self.name = name
        self.ctx = ctx
        self.inner_query = inner_query
        self.cluster = cluster
        # When True, a restarted session resumes from the last validated
        # progress timestamp instead of taking a fresh snapshot, carrying
        # the reconstructed state across reconnects (the documented durable
        # subscription pattern). The subscribed object needs RETAIN HISTORY
        # so the resume timestamp stays readable.
        self.durable = durable
        # Fresh sessions may pin a historical start, e.g. the end-of-run
        # history audit replays from the earliest retained timestamp.
        self.as_of_clause = ""
        self.client = MzClient(ctx, name)
        self._cursor = "c_" + name.replace("-", "_")
        self._active = False
        self.last_validated_ts: int | None = None
        self.resumes = 0
        self._reset_session()

    def _reset_session(self) -> None:
        self._state: Counter[tuple] = Counter()
        self._pending: list[tuple[int, int, tuple]] = []
        self._last_ts: int | None = None
        self._as_of: int | None = None
        self._resumed = False

    def check_once(self) -> None:
        try:
            if not self._active:
                self._start_session()
                self._active = True
            rows = self.client.query(
                f"FETCH ALL {self._cursor} WITH (timeout = '1s')",
                timeout=max(30.0, self.ctx.complexity.query_timeout),
            )
        except TransientError:
            self._active = False
            raise
        self._process(rows)

    def _start_session(self) -> None:
        resume_from = self.last_validated_ts if self.durable else None
        if resume_from is not None:
            # Reset the per-session fields but keep the carried state: a
            # transiently failing resume (e.g. envd still restarting) must
            # not lose it, the next round retries the resume with it.
            self._pending = []
            self._last_ts = None
            self._as_of = None
            self._resumed = True
            try:
                self.client.query(f"SET cluster = {self.cluster}")
                self.client.query("BEGIN")
                # AS OF resume_from - 1 with SNAPSHOT false emits exactly the
                # updates with ts >= resume_from, which is the suffix the
                # carried state (complete below resume_from) is missing.
                self.client.query(
                    f"DECLARE {self._cursor} CURSOR FOR"
                    f" SUBSCRIBE ({self.inner_query})"
                    f" WITH (PROGRESS, SNAPSHOT = false) AS OF {resume_from - 1}"
                )
                self.resumes += 1
                return
            except UnexpectedQueryError as e:
                # The resume timestamp was compacted away. Fall back to a
                # fresh snapshot. The failed DECLARE dropped the connection,
                # so start over from a clean session.
                self.ctx.log.log(
                    "check", f"{self.name}: resume failed, fresh snapshot: {e}"
                )
                self.last_validated_ts = None
        self._reset_session()
        self.client.query(f"SET cluster = {self.cluster}")
        self.client.query("BEGIN")
        self.client.query(
            f"DECLARE {self._cursor} CURSOR FOR"
            f" SUBSCRIBE ({self.inner_query}) WITH (PROGRESS) {self.as_of_clause}"
        )

    def _process(self, rows: list[tuple]) -> None:
        for row in rows:
            ts = int(row[0])
            progressed = bool(row[1])
            if self._last_ts is not None and ts < self._last_ts:
                raise InvariantViolation(
                    f"{self.name}: SUBSCRIBE timestamp went backwards:"
                    f" {ts} < {self._last_ts}"
                )
            self._last_ts = ts
            if self._as_of is None:
                # The first row is guaranteed to be a progress message at the
                # subscription's as_of.
                self._as_of = ts
            if progressed:
                self._apply_pending(ts)
            else:
                self._pending.append((ts, int(row[2]), tuple(row[3:])))

    def _apply_pending(self, progress_ts: int) -> None:
        ready = sorted(
            (p for p in self._pending if p[0] < progress_ts), key=lambda p: p[0]
        )
        self._pending = [p for p in self._pending if p[0] >= progress_ts]
        # A progress row only proves completeness strictly below its
        # timestamp, and snapshot updates carry the as_of itself, so states
        # are validatable snapshots only for progress strictly beyond the
        # as_of.
        gate_open = self._as_of is not None and progress_ts > self._as_of
        # Validate after every distinct proven-complete timestamp, not just
        # at progress rows: a dataflow catching up from a historical as_of
        # (the history audit) advances its frontier in giant steps, and the
        # per-timestamp states in between must be consistent too.
        index = 0
        while index < len(ready):
            ts = ready[index][0]
            while index < len(ready) and ready[index][0] == ts:
                _, diff, data = ready[index]
                self._state[data] += diff
                if self._state[data] == 0:
                    del self._state[data]
                index += 1
            if gate_open:
                self._validate_snapshot(ts)
        if not gate_open:
            return
        self._validate_snapshot(progress_ts)
        self.last_validated_ts = progress_ts

    def _validate_snapshot(self, ts: int) -> None:
        for data, count in self._state.items():
            if count < 0:
                raise InvariantViolation(
                    f"{self.name}: negative multiplicity {count} for row"
                    f" {data} at {ts}"
                )
        try:
            self.validate_state(dict(self._state), ts)
        except InvariantViolation as e:
            # Attach what triage needs to classify the violation: a direct
            # read of the same query at the violating timestamp (on a fresh
            # connection, the subscribe connection is inside a transaction)
            # distinguishes an inconsistent shard from an inconsistent
            # subscribe stream, and the session context shows whether a
            # durable resume was involved.
            probe: object = "unavailable"
            try:
                probe_client = MzClient(self.ctx, f"{self.name}-probe")
                probe = probe_client.query(f"{self.inner_query} AS OF {ts}", timeout=30)
                probe_client.reset()
            except Exception as probe_error:
                probe = f"failed: {probe_error}"
            raise InvariantViolation(
                f"{e} [session={'resumed' if self._resumed else 'fresh'}"
                f" as_of={self._as_of} resumes={self.resumes}"
                f" last_validated={self.last_validated_ts};"
                f" direct read AS OF {ts}: {probe}]"
            ) from None
        self.validations += 1

    @abstractmethod
    def validate_state(self, state: dict[tuple, int], ts: int) -> None:
        """Verify one transactionally consistent snapshot of the query."""

    def close(self) -> None:
        self.client.reset()
