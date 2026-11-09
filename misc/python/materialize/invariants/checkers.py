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
from collections.abc import Callable

from materialize.invariants.framework import (
    Checker,
    InvariantViolation,
    ScenarioContext,
    TransientError,
    Watermark,
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
        self.last_cluster: str | None = None
        self.client = MzClient(ctx, name)

    def peek(self, sql: str) -> list[tuple]:
        cluster = self.clusters[self._round % len(self.clusters)]
        self._round += 1
        # Recorded so a violation can name the cluster that produced it.
        self.last_cluster = cluster
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
        snapshot: bool = True,
        append_only: bool = False,
    ) -> None:
        super().__init__(rng)
        self.name = name
        self.ctx = ctx
        self.inner_query = inner_query
        self.cluster = cluster
        # False subscribes to the changes from the subscription's as_of on,
        # which is the only affordable way to watch a relation whose snapshot
        # is large.
        self.snapshot = snapshot
        # For a relation nothing ever deletes from, a retraction in the change
        # stream is a bug on its own, no matter what the folded state says: an
        # insert and its bogus retraction cancel out and leave the state
        # looking right.
        self.append_only = append_only
        # When True, a restarted session resumes from the last validated
        # progress timestamp instead of taking a fresh snapshot, carrying
        # the reconstructed state across reconnects (the documented durable
        # subscription pattern). The subscribed object needs RETAIN HISTORY
        # so the resume timestamp stays readable.
        self.durable = durable
        # Fresh sessions may pin a historical start, e.g. the end-of-run
        # history audit replays from the earliest retained timestamp.
        self.as_of_clause = ""
        # Optional "WITHIN TIMESTAMP ORDER BY <expr>" clause plus the index
        # into the data tuple to assert the documented in-timestamp ordering.
        self.order_clause = ""
        self.order_index: int | None = None
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
        self._last_order_key: tuple[int, tuple] | None = None

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

    def drop_session(self) -> None:
        """Drop the connection while keeping the carried state.

        The next round reconnects, which for a durable subscriber is the
        resume path, so a caller can exercise that path without waiting for a
        disruption to happen to break this particular session.
        """
        self._active = False
        self.client.reset()

    def _start_session(self) -> None:
        resume_from = self.last_validated_ts if self.durable else None
        if resume_from is not None:
            # Reset the per-session fields but keep the carried state: a
            # transiently failing resume (e.g. envd still restarting) must
            # not lose it, the next round retries the resume with it.
            self._pending = []
            self._last_ts = None
            self._as_of = None
            self._last_order_key = None
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
        snapshot_clause = "" if self.snapshot else ", SNAPSHOT = false"
        self.client.query(
            f"DECLARE {self._cursor} CURSOR FOR"
            f" SUBSCRIBE ({self.inner_query}) {self.order_clause}"
            f" WITH (PROGRESS{snapshot_clause}) {self.as_of_clause}"
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
                data = tuple(row[3:])
                diff = int(row[2])
                if self.append_only and diff < 0:
                    raise InvariantViolation(
                        f"{self.name}: retraction {diff} of row {data} at {ts}"
                        " in an append-only relation"
                    )
                if self.order_index is not None:
                    # WITHIN TIMESTAMP ORDER BY: rows of one timestamp must
                    # arrive sorted by the ordering expression.
                    key = (ts, (data[self.order_index],))
                    last = self._last_order_key
                    if last is not None and last[0] == ts and key[1] < last[1]:
                        raise InvariantViolation(
                            f"{self.name}: rows within timestamp {ts} out of"
                            f" order: {key[1]} after {last[1]}"
                        )
                    self._last_order_key = key
                self._pending.append((ts, int(row[2]), data))

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
        validated_ready = False
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
                validated_ready = True
        if not gate_open:
            return
        # The state at progress_ts equals the state after the last folded
        # timestamp, so only progress rows that folded nothing validate here.
        if not validated_ready:
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

    def recycle(self) -> None:
        """Start over with a fresh subscription and no carried state.

        A snapshot-free subscription accumulates every change it has seen, so a
        checker that validates the whole accumulated state has to bound it.
        """
        self.client.reset()
        self._active = False
        self.last_validated_ts = None
        self._reset_session()

    def close(self) -> None:
        self.client.reset()


class ProgressPeek(PeekChecker):
    """A source's ingestion progress must never move backwards.

    NOTE: doc/user documents a `<name>_progress` relation per source, but
    sources created with the current syntax do not create one (see
    FINDINGS-BUGS.md B2), so this reads the source's write frontier from
    mz_internal.mz_frontiers instead. Reads run on one session under the
    default strict serializable isolation, so two consecutive reads are
    ordered in real time and the frontier must be non-decreasing.
    """

    pause = (1.0, 4.0)

    def __init__(self, rng, ctx, object_name: str, name: str = "progress-peek") -> None:
        super().__init__(rng, ctx, name, ["quickstart"])
        self.query = (
            "SELECT f.write_frontier::text::numeric"
            " FROM mz_internal.mz_frontiers f"
            " JOIN mz_objects o ON f.object_id = o.id"
            f" WHERE o.name = '{object_name}'"
        )
        self.last: int | None = None

    def check_once(self) -> None:
        rows = self.peek(self.query)
        if len(rows) != 1 or rows[0][0] is None:
            # The catalog row can lag object creation, and a NULL frontier
            # (the empty frontier) only occurs for completed collections.
            raise TransientError(f"{self.name}: no frontier row: {rows}")
        value = int(rows[0][0])
        if self.last is not None and value < self.last:
            raise InvariantViolation(
                f"{self.name}: write frontier moved backwards:"
                f" {value} < {self.last}"
            )
        self.last = value
        self.validations += 1


class GroupCompletenessPeek(PeekChecker):
    """Every group written by one upstream transaction must be whole.

    A write that spans several rows is only atomic if all of its rows become
    visible at the same timestamp. Per-row checks cannot see a violation of
    that, and a conserved-sum check only sees the ones that happen to change
    the sum, so this asks the question directly: group the rows by the
    transaction that wrote them and require every group to have the size that
    transaction gave it.

    The invariant is carried by the data rather than by the checker, so
    nothing here has to know which transactions ran or which of them
    committed. That is what makes it usable during chaos, where most op
    outcomes are unknown. It also holds at every timestamp, so it can be
    asked of retained history, where a tear that has since healed is still
    recorded.

    `predicate` must select the offending groups and return no rows when the
    invariant holds.
    """

    pause = (0.5, 2.0)

    def __init__(
        self,
        rng,
        ctx,
        name: str,
        predicate: str,
        clusters: list[str] | None = None,
        history: Watermark | None = None,
    ) -> None:
        super().__init__(rng, ctx, name, clusters or ["quickstart", "compute"])
        self.predicate = predicate
        # When given, some rounds ask the same question of a past timestamp.
        # A torn transaction is durable in the shard's history even after the
        # live state has converged, which turns a race that reproduces once a
        # week into an artifact that can be found after the fact.
        self.history = history

    def check_once(self) -> None:
        query, at = self.predicate, "now"
        recent = self.history.get() if self.history is not None else 0
        if recent > 0 and self.rng.random() < 0.25:
            as_of = self.rng.randint(max(recent - 120_000, 1), recent)
            query, at = f"{self.predicate} AS OF {as_of}", str(as_of)
        try:
            rows = self.peek(query)
        except UnexpectedQueryError as e:
            # The probed timestamp can fall behind the since while a
            # disruption stalls this round, the same race the other history
            # probes hit. Unreadable is a skip, a torn group is not.
            if "could not find a valid timestamp" in str(e):
                raise TransientError(f"{at} no longer readable") from None
            raise
        if rows:
            raise InvariantViolation(
                f"{self.name}: transaction not applied atomically at {at} on"
                f" {self.last_cluster}: {rows[:5]}"
            )
        self.validations += 1


class TernaryPartitionPeek(PeekChecker):
    """Splitting a relation by a predicate must not create or lose rows.

    For any total predicate p, the rows where p holds, where it does not, and
    where it is NULL partition the relation exactly, at every timestamp and
    whatever the data is. So this holds without knowing anything about the
    workload or which of its operations committed, which is what lets it run
    during a disruption, and it is a different question from the conserved
    totals the scenarios otherwise assert: it is about whether predicates are
    evaluated correctly, including when persist skips parts by their
    statistics rather than reading them.

    Both the row count and a summed value are partitioned, because a filter
    that drops one row and duplicates another keeps the count.

    `predicate` must produce **total** expressions. Anything that can raise,
    a division or a fallible cast, breaks the comparison: the three branches
    would fail differently rather than disagree.
    """

    pause = (0.5, 2.0)

    def __init__(
        self,
        rng,
        ctx,
        name: str,
        relation: str,
        predicate: Callable[[random.Random], str],
        value: str,
        history: Watermark | None = None,
    ) -> None:
        super().__init__(rng, ctx, name, ["quickstart", "compute"])
        self.relation = relation
        self.predicate = predicate
        self.value = value
        self.history = history

    def check_once(self) -> None:
        p = self.predicate(self.rng)
        # One statement, so all four reads share a timestamp. Written as
        # differences so the check is "both zero" rather than a comparison of
        # four numbers that a disruption could interleave.
        query = (
            f"SELECT (SELECT count(*) FROM {self.relation})"
            f" - (SELECT count(*) FROM {self.relation} WHERE {p})"
            f" - (SELECT count(*) FROM {self.relation} WHERE NOT ({p}))"
            f" - (SELECT count(*) FROM {self.relation} WHERE ({p}) IS NULL),"
            f" (SELECT coalesce(sum({self.value}), 0) FROM {self.relation})"
            f" - (SELECT coalesce(sum({self.value}), 0) FROM {self.relation}"
            f" WHERE {p})"
            f" - (SELECT coalesce(sum({self.value}), 0) FROM {self.relation}"
            f" WHERE NOT ({p}))"
            f" - (SELECT coalesce(sum({self.value}), 0) FROM {self.relation}"
            f" WHERE ({p}) IS NULL)"
        )
        at = "now"
        recent = self.history.get() if self.history is not None else 0
        if recent > 0 and self.rng.random() < 0.25:
            as_of = self.rng.randint(max(recent - 120_000, 1), recent)
            query, at = f"{query} AS OF {as_of}", str(as_of)
        try:
            rows = self.peek(query)
        except UnexpectedQueryError as e:
            if "could not find a valid timestamp" in str(e):
                raise TransientError(f"{at} no longer readable") from None
            raise
        count_diff, value_diff = int(rows[0][0]), int(rows[0][1])
        if count_diff or value_diff:
            raise InvariantViolation(
                f"{self.name}: `{p}` does not partition {self.relation} at {at}"
                f" on {self.last_cluster}: {count_diff} rows and {value_diff}"
                f" of {self.value} unaccounted for"
            )
        self.validations += 1


class ReplicaDivergence(Checker):
    """Both replicas of a cluster must answer identically.

    The other checkers read through the cluster, so whichever replica the
    coordinator picks is the one that gets verified, and a replica that
    computes the wrong answer is caught only if it happens to be chosen. This
    asks each replica directly, at one explicit timestamp, so the two answers
    are comparable by construction: same query, same logical time, different
    process. Any difference is a compute determinism bug, not a race.

    The queries must read objects the cluster maintains, so each replica
    computes the answer independently, unlike a table read which every
    replica serves from persist. Those objects also need a RETAIN HISTORY
    long enough to cover the probed timestamp.

    A disrupted replica cannot serve the timestamp and the round is skipped,
    which is the same contract every other checker follows.
    """

    name = "replica-divergence"
    pause = (1.0, 3.0)

    def __init__(
        self,
        rng: random.Random,
        ctx: ScenarioContext,
        queries: tuple[str, ...],
        cluster: str = "compute",
        replicas: tuple[str, ...] = ("r1", "r2"),
    ) -> None:
        super().__init__(rng)
        self.ctx = ctx
        self.client = MzClient(ctx, self.name)
        self.queries = queries
        self.cluster = cluster
        self.replicas = replicas

    def check_once(self) -> None:
        self.client.query(f"SET cluster = {self.cluster}")
        # One timestamp for every read of a round. Slightly in the past so it
        # is already readable rather than something both replicas have to wait
        # for, and well inside the RETAIN HISTORY of the objects involved.
        now = int(self.client.query("SELECT mz_now()::text")[0][0])
        as_of = now - 1000
        for sql in self.queries:
            answers = {}
            for replica in self.replicas:
                self.client.query(f"SET cluster_replica = {replica}")
                try:
                    rows = self.client.query(f"{sql} AS OF {as_of}")
                except UnexpectedQueryError as e:
                    # Same race the history probes hit: a timestamp that was
                    # readable when it was chosen can fall behind the since
                    # while a disruption stalls this round. Unreadable is a
                    # skip, disagreement at a readable one stays fatal.
                    if "could not find a valid timestamp" in str(e):
                        raise TransientError(f"{as_of} no longer readable") from None
                    raise
                answers[replica] = tuple(tuple(row) for row in rows)
            self.validations += 1
            first = self.replicas[0]
            for replica in self.replicas[1:]:
                if answers[replica] != answers[first]:
                    raise InvariantViolation(
                        f"replicas disagree at {as_of} on `{sql}`:"
                        f" {first}={answers[first]} {replica}={answers[replica]}"
                    )
        # Later rounds must not inherit a pin to one replica.
        self.client.query("RESET cluster_replica")

    def close(self) -> None:
        self.client.reset()
