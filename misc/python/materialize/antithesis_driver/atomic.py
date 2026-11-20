# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Multi-table write transactions: no read sees part of one.

A transaction inserts one row with its marker into each of two or three
tables, then commits, or rolls back on purpose. A read of all tables in one
statement must show each marker in all of its tables or in none, never a
rolled-back marker, and every marker acknowledged, or seen whole by an
earlier read, before the read began. `atomic_commits_model.violations` judges
one read. The assertion messages are those of the legacy atomic commits
driver.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    reachable,
    sometimes,
)

from materialize.antithesis import atomic_commits_model as model
from materialize.antithesis_driver.client import Op, Outcome, Status, commit
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context
from materialize.antithesis_driver.draw import Draw

SCHEMA = "atomic"
SHARDS = 3
PRUNE_ABOVE = 128
"""Settled transactions kept before their rows are deleted."""
SETTINGS = {"transaction_isolation": "'strict serializable'", "cluster": "quickstart"}


def setup_statements() -> list[str]:
    return [f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"] + [
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t{s} (id bigint, v bigint)"
        for s in range(SHARDS)
    ]


@dataclass
class Txn:
    shards: tuple[int, ...]
    value: int
    rollback: bool
    outcome: str = "pending"
    """`pending`, `ok`, `rejected` or `indeterminate`."""
    acked_at: float | None = None
    observed_at: float | None = None
    """When the first read that saw it whole completed."""
    retired: bool = False
    """Its rows are being deleted, so a read may see some of them."""


class Atomic:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self._next = 1
        self.txns: dict[int, Txn] = {}
        self.rollback_share = 0.25
        self._pruning = False

    def configure(self, d: Draw) -> None:
        self.rollback_share = d.choice((0.0, 0.25, 0.5))

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def write(self, d: Draw) -> Status:
        shards = d.choice(((0, 1), (0, 2), (1, 2), (0, 1, 2)))
        txn = Txn(shards, d.integer(-(2**31), 2**31), d.boolean(self.rollback_share))
        with self._lock:
            marker = self._next
            self._next += 1
            self.txns[marker] = txn

        sent_commit: list[bool] = []

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(b"BEGIN")
            for s in shards:
                conn.execute(
                    f"INSERT INTO {SCHEMA}.t{s} VALUES ({marker}, {txn.value})".encode()
                )
            if txn.rollback:
                conn.execute(b"ROLLBACK")
            else:
                sent_commit.append(True)
                commit(conn)

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                if outcome.ok and not txn.rollback:
                    txn.outcome, txn.acked_at = "ok", outcome.end
                elif outcome.ok or outcome.rejected or not sent_commit:
                    # A transaction that never sent COMMIT never commits.
                    txn.outcome = "rejected"
                else:
                    txn.outcome = "indeterminate"
                observed = txn.observed_at is not None
            if txn.outcome == "rejected":
                always(
                    not observed,
                    "atomic commits: an observed committed transaction is never reported rejected",
                    {"marker": marker, "rollback": txn.rollback},
                )

        return self.ctx.sessions.call(
            self._session(d),
            Op("atomic write", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def _read_op(self, done: list[bool]) -> Op:
        query = " UNION ALL ".join(
            f"SELECT {s}::int, id, v FROM {SCHEMA}.t{s}" for s in range(SHARDS)
        )

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(query.encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            rows = [(int(s), int(m), int(v)) for s, m, v in outcome.value]
            with self._lock:
                plans = {m: self._plan(t, outcome.start) for m, t in self.txns.items()}
            errors = model.violations(plans, rows)
            visible: dict[int, set[int]] = {}
            for s, m, _ in rows:
                visible.setdefault(m, set()).add(s)
            always(
                not errors,
                "atomic commits: one snapshot preserves complete transaction markers across tables",
                {"errors": errors[:10], "rows": len(rows)},
            )
            with self._lock:
                for m, shards in visible.items():
                    t = self.txns.get(m)
                    if (
                        t is not None
                        and shards == set(t.shards)
                        and t.observed_at is None
                    ):
                        t.observed_at = outcome.end
                committed = sum(
                    1
                    for m in visible
                    if m in self.txns and len(self.txns[m].shards) > 1
                )
                rolled_back = any(
                    t.rollback and t.outcome == "rejected" for t in self.txns.values()
                )
            sometimes(
                committed > 0,
                "atomic commits: a snapshot checked a committed multi-table transaction",
                {"visible": committed},
            )
            sometimes(
                rolled_back,
                "atomic commits: a snapshot checked that a rolled-back transaction stays invisible",
                {},
            )
            done.append(True)

        return Op("atomic read", execute, complete, SETTINGS)

    def _plan(self, t: Txn, start: float) -> model.Plan:
        """What a read that began at `start` may see of `t`."""
        if t.outcome == "ok" and t.acked_at is not None and t.acked_at < start:
            outcome = "ok"
        elif t.observed_at is not None and t.observed_at < start:
            outcome = "observed"
        elif t.outcome == "rejected":
            outcome = "rejected"
        else:
            outcome = "indeterminate"
        return model.Plan(t.shards, t.value, outcome, t.retired)

    def read(self, d: Draw) -> Status:
        return self.ctx.sessions.call(
            self._session(d), self._read_op([]), RULE_WAIT_SECONDS
        )

    def prune(self, d: Draw) -> Status:
        """Delete the rows of settled transactions, so the tables stay small."""
        with self._lock:
            settled = [
                m
                for m, t in self.txns.items()
                if t.outcome in ("ok", "rejected") and not t.retired
            ]
            if self._pruning or len(settled) <= PRUNE_ABOVE:
                return Status.NOT_SENT
            self._pruning = True
            for m in settled:
                self.txns[m].retired = True
        ids = ", ".join(str(m) for m in settled)

        def execute(conn: psycopg.Connection) -> None:
            for s in range(SHARDS):
                conn.execute(f"DELETE FROM {SCHEMA}.t{s} WHERE id IN ({ids})".encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                self._pruning = False
                if outcome.ok:
                    # Every later read must miss them, and a row of one would
                    # be reported as unattempted.
                    for m in settled:
                        self.txns.pop(m, None)
            if outcome.ok:
                reachable(
                    "atomic commits: settled transactions were pruned",
                    {"count": len(settled)},
                )

        return self.ctx.sessions.call(
            self._session(d),
            Op("atomic prune", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def eventually(self, deadline: float) -> None:
        done: list[bool] = []
        while not done and time.monotonic() < deadline:
            for i in range(len(self.ctx.sessions.sessions)):
                if (
                    self.ctx.sessions.call(i, self._read_op(done), 60.0)
                    != Status.NOT_SENT
                ):
                    break
            if not done:
                time.sleep(1)
        always(
            bool(done),
            "atomic commits: a snapshot of every table succeeds after faults stop",
            {},
        )
