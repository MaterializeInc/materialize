# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Postgres source: exports equal the upstream tables at a consistent prefix.

One upstream session writes transactions of inserts, updates and deletes to
three tables, and a ledger row per table it touched. Writes are serial, so
the commit order is the order the model applies them in, and a COMMIT whose
outcome is unknown is resolved with `txid_status` before the next write: the
model of the upstream is exact.

A check reads the source's progress LSN, the ledger and every export in one
`SELECT`, so all at one timestamp. The visible ledger must be a prefix of
the commit order, each transaction whole, consistent with the mapped LSN.
Every export must equal its table replayed to that prefix, which also checks
that transactions spanning tables arrive atomically.

The safety assertions keep the messages of the legacy Postgres source
driver. The real-time recency, coverage and post-fault ones are new. That
driver bucketed its export checks by whether a TRUNCATE or DROP preceded the
last ingestion restart, known bug classes. This one issues neither, so every
check falls in the bucket without one.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote, urlparse

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)

from materialize.antithesis import sql
from materialize.antithesis_driver.client import (
    Op,
    Outcome,
    Pool,
    RolledBack,
    Status,
    Upstream,
    commit,
    connect,
)
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

UPSTREAM_SCHEMA = "drv"
PUBLICATION = "drv_pub"
SCHEMA = "pgsrc"
"""The Materialize schema of the source, its connection and its exports."""
CLUSTER = "sources"
TABLES = 3
CHECKPOINT_EVERY = 64
MAX_ADDED_COLUMNS = 8
RESOLVE_SECONDS = 20.0
SETTINGS = {"transaction_isolation": "'strict serializable'", "cluster": "quickstart"}
INTERESTING_VALUES = ("", "NULL", "'", '"', "\\", "κλειδί", "x" * 3000)


def parse_lsn(text: str) -> int:
    """An LSN as `X/Y`, as Postgres prints it, or as a decimal `uint8`."""
    if "/" in text:
        hi, lo = text.split("/")
        return (int(hi, 16) << 32) | int(lo, 16)
    return int(text)


def classify_upstream(error: BaseException) -> sql.Classified:
    """The upstream is not the system under test: any error it returns is
    the statement not taking effect, unless the connection went away."""
    if isinstance(error, RolledBack):
        return sql.Classified(sql.Outcome.REJECTED, None, "COMMIT answered ROLLBACK")
    if isinstance(error, psycopg.Error):
        state = error.sqlstate
        if state is None or state.startswith(("08", "57")):
            return sql.Classified(sql.Outcome.INDETERMINATE, state, str(error)[:200])
        return sql.Classified(sql.Outcome.REJECTED, state, str(error)[:200])
    return sql.Classified(sql.Outcome.INDETERMINATE, None, type(error).__name__)


@dataclass(frozen=True)
class Plan:
    """One DML statement, as drawn by a rule."""

    table: int
    kind: str
    """`upsert`, `update`, `delete` or `delete_range`."""
    key: int
    value: str | None = None
    high: int = 0
    """The upper key of a `delete_range`."""

    def statement(self) -> tuple[str, tuple[Any, ...]]:
        t = f"{UPSTREAM_SCHEMA}.t{self.table}"
        if self.kind == "upsert":
            return (
                f"INSERT INTO {t} VALUES (%s, %s) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
                (self.key, self.value),
            )
        if self.kind == "update":
            return f"UPDATE {t} SET v = %s WHERE k = %s", (self.value, self.key)
        if self.kind == "delete":
            return f"DELETE FROM {t} WHERE k = %s", (self.key,)
        assert self.kind == "delete_range"
        return f"DELETE FROM {t} WHERE k BETWEEN %s AND %s", (self.key, self.high)


def apply(states: list[dict[int, str | None]], plan: Plan) -> None:
    """Apply one statement's effect to table states, as Postgres does."""
    state = states[plan.table]
    if plan.kind == "upsert":
        state[plan.key] = plan.value
    elif plan.kind == "update":
        if plan.key in state:
            state[plan.key] = plan.value
    elif plan.kind == "delete":
        state.pop(plan.key, None)
    else:
        for k in [k for k in state if plan.key <= k <= plan.high]:
            del state[k]


@dataclass(frozen=True)
class Txn:
    id: int
    plans: tuple[Plan, ...]
    lsn_before: int
    lsn_after: int | None
    """None when the commit was resolved after its response was lost."""
    acked_at: float | None
    """When the client saw the commit acknowledged, if it did."""

    def tables(self) -> frozenset[int]:
        return frozenset(p.table for p in self.plans)


@dataclass
class Model:
    """The upstream tables and the committed transactions, in commit order."""

    committed: list[Txn] = field(default_factory=list)
    states: list[dict[int, str | None]] = field(
        default_factory=lambda: [{} for _ in range(TABLES)]
    )
    checkpoints: dict[int, list[dict[int, str | None]]] = field(
        default_factory=lambda: {0: [{} for _ in range(TABLES)]}
    )
    aborted: dict[int, frozenset[int]] = field(default_factory=dict)
    """Transactions that definitely did not commit, with their tables."""

    def commit(self, txn: Txn) -> None:
        for plan in txn.plans:
            apply(self.states, plan)
        self.committed.append(txn)
        if len(self.committed) % CHECKPOINT_EVERY == 0:
            self.checkpoints[len(self.committed)] = [dict(s) for s in self.states]

    def state_at(self, k: int) -> list[dict[int, str | None]]:
        """The tables after the first `k` committed transactions."""
        base = max(c for c in self.checkpoints if c <= k)
        states = [dict(s) for s in self.checkpoints[base]]
        for txn in self.committed[base:k]:
            for plan in txn.plans:
                apply(states, plan)
        return states


@dataclass
class Snapshot:
    """What one check read, all at one timestamp."""

    ts: int
    lsn: int | None
    lsn_rows: int
    ledger: list[tuple[int, int]]
    exports: list[list[tuple[int, str | None]]]


@dataclass
class Verdict:
    duplicate_ledger: list[tuple[int, int]] = field(default_factory=list)
    unattempted: list[int] = field(default_factory=list)
    metadata: list[int] = field(default_factory=list)
    torn: list[int] = field(default_factory=list)
    gaps: list[int] = field(default_factory=list)
    committed_below_missing: list[int] = field(default_factory=list)
    committed_below: int = 0
    begun_above_visible: list[int] = field(default_factory=list)
    prefix: int | None = None
    """How many committed transactions the read saw, if it saw a prefix."""
    duplicate_keys: dict[int, list[int]] = field(default_factory=dict)
    mismatches: dict[int, dict[str, Any]] = field(default_factory=dict)
    compared_rows: int = 0


def judge(model: Model, snap: Snapshot, pending: Txn | None = None) -> Verdict:
    """`pending` is a transaction whose COMMIT was sent and whose outcome the
    model does not know yet. Writes are serial, so if it committed it is the
    next in commit order, and a read that shows it proves it committed."""
    v = Verdict()
    counts = Counter(snap.ledger)
    v.duplicate_ledger = sorted(r for r, n in counts.items() if n > 1)
    seen: dict[int, set[int]] = {}
    for txn, tbl in snap.ledger:
        seen.setdefault(txn, set()).add(tbl)
    committed = list(model.committed)
    if pending is not None and pending.id in seen:
        committed.append(pending)
    by_id = {t.id: t for t in committed}
    for txn, tables in sorted(seen.items()):
        if txn in by_id:
            if not tables <= by_id[txn].tables():
                v.metadata.append(txn)
            elif tables != by_id[txn].tables():
                v.torn.append(txn)
        elif txn in model.aborted:
            v.metadata.append(txn)
        else:
            v.unattempted.append(txn)
    visible = set(seen) & set(by_id)
    k = 0
    while k < len(committed) and committed[k].id in visible:
        k += 1
    v.gaps = sorted(visible - {t.id for t in committed[:k]})
    if snap.lsn is not None:
        below = [
            t for t in committed if t.lsn_after is not None and t.lsn_after < snap.lsn
        ]
        v.committed_below = len(below)
        v.committed_below_missing = [t.id for t in below if t.id not in visible]
        v.begun_above_visible = sorted(
            t.id for t in committed if t.lsn_before > snap.lsn and t.id in visible
        )
    if v.duplicate_ledger or v.unattempted or v.metadata or v.torn or v.gaps:
        return v
    v.prefix = k
    expected = model.state_at(min(k, len(model.committed)))
    for txn in committed[len(model.committed) : k]:
        for plan in txn.plans:
            apply(expected, plan)
    for j, rows in enumerate(snap.exports):
        keys = Counter(key for key, _ in rows)
        dups = sorted(key for key, n in keys.items() if n > 1)
        if dups:
            v.duplicate_keys[j] = dups[:10]
            continue
        observed = dict(rows)
        if observed != expected[j]:
            missing = sorted(set(expected[j]) - set(observed))
            extra = sorted(set(observed) - set(expected[j]))
            changed = sorted(
                key
                for key in set(observed) & set(expected[j])
                if observed[key] != expected[j][key]
            )
            v.mismatches[j] = {
                "missing": missing[:10],
                "extra": extra[:10],
                "changed": changed[:10],
                "expected_rows": len(expected[j]),
                "observed_rows": len(observed),
            }
        v.compared_rows += len(observed)
    return v


class Postgres:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        url = ctx.config.upstream_postgres_url
        assert url is not None
        self.url = url
        self.upstream = Pool("pg", Upstream(url), 1)
        self._lock = threading.Lock()
        self.model = Model()
        self._next_txn = 1
        self.unresolved: tuple[Txn, int] | None = None
        """A transaction whose COMMIT was sent but not acknowledged, with its xid."""
        self.committing: Txn | None = None
        """The transaction whose COMMIT is in flight, until its op completes."""
        self.observed: list[tuple[int, int]] = []
        """(mz timestamp, mapped LSN) of checks, for monotonicity."""
        self.added_columns = [0] * TABLES
        self.keyspace = 8
        self.ops_per_txn = 2
        self.rtr_share = 0.0

    def configure(self, d: Draw) -> None:
        self.keyspace = d.choice((1, 8, 64, 512))
        self.ops_per_txn = d.choice((1, 2, 5, 20))
        self.rtr_share = d.choice((0.3, 0.0))

    def setup_upstream(self) -> None:
        """Create the upstream tables and publication, before setup completes."""
        with connect(Upstream(self.url), "driver-setup") as conn:
            conn.execute(f"DROP PUBLICATION IF EXISTS {PUBLICATION}".encode())
            conn.execute(f"DROP SCHEMA IF EXISTS {UPSTREAM_SCHEMA} CASCADE".encode())
            # A local loop rerun leaves the last run's slots behind.
            conn.execute(
                b"SELECT pg_drop_replication_slot(slot_name)"
                b" FROM pg_replication_slots WHERE NOT active"
            )
            conn.execute(f"CREATE SCHEMA {UPSTREAM_SCHEMA}".encode())
            for j in range(TABLES):
                t = f"{UPSTREAM_SCHEMA}.t{j}"
                conn.execute(f"CREATE TABLE {t} (k int PRIMARY KEY, v text)".encode())
                conn.execute(f"ALTER TABLE {t} REPLICA IDENTITY FULL".encode())
            conn.execute(
                f"CREATE TABLE {UPSTREAM_SCHEMA}.ledger"
                " (txn bigint, tbl int, PRIMARY KEY (txn, tbl))".encode()
            )
            conn.execute(
                f"ALTER TABLE {UPSTREAM_SCHEMA}.ledger REPLICA IDENTITY FULL".encode()
            )
            conn.execute(
                f"CREATE PUBLICATION {PUBLICATION} FOR TABLES IN SCHEMA {UPSTREAM_SCHEMA}".encode()
            )

    def setup_statements(self) -> list[str]:
        url = urlparse(self.url)
        # Materialize may reach the server at another address than the
        # driver, such as inside a Docker network: `host` or `host:port`.
        mz = urlparse(
            f"//{self.ctx.config.upstream_postgres_mz_host or url.netloc.split('@')[-1]}"
        )
        host, port = mz.hostname, mz.port or 5432
        password = unquote(url.password or "").replace("'", "''")
        database = url.path.lstrip("/") or "postgres"
        return [
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            f"CREATE SECRET IF NOT EXISTS {SCHEMA}.password AS '{password}'",
            f"CREATE CONNECTION IF NOT EXISTS {SCHEMA}.conn TO POSTGRES"
            f" (HOST '{host}', PORT {port}, USER '{unquote(url.username or 'postgres')}',"
            f" PASSWORD SECRET {SCHEMA}.password, DATABASE '{database}')",
            f"CREATE SOURCE IF NOT EXISTS {SCHEMA}.src IN CLUSTER {CLUSTER}"
            f" FROM POSTGRES CONNECTION {SCHEMA}.conn (PUBLICATION '{PUBLICATION}')",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.ledger FROM SOURCE {SCHEMA}.src"
            f" (REFERENCE {UPSTREAM_SCHEMA}.ledger)",
            *(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t{j} FROM SOURCE {SCHEMA}.src"
                f" (REFERENCE {UPSTREAM_SCHEMA}.t{j})"
                for j in range(TABLES)
            ),
        ]

    def _value(self, d: Draw) -> str | None:
        if d.boolean(0.1):
            return None
        if d.boolean(0.2):
            return d.choice(INTERESTING_VALUES)
        return f"{d.integer(0, 2**32):x}"

    def _plans(self, d: Draw) -> tuple[Plan, ...]:
        plans = []
        for _ in range(d.integer(1, self.ops_per_txn)):
            key = d.integer(0, self.keyspace - 1)
            kind = d.choice(
                ("upsert", "upsert", "upsert", "update", "delete", "delete_range")
            )
            plans.append(
                Plan(
                    table=d.integer(0, TABLES - 1),
                    kind=kind,
                    key=key,
                    value=self._value(d) if kind in ("upsert", "update") else None,
                    high=key + d.choice((0, 2, 16)),
                )
            )
        return tuple(plans)

    def _resolve(self) -> bool:
        """Settle the unresolved transaction from the upstream. Returns whether
        nothing is left unresolved. Runs on the upstream session's thread."""
        if self.unresolved is None:
            return True
        txn, xid = self.unresolved
        deadline = time.monotonic() + RESOLVE_SECONDS
        while time.monotonic() < deadline:
            try:
                with connect(Upstream(self.url), "driver-resolve") as conn:
                    row = conn.execute(b"SELECT txid_status(%s)", (xid,)).fetchone()
            except (psycopg.Error, OSError):
                time.sleep(1)
                continue
            status = row[0] if row else None
            if status in ("committed", "aborted"):
                with self._lock:
                    if status == "committed":
                        self.model.commit(txn)
                    else:
                        self.model.aborted[txn.id] = txn.tables()
                    self.unresolved = None
                sometimes(
                    status == "committed",
                    "postgres source: a transaction whose commit response was lost turned out committed",
                    {"txn": txn.id},
                )
                return True
            time.sleep(1)
        return False

    def write(self, d: Draw) -> Status:
        plans = self._plans(d)
        with self._lock:
            txn_id = self._next_txn
            self._next_txn += 1
        sent: dict[str, Any] = {}

        def execute(conn: psycopg.Connection) -> tuple[int, int] | None:
            if not self._resolve():
                return None
            conn.execute(b"BEGIN")
            row = conn.execute(
                b"SELECT pg_current_wal_insert_lsn()::text, txid_current()"
            ).fetchone()
            assert row is not None
            sent["lsn_before"], sent["xid"] = parse_lsn(row[0]), int(row[1])
            for plan in plans:
                statement, params = plan.statement()
                conn.execute(statement.encode(), params)
            for tbl in sorted({p.table for p in plans}):
                conn.execute(
                    f"INSERT INTO {UPSTREAM_SCHEMA}.ledger VALUES (%s, %s)".encode(),
                    (txn_id, tbl),
                )
            sent["commit"] = True
            with self._lock:
                self.committing = Txn(txn_id, plans, sent["lsn_before"], None, None)
            commit(conn)
            row = conn.execute(b"SELECT pg_current_wal_lsn()::text").fetchone()
            assert row is not None
            return sent["lsn_before"], parse_lsn(row[0])

        def complete(outcome: Outcome[tuple[int, int] | None]) -> None:
            if outcome.ok and outcome.value is None:
                return
            txn = Txn(txn_id, plans, sent.get("lsn_before", 0), None, None)
            with self._lock:
                self.committing = None
                if outcome.ok:
                    assert outcome.value is not None
                    lsn_before, lsn_after = outcome.value
                    self.model.commit(
                        Txn(txn_id, plans, lsn_before, lsn_after, outcome.end)
                    )
                elif outcome.unknown and sent.get("commit"):
                    self.unresolved = (txn, sent["xid"])
                else:
                    # Never committed: rejected, or lost before COMMIT was sent.
                    self.model.aborted[txn_id] = txn.tables()

        return self.upstream.call(
            0,
            Op(
                "postgres write",
                execute,
                complete,
                {"lock_timeout": "'10s'"},
                classify=classify_upstream,
            ),
            RULE_WAIT_SECONDS,
        )

    def add_column(self, d: Draw) -> Status:
        """`ALTER TABLE ... ADD COLUMN` upstream, which the exports ignore."""
        j = d.integer(0, TABLES - 1)
        if self.added_columns[j] >= MAX_ADDED_COLUMNS:
            return Status.NOT_SENT
        n = self.added_columns[j]
        statement = (
            f"ALTER TABLE {UPSTREAM_SCHEMA}.t{j} ADD COLUMN c{n} text DEFAULT 'c'"
        )

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            if outcome.ok:
                self.added_columns[j] = n + 1
            elif outcome.unknown:
                # The column may exist: never try this name again.
                self.added_columns[j] = n + 1

        return self.upstream.call(
            0,
            Op("postgres add column", execute, complete, classify=classify_upstream),
            RULE_WAIT_SECONDS,
        )

    def _read_op(self, rtr: bool, done: list[Verdict]) -> Op:
        branches = [f"SELECT 'p', lsn::text, NULL::text FROM {SCHEMA}.src"]
        branches.append(f"SELECT 'l', txn::text, tbl::text FROM {SCHEMA}.ledger")
        branches += [
            f"SELECT 't{j}', k::text, v FROM {SCHEMA}.t{j}" for j in range(TABLES)
        ]
        query = f"SELECT mz_now()::text, * FROM ({' UNION ALL '.join(branches)}) AS q"

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(query.encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            done.append(self._judge(outcome.value, outcome.start, rtr))

        settings = dict(SETTINGS)
        if rtr:
            settings["real_time_recency"] = "true"
        return Op(
            "postgres check",
            execute,
            complete,
            settings,
            classify=sql.classify_rtr_read if rtr else sql.classify,
        )

    def _judge(self, rows: list[tuple[Any, ...]], start: float, rtr: bool) -> Verdict:
        lsns = [r[2] for r in rows if r[1] == "p" and r[2] is not None]
        snap = Snapshot(
            ts=int(rows[0][0]) if rows else 0,
            lsn=max(parse_lsn(x) for x in lsns) if lsns else None,
            lsn_rows=len(lsns),
            ledger=[(int(r[2]), int(r[3])) for r in rows if r[1] == "l"],
            exports=[
                [(int(r[2]), r[3]) for r in rows if r[1] == f"t{j}"]
                for j in range(TABLES)
            ],
        )
        with self._lock:
            pending = self.committing or (
                self.unresolved[0] if self.unresolved else None
            )
            v = judge(self.model, snap, pending)
            acked_before = [
                t.id
                for t in self.model.committed
                if t.acked_at is not None and t.acked_at < start
            ]
            seen = {t.id for t in self.model.committed[: v.prefix or 0]}
            regressions = [
                (ts, lsn)
                for ts, lsn in self.observed
                if snap.lsn is not None
                and (
                    (ts <= snap.ts and lsn > snap.lsn)
                    or (ts >= snap.ts and lsn < snap.lsn)
                )
            ]
            if snap.lsn is not None:
                self.observed.append((snap.ts, snap.lsn))
                self.observed = self.observed[-1000:]
        base = {
            "ts": snap.ts,
            "lsn": snap.lsn,
            "ledger_rows": len(snap.ledger),
            "rtr": rtr,
        }
        always(
            not v.duplicate_ledger,
            "postgres source: the ledger export holds at most one row per primary key",
            {**base, "duplicates": v.duplicate_ledger[:10]},
        )
        always(
            not v.unattempted,
            "postgres source: every ledger row visible in Materialize was written by the workload",
            {**base, "unattempted": v.unattempted[:10]},
        )
        always(
            not v.metadata,
            "postgres source: visible ledger rows preserve recorded transaction metadata and exclude aborted transactions",
            {**base, "transactions": v.metadata[:10]},
        )
        always(
            not v.torn,
            "postgres source: a transaction's ledger rows are visible together or not at all",
            {**base, "torn": v.torn[:10]},
        )
        always(
            not v.gaps,
            "postgres source: visible ledger rows form a gap-free prefix of each table's commit order",
            {**base, "beyond_the_prefix": v.gaps[:10]},
        )
        always(
            not v.committed_below_missing,
            "postgres source: transactions committed below the mapped LSN are visible in the ledger",
            {**base, "missing": v.committed_below_missing[:10]},
        )
        always(
            not v.begun_above_visible,
            "postgres source: transactions begun above the mapped LSN are not visible in the ledger",
            {**base, "early": v.begun_above_visible[:10]},
        )
        always(
            not regressions,
            "postgres source: the mapped LSN is monotone in mz time",
            {**base, "conflicting": regressions[:5]},
        )
        if v.prefix is not None:
            always(
                not v.duplicate_keys,
                "postgres source: an export holds at most one row per upstream primary key, with no terminal upstream op before the last ingestion restart",
                {**base, "duplicates": v.duplicate_keys},
            )
            always(
                not v.mismatches,
                "postgres source: an export equals its upstream table replayed to the mapped LSN, with no terminal upstream op before the last ingestion restart",
                {**base, "prefix": v.prefix, "mismatches": v.mismatches},
            )
        if rtr:
            missing = sorted(set(acked_before) - seen)
            always(
                v.prefix is not None and not missing,
                "postgres source: a real-time recency read observes every upstream transaction committed before it began",
                {**base, "missing": missing[:10], "acked_before": len(acked_before)},
            )
        sometimes(
            v.committed_below > len(v.committed_below_missing),
            "postgres source: a ledger check found an acknowledged transaction committed below the mapped LSN",
            {**base, "committed_below": v.committed_below},
        )
        sometimes(
            v.compared_rows > 0 and not v.mismatches,
            "postgres source: an export check matched nonempty exports",
            {**base, "rows": v.compared_rows},
        )
        sometimes(
            sum(self.added_columns) > 0 and v.prefix is not None and not v.mismatches,
            "postgres source: exports still match after an upstream ADD COLUMN",
            {**base, "added": list(self.added_columns)},
        )
        return v

    def check(self, d: Draw) -> Status:
        return self.ctx.sessions.call(
            d.integer(0, len(self.ctx.sessions.sessions) - 1),
            self._read_op(d.boolean(self.rtr_share), []),
            RULE_WAIT_SECONDS,
        )

    def eventually(self, deadline: float) -> None:
        """After faults stop, the source catches up with every committed write."""
        while self.unresolved is not None and time.monotonic() < deadline:
            done_resolving: list[bool] = []

            def complete(outcome: Outcome[bool]) -> None:
                if outcome.ok and outcome.value:
                    done_resolving.append(True)

            # `_resolve` must run on the upstream session's thread, which
            # also runs writes, or two threads could commit one transaction.
            op = Op(
                "postgres resolve",
                lambda conn: self._resolve(),
                complete,
                classify=classify_upstream,
            )
            self.upstream.call(0, op, 60.0)
            if not done_resolving:
                time.sleep(1)
        with self._lock:
            committed = len(self.model.committed)
        caught_up = False
        verdict: Verdict | None = None
        while time.monotonic() < deadline:
            done: list[Verdict] = []
            for i in range(len(self.ctx.sessions.sessions)):
                if (
                    self.ctx.sessions.call(i, self._read_op(False, done), 60.0)
                    != Status.NOT_SENT
                ):
                    break
            if done:
                verdict = done[0]
                if verdict.prefix is not None and verdict.prefix >= committed:
                    caught_up = True
                    break
                if verdict.prefix is None:
                    break
            time.sleep(2)
        log(
            f"postgres source: caught up {caught_up}, prefix {verdict.prefix if verdict else None} of {committed}"
        )
        always(
            caught_up,
            "postgres source: after faults stop the source reflects every committed upstream transaction",
            {
                "committed": committed,
                "prefix": verdict.prefix if verdict else None,
                "unresolved": self.unresolved is not None,
            },
        )
