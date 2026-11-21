# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""CDC sources: exports equal the upstream tables at a consistent prefix.

One upstream session writes transactions of inserts, updates and deletes to
three tables, and a ledger row per table it touched. Writes are serial, so
the commit order is the order the model applies them in. Before the next
write, every session a failure left behind is ended, and then the ledger
shows whether a COMMIT whose response was lost took effect: the model of the
upstream is exact.

A check reads the source's progress, the ledger and every export in one
`SELECT`, so all at one timestamp. The visible ledger must be a prefix of
the commit order, each transaction whole, consistent with the progress
position. Every export must equal its table replayed to that prefix, which
also checks that transactions spanning tables arrive atomically.

A share of the writes and checks goes to a fourth table, `types`, with one
column per upstream type (`cdc_types`). Its rows say what they hold, so its
check judges every value the export shows without the model, and the ledger
does not cover it.

A further export of one of the three tables is created and dropped while the
source ingests. Its snapshot is taken at a later upstream position than the
one the source resumes the other tables from, and the source rewinds it to
that position, so at every timestamp it is readable it must equal its table
at the same ledger prefix as the others.

Each upstream is a `Dialect` (`pg.py`, `mysql.py`, `sqlserver.py`). It makes
its own assertion calls, because the Antithesis catalog needs each message
as a literal at its call site.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Protocol

import psycopg

from materialize.antithesis import sql
from materialize.antithesis_driver import cdc_types
from materialize.antithesis_driver.client import Op, Outcome, Status, Worker
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

CLUSTER = "sources"
TABLES = 3
CHECKPOINT_EVERY = 64
DROP_COMMIT_RESPONSE_SHARE = 0.02
MAX_ADDED_COLUMNS = 8
TYPES_SHARE = 0.25
"""How many of the writes and of the checks go to the `types` table."""
SETTINGS = {"transaction_isolation": "'strict serializable'", "cluster": "quickstart"}
INTERESTING_VALUES = ("", "NULL", "'", '"', "\\", "κλειδί", "x" * 3000)


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


def apply(states: list[dict[int, str | None]], plan: Plan) -> None:
    """Apply one statement's effect to table states."""
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
    begun: int
    """A position at or below the one its commit got."""
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
    positions: dict[int, int] = field(default_factory=dict)
    """A position at or above each commit's, where known. A source whose
    progress passed it has ingested the transaction."""
    captured_at: dict[int, float] = field(default_factory=dict)
    """When `Dialect.positions` reported a commit, for dialects whose
    real-time recency needs capture."""

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
    position: int | None
    """The source's progress: every commit below it is ingested."""
    position_rows: int
    ledger: list[tuple[int, int]]
    exports: list[list[tuple[int, str | None]]]
    added: tuple[int, list[tuple[int, str | None]]] | None = None
    """The upstream table of the export added to the running source and the
    export's rows, if the check read it."""


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
    added_rows: int | None = None
    """How many rows of the added export were compared, if it was read."""
    added_mismatch: dict[str, Any] | None = None
    added_ahead: int | None = None
    """The later prefix at which the added export equals its table, if it
    differs at the read's prefix and matches there."""


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
    if snap.position is not None:
        position = snap.position
        below = [
            t
            for t in committed
            if t.id in model.positions and model.positions[t.id] < position
        ]
        v.committed_below = len(below)
        v.committed_below_missing = [t.id for t in below if t.id not in visible]
        v.begun_above_visible = sorted(
            t.id for t in committed if t.begun > position and t.id in visible
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
            v.mismatches[j] = _difference(observed, expected[j])
        v.compared_rows += len(observed)
    if snap.added is not None:
        j, rows = snap.added
        observed = dict(rows)
        v.added_rows = len(rows)
        if len(observed) != len(rows) or observed != expected[j]:
            v.added_mismatch = {"table": j, **_difference(observed, expected[j])}
            v.added_mismatch["observed_rows"] = len(rows)
            later = [dict(table) for table in expected]
            for n, txn in enumerate(committed[k:], start=k + 1):
                for plan in txn.plans:
                    apply(later, plan)
                if len(observed) == len(rows) and later[j] == observed:
                    v.added_ahead = n
                    break
    return v


def _difference(
    observed: dict[int, str | None], expected: dict[int, str | None]
) -> dict[str, Any]:
    return {
        "missing": sorted(set(expected) - set(observed))[:10],
        "extra": sorted(set(observed) - set(expected))[:10],
        "changed": sorted(
            key
            for key in set(observed) & set(expected)
            if observed[key] != expected[key]
        )[:10],
        "expected_rows": len(expected),
        "observed_rows": len(observed),
    }


@dataclass
class Review:
    """One check's verdict and what the component knew around it, for the
    dialect's assertions."""

    verdict: Verdict
    details: dict[str, Any]
    regressions: list[tuple[int, int]]
    """Earlier checks whose position conflicts with this one in mz time."""
    rtr: bool
    rtr_missing: list[int]
    """Transactions a real-time recency read did not see although it began
    after they were acknowledged, or captured if the dialect needs that."""
    added_columns: bool


class Dialect(Protocol):
    """One upstream database. Its cursors take `%s` parameters."""

    schema: str
    """The Materialize schema of the source, its connection and exports."""
    ledger: str
    """The upstream ledger table."""

    @property
    def errors(self) -> tuple[type[BaseException], ...]:
        """What the client raises when a statement or the connection fails."""
        ...

    rtr_needs_capture: bool
    """Whether real-time recency covers only commits the upstream's change
    capture has processed, which `positions` reports, rather than every
    acknowledged commit."""

    def connect(self) -> Any:
        """An autocommit connection."""
        ...

    def setup_upstream(self) -> None: ...

    def setup_statements(self) -> list[str]: ...

    def session(self, cur: Any) -> Any:
        """An id of the cursor's server session that is never reused."""
        ...

    def end_session(self, cur: Any, session: Any) -> bool:
        """Terminate `session` if it exists. Returns whether it is gone, and
        with it any transaction it had open."""
        ...

    def begin(self, cur: Any) -> int:
        """Begin a transaction. Returns a position at or below its commit's."""
        ...

    def statement(self, plan: Plan) -> tuple[str, tuple[Any, ...]]: ...

    def commit(self, cur: Any) -> None: ...

    def position_after(self, cur: Any) -> int | None:
        """Right after a commit, a position at or above its own, if known."""
        ...

    def positions(self, cur: Any, txns: list[int]) -> dict[int, int]:
        """Positions of committed transactions `position_after` did not know."""
        ...

    def add_column(self, table: int, n: int) -> str: ...

    def reference(self, table: int) -> str:
        """The upstream name of table `table`, as `REFERENCE` takes it."""
        ...

    types: tuple[cdc_types.Column, ...]
    """The columns of the upstream `types` table, exported as `types`."""
    types_table: str

    def progress(self) -> str:
        """A `SELECT` of `('p', a, b)` text rows from the source's progress."""
        ...

    def parse_progress(self, rows: list[tuple[str | None, str | None]]) -> int | None:
        """The position below which every commit is ingested."""
        ...

    def report(self, r: Review) -> None: ...

    def report_resolved(self, committed: bool, txn: int) -> None:
        """A COMMIT whose response was lost turned out `committed` or not."""
        ...

    def report_caught_up(self, caught_up: bool, details: dict[str, Any]) -> None: ...

    def report_added(self, v: Verdict, details: dict[str, Any]) -> None:
        """A check read the export added to the running source."""
        ...

    def report_types(self, v: cdc_types.Verdict, details: dict[str, Any]) -> None: ...

    def report_types_caught_up(self, equal: bool, details: dict[str, Any]) -> None:
        """After faults stop, whether the `types` export equals its table."""
        ...


class ResponseDropped(Exception):
    pass


@dataclass
class Added:
    """The export created while the source ingests."""

    n: int
    table: int
    state: str = "creating"
    """`creating`, `live`, `dropping` or `unknown`."""


class CdcSource:
    def __init__(self, ctx: Context, dialect: Dialect) -> None:
        self.ctx = ctx
        self.dialect = dialect
        self.worker = Worker(f"{dialect.schema}-upstream")
        self._lock = threading.Lock()
        self.model = Model()
        self._next_txn = 1
        self._conn: Any = None
        self._session: Any = None
        self.stale: list[Any] = []
        """Sessions a failure left behind, maybe with a transaction open.
        Only the worker thread uses this and the connection."""
        self.unresolved: Txn | None = None
        """A transaction whose COMMIT was sent but not acknowledged."""
        self.committing: Txn | None = None
        """The transaction whose COMMIT is in flight."""
        self.observed: list[tuple[int, int]] = []
        """(mz timestamp, position) of checks, for monotonicity."""
        self.added_columns = [0] * TABLES
        self.added: Added | None = None
        self._next_added = 1
        self.keyspace = 8
        self.ops_per_txn = 2
        self.rtr_share = 0.0

    def setup_upstream(self) -> None:
        self.dialect.setup_upstream()

    def setup_statements(self) -> list[str]:
        return self.dialect.setup_statements()

    def configure(self, d: Draw) -> None:
        self.keyspace = d.choice((1, 8, 64, 512))
        self.ops_per_txn = d.choice((1, 2, 5, 20))
        self.rtr_share = d.choice((0.0, 0.3))

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

    def _cursor(self) -> Any:
        if self._conn is None:
            self._conn = self.dialect.connect()
            cur = self._conn.cursor()
            self._session = self.dialect.session(cur)
            return cur
        return self._conn.cursor()

    def _discard(self, error: BaseException) -> None:
        log(f"{self.dialect.schema}: upstream: {type(error).__name__}: {error}")
        if self._session is not None:
            self.stale.append(self._session)
        conn, self._conn, self._session = self._conn, None, None
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

    def _settle(self, cur: Any) -> bool:
        """End what earlier failures left open and resolve a lost COMMIT.
        Returns whether the next write may start."""
        while self.stale:
            if not self.dialect.end_session(cur, self.stale[0]):
                return False
            self.stale.pop(0)
        txn = self.unresolved
        if txn is not None:
            cur.execute(
                f"SELECT count(*) FROM {self.dialect.ledger} WHERE txn = %s", (txn.id,)
            )
            row = cur.fetchone()
            committed = row is not None and int(row[0]) > 0
            with self._lock:
                if committed:
                    self.model.commit(txn)
                else:
                    self.model.aborted[txn.id] = txn.tables()
                self.unresolved = None
            self.dialect.report_resolved(committed, txn.id)
        with self._lock:
            missing = [
                t.id
                for t in self.model.committed[-CHECKPOINT_EVERY:]
                if t.id not in self.model.positions
            ]
        if missing:
            found = self.dialect.positions(cur, missing)
            now = time.monotonic()
            with self._lock:
                self.model.positions.update(found)
                self.model.captured_at.update((txn, now) for txn in found)
        return True

    def _write_types(self, d: Draw) -> Status:
        """Change rows of the `types` table in a transaction of its own."""
        table, columns = self.dialect.types_table, self.dialect.types

        def seed() -> int:
            if d.boolean(0.5):
                return d.integer(0, cdc_types.SWEEP - 1)
            return d.integer(cdc_types.SWEEP, cdc_types.UNTOUCHED - 1)

        lo = cdc_types.SWEEP + d.integer(0, cdc_types.KEYSPACE - 1)
        hi = min(lo + d.choice((0, 0, 2)), cdc_types.SWEEP + cdc_types.KEYSPACE - 1)
        kind = d.choice(("insert", "insert", "insert", "update", "untouched", "delete"))
        if kind == "insert":
            statements = [cdc_types.delete(table, lo, hi)]
            statements += [
                cdc_types.insert(table, columns, k, seed(), seed())
                for k in range(lo, hi + 1)
            ]
        elif kind == "update":
            statements = [cdc_types.update(table, columns, lo, hi, seed(), seed())]
        elif kind == "untouched":
            untouched = d.integer(cdc_types.UNTOUCHED, 2 * cdc_types.UNTOUCHED - 1)
            statements = [cdc_types.update(table, columns, lo, hi, untouched, None)]
        else:
            statements = [cdc_types.delete(table, lo, hi)]

        def action() -> None:
            try:
                cur = self._cursor()
                if not self._settle(cur):
                    return
                self.dialect.begin(cur)
                for statement, params in statements:
                    cur.execute(statement, params)
                self.dialect.commit(cur)
            except self.dialect.errors as e:
                self._discard(e)

        return self.worker.run(action, RULE_WAIT_SECONDS)

    def write(self, d: Draw) -> Status:
        if d.boolean(TYPES_SHARE):
            return self._write_types(d)
        plans = self._plans(d)
        drop_response = d.boolean(DROP_COMMIT_RESPONSE_SHARE)
        with self._lock:
            txn_id = self._next_txn
            self._next_txn += 1

        def action() -> None:
            try:
                cur = self._cursor()
                if not self._settle(cur):
                    return
            except self.dialect.errors as e:
                self._discard(e)
                return
            phase = "begin"
            txn = Txn(txn_id, plans, 0, None)
            try:
                txn = Txn(txn_id, plans, self.dialect.begin(cur), None)
                for plan in plans:
                    cur.execute(*self.dialect.statement(plan))
                for tbl in sorted(txn.tables()):
                    cur.execute(
                        f"INSERT INTO {self.dialect.ledger} VALUES (%s, %s)",
                        (txn_id, tbl),
                    )
                with self._lock:
                    self.committing = txn
                phase = "commit"
                self.dialect.commit(cur)
                if drop_response:
                    # Proceed as if the connection had died before the
                    # acknowledgement arrived. A fault does that only when it
                    # lands in the few milliseconds a COMMIT is in flight.
                    self._discard(
                        ResponseDropped("the driver dropped a COMMIT response")
                    )
                    with self._lock:
                        self.committing = None
                        self.unresolved = txn
                    return
                acked = Txn(txn_id, plans, txn.begun, time.monotonic())
                with self._lock:
                    self.committing = None
                    self.model.commit(acked)
                phase = "committed"
                after = self.dialect.position_after(cur)
                if after is not None:
                    with self._lock:
                        self.model.positions[txn_id] = after
            except self.dialect.errors as e:
                self._discard(e)
                with self._lock:
                    self.committing = None
                    if phase == "begin":
                        # The session ends before the next write, and with it
                        # the transaction, which never sent COMMIT.
                        self.model.aborted[txn_id] = txn.tables()
                    elif phase == "commit":
                        self.unresolved = txn

        return self.worker.run(action, RULE_WAIT_SECONDS)

    def add_column(self, d: Draw) -> Status:
        """An upstream `ADD COLUMN`, which the exports ignore."""
        j = d.integer(0, TABLES - 1)
        if self.added_columns[j] >= MAX_ADDED_COLUMNS:
            return Status.NOT_SENT

        def action() -> None:
            try:
                cur = self._cursor()
                if not self._settle(cur):
                    return
                n = self.added_columns[j]
                # A failed attempt may have added the column: never retry a name.
                self.added_columns[j] = n + 1
                cur.execute(self.dialect.add_column(j, n))
            except self.dialect.errors as e:
                self._discard(e)

        return self.worker.run(action, RULE_WAIT_SECONDS)

    def export(self, d: Draw) -> Status:
        """Create a further export of one table while the source ingests, or
        drop the one there is."""
        s = self.dialect.schema
        with self._lock:
            added = self.added
            if added is None:
                added = Added(self._next_added, d.integer(0, TABLES - 1))
                self._next_added += 1
                self.added = added
                statement = (
                    f"CREATE TABLE {s}.x{added.n} FROM SOURCE {s}.src"
                    f" (REFERENCE {self.dialect.reference(added.table)})"
                )
            elif added.state == "live" and d.boolean(0.3):
                added.state = "dropping"
                statement = f"DROP TABLE {s}.x{added.n}"
            elif added.state == "unknown":
                added.state = "dropping"
                statement = f"DROP TABLE IF EXISTS {s}.x{added.n}"
            else:
                return Status.NOT_SENT
        create = statement.startswith("CREATE")

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                if outcome.ok:
                    if create:
                        added.state = "live"
                    else:
                        self.added = None
                elif outcome.rejected and create:
                    self.added = None
                else:
                    # Also a rejected drop, such as one a busy session never
                    # sent: the next attempt drops it if it exists.
                    added.state = "unknown"

        return self.ctx.sessions.call(
            d.integer(0, len(self.ctx.sessions.sessions) - 1),
            Op(
                f"{s} export",
                execute,
                complete,
                SETTINGS,
                classify=sql.classify_source_ddl,
            ),
            RULE_WAIT_SECONDS,
        )

    def _read_op(self, rtr: bool, done: list[Verdict], added: bool = True) -> Op:
        s = self.dialect.schema
        branches = [self.dialect.progress()]
        branches.append(f"SELECT 'l', txn::text, tbl::text FROM {s}.ledger")
        branches += [f"SELECT 't{j}', k::text, v FROM {s}.t{j}" for j in range(TABLES)]
        with self._lock:
            # A read of an export that is still taking its snapshot waits for
            # it, so only some checks include it.
            x = self.added if added else None
            table = x.table if x is not None and x.state == "live" else None
        if x is not None and table is not None:
            branches.append(f"SELECT 'x', k::text, v FROM {s}.x{x.n}")
        query = f"SELECT mz_now()::text, * FROM ({' UNION ALL '.join(branches)}) AS q"

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(query.encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            done.append(self._judge(outcome.value, outcome.start, rtr, table))

        settings = dict(SETTINGS)
        if rtr:
            settings["real_time_recency"] = "true"
        return Op(
            f"{s} check",
            execute,
            complete,
            settings,
            classify=sql.classify_rtr_read if rtr else sql.classify,
        )

    def _judge(
        self,
        rows: list[tuple[Any, ...]],
        start: float,
        rtr: bool,
        added: int | None = None,
    ) -> Verdict:
        progress = [(r[2], r[3]) for r in rows if r[1] == "p"]
        snap = Snapshot(
            ts=int(rows[0][0]) if rows else 0,
            position=self.dialect.parse_progress(progress),
            position_rows=len(progress),
            ledger=[(int(r[2]), int(r[3])) for r in rows if r[1] == "l"],
            exports=[
                [(int(r[2]), r[3]) for r in rows if r[1] == f"t{j}"]
                for j in range(TABLES)
            ],
            added=(
                None
                if added is None
                else (added, [(int(r[2]), r[3]) for r in rows if r[1] == "x"])
            ),
        )
        with self._lock:
            pending = self.committing or self.unresolved
            v = judge(self.model, snap, pending)
            if self.dialect.rtr_needs_capture:
                captured = self.model.captured_at
                acked_before = [t for t, at in captured.items() if at < start]
            else:
                acked_before = [
                    t.id
                    for t in self.model.committed
                    if t.acked_at is not None and t.acked_at < start
                ]
            seen = {t.id for t in self.model.committed[: v.prefix or 0]}
            regressions = [
                (ts, position)
                for ts, position in self.observed
                if snap.position is not None
                and (
                    (ts <= snap.ts and position > snap.position)
                    or (ts >= snap.ts and position < snap.position)
                )
            ]
            if snap.position is not None:
                self.observed.append((snap.ts, snap.position))
                self.observed = self.observed[-1000:]
        self.dialect.report(
            Review(
                verdict=v,
                details={
                    "ts": snap.ts,
                    "position": snap.position,
                    "ledger_rows": len(snap.ledger),
                    "rtr": rtr,
                },
                regressions=regressions,
                rtr=rtr,
                rtr_missing=sorted(set(acked_before) - seen),
                added_columns=sum(self.added_columns) > 0,
            )
        )
        if v.added_rows is not None:
            self.dialect.report_added(
                v, {"ts": snap.ts, "position": snap.position, "prefix": v.prefix}
            )
        return v

    def _types_op(self, done: list[cdc_types.Verdict]) -> Op:
        s = self.dialect.schema
        query = cdc_types.select(f"{s}.types", self.dialect.types)

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(query.encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            v = cdc_types.judge(self.dialect.types, outcome.value)
            self.dialect.report_types(v, {"rows": v.rows})
            done.append(v)

        return Op(f"{s} types check", execute, complete, SETTINGS)

    def check(self, d: Draw) -> Status:
        session = d.integer(0, len(self.ctx.sessions.sessions) - 1)
        if d.boolean(TYPES_SHARE):
            return self.ctx.sessions.call(
                session, self._types_op([]), RULE_WAIT_SECONDS
            )
        op = self._read_op(d.boolean(self.rtr_share), [], added=d.boolean())
        return self.ctx.sessions.call(session, op, RULE_WAIT_SECONDS)

    def eventually(self, deadline: float) -> None:
        """After faults stop, the source catches up with every committed write.

        Like `health.poll`, each loop tries once even past `deadline`, which
        earlier checks share and may have used up.
        """
        settled: list[bool] = []

        def settle() -> None:
            try:
                if self._settle(self._cursor()):
                    settled.append(True)
            except self.dialect.errors as e:
                self._discard(e)

        while True:
            self.worker.run(settle, 60.0)
            if settled or time.monotonic() >= deadline:
                break
            time.sleep(1)
        with self._lock:
            committed = len(self.model.committed)
        caught_up = False
        verdict: Verdict | None = None
        while True:
            done: list[Verdict] = []
            for i in range(len(self.ctx.sessions.sessions)):
                op = self._read_op(False, done)
                if self.ctx.sessions.call(i, op, 60.0) != Status.NOT_SENT:
                    break
            if done:
                verdict = done[0]
                if verdict.prefix is not None and verdict.prefix >= committed:
                    caught_up = True
                    break
                if verdict.prefix is None:
                    break
            if time.monotonic() >= deadline:
                break
            time.sleep(2)
        prefix = verdict.prefix if verdict else None
        log(
            f"{self.dialect.schema}: caught up {caught_up}, prefix {prefix} of {committed}"
        )
        if verdict is not None:
            self.dialect.report_caught_up(
                caught_up,
                {
                    "committed": committed,
                    "prefix": prefix,
                    "settled": bool(settled),
                },
            )
        # Otherwise no read completed. The SQL and source progress checks
        # report that, and there is nothing to judge here.
        self._types_eventually(deadline)

    def _types_eventually(self, deadline: float) -> None:
        """After faults stop, the `types` export equals the upstream table,
        which no write changes any more."""
        upstream: list[dict[int, tuple[int, int]]] = []

        def read() -> None:
            try:
                cur = self._cursor()
                # Ends the sessions that could still commit a change.
                if not self._settle(cur):
                    return
                cur.execute(f"SELECT k, seed, tseed FROM {self.dialect.types_table}")
                upstream.append(
                    {int(k): (int(seed), int(tseed)) for k, seed, tseed in cur}
                )
            except self.dialect.errors as e:
                self._discard(e)

        while True:
            self.worker.run(read, 60.0)
            if upstream or time.monotonic() >= deadline:
                break
            time.sleep(1)
        if not upstream:
            return
        expected = upstream[0]
        verdict: cdc_types.Verdict | None = None
        while True:
            done: list[cdc_types.Verdict] = []
            for i in range(len(self.ctx.sessions.sessions)):
                if (
                    self.ctx.sessions.call(i, self._types_op(done), 60.0)
                    != Status.NOT_SENT
                ):
                    break
            if done:
                verdict = done[0]
            if (
                verdict is not None and verdict.keys == expected
            ) or time.monotonic() >= deadline:
                break
            time.sleep(2)
        if verdict is None:
            return
        observed = verdict.keys
        self.dialect.report_types_caught_up(
            observed == expected,
            {
                "upstream_rows": len(expected),
                "export_rows": verdict.rows,
                "missing": sorted(set(expected) - set(observed))[:10],
                "extra": sorted(set(observed) - set(expected))[:10],
                "changed": sorted(
                    k
                    for k in set(observed) & set(expected)
                    if observed[k] != expected[k]
                )[:10],
            },
        )
