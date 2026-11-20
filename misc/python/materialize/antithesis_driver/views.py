# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Incremental view maintenance: maintained views equal recomputation.

Writes insert, update and delete rows of the base tables of `ivm_model`.
Every view kind there is maintained twice, as an indexed view and as a
materialized view. A check reads the base tables and every maintained view in
one `SELECT` over `ivm_model.subscribe_query()`, so all rows share one
timestamp, recomputes each view from the base rows with `ivm_model.reference`,
and compares. The reference evaluates the base rows the read returned, so a
check catches wrong maintenance, not a lost write: the client history owns
durability.

Base rows carry the id of the write that last set them, which must be a
write the driver attempted and the system did not definitely reject.

A background subscriber also holds a `SUBSCRIBE ... WITH (PROGRESS)` over the
same union, and compares every view with the reference at each closed
timestamp, with `ivm_model.Stream` and `ivm_model.ViewChecker`. It checks the
diff stream itself, and the views at every timestamp rather than only at
those a read happens to pick.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
    unreachable,
)

from materialize.antithesis import ivm_model as model
from materialize.antithesis import sql
from materialize.antithesis_driver.client import (
    Op,
    Outcome,
    Status,
    Target,
    classify,
    connect,
    report_driver_error,
)
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

CLUSTER = "driver"
KEYSPACE = 48
GROUPS = 6
DIM_GROUPS = GROUPS + 2
NODES = 6
MAX_ROWS = {"t": 120, "d": 12, "ev": 30, "e": 20}
EXPIRY_OFFSET_MS = (-5_000, 45_000)
"""Offsets of `ev.expires` from the last read timestamp. With
`ivm_model.TEMPORAL_LEAD_MS`, rows appear and disappear on their own."""
WRITE_KINDS = (
    "t_insert",
    "t_insert",
    "t_insert_many",
    "t_update",
    "t_move",
    "t_delete",
    "t_delete_range",
    "t_delete_group",
    "d_insert",
    "d_insert_many",
    "d_update",
    "d_delete",
    "ev_insert",
    "ev_insert",
    "ev_delete",
    "e_insert",
    "e_insert",
    "e_delete",
)
DELETE_WHEN_FULL = {
    "t": "t_delete_range",
    "d": "d_delete",
    "ev": "ev_delete",
    "e": "e_delete",
}


def setup_statements() -> list[str]:
    s = model.SCHEMA
    statements = [f"CREATE SCHEMA IF NOT EXISTS {s}"]
    statements += [
        f"CREATE TABLE IF NOT EXISTS {s}.{t.name} ({t.ddl})" for t in model.TABLES
    ]
    for v in model.VIEWS:
        statements += [
            f"CREATE VIEW IF NOT EXISTS {s}.{v.kind}_v AS {v.sql}",
            f"CREATE INDEX IF NOT EXISTS {v.kind}_v_idx IN CLUSTER {CLUSTER}"
            f" ON {s}.{v.kind}_v ({v.columns[0]})",
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {s}.{v.kind}_mv"
            f" IN CLUSTER {CLUSTER} AS {v.sql}",
        ]
    return statements


def write_statement(kind: str, op: int, now_ms: int, d: Draw) -> str:
    s = model.SCHEMA

    def value() -> int:
        return d.integer(-1000, 1000)

    if kind == "t_insert":
        return f"INSERT INTO {s}.t VALUES ({d.integer(0, KEYSPACE - 1)}, {d.integer(0, GROUPS - 1)}, {value()}, {op})"
    if kind == "t_insert_many":
        rows = ", ".join(
            f"({d.integer(0, KEYSPACE - 1)}, {d.integer(0, GROUPS - 1)}, {value()}, {op})"
            for _ in range(d.integer(2, 5))
        )
        return f"INSERT INTO {s}.t VALUES {rows}"
    if kind == "t_update":
        return f"UPDATE {s}.t SET v = {value()}, w = {op} WHERE k = {d.integer(0, KEYSPACE - 1)}"
    if kind == "t_move":
        return f"UPDATE {s}.t SET g = {d.integer(0, GROUPS - 1)}, w = {op} WHERE k = {d.integer(0, KEYSPACE - 1)}"
    if kind == "t_delete":
        return f"DELETE FROM {s}.t WHERE k = {d.integer(0, KEYSPACE - 1)}"
    if kind == "t_delete_range":
        lo = d.integer(0, KEYSPACE - 1)
        return f"DELETE FROM {s}.t WHERE k BETWEEN {lo} AND {lo + d.choice((2, 8, 16))}"
    if kind == "t_delete_group":
        return f"DELETE FROM {s}.t WHERE g = {d.integer(0, GROUPS - 1)}"
    if kind == "d_insert":
        return f"INSERT INTO {s}.d VALUES ({d.integer(0, DIM_GROUPS - 1)}, {op})"
    if kind == "d_insert_many":
        group = d.integer(0, DIM_GROUPS - 1)
        rows = ", ".join(f"({group}, {op})" for _ in range(d.integer(2, 4)))
        return f"INSERT INTO {s}.d VALUES {rows}"
    if kind == "d_update":
        return f"UPDATE {s}.d SET label = {op} WHERE g = {d.integer(0, DIM_GROUPS - 1)}"
    if kind == "d_delete":
        return f"DELETE FROM {s}.d WHERE g = {d.integer(0, DIM_GROUPS - 1)}"
    if kind == "ev_insert":
        expires = now_ms + d.integer(*EXPIRY_OFFSET_MS)
        return f"INSERT INTO {s}.ev VALUES ({d.integer(0, KEYSPACE - 1)}, {expires})"
    if kind == "ev_delete":
        return f"DELETE FROM {s}.ev WHERE k = {d.integer(0, KEYSPACE - 1)}"
    if kind == "e_insert":
        return f"INSERT INTO {s}.e VALUES ({d.integer(0, NODES - 1)}, {d.integer(0, NODES - 1)})"
    assert kind == "e_delete"
    return f"DELETE FROM {s}.e WHERE src = {d.integer(0, NODES - 1)}"


class Views:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self._next_op = 1
        self.writes: dict[int, str] = {}
        """Each write's id and its outcome: `pending`, `ok`, `rejected`, `unknown`."""
        self.counts = Counter[str]()
        """Base table row counts at the last check."""
        # Materialize timestamps are wall-clock milliseconds, which serve until
        # the first check reads one.
        self.now_ms = int(time.time() * 1000)
        """The timestamp of the last check, in milliseconds."""
        self.serializable_share = 0.0
        self.query = model.subscribe_query()
        self.subscriber: Subscriber | None = None

    def configure(self, d: Draw) -> None:
        self.serializable_share = d.choice((0.0, 0.3))
        if self.subscriber is None:
            self.subscriber = Subscriber(self.ctx, d.choice((20.0, 60.0, 180.0)))
            self.subscriber.start()

    def write(self, d: Draw) -> Status:
        full = [name for name, n in self.counts.items() if n >= MAX_ROWS[name]]
        kind = DELETE_WHEN_FULL[d.choice(full)] if full else d.choice(WRITE_KINDS)
        with self._lock:
            op = self._next_op
            self._next_op += 1
            self.writes[op] = "pending"
        statement = write_statement(kind, op, self.now_ms, d)

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                self.writes[op] = (
                    "ok"
                    if outcome.ok
                    else "rejected" if outcome.rejected else "unknown"
                )

        return self.ctx.sessions.call(
            d.integer(0, len(self.ctx.sessions.sessions) - 1),
            Op(
                f"ivm {kind}",
                execute,
                complete,
                {"cluster": CLUSTER, "transaction_isolation": "'strict serializable'"},
            ),
            RULE_WAIT_SECONDS,
        )

    def _read_op(self, isolation: str, compared: list[bool]) -> Op:
        query = (
            f"SELECT mz_now()::text, tag, c1, c2, c3, c4, c5 FROM ({self.query}) AS q"
        )

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(query.encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            self.compare(outcome.value, isolation)
            compared.append(True)

        return Op(
            "ivm check",
            execute,
            complete,
            {"cluster": CLUSTER, "transaction_isolation": f"'{isolation}'"},
        )

    def check(self, d: Draw) -> Status:
        isolation = (
            "serializable"
            if d.boolean(self.serializable_share)
            else "strict serializable"
        )
        return self.ctx.sessions.call(
            d.integer(0, len(self.ctx.sessions.sessions) - 1),
            self._read_op(isolation, []),
            RULE_WAIT_SECONDS,
        )

    def compare(self, rows: list[tuple[Any, ...]], isolation: str) -> None:
        timestamps = {r[0] for r in rows}
        # The global_reduce views always hold one row, so the result is never
        # empty and carries the read's timestamp.
        ts = int(min(timestamps)) if timestamps else self.now_ms
        always(
            len(timestamps) <= 1,
            "ivm: a read observes one timestamp across all inputs and maintained views",
            {"timestamps": sorted(timestamps)[:5]},
        )
        state: dict[str, Counter[tuple[Any, ...]]] = {}
        for _, tag, *payload in rows:
            state.setdefault(tag, Counter())[tuple(payload)] += 1
        base = model.base_from_state(state)
        with self._lock:
            self.now_ms = max(self.now_ms, ts)
            self.counts = Counter(
                {name: sum(bag.values()) for name, bag in base.items()}
            )
            writes = dict(self.writes)
        details: dict[str, Any] = {"ts": ts, "isolation": isolation}
        results = {}
        for view in model.VIEWS:
            expected = model.reference(view.kind, base, ts)
            for variant in model.VARIANTS:
                observed = state.get(model.tag(view.kind, variant), Counter())
                results[(view.kind, variant)] = (
                    expected == observed,
                    expected,
                    observed,
                )

        def ok(kind: str) -> bool:
            return all(results[(kind, v)][0] for v in model.VARIANTS)

        def diff(kind: str) -> dict[str, Any]:
            out: dict[str, Any] = dict(details)
            for variant in model.VARIANTS:
                same, expected, observed = results[(kind, variant)]
                if not same:
                    out[variant] = model.bag_diff(expected, observed)
            return out

        # One literal call per view kind: Antithesis catalogs assertions by
        # the message at each call site.
        always(
            ok("filter_map"),
            "ivm: the filter_map view equals the reference at a read's timestamp",
            diff("filter_map"),
        )
        always(
            ok("inner_join"),
            "ivm: the inner_join view equals the reference at a read's timestamp",
            diff("inner_join"),
        )
        always(
            ok("left_join"),
            "ivm: the left_join view equals the reference at a read's timestamp",
            diff("left_join"),
        )
        always(
            ok("full_join"),
            "ivm: the full_join view equals the reference at a read's timestamp",
            diff("full_join"),
        )
        always(
            ok("reduce"),
            "ivm: the reduce view equals the reference at a read's timestamp",
            diff("reduce"),
        )
        always(
            ok("global_reduce"),
            "ivm: the global_reduce view equals the reference at a read's timestamp",
            diff("global_reduce"),
        )
        always(
            ok("distinct"),
            "ivm: the distinct view equals the reference at a read's timestamp",
            diff("distinct"),
        )
        always(
            ok("intersect_all"),
            "ivm: the intersect_all view equals the reference at a read's timestamp",
            diff("intersect_all"),
        )
        always(
            ok("except_all"),
            "ivm: the except_all view equals the reference at a read's timestamp",
            diff("except_all"),
        )
        always(
            ok("topk"),
            "ivm: the topk view equals the reference at a read's timestamp",
            diff("topk"),
        )
        always(
            ok("temporal"),
            "ivm: the temporal view equals the reference at a read's timestamp",
            diff("temporal"),
        )
        always(
            ok("recursive"),
            "ivm: the recursive view equals the reference at a read's timestamp",
            diff("recursive"),
        )
        assert {v.kind for v in model.VIEWS} == {
            "filter_map",
            "inner_join",
            "left_join",
            "full_join",
            "reduce",
            "global_reduce",
            "distinct",
            "intersect_all",
            "except_all",
            "topk",
            "temporal",
            "recursive",
        }

        ids = [
            model.written_ids(tag, row)
            for tag in ("t", "d")
            for row in state.get(tag, Counter())
        ]
        bad = sorted(
            {i for i in ids if i is not None and writes.get(i) in (None, "rejected")}
        )
        always(
            not bad,
            "ivm: every base row carries the id of a write that was attempted and not definitely rejected",
            {**details, "ids": bad[:10]},
        )
        sometimes(
            sum(base["t"].values()) > 0 and sum(base["d"].values()) > 0,
            "ivm: a check compared views over nonempty fact and dimension tables",
            {**details, "t": sum(base["t"].values()), "d": sum(base["d"].values())},
        )
        sometimes(
            sum(model.reference("temporal", base, ts).values()) > 0,
            "ivm: a check compared a temporal view with rows inside their validity window",
            details,
        )
        sometimes(
            any(i is not None and writes.get(i) == "unknown" for i in ids),
            "ivm: a check observed a write whose outcome was indeterminate",
            details,
        )

    def eventually(self, deadline: float) -> None:
        started = time.monotonic()
        compared: list[bool] = []
        while not compared and time.monotonic() < deadline:
            for i in range(len(self.ctx.sessions.sessions)):
                if (
                    self.ctx.sessions.call(
                        i, self._read_op("strict serializable", compared), 60.0
                    )
                    != Status.NOT_SENT
                ):
                    break
            if not compared:
                time.sleep(1)
        always(
            bool(compared),
            "ivm: every maintained view can be compared with a current reference after faults stop",
            {},
        )
        if self.subscriber is not None:
            self.subscriber.eventually(started, deadline)


class Subscriber:
    """A `SUBSCRIBE` over every input and maintained view, on its own thread.

    Each window subscribes afresh, so it starts from a snapshot, and lasts
    `window_seconds` or until its connection breaks.
    """

    def __init__(self, ctx: Context, window_seconds: float) -> None:
        self.ctx = ctx
        self.window_seconds = window_seconds
        self.last_progress: float | None = None
        """When the last progress row arrived."""

    def start(self) -> None:
        threading.Thread(target=self._loop, name="ivm-subscribe", daemon=True).start()

    def _loop(self) -> None:
        while True:
            try:
                self._window()
            except (psycopg.Error, OSError) as e:
                c = classify(e)
                # A subscription that does not keep up is ended by the server
                # (`SubscribeFellBehind`). The next window starts afresh.
                if c.outcome == sql.Outcome.VIOLATION and c.sqlstate != "53200":
                    unreachable(
                        "SQL statements fail only with errors a correct system returns",
                        {
                            "op": "ivm subscribe",
                            "sqlstate": c.sqlstate,
                            "error": c.template,
                        },
                    )
            except Exception as e:
                report_driver_error("ivm subscribe", e)
            time.sleep(1)

    def _window(self) -> None:
        streams: list[model.Stream] = []
        checker = model.ViewChecker()
        with connect(Target(self.ctx.config.host), "driver-subscribe") as conn:
            conn.execute(f"SET cluster = {CLUSTER}".encode())
            conn.execute(b"SET transaction_isolation = 'serializable'")
            # The cursor stays open across FETCHes for the whole window.
            conn.execute(b"SET idle_in_transaction_session_timeout = 0")
            conn.execute(b"BEGIN")
            conn.execute(
                f"DECLARE c CURSOR FOR SUBSCRIBE ({model.subscribe_query()})"
                " WITH (PROGRESS)".encode()
            )
            start = time.monotonic()
            while time.monotonic() - start < self.window_seconds:
                rows = conn.execute(b"FETCH ALL c WITH (timeout = '1s')").fetchall()
                if self._feed(streams, checker, rows):
                    self._assert(streams[0], checker)
            conn.execute(b"CLOSE c")
            conn.execute(b"COMMIT")
        sometimes(
            checker.steps >= 100,
            "ivm: a SUBSCRIBE window compared views at 100 or more closed timestamps",
            {"steps": checker.steps, "window": self.window_seconds},
        )

    def _feed(
        self,
        streams: list[model.Stream],
        checker: model.ViewChecker,
        rows: list[tuple[Any, ...]],
    ) -> bool:
        """Accumulate the rows, checking every view at each closed timestamp.
        Returns whether any timestamp closed.

        The subscription has no `AS OF`, so its first row, a progress row at
        the timestamp the server chose, gives the stream its as_of. Starting
        the stream earlier would compare the views with empty inputs before
        the snapshot.
        """
        advanced = False
        for r in rows:
            ts = int(r[0])
            if not streams:
                streams.append(model.Stream(ts))
            stream = streams[0]
            if r[1]:
                self.last_progress = time.monotonic()
                for step in stream.advance(ts):
                    checker.check(stream.state, step)
                    advanced = True
                continue
            payload = tuple(None if x is None else int(x) for x in r[4:9])
            stream.update(ts, str(r[3]), payload, int(r[2]))
        return advanced

    def _assert(self, stream: model.Stream, checker: model.ViewChecker) -> None:
        details = {"frontier": stream.frontier, "steps": checker.steps}
        always(
            not stream.late,
            "ivm: SUBSCRIBE never emits an update below its as_of or a closed progress timestamp",
            {**details, "late": stream.late[:10]},
        )
        always(
            not stream.regressions,
            "ivm: SUBSCRIBE progress timestamps never go backwards",
            {**details, "regressions": stream.regressions[:10]},
        )
        negative = {
            t: s.negative for t, s in checker.stats.items() if s.negative is not None
        }
        always(
            not negative,
            "ivm: consolidated checked state never has a negative multiplicity",
            {**details, "negative": negative},
        )

        def ok(kind: str) -> bool:
            return all(
                checker.stats[model.tag(kind, v)].mismatch is None
                for v in model.VARIANTS
            )

        def diff(kind: str) -> dict[str, Any]:
            return {
                **details,
                **{
                    v: checker.stats[model.tag(kind, v)].mismatch
                    for v in model.VARIANTS
                },
            }

        # One literal call per view kind, as in `Views.compare`.
        always(
            ok("filter_map"),
            "ivm: the filter_map view equals the reference at every closed timestamp",
            diff("filter_map"),
        )
        always(
            ok("inner_join"),
            "ivm: the inner_join view equals the reference at every closed timestamp",
            diff("inner_join"),
        )
        always(
            ok("left_join"),
            "ivm: the left_join view equals the reference at every closed timestamp",
            diff("left_join"),
        )
        always(
            ok("full_join"),
            "ivm: the full_join view equals the reference at every closed timestamp",
            diff("full_join"),
        )
        always(
            ok("reduce"),
            "ivm: the reduce view equals the reference at every closed timestamp",
            diff("reduce"),
        )
        always(
            ok("global_reduce"),
            "ivm: the global_reduce view equals the reference at every closed timestamp",
            diff("global_reduce"),
        )
        always(
            ok("distinct"),
            "ivm: the distinct view equals the reference at every closed timestamp",
            diff("distinct"),
        )
        always(
            ok("intersect_all"),
            "ivm: the intersect_all view equals the reference at every closed timestamp",
            diff("intersect_all"),
        )
        always(
            ok("except_all"),
            "ivm: the except_all view equals the reference at every closed timestamp",
            diff("except_all"),
        )
        always(
            ok("topk"),
            "ivm: the topk view equals the reference at every closed timestamp",
            diff("topk"),
        )
        always(
            ok("temporal"),
            "ivm: the temporal view equals the reference at every closed timestamp",
            diff("temporal"),
        )
        always(
            ok("recursive"),
            "ivm: the recursive view equals the reference at every closed timestamp",
            diff("recursive"),
        )

    def eventually(self, since: float, deadline: float) -> None:
        """A subscription makes progress after faults stopped at `since`."""
        while time.monotonic() < deadline:
            last = self.last_progress
            if last is not None and last > since:
                break
            time.sleep(1)
        last = self.last_progress
        log(
            f"ivm subscribe: last progress {None if last is None else round(time.monotonic() - last)}s ago"
        )
        always(
            last is not None and last > since,
            "ivm: a SUBSCRIBE makes progress after faults stop",
            {
                "since_last_progress_s": (
                    None if last is None else time.monotonic() - last
                )
            },
        )
