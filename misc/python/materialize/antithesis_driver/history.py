# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Client histories: appends and reads of several keys, checked for anomalies.

Each key is a table of unique values, the op ids of the writes that appended
them. Writes append to one key: a blind `INSERT`, in autocommit or an
explicit transaction, or a read-modify-write `INSERT ... SELECT count(*)`
that also stores how many values another key held. Reads observe one or more
keys at one timestamp, through the table, an index, a materialized view, or a
join materialized view. `serializability.check` infers the version order of
each key from the observed sets and searches the dependency graph for
cycles, including real-time edges between strict serializable operations.

Keys are grouped into epochs, so the history and the tables stay bounded.
Every operation of an epoch touches only that epoch's tables, which makes an
epoch's history closed: once its operations finish, a final read of every
key and one last check settle it, and its tables are dropped. A check never
loses precision by ignoring other epochs, only the real-time edges between
them.

The assertion messages are those of the legacy client history driver, so
their properties keep their history.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    reachable,
    sometimes,
)

from materialize.antithesis import serializability as sz
from materialize.antithesis_driver.client import (
    ABANDON_LINGER_SECONDS,
    ABANDON_SHARE,
    NOT_SENT,
    Op,
    Outcome,
    Status,
    abandon,
    commit,
)
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

SCHEMA = "history"
KEYS = 8
INDEXED_KEY = 0
MV_KEY = 1
JOIN_KEYS = (2, 3)
DERIVED_CLUSTER = "driver"
"""Holds the index and the materialized views. Reads through the index must
run on it."""
PLAIN_CLUSTER = "quickstart"

CHECK_EVERY = 100
"""Completed operations between two checks of one epoch."""
RETIRE_AFTER_SECONDS = 120.0
"""A retiring epoch waits this long for its pending operations, which then
count as indeterminate."""
TIMED_READS = 4000
"""Strict and strong session reads kept for the timestamp order checks."""
LARGE_HISTORY_OPS = 500
BURST_MAX = 8
"""Sessions one burst may use, which leaves some to the other components."""
ACK_TO_READ_GAP_S = 0.1

ISOLATIONS = (sz.STRICT, sz.SERIALIZABLE, sz.STRONG_SESSION)

CYCLE_ANOMALIES = (
    "G0",
    "G1c",
    "G-single",
    "G2",
    "G0-realtime",
    "G1c-realtime",
    "G-single-realtime",
    "G2-realtime",
)
# Each has its own literal assertion in `assert_report`.
assert set(CYCLE_ANOMALIES) == {a.name for a in sz.ANOMALIES}


def table(epoch: int, key: int) -> str:
    return f"{SCHEMA}.k{key}_e{epoch}"


@dataclass(frozen=True)
class ReadPath:
    name: str
    """`table`, `index`, `mv` or `join_mv`."""
    key: int
    relation: str
    value: str = "v"
    count: str = "rn"

    def aggregate(self) -> str:
        """One row: the key's values, and `value:count` for read-modify-write rows."""
        where = f" WHERE {self.value} IS NOT NULL" if self.name == "join_mv" else ""
        return (
            f"(SELECT string_agg({self.value}::text, ',') AS vs,"
            f" string_agg(CASE WHEN {self.count} IS NOT NULL THEN"
            f" {self.value}::text || ':' || {self.count}::text END, ',') AS cs"
            f" FROM {self.relation}{where})"
        )


def paths(epoch: int, key: int, derived_cluster: bool) -> list[ReadPath]:
    """Paths that read `key`. The index serves reads only on its own cluster."""
    t = ReadPath("table", key, table(epoch, key))
    if key == INDEXED_KEY:
        return [replace(t, name="index")] if derived_cluster else [t]
    if key == MV_KEY:
        return [t, ReadPath("mv", key, f"{SCHEMA}.mv{MV_KEY}_e{epoch}")]
    if key in JOIN_KEYS:
        side = "a" if key == JOIN_KEYS[0] else "b"
        return [
            t,
            ReadPath("join_mv", key, f"{SCHEMA}.mvj_e{epoch}", f"{side}v", f"{side}rn"),
        ]
    return [t]


def create_statements(epoch: int) -> list[str]:
    a, b = (table(epoch, k) for k in JOIN_KEYS)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        *(
            f"CREATE TABLE IF NOT EXISTS {table(epoch, k)} (v bigint NOT NULL, rn bigint)"
            for k in range(KEYS)
        ),
        f"CREATE INDEX IF NOT EXISTS k{INDEXED_KEY}_e{epoch}_idx"
        f" IN CLUSTER {DERIVED_CLUSTER} ON {table(epoch, INDEXED_KEY)} (v)",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.mv{MV_KEY}_e{epoch}"
        f" IN CLUSTER {DERIVED_CLUSTER}"
        f" AS SELECT v, rn FROM {table(epoch, MV_KEY)}",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.mvj_e{epoch}"
        f" IN CLUSTER {DERIVED_CLUSTER}"
        " AS SELECT a.v AS av, a.rn AS arn, b.v AS bv, b.rn AS brn"
        f" FROM {a} AS a FULL OUTER JOIN {b} AS b ON a.v = b.v",
    ]


def parse_observation(path: ReadPath, vs: str | None, cs: str | None) -> sz.Observation:
    values = tuple(int(v) for v in vs.split(",")) if vs else ()
    counts = (
        tuple((int(v), int(c)) for v, c in (pair.split(":") for pair in cs.split(",")))
        if cs
        else ()
    )
    return sz.Observation(path.key, path.name, values, counts)


@dataclass
class Record:
    """One operation, mutable until its outcome arrives."""

    op_id: int
    kind: sz.Kind
    invoke: float
    isolation: str
    write_key: int | None = None
    read_key: int | None = None
    outcome: sz.Outcome | None = None
    """None while the operation is pending, which a check treats as unknown."""
    complete: float | None = None
    session: str = ""
    observations: tuple[sz.Observation, ...] = ()
    ts: int | None = None
    uptime_s: float | None = None

    def freeze(self) -> sz.Op:
        return sz.Op(
            op_id=self.op_id,
            kind=self.kind,
            outcome=self.outcome or sz.Outcome.INFO,
            invoke=self.invoke,
            complete=self.complete,
            session=self.session,
            isolation=self.isolation,
            write_key=self.write_key,
            read_key=self.read_key,
            observations=self.observations,
            ts=self.ts,
            uptime_s=self.uptime_s,
        )


@dataclass
class Epoch:
    number: int
    records: dict[int, Record] = field(default_factory=dict)
    retiring_since: float | None = None
    final_read: bool = False
    """A strict read of every key, after all other operations ended, completed."""
    checked: int = 0
    """Completed operations at the last check."""
    dropping: bool = False
    """Its final check ran and its tables are being dropped, so nothing reads
    it again."""

    def completed(self) -> int:
        return sum(1 for r in self.records.values() if r.outcome is not None)

    def pending(self) -> int:
        return sum(1 for r in self.records.values() if r.outcome is None)


class History:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self._next_op = 1
        self.epochs: dict[int, Epoch] = {}
        self.current: int | None = None
        self._creating = False
        self._timed_reads: deque[sz.Op] = deque(maxlen=TIMED_READS)
        self.isolation_weights = (1, 0, 0)
        self.rmw_share = 0.0
        self.txn_append_share = 0.0
        self.txn_read_share = 0.0
        self.epoch_size = 800

    def configure(self, d: Draw) -> None:
        # Swarm parameters, drawn once per timeline so timelines skew
        # differently. Strict serializable is never excluded: it is the
        # default and the only level with real-time guarantees.
        self.isolation_weights = (
            d.choice((1, 4)),
            d.choice((0, 1)),
            d.choice((0, 1)),
        )
        self.rmw_share = d.choice((0.0, 0.2, 0.5))
        self.txn_append_share = d.choice((0.0, 0.3))
        self.txn_read_share = d.choice((0.2, 0.5))
        self.epoch_size = d.choice((300, 800, 1500))

    def start(self, number: int) -> None:
        """Adopt an epoch whose tables setup created."""
        self.epochs[number] = Epoch(number)
        self.current = number

    def _record(self, epoch: Epoch, **kwargs: Any) -> Record:
        with self._lock:
            op_id = self._next_op
            self._next_op += 1
            record = Record(op_id=op_id, invoke=time.monotonic(), **kwargs)
            epoch.records[op_id] = record
            return record

    def _isolation(self, d: Draw) -> str:
        weights = self.isolation_weights
        total = sum(weights)
        pick = d.integer(0, total - 1)
        for isolation, weight in zip(ISOLATIONS, weights):
            if pick < weight:
                return isolation
            pick -= weight
        return sz.STRICT

    def _live_epoch(self) -> Epoch | None:
        if self.current is None:
            return None
        return self.epochs[self.current]

    def write(
        self, d: Draw, session: int | None = None, wait: float = RULE_WAIT_SECONDS
    ) -> Status:
        epoch = self._live_epoch()
        if epoch is None:
            return Status.NOT_SENT
        if session is None:
            session = d.integer(0, len(self.ctx.sessions.sessions) - 1)
        isolation = self._isolation(d)
        key = d.integer(0, KEYS - 1)
        # `INSERT ... SELECT` is a read-modify-write, which Materialize does
        # not allow in an explicit transaction, and whose read has real-time
        # guarantees only at strict serializable.
        rmw = isolation == sz.STRICT and d.boolean(self.rmw_share)
        if rmw:
            read_key = d.integer(0, KEYS - 1)
            record = self._record(
                epoch,
                kind=sz.Kind.RMW,
                isolation=isolation,
                write_key=key,
                read_key=read_key,
            )
            statement = (
                f"INSERT INTO {table(epoch.number, key)} (v, rn)"
                f" SELECT {record.op_id}, count(*) FROM {table(epoch.number, read_key)}"
            )
            in_txn = False
        else:
            record = self._record(
                epoch, kind=sz.Kind.APPEND, isolation=isolation, write_key=key
            )
            statement = (
                f"INSERT INTO {table(epoch.number, key)} (v) VALUES ({record.op_id})"
            )
            in_txn = d.boolean(self.txn_append_share)
        linger = d.choice(ABANDON_LINGER_SECONDS) if d.boolean(ABANDON_SHARE) else None

        def execute(conn: psycopg.Connection) -> int:
            if not in_txn:
                if linger is not None:
                    abandon(conn, statement.encode(), linger)
                return conn.execute(statement.encode()).rowcount
            conn.execute(b"BEGIN")
            rowcount = conn.execute(statement.encode()).rowcount
            if linger is not None:
                abandon(conn, b"COMMIT", linger)
            commit(conn)
            return rowcount

        def complete(outcome: Outcome[int]) -> None:
            if self._forget_unsent(epoch, record, outcome):
                return
            with self._lock:
                record.complete = outcome.end
                record.session = outcome.session
                # The session's order of operations is their invoke order, so
                # take it from the session, not from when the rule drew it.
                record.invoke = outcome.start
                if outcome.ok:
                    record.outcome = sz.Outcome.OK
                elif outcome.rejected:
                    record.outcome = sz.Outcome.FAIL
                else:
                    record.outcome = sz.Outcome.INFO
            if outcome.ok:
                always(
                    outcome.value == 1,
                    "client history: a write appends exactly one row",
                    {"op": record.op_id, "rowcount": outcome.value},
                )

        return self.ctx.sessions.call(
            session,
            Op(
                "history write",
                execute,
                complete,
                {"transaction_isolation": f"'{isolation}'", "cluster": PLAIN_CLUSTER},
            ),
            wait,
        )

    def _forget_unsent(self, epoch: Epoch, record: Record, outcome: Outcome) -> bool:
        """Drop the record of an operation whose session was busy.

        While sessions are stuck most operations are not sent. Kept, they
        would fill an epoch that then rotates with little to check.
        """
        if outcome.error is not NOT_SENT:
            return False
        with self._lock:
            epoch.records.pop(record.op_id, None)
        return True

    def _read_op(
        self, epoch: Epoch, record: Record, read_paths: list[ReadPath], in_txn: bool
    ) -> Op:
        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            if not in_txn:
                columns = ", ".join(f"p{i}.vs, p{i}.cs" for i in range(len(read_paths)))
                sources = ", ".join(
                    f"{p.aggregate()} AS p{i}" for i, p in enumerate(read_paths)
                )
                row = conn.execute(
                    "SELECT mz_now()::text, extract(epoch FROM mz_uptime())::float8,"
                    f" {columns} FROM {sources}".encode()
                ).fetchone()
                assert row is not None
                return [row]
            rows = []
            # A strict serializable read in a transaction is linearized at
            # COMMIT, so its outcome is known only once COMMIT answers.
            conn.execute(b"BEGIN READ ONLY")
            for p in read_paths:
                row = conn.execute(
                    "SELECT mz_now()::text, extract(epoch FROM mz_uptime())::float8,"
                    f" p.vs, p.cs FROM {p.aggregate()} AS p".encode()
                ).fetchone()
                assert row is not None
                rows.append(row)
            commit(conn)
            return rows

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if self._forget_unsent(epoch, record, outcome):
                return
            observations: list[sz.Observation] = []
            timestamps: set[str] = set()
            uptime = None
            if outcome.ok:
                assert outcome.value is not None
                if in_txn:
                    for p, row in zip(read_paths, outcome.value):
                        timestamps.add(row[0])
                        uptime = row[1]
                        observations.append(parse_observation(p, row[2], row[3]))
                else:
                    row = outcome.value[0]
                    timestamps.add(row[0])
                    uptime = row[1]
                    for i, p in enumerate(read_paths):
                        observations.append(
                            parse_observation(p, row[2 + 2 * i], row[3 + 2 * i])
                        )
                if in_txn:
                    always(
                        len(timestamps) == 1,
                        "client history: every statement of one read transaction observes one mz_now()",
                        {"op": record.op_id, "timestamps": sorted(timestamps)},
                    )
            with self._lock:
                record.complete = outcome.end
                record.session = outcome.session
                if outcome.ok:
                    record.outcome = sz.Outcome.OK
                    record.observations = tuple(observations)
                    record.ts = int(min(timestamps))
                    record.uptime_s = uptime
                    record.invoke = outcome.start
                    if record.isolation in (sz.STRICT, sz.STRONG_SESSION):
                        self._timed_reads.append(record.freeze())
                else:
                    # A failed read observed nothing and constrains nothing.
                    record.outcome = sz.Outcome.FAIL

        settings = {
            "transaction_isolation": f"'{record.isolation}'",
            "cluster": (
                DERIVED_CLUSTER
                if any(p.name == "index" for p in read_paths)
                else PLAIN_CLUSTER
            ),
        }
        return Op("history read", execute, complete, settings)

    def read(
        self, d: Draw, session: int | None = None, wait: float = RULE_WAIT_SECONDS
    ) -> Status:
        epoch = self._live_epoch()
        if epoch is None:
            return Status.NOT_SENT
        if session is None:
            session = d.integer(0, len(self.ctx.sessions.sessions) - 1)
        isolation = self._isolation(d)
        derived = d.boolean()
        read_paths: list[ReadPath] = []
        for _ in range(d.integer(1, 3)):
            key = d.choice((1, 1, 1, 2, 2, 2, 3, 3, 0))
            options = paths(epoch.number, key, derived)
            read_paths.append(d.choice(options))
            if len(options) > 1 and d.boolean(0.2):
                read_paths.extend(p for p in options if p not in read_paths)
        read_paths = list(dict.fromkeys(read_paths))
        in_txn = len(read_paths) > 1 and d.boolean(self.txn_read_share)
        record = self._record(epoch, kind=sz.Kind.READ, isolation=isolation)
        return self.ctx.sessions.call(
            session, self._read_op(epoch, record, read_paths, in_txn), wait
        )

    def burst(self, d: Draw) -> Status:
        """Operations on several sessions at once.

        One operation per rule leaves most sessions idle, and an epoch with
        too few operations for a large cycle search.
        """
        sessions = len(self.ctx.sessions.sessions)
        first = d.integer(0, sessions - 1)
        count = d.integer(2, min(BURST_MAX, sessions))
        status = Status.NOT_SENT
        for i in range(count):
            operation = self.write if d.boolean() else self.read
            # Only the last one waits, so the others are in flight together.
            wait = RULE_WAIT_SECONDS if i == count - 1 else 0.0
            sent = operation(d, session=(first + i) % sessions, wait=wait)
            if sent != Status.NOT_SENT:
                status = sent
        return status

    def rotate(self, d: Draw) -> Status:
        """Start the next epoch once the current one is full."""
        with self._lock:
            epoch = self._live_epoch()
            if epoch is None or self._creating or len(epoch.records) < self.epoch_size:
                return Status.NOT_SENT
            number = epoch.number + 1
            self._creating = True

        def execute(conn: psycopg.Connection) -> None:
            for statement in create_statements(number):
                conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                self._creating = False
                if not outcome.ok:
                    return
                if number in self.epochs:
                    return
                self.epochs[epoch.number].retiring_since = time.monotonic()
                self.epochs[number] = Epoch(number)
                self.current = number
            log(f"history: epoch {number} started")

        return self.ctx.sessions.call(
            d.integer(0, len(self.ctx.sessions.sessions) - 1),
            Op(
                "history rotate",
                execute,
                complete,
                {"cluster": PLAIN_CLUSTER},
                ddl=True,
            ),
            RULE_WAIT_SECONDS,
        )

    def final_read(self, epoch: Epoch, session: int, wait: float) -> Status:
        """A strict read of every key on its plain path."""
        record = self._record(epoch, kind=sz.Kind.READ, isolation=sz.STRICT)
        read_paths = [paths(epoch.number, k, False)[0] for k in range(KEYS)]
        op = self._read_op(epoch, record, read_paths, False)
        original = op.complete

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            original(outcome)
            if outcome.ok:
                with self._lock:
                    epoch.final_read = True

        op.complete = complete
        op.name = "history final read"
        return self.ctx.sessions.call(session, op, wait)

    def retire(self, d: Draw) -> Status:
        """Settle one retiring epoch: final read, last check, drop its tables."""
        with self._lock:
            retiring = sorted(
                (e for e in self.epochs.values() if e.retiring_since is not None),
                key=lambda e: e.number,
            )
            if not retiring:
                return Status.NOT_SENT
            epoch = retiring[0]
            assert epoch.retiring_since is not None
            waited = time.monotonic() - epoch.retiring_since
            pending = epoch.pending()
        if not epoch.dropping and pending > 0 and waited < RETIRE_AFTER_SECONDS:
            return Status.NOT_SENT
        session = d.integer(0, len(self.ctx.sessions.sessions) - 1)
        if not epoch.dropping and not epoch.final_read:
            return self.final_read(epoch, session, RULE_WAIT_SECONDS)
        if not epoch.dropping:
            self.check_epoch(epoch, "retire")
            # A drop that fails partway leaves some tables gone. Its epoch
            # must not be read again, only dropped again.
            epoch.dropping = True
        number = epoch.number

        def execute(conn: psycopg.Connection) -> None:
            for k in range(KEYS):
                conn.execute(
                    f"DROP TABLE IF EXISTS {table(number, k)} CASCADE".encode()
                )

        def complete(outcome: Outcome[None]) -> None:
            if outcome.ok:
                with self._lock:
                    self.epochs.pop(number, None)

        return self.ctx.sessions.call(
            session,
            Op(
                "history drop epoch",
                execute,
                complete,
                {"cluster": PLAIN_CLUSTER},
                ddl=True,
            ),
            RULE_WAIT_SECONDS,
        )

    def check(self) -> None:
        """Check each epoch that completed enough operations since its last check."""
        with self._lock:
            due = [
                e
                for e in self.epochs.values()
                if e.completed() - e.checked >= CHECK_EVERY
            ]
        for epoch in due:
            self.check_epoch(epoch, "periodic")
        if due:
            self.check_timestamp_order()

    def check_epoch(self, epoch: Epoch, scope: str) -> sz.Report:
        with self._lock:
            ops = [r.freeze() for r in epoch.records.values()]
            epoch.checked = epoch.completed()
        report = sz.check(ops)
        assert_report(report, epoch.number, scope)
        return report

    def check_timestamp_order(self) -> None:
        with self._lock:
            reads = list(self._timed_reads)
        realtime = sz.realtime_timestamp_order(reads)
        always(
            not realtime.regressions,
            "client history: strict serializable read timestamps never go backwards in real time",
            {"count": len(realtime.regressions), "first": _first(realtime.regressions)},
        )
        sometimes(
            realtime.across_restart > 0,
            "client history: strict serializable monotonicity checked across an environmentd restart",
            {"checked": realtime.checked},
        )
        session = sz.session_timestamp_order(reads)
        always(
            not session.regressions,
            "client history: strong session serializable reads in one session never go backwards",
            {"count": len(session.regressions), "first": _first(session.regressions)},
        )

    def eventually(self, deadline: float) -> None:
        """After faults stop: read every key of every epoch, then check everything."""
        with self._lock:
            epochs = sorted(self.epochs.values(), key=lambda e: e.number)
        for epoch in epochs:
            if epoch.dropping:
                continue
            epoch.final_read = False
            while not epoch.final_read and time.monotonic() < deadline:
                for i in range(len(self.ctx.sessions.sessions)):
                    status = self.final_read(epoch, i, 30.0)
                    if status != Status.NOT_SENT:
                        break
                if not epoch.final_read:
                    time.sleep(1)
            always(
                epoch.final_read,
                "client history: a strict serializable read of every key succeeds after faults stop",
                {"epoch": epoch.number},
            )
            if epoch.final_read:
                report = self.check_epoch(epoch, "final")
                reachable(
                    "client history: the final check covered a live epoch",
                    {"epoch": epoch.number, "ops": report.stats.ops},
                )
        self.check_timestamp_order()


def _first(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    return items[0] if items else None


def assert_report(report: sz.Report, epoch: int, scope: str) -> None:
    base = {"epoch": epoch, "scope": scope}

    def details(items: list[dict[str, Any]]) -> dict[str, Any]:
        return {**base, "count": len(items), "first": _first(items)}

    always(
        not report.phantom_reads,
        "client history: reads observe only writes attempted before the read completed",
        details(report.phantom_reads),
    )
    always(
        not report.aborted_reads,
        "client history: writes rejected with a definite error are never observed",
        details(report.aborted_reads),
    )
    always(
        not report.duplicates,
        "client history: no written id appears twice in the table",
        details(report.duplicates),
    )
    always(
        not report.internal,
        "client history: one read observes consistent values and counts on every read path",
        details(report.internal),
    )
    always(
        not report.invalid_rmw_counts,
        "client history: read-modify-write counts stay within the bounds of writes that could have preceded them",
        details(report.invalid_rmw_counts),
    )
    always(
        not report.incompatible_orders,
        "client history: the sets observed for one key are ordered by inclusion",
        details(report.incompatible_orders),
    )
    always(
        not report.rmw_self_reads,
        "client history: a read-modify-write never counts its own append",
        details(report.rmw_self_reads),
    )
    always(
        not report.stale_reads,
        "client history: strict serializable reads observe every write acknowledged before they began",
        details(report.stale_reads),
    )
    always(
        not report.lost_writes,
        "client history: no write is acknowledged and then lost",
        details(report.lost_writes),
    )
    always(
        not report.timestamp_regressions,
        "client history: a later timestamp observes every write an earlier timestamp observed",
        details(report.timestamp_regressions),
    )
    always(
        not report.indeterminate_regressions,
        "client history: an indeterminate write once observed stays observed at later timestamps",
        details(report.indeterminate_regressions),
    )
    always(
        not report.same_timestamp_mismatches,
        "client history: reads at the same timestamp observe the same writes",
        details(report.same_timestamp_mismatches),
    )
    # One call per anomaly, each with a literal message: Antithesis catalogs
    # assertions by the message at each call site.

    def cycle(name: str) -> dict[str, Any]:
        return {
            **base,
            "count": report.anomaly_counts[name],
            "cycle": _first(report.cycles[name]),
        }

    always(
        not report.cycles["G0"],
        "client history: no G0 anomaly (cycle of ww edges)",
        cycle("G0"),
    )
    always(
        not report.cycles["G1c"],
        "client history: no G1c anomaly (cycle of ww and wr edges)",
        cycle("G1c"),
    )
    always(
        not report.cycles["G-single"],
        "client history: no G-single anomaly (cycle with exactly one rw edge)",
        cycle("G-single"),
    )
    always(
        not report.cycles["G2"],
        "client history: no G2 anomaly (cycle with rw edges)",
        cycle("G2"),
    )
    always(
        not report.cycles["G0-realtime"],
        "client history: no G0-realtime anomaly (cycle of ww and real-time edges)",
        cycle("G0-realtime"),
    )
    always(
        not report.cycles["G1c-realtime"],
        "client history: no G1c-realtime anomaly (cycle of ww, wr and real-time edges)",
        cycle("G1c-realtime"),
    )
    always(
        not report.cycles["G-single-realtime"],
        "client history: no G-single-realtime anomaly (cycle with real-time edges and exactly one rw edge)",
        cycle("G-single-realtime"),
    )
    always(
        not report.cycles["G2-realtime"],
        "client history: no G2-realtime anomaly (cycle with real-time and rw edges)",
        cycle("G2-realtime"),
    )

    stats = report.stats
    summary = {
        **base,
        "ops": stats.ops,
        "graph_nodes": stats.graph_nodes,
        "graph_edges": stats.graph_edges,
    }
    sometimes(
        stats.multi_key_strict_reads > 0,
        "client history: a multi-key strict serializable read was checked",
        {**summary, "reads": stats.multi_key_strict_reads},
    )
    sometimes(
        "index" in stats.paths,
        "client history: a read through the index was checked",
        summary,
    )
    sometimes(
        "mv" in stats.paths,
        "client history: a read through the materialized view was checked",
        summary,
    )
    sometimes(
        "join_mv" in stats.paths,
        "client history: a read through the join materialized view was checked",
        summary,
    )
    sometimes(
        stats.graph_nodes >= LARGE_HISTORY_OPS,
        "client history: a cycle search covered at least 500 operations",
        summary,
    )
    sometimes(
        stats.rmw_reads_placed > 0,
        "client history: a read-modify-write's read was placed in the version order",
        {**summary, "placed": stats.rmw_reads_placed},
    )
    sometimes(
        stats.strict_reads_after_ack > 0,
        "client history: a strict serializable read was checked against an earlier acknowledged write",
        summary,
    )
    sometimes(
        stats.min_ack_gap_s is not None and stats.min_ack_gap_s < ACK_TO_READ_GAP_S,
        "client history: strict serializable read began within 100 ms of a write acknowledgement",
        {**summary, "gap_s": stats.min_ack_gap_s},
    )
    sometimes(
        stats.indeterminate_observed > 0,
        "client history: a read observed a write whose outcome was indeterminate",
        summary,
    )
    sometimes(
        stats.inclusion_checks_nonempty > 0,
        "client history: snapshot inclusion was checked on a read that observed writes",
        summary,
    )
