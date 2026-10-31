# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check interval aggregate errors, their retractions, and SUBSCRIBE poisoning."""

from __future__ import annotations

import os
import sqlite3
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NoReturn

import psycopg
from antithesis.assertions import (
    always_or_unreachable as always,  # pyright: ignore[reportMissingModuleSource]
)
from antithesis.assertions import (
    unreachable,
)

from materialize.antithesis import aggregate_retractions_model as model
from materialize.antithesis import ddl_hold, quiet, sql, state, telemetry
from materialize.antithesis.drivers import configure, history, lifecycle, pod_restarts
from materialize.antithesis.drivers.rollouts import TRANSIENT_ERRORS, Kube
from materialize.antithesis.environment import Environment
from materialize.antithesis.observations import sometimes
from materialize.antithesis.rng import rng

SCHEMA = "aggregate_retractions"
STATE_DB = "aggregate_retractions"
ROUND_SECONDS = 45.0
STATEMENT_TIMEOUT_MS = 10_000
SLOTS = 2


class Interrupted(Exception):
    pass


@dataclass(frozen=True)
class Slot:
    number: int
    epoch: int

    @property
    def table(self) -> str:
        return f"{SCHEMA}.t{self.number}"

    @property
    def view(self) -> str:
        return f"{SCHEMA}.v{self.number}_e{self.epoch}"


def claim(db: sqlite3.Connection) -> Slot | None:
    db.executescript(
        "CREATE TABLE IF NOT EXISTS slots (slot INTEGER PRIMARY KEY, epoch INTEGER, pid INTEGER);"
        "CREATE TABLE IF NOT EXISTS counter (slot INTEGER PRIMARY KEY, value INTEGER);"
        "INSERT OR IGNORE INTO counter VALUES (0, 0);"
    )
    db.execute("BEGIN IMMEDIATE")
    try:
        busy = {
            int(number)
            for number, pid in db.execute("SELECT slot, pid FROM slots")
            if lifecycle.pid_alive(int(pid))
        }
        available = sorted(set(range(SLOTS)) - busy)
        if not available:
            db.rollback()
            return None
        number = rng.choice(available)
        epoch = int(db.execute("SELECT value FROM counter").fetchone()[0]) + 1
        db.execute("UPDATE counter SET value = ?", (epoch,))
        db.execute(
            "INSERT OR REPLACE INTO slots VALUES (?, ?, ?)",
            (number, epoch, os.getpid()),
        )
        db.commit()
        return Slot(number, epoch)
    except BaseException:
        db.rollback()
        raise


def release(db: sqlite3.Connection, slot: Slot) -> None:
    with db:
        db.execute(
            "DELETE FROM slots WHERE slot = ? AND epoch = ?", (slot.number, slot.epoch)
        )


def _connect(host: str) -> psycopg.Connection:
    conn = sql.connect(
        host,
        connect_timeout=5,
        statement_timeout_ms=STATEMENT_TIMEOUT_MS,
        options={"cluster": configure.SHARED_CLUSTER},
    )
    try:
        conn.execute("SET transaction_isolation = 'strict serializable'")
    except BaseException:
        conn.close()
        raise
    return conn


def _check_deadline(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise Interrupted


def _unexpected(error: Exception, phase: str) -> NoReturn:
    classified = sql.classify(error)
    if classified.outcome is sql.Outcome.VIOLATION:
        unreachable(
            "aggregate retractions: SQL returns only classified errors outside modeled overflow",
            {
                "phase": phase,
                "sqlstate": classified.sqlstate,
                "template": classified.template,
            },
        )
    raise Interrupted from error


def expected_overflow(
    error: psycopg.Error, expected: model.Expected, *, subscribe: bool
) -> bool:
    return (
        bool(expected.overflows)
        and error.sqlstate == ("XX000" if subscribe else "22015")
        and "interval out of range" in str(error).lower()
    )


def _projection(source: str) -> str:
    return (
        "SELECT g, (extract(year FROM s) * 12 + extract(month FROM s))::bigint,"
        " extract(day FROM s)::bigint,"
        " ((extract(hour FROM s) * 3600 + extract(minute FROM s) * 60"
        " + extract(second FROM s)) * 1000000)::bigint, n, total "
        f"FROM {source}"
    )


def _recompute(slot: Slot) -> str:
    return (
        f"(SELECT g, sum(v) AS s, count(v) AS n, count(*) AS total FROM {slot.table}"
        f" WHERE epoch = {slot.epoch} GROUP BY g) AS recomputed"
    )


def check_rows(
    actual: list[tuple[Any, ...]], expected: model.Expected, path: str
) -> None:
    actual_bag = Counter(tuple(row) for row in actual)
    _check_bag(actual_bag, expected, path)


def _check_bag(
    actual_bag: Counter[tuple[Any, ...]], expected: model.Expected, path: str
) -> None:
    passed = not expected.overflows and actual_bag == Counter(expected.rows)
    always(
        passed,
        "aggregate retractions: representable grouped interval sums equal the independent component model",
        {
            "path": path,
            "expected": expected.rows,
            "actual": list(actual_bag.items()),
            "overflow": expected.overflows,
        },
    )
    telemetry.comparison(
        "aggregate_retractions", path, passed, max(1, len(expected.rows))
    )
    if not passed:
        raise Interrupted("aggregate result differs from the component model")


def _peek(
    conn: psycopg.Connection, query: str, expected: model.Expected, path: str
) -> bool:
    try:
        rows = conn.execute(query.encode()).fetchall()
    except psycopg.Error as error:
        if expected_overflow(error, expected, subscribe=False):
            always(
                True,
                "aggregate retractions: a grouped interval sum outside its component range reports overflow",
                {"path": path, "overflow": expected.overflows},
            )
            telemetry.comparison("aggregate_retractions", path, True)
            return True
        _unexpected(error, path)
    if expected.overflows:
        always(
            False,
            "aggregate retractions: a grouped interval sum outside its component range reports overflow",
            {"path": path, "overflow": expected.overflows, "actual": rows},
        )
        telemetry.comparison("aggregate_retractions", path, False)
        raise Interrupted
    check_rows(rows, expected, path)
    return False


def _begin_stream(conn: psycopg.Connection, query: str) -> int:
    conn.execute("BEGIN")
    row = conn.execute(
        f"SELECT mz_now(), count(*) FROM ({query}) AS snapshot".encode()
    ).fetchone()
    assert row is not None
    as_of = int(row[0])
    conn.execute(
        f"DECLARE c CURSOR FOR SUBSCRIBE ({query}) WITH (PROGRESS) AS OF {as_of}".encode()
    )
    return as_of


def _snapshot(
    conn: psycopg.Connection, expected: model.Expected, deadline: float, as_of: int
) -> None:
    state_rows: Counter[tuple[Any, ...]] = Counter()
    while True:
        _check_deadline(deadline)
        try:
            rows = conn.execute("FETCH ALL c WITH (timeout = '1s')").fetchall()
        except (psycopg.Error, OSError) as error:
            _unexpected(error, "subscribe snapshot")
        for row in rows:
            timestamp = int(row[0])
            if row[1]:
                if timestamp > as_of:
                    always(
                        all(count >= 0 for count in state_rows.values()),
                        "aggregate retractions: a closed SUBSCRIBE snapshot has no negative multiplicities",
                        {"state": list(state_rows.items())},
                    )
                    _check_bag(state_rows, expected, "subscribe")
                    return
                continue
            if timestamp <= as_of:
                state_rows[tuple(row[3:])] += int(row[2])


def _poison(conn: psycopg.Connection, expected: model.Expected, deadline: float) -> str:
    while True:
        _check_deadline(deadline)
        try:
            conn.execute("FETCH ALL c WITH (timeout = '1s')").fetchall()
        except psycopg.Error as error:
            if expected_overflow(error, expected, subscribe=True):
                telemetry.comparison("aggregate_retractions", "subscribe", True)
                return str(error)
            _unexpected(error, "subscribe overflow")


def _old_stream_stays_failed(conn: psycopg.Connection, poison: str) -> None:
    try:
        conn.execute("FETCH ALL c WITH (timeout = '1s')").fetchall()
    except psycopg.Error as error:
        if (
            error.sqlstate == "25P02"
            or error.sqlstate == "XX000"
            and str(error) == poison
        ):
            always(
                True,
                "aggregate retractions: an errored SUBSCRIBE never silently becomes healthy",
                {},
            )
            telemetry.comparison("aggregate_retractions", "subscribe", True)
            return
        _unexpected(error, "poisoned subscribe")
    always(
        False,
        "aggregate retractions: an errored SUBSCRIBE never silently becomes healthy",
        {},
    )
    raise Interrupted


def _insert(
    conn: psycopg.Connection, slot: Slot, batch: int, values: tuple[model.Input, ...]
) -> None:
    marks = ", ".join("(%s, %s, %s, %s::interval)" for _ in values)
    params: list[Any] = []
    for group, value in values:
        text = (
            None
            if value is None
            else f"{value[0]} months {value[1]} days {value[2]} microseconds"
        )
        params.extend((slot.epoch, batch, group, text))
    conn.execute(f"INSERT INTO {slot.table} VALUES {marks}".encode(), params)


def _prepare(writer: psycopg.Connection, slot: Slot) -> None:
    if ddl_hold.active():
        raise Interrupted("DDL is held")
    writer.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}".encode())
    writer.execute(
        f"CREATE TABLE IF NOT EXISTS {slot.table} (epoch bigint, batch bigint, g bigint, v interval)".encode()
    )
    old_views = writer.execute(
        "SELECT v.name FROM mz_materialized_views AS v JOIN mz_schemas AS s ON v.schema_id = s.id "
        "WHERE s.name = %s AND v.name LIKE %s",
        (SCHEMA, f"v{slot.number}_e%"),
    ).fetchall()
    for (name,) in old_views:
        suffix = str(name).removeprefix(f"v{slot.number}_e")
        if suffix.isdecimal() and int(suffix) < slot.epoch:
            writer.execute(
                f"DROP MATERIALIZED VIEW IF EXISTS {SCHEMA}.v{slot.number}_e{int(suffix)}".encode()
            )
    # A former owner's late cleanup must never delete the new epoch's rows.
    writer.execute(f"DELETE FROM {slot.table} WHERE epoch < {slot.epoch}".encode())
    # Filter before reduction so late old writes cannot poison the current sum.
    writer.execute(
        (
            f"CREATE MATERIALIZED VIEW {slot.view} IN CLUSTER {configure.SHARED_CLUSTER} AS "
            f"SELECT g, sum(v) AS s, count(v) AS n, count(*) AS total FROM {slot.table} "
            f"WHERE epoch = {slot.epoch} GROUP BY g"
        ).encode()
    )


def run_round(
    host: str,
    slot: Slot,
    plan: model.Plan,
    deadline: float,
    fault: Callable[[], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "slot": slot.number,
        "epoch": slot.epoch,
        "axis": plan.axis,
    }
    with _connect(host) as writer:
        _prepare(writer, slot)
        _check_deadline(deadline)
        _insert(writer, slot, 0, plan.baseline)
        healthy = model.evaluate(plan.baseline)
        _peek(writer, _projection(slot.view), healthy, "mv")
        _peek(writer, _projection(_recompute(slot)), healthy, "recompute")
        with _connect(host) as old:
            as_of = _begin_stream(old, _projection(slot.view))
            _snapshot(old, healthy, deadline, as_of)
            _insert(writer, slot, 1, plan.poison)
            dirty = model.evaluate((*plan.baseline, *plan.poison))
            evidence["overflow_seen"] = _peek(
                writer, _projection(slot.view), dirty, "mv"
            )
            _peek(writer, _projection(_recompute(slot)), dirty, "recompute")
            poison = _poison(old, dirty, deadline)
            evidence["poison_seen"] = True
            if fault is not None:
                evidence["fault"] = fault()
            _check_deadline(deadline)
            writer.execute(
                f"DELETE FROM {slot.table} WHERE epoch = {slot.epoch} AND batch = 1".encode()
            )
            _insert(writer, slot, 2, (plan.extra,))
            repaired = model.evaluate((*plan.baseline, plan.extra))
            _peek(writer, _projection(slot.view), repaired, "mv")
            _peek(writer, _projection(_recompute(slot)), repaired, "recompute")
            _old_stream_stays_failed(old, poison)
            old.execute("ROLLBACK")
        with _connect(host) as fresh:
            as_of = _begin_stream(fresh, _projection(slot.view))
            _snapshot(fresh, repaired, deadline, as_of)
            fresh.execute("ROLLBACK")
        evidence["repaired_nonempty"] = bool(repaired.rows)
        return evidence


def _fault(env: Environment, host: str, deadline: float) -> dict[str, Any] | None:
    if (
        quiet.active_quiet_period()
        or time.monotonic() >= deadline
        or rng.random() >= 0.3
    ):
        return None
    cid = telemetry.resolve_cluster_id(host, configure.SHARED_CLUSTER)
    if cid is None:
        return None
    try:
        kube = Kube(env)
        victim = pod_restarts.pick_victim(kube, "clusterd", cluster_id=cid)
        if (
            victim is None
            or victim.uid is None
            or victim.cluster_id != cid
            or victim.role != "clusterd"
        ):
            return None
        acked = pod_restarts.delete_recorded(
            kube,
            origin="aggregate_retractions",
            role=victim.role,
            namespace=victim.namespace,
            pod=victim.pod,
            uid=victim.uid,
            grace=1,
            reason="modeled aggregate overflow",
            cluster_id=cid,
            deadline=deadline,
        )
        return {"acked": acked, "cluster_id": cid, "pod_uid": victim.uid}
    except TRANSIENT_ERRORS:
        return None


def main() -> int:
    history.start_watchdog(ROUND_SECONDS + 60)
    deadline = time.monotonic() + ROUND_SECONDS
    db = state.open_db(STATE_DB)
    slot = claim(db)
    if slot is None:
        db.close()
        return 0
    evidence: dict[str, Any] = {}
    try:
        try:
            env = Environment()
            host = env.sql_host()
        except TRANSIENT_ERRORS as error:
            telemetry.comparison("aggregate_retractions", "mv", None)
            raise Interrupted("SQL service is unavailable") from error
        evidence = run_round(
            host,
            slot,
            model.generate(rng),
            deadline,
            lambda: _fault(env, host, deadline),
        )
    except Interrupted:
        pass
    except (psycopg.Error, OSError) as error:
        try:
            _unexpected(error, "protocol")
        except Interrupted:
            pass
    finally:
        sometimes(
            bool(evidence.get("overflow_seen") and evidence.get("repaired_nonempty")),
            "aggregate retractions: a modeled overflow was retracted and a nonempty maintained result verified",
            evidence,
        )
        sometimes(
            bool(evidence.get("poison_seen") and evidence.get("repaired_nonempty")),
            "aggregate retractions: a fresh SUBSCRIBE verified repaired state after an older stream was poisoned",
            evidence,
        )
        sometimes(
            bool(
                (evidence.get("fault") or {}).get("acked")
                and evidence.get("repaired_nonempty")
            ),
            "aggregate retractions: a modeled overflow was repaired after an acknowledged replica delete in its cluster",
            evidence,
        )
        release(db, slot)
        db.close()
    return 0
