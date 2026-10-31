# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import sqlite3
from collections import Counter
from contextlib import nullcontext
from typing import Any
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import index_handover as h


@pytest.fixture
def db(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(h.state, "open_db", lambda _: sqlite3.connect(":memory:"))
    conn = h.open_state()
    yield conn
    conn.close()


def test_ledger_keeps_only_acknowledged_mutations(monkeypatch, db):
    with db:
        db.execute("INSERT INTO runs (id) VALUES (1)")
    execute = Mock()
    monkeypatch.setattr(h, "_execute", execute)
    h._write(Mock(), db, 1, h.Mutation(1, (1, 2, 7), "insert"), 100)
    h._write(Mock(), db, 1, h.Mutation(2, (2, 2, 7), "insert"), 100)
    h._write(Mock(), db, 1, h.Mutation(1, (1, 3, -8), "update"), 100)
    h._write(Mock(), db, 1, h.Mutation(2, None, "delete"), 100)
    execute.side_effect = psycopg.OperationalError(
        "response lost after possible commit"
    )
    with pytest.raises(psycopg.OperationalError):
        h._write(Mock(), db, 1, h.Mutation(1, (1, 9, 99), "update"), 100)
    assert h.expected_rows(db, 1) == Counter({(1, 3, -8): 1})
    assert db.execute(
        "SELECT count(*) FROM writes WHERE acknowledged=0"
    ).fetchone() == (1,)


def test_stream_applies_all_updates_only_at_a_closed_timestamp():
    stream = h.Stream()
    stream.feed([(10, True, None, None, None, None), (10, False, 1, 1, 0, 7)])
    assert not stream.snapshot_closed
    assert not stream.bag
    stream.feed([(11, True, None, None, None, None), (12, False, -1, 1, 0, 7)])
    assert stream.snapshot_closed
    assert stream.bag == Counter({(1, 0, 7): 1})
    stream.feed([(12, False, 1, 1, 0, -3), (13, True, None, None, None, None)])
    assert stream.bag == Counter({(1, 0, -3): 1})
    assert not stream.errors


@pytest.mark.parametrize(
    "batch, expected",
    [
        ([(9, True, None, None, None, None)], "progress regressed"),
        ([(9, False, 1, 1, 0, 3)], "below closed progress"),
        (
            [(10, False, -1, 1, 0, 3), (11, True, None, None, None, None)],
            "negative multiplicity",
        ),
    ],
)
def test_stream_rejects_invalid_frontiers_and_multiplicities(batch, expected):
    stream = h.Stream()
    stream.feed([(10, True, None, None, None, None)])
    stream.feed(batch)
    assert any(expected in error for error in stream.errors)


def test_stream_requires_initial_progress():
    stream = h.Stream()
    stream.feed([(10, False, 1, 1, 0, 7)])
    assert "subscription data precedes initial progress" in stream.errors


def test_progress_payload_fields_are_ignored():
    stream = h.Stream()
    stream.feed(
        [
            (10, True, "ignored", "ignored", None, object()),
            (11, True, None, None, None, None),
        ]
    )
    assert stream.snapshot_closed
    assert not stream.errors


@pytest.mark.parametrize(
    "row",
    [
        None,
        (None, True, None, None, None, None),
        (10, None, None, None, None, None),
        (10, False, None, 1, 2, 3),
        (10, False, 1, None, 2, 3),
        (10, False, 1, 1, 2, None),
        (10, False, 1),
    ],
)
def test_malformed_stream_rows_are_product_protocol_failures(monkeypatch, row):
    monkeypatch.setattr(h, "_execute", lambda *args: Mock(fetchall=lambda: [row]))
    report = Mock()
    monkeypatch.setattr(h, "always", report)
    with pytest.raises(h.Incomplete, match="violated its stream protocol"):
        h._drain(Mock(), h.Stream(), h.time.monotonic() + 5, None)
    report.assert_called_once()
    assert report.call_args.args[0] is False


class Protocol:
    def __init__(
        self, monkeypatch, *, flag=False, durable=True, transient=True, corrupt=None
    ):
        self.rows: dict[int, h.Row] = {}
        self.namespaces: dict[str, dict[int, h.Row]] = {}
        self.schema: str | None = None
        self.index = None
        self.index_number = 0
        self.mv = False
        self.stream_bag: Counter[h.Row] = Counter()
        self.cursor_open = False
        self.commands = []
        self.assertions = []
        self.coverage = []
        self.flag = flag
        self.durable = durable
        self.transient = transient
        self.corrupt = corrupt
        monkeypatch.setattr(h, "_connect", lambda *args, **kwargs: self)
        monkeypatch.setattr(h, "_execute", self.execute)
        monkeypatch.setattr(h, "always", self.check)
        monkeypatch.setattr(
            h,
            "sometimes",
            lambda condition, message, details: self.coverage.append(
                (condition, message, details)
            ),
        )
        monkeypatch.setattr(h.ddl_hold, "active", lambda: False)
        monkeypatch.setattr(h, "METADATA_WAIT_S", 0.001)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def transaction(self):
        return nullcontext()

    def check(self, condition, message, details):
        self.assertions.append((bool(condition), message, details))
        if not condition:
            raise AssertionError((message, details))

    def execute(self, conn, deadline, query, params: Any = None):
        self.commands.append(query)
        rows: list[tuple[Any, ...]] = []
        if query.startswith("CREATE SCHEMA"):
            schema = query.split()[-1]
            self.schema = schema
            self.rows = self.namespaces.setdefault(schema, {})
            self.stream_bag.clear()
        elif query.startswith("DROP SCHEMA"):
            schema = query.split()[-2]
            old_rows = self.namespaces.pop(schema, None)
            if old_rows is not None:
                old_rows.clear()
        elif query.startswith("CREATE INDEX"):
            self.index_number += 1
            self.index = f"u{self.index_number}"
        elif query.startswith("CREATE MATERIALIZED VIEW"):
            self.mv = True
        elif query.startswith("DROP MATERIALIZED VIEW"):
            self.mv = False
        elif query.startswith("DROP INDEX"):
            if self.mv and self.flag is False and self.durable:
                raise psycopg.errors.DependentObjectsStillExist("physical reader")
            self.index = None
        elif query.startswith("INSERT INTO"):
            self.rows[params[0]] = tuple(params)
        elif query.startswith("UPDATE"):
            self.rows[params[2]] = (params[2], params[0], params[1])
        elif query.startswith("DELETE"):
            self.rows.pop(params[0])
        elif "SELECT o.id, g.global_id" in query:
            identity = self.index if params[1] == "i" else "u_mv"
            rows = [(identity, identity)] if identity else []
        elif query.startswith("SHOW"):
            if self.flag is None:
                raise psycopg.errors.UndefinedObject("unsupported on old release")
            rows = [("on" if self.flag else "off",)]
        elif "SELECT object_id FROM mz_internal.mz_compute_dependencies" in query:
            rows = [("u_mv",)] if self.mv and self.durable else []
        elif "SELECT s.id FROM mz_internal.mz_subscriptions" in query:
            assert self.cursor_open
            rows = [("t_live",)] if self.transient else []
        elif "SELECT id::text FROM mz_internal.mz_sessions" in query:
            rows = [("session",)]
        elif query.startswith("DECLARE"):
            self.cursor_open = True
        elif query.startswith("CLOSE"):
            self.cursor_open = False
        elif query.startswith("FETCH"):
            current = Counter(self.rows.values())
            rows = [(10, True, None, None, None, None)] if not self.stream_bag else []
            rows.extend(
                (
                    10 if not self.stream_bag else 20,
                    False,
                    count - self.stream_bag[row],
                    *row,
                )
                for row, count in current.items()
                if count != self.stream_bag[row]
            )
            rows.extend(
                (20, False, -count, *row)
                for row, count in self.stream_bag.items()
                if row not in current
            )
            rows.append(
                (11 if not self.stream_bag else 22, True, None, None, None, None)
            )
            self.stream_bag = current
        elif query.startswith("SELECT id,k,v FROM"):
            rows = list(self.rows.values())
        elif query.startswith("SELECT mz_now()"):
            rows = [("21", *row) for row in self.rows.values()]
            if self.corrupt == "value":
                rows[0] = (*rows[0][:-1], 123456)
            elif self.corrupt == "empty":
                rows = []
        return Mock(fetchall=lambda: rows, fetchone=lambda: rows[0] if rows else None)


def run(db):
    return h.run_round(
        "host",
        db,
        seed=[(1, 0, 7), (2, 0, 7), (3, 2, -8)],
        mutations=[
            h.Mutation(4, (4, 2, 0), "insert"),
            h.Mutation(1, (1, 1, -3), "update"),
            h.Mutation(3, None, "delete"),
        ],
        cuts=(1, 2),
        index_columns="v,id",
    )


def test_protocol_checks_both_guard_modes_and_live_reader_handover(monkeypatch, db):
    protocol = Protocol(monkeypatch)
    assert run(db) == 1
    drops = [query for query in protocol.commands if query.startswith("DROP INDEX")]
    assert drops == [
        "DROP INDEX index_handover_r1.i RESTRICT",
        "DROP INDEX index_handover_r1.i CASCADE",
        "DROP INDEX index_handover_r1.i",
    ]
    witness = [
        details
        for condition, message, details in protocol.coverage
        if condition and "old physical subscriber" in message
    ]
    assert len(witness) == 1
    assert witness[0]["subscription"] == "t_live"
    assert witness[0]["old_index"] != witness[0]["new_index"]
    assert witness[0]["drop_acknowledged"]
    assert witness[0]["closed_frontier"] > witness[0]["barrier"]


@pytest.mark.parametrize("flag,durable", [(True, True), (None, True), (False, False)])
def test_durable_guard_requires_known_false_flag_and_actual_import(
    monkeypatch, db, flag, durable
):
    protocol = Protocol(monkeypatch, flag=flag, durable=durable)
    assert run(db) == 1
    assert not any(
        condition
        for condition, message, _ in protocol.coverage
        if "both DROP modes" in message
    )
    assert not any(
        "durable physical importer blocks" in message
        for _, message, _ in protocol.assertions
    )


def test_missing_physical_subscription_never_credits_replacement(monkeypatch, db):
    protocol = Protocol(monkeypatch, transient=False)
    with pytest.raises(h.Incomplete, match="physical import"):
        run(db)
    assert not any(
        condition
        for condition, message, _ in protocol.coverage
        if "old physical subscriber" in message
    )
    assert not any(
        query == "DROP INDEX index_handover_r1.i" for query in protocol.commands
    )


@pytest.mark.parametrize("corrupt", ["value", "empty"])
def test_wrong_fresh_read_is_a_product_failure_even_when_empty(
    monkeypatch, db, corrupt
):
    protocol = Protocol(monkeypatch, corrupt=corrupt)
    with pytest.raises(AssertionError, match="reads equal"):
        run(db)
    assert any(
        not condition and "reads equal" in message
        for condition, message, _ in protocol.assertions
    )
    assert protocol.commands[-1] == "DROP SCHEMA IF EXISTS index_handover_r1 CASCADE"


def test_cleanup_failure_cannot_mask_an_oracle_failure(monkeypatch, db):
    protocol = Protocol(monkeypatch, corrupt="value")
    execute = protocol.execute
    calls = 0

    def cleanup_failure(conn, deadline, query, params=None):
        nonlocal calls
        if query == "DROP SCHEMA IF EXISTS index_handover_r1 CASCADE":
            calls += 1
            if calls == 1:
                raise RuntimeError("cleanup connection failed")
        return execute(conn, deadline, query, params)

    monkeypatch.setattr(h, "_execute", cleanup_failure)
    with pytest.raises(AssertionError, match="reads equal"):
        run(db)


def test_lost_write_ack_aborts_without_comparing_acknowledged_only_input(
    monkeypatch, db
):
    protocol = Protocol(monkeypatch)
    execute = protocol.execute
    committed = []

    def lose_insert_response(conn, deadline, query, params=None):
        result = execute(conn, deadline, query, params)
        if query.startswith("INSERT") and protocol.cursor_open:
            committed.append(dict(protocol.rows))
            raise psycopg.OperationalError("commit succeeded but response lost")
        return result

    monkeypatch.setattr(h, "_execute", lose_insert_response)
    with pytest.raises(psycopg.OperationalError, match="response lost"):
        run(db)
    assert 4 in committed[0]
    assert db.execute(
        "SELECT count(*) FROM writes WHERE acknowledged=0"
    ).fetchone() == (1,)
    assert all(condition for condition, _, _ in protocol.assertions)
    assert not any(
        condition
        for condition, message, _ in protocol.coverage
        if "old physical subscriber" in message
    )


def test_a_new_ddl_hold_defers_cleanup_and_all_further_ddl(monkeypatch, db):
    protocol = Protocol(monkeypatch)
    monkeypatch.setattr(h.ddl_hold, "active", lambda: protocol.cursor_open)
    with pytest.raises(h.Incomplete, match="DDL is held"):
        run(db)
    assert "DROP INDEX index_handover_r1.i" not in protocol.commands
    assert not any(query.startswith("DROP SCHEMA") for query in protocol.commands)


def test_round_expiry_is_incomplete_without_a_product_liveness_claim(monkeypatch, db):
    protocol = Protocol(monkeypatch)
    execute = protocol.execute

    def fetch_timeout(conn, deadline, query, params=None):
        if query.startswith("FETCH"):
            raise h.Incomplete("round budget elapsed")
        return execute(conn, deadline, query, params)

    monkeypatch.setattr(h, "_execute", fetch_timeout)
    with pytest.raises(h.Incomplete, match="budget"):
        run(db)
    assert all(condition for condition, _, _ in protocol.assertions)
    assert not any(
        condition
        for condition, message, _ in protocol.coverage
        if "old physical subscriber" in message
    )


def test_unavailable_service_discovery_does_not_become_a_product_failure(
    monkeypatch, tmp_path, db
):
    env = Mock(endpoints=Mock(state_dir=tmp_path))
    env.sql_host.side_effect = RuntimeError("API server unavailable")
    monkeypatch.setattr(h, "Environment", lambda: env)
    monkeypatch.setattr(h.history, "start_watchdog", Mock())
    monkeypatch.setattr(h, "open_state", lambda: db)
    monkeypatch.setattr(h.ddl_hold, "active", lambda: False)
    report = Mock()
    monkeypatch.setattr(h, "always", report)
    run = Mock()
    monkeypatch.setattr(h, "run_round", run)
    assert h.main() == 0
    report.assert_not_called()
    run.assert_not_called()


def test_late_old_cleanup_cannot_drop_the_next_round_namespace(monkeypatch, db):
    protocol = Protocol(monkeypatch)
    execute = protocol.execute
    deferred = []
    closed = False
    rounds = 0

    def delayed_cleanup(conn, deadline, query, params=None):
        nonlocal closed, rounds
        if query.startswith("CREATE SCHEMA"):
            rounds += 1
        if query.startswith("CLOSE"):
            closed = True
        if query.startswith("DROP SCHEMA") and closed and not deferred:
            deferred.append(query)
            raise psycopg.OperationalError("cleanup response lost, DDL still pending")
        if query.startswith("SELECT mz_now()") and rounds == 2:
            execute(conn, deadline, deferred[0])
        return execute(conn, deadline, query, params)

    monkeypatch.setattr(h, "_execute", delayed_cleanup)
    assert run(db) == 1
    assert run(db) == 1
    namespaces = [
        query.split()[-1]
        for query in protocol.commands
        if query.startswith("CREATE SCHEMA")
    ]
    assert len(set(namespaces)) == 2


def test_cleanup_journal_keeps_ambiguous_writer_and_caps_live_rounds(monkeypatch, db):
    first = h._reserve(db)
    with db:
        db.execute(
            "UPDATE runs SET writer_session='original-writer',started=1 WHERE id=?",
            (first,),
        )
    active = True
    drops = []
    monkeypatch.setattr(h.ddl_hold, "active", lambda: False)
    monkeypatch.setattr(h, "_connect", lambda *args, **kwargs: nullcontext(Mock()))

    def execute(conn, deadline, query, params=None):
        if query.startswith("SELECT 1"):
            assert params == ("original-writer",)
            return Mock(fetchone=lambda: (1,) if active else None)
        drops.append(query)
        return Mock()

    monkeypatch.setattr(h, "_execute", execute)
    h._cleanup("host", db, first)
    assert not drops
    second = h._reserve(db)
    with pytest.raises(h.Incomplete, match="backpressure"):
        h._reserve(db)
    assert db.execute(
        "SELECT count(*) FROM runs WHERE cleanup_complete=0"
    ).fetchone() == (2,)
    active = False
    h._cleanup("host", db, first)
    assert drops == [f"DROP SCHEMA IF EXISTS {h.schema_name(first)} CASCADE"]
    third = h._reserve(db)
    assert first < second < third
    assert len({h.schema_name(first), h.schema_name(second), h.schema_name(third)}) == 3


def test_late_old_create_remains_journaled_and_cannot_pollute_new_round(
    monkeypatch, db
):
    protocol = Protocol(monkeypatch)
    execute = protocol.execute
    deferred = []
    created = []
    writer_active = True

    def late_create(conn, deadline, query, params=None):
        if query.startswith("SELECT 1 FROM mz_internal.mz_sessions"):
            return Mock(fetchone=lambda: (1,) if writer_active else None)
        if query.startswith("CREATE SCHEMA"):
            created.append(query.split()[-1])
        if query.startswith("CREATE TABLE") and len(created) == 1 and not deferred:
            deferred.append(query)
            raise psycopg.OperationalError("CREATE is still queued after response loss")
        if query.startswith("SELECT mz_now()") and len(created) == 2:
            execute(conn, deadline, deferred[0])
        return execute(conn, deadline, query, params)

    monkeypatch.setattr(h, "_execute", late_create)
    with pytest.raises(psycopg.OperationalError):
        run(db)
    assert db.execute("SELECT cleanup_complete FROM runs WHERE id=1").fetchone() == (0,)
    assert run(db) == 1
    assert created == ["index_handover_r1", "index_handover_r2"]
    assert db.execute("SELECT cleanup_complete FROM runs WHERE id=1").fetchone() == (0,)
    writer_active = False
    h._reap("host", db, h.time.monotonic() + 5)
    assert db.execute("SELECT cleanup_complete FROM runs WHERE id=1").fetchone() == (1,)
    assert not protocol.namespaces


def test_namespace_reservation_survives_history_pruning(db):
    unresolved = h._reserve(db)
    names = []
    for _ in range(20):
        run = h._reserve(db)
        names.append(h.schema_name(run))
        with db:
            db.execute("UPDATE runs SET cleanup_complete=1 WHERE id=?", (run,))
    assert len(set(names)) == 20
    assert names[-1] == "index_handover_r21"
    assert db.execute("SELECT count(*) FROM runs").fetchone()[0] <= 10
    assert db.execute(
        "SELECT cleanup_complete FROM runs WHERE id=?", (unresolved,)
    ).fetchone() == (0,)


def test_namespace_and_original_writer_are_journaled_before_create(monkeypatch, db):
    protocol = Protocol(monkeypatch)
    execute = protocol.execute
    observed = []

    def inspect_journal(conn, deadline, query, params=None):
        if query.startswith("CREATE SCHEMA"):
            rows = db.execute(
                "SELECT id,writer_session,started,cleanup_complete FROM runs"
            ).fetchall()
            assert rows == [(1, "session", 1, 0)]
            assert query == f"CREATE SCHEMA {h.schema_name(rows[0][0])}"
            observed.append(query)
        return execute(conn, deadline, query, params)

    monkeypatch.setattr(h, "_execute", inspect_journal)
    assert run(db) == 1
    assert len(observed) == 1
