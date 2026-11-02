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
from contextlib import nullcontext
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import session_rebinding_model as model
from materialize.antithesis.drivers import session_rebinding as h

A = ((0, None, "a"), (1, 5, "a"), (1, 5, "a"), (2, -2, "a"))
B = ((0, None, "b"), (1, -7, "b"), (2, 3, "b"))
PARAMETERS = model.Parameters(-2, 3, False)
PA = ((0, None, "a"), (1, -7, "a"), (1, -7, "a"), (2, 7, "a"))
PB = ((0, None, "b"), (1, 17, "b"), (2, -3, "b"))


def test_model_freezes_begun_cursor_and_keeps_session_bindings_separate():
    expected = model.Model((A, B))
    expected.begin_cursor("first", PARAMETERS)
    assert expected.fetch("first", 2) == PA[:2]
    expected.replace(1)
    assert expected.rows("second", PARAMETERS) == PB
    expected.create_temp("first", 0)
    expected.create_temp("second", 1)
    expected.replace(0)
    assert expected.rows("first", PARAMETERS) == PA
    assert expected.rows("second", PARAMETERS) == PB
    assert expected.rows("second", PARAMETERS, qualified=True) == PA
    assert expected.fetch("first", None) == PA[2:]
    assert expected.fetch("first", None) == ()
    expected.commit("first")
    with pytest.raises(KeyError):
        expected.fetch("first", None)
    expected.drop_temp("second")
    assert expected.rows("second", PARAMETERS) == PA


@pytest.mark.parametrize("factor,offset", [(0, -5), (-7, 2), (1, 0), (5, 29)])
def test_parameter_projection_preserves_nulls_duplicates_and_order(factor, offset):
    parameters = model.Parameters(factor, offset, True)
    assert parameters.project(A) == (
        (2, -2 * factor + offset, "a"),
        (1, 5 * factor + offset, "a"),
        (1, 5 * factor + offset, "a"),
        (0, None, "a"),
    )


def test_cursor_rejects_negative_fetch_without_advancing():
    cursor = model.Cursor(PA)
    with pytest.raises(ValueError):
        cursor.fetch(-1)
    assert cursor.fetch(None) == PA


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr(h.state, "open_db", lambda _: sqlite3.connect(":memory:"))
    connection = h.open_state()
    yield connection
    connection.close()


def test_bounded_cleanup_retains_unknown_namespaces_and_never_reuses_names(db):
    assert h._reserve(db) == 1
    assert h._reserve(db) == 2
    with pytest.raises(h.Incomplete, match="backpressure"):
        h._reserve(db)
    with db:
        db.execute("UPDATE runs SET cleanup_complete=1 WHERE id=1")
    assert h._reserve(db) == 3
    assert h.schema_name(3) != h.schema_name(1)
    assert db.execute("SELECT cleanup_complete FROM runs WHERE id=2").fetchone() == (0,)


def stamped(rows, timestamp="100"):
    return [(*row, timestamp) for row in rows]


class Schedule:
    def __init__(self):
        self.responses = [
            stamped(PA),
            stamped(PA),
            stamped(PA[:2]),
            stamped(PB, "200"),
            stamped(PA[2:]),
            [],
            stamped(PB, "200"),
            stamped(PA, "200"),
            stamped(PB, "200"),
            stamped(PA, "200"),
            stamped(PB, "200"),
            stamped(PB, "200"),
            stamped(PA, "300"),
            stamped(PB, "300"),
            stamped(PA, "300"),
            stamped(PA, "300"),
        ]
        self.statements: list[str] = []
        self.post_commit = False
        self.return_dead_portal = False
        self.interrupt_at: str | None = None

    def execute(self, conn, deadline, statement, params=None):
        self.statements.append(statement)
        if self.interrupt_at and self.interrupt_at in statement:
            raise psycopg.OperationalError("connection lost")
        if statement.startswith("SELECT id::text FROM"):
            return Mock(fetchone=lambda: ("admin-session",))
        if statement == "COMMIT":
            self.post_commit = True
        if statement.startswith("FETCH") and self.post_commit:
            self.post_commit = False
            if not self.return_dead_portal:
                raise psycopg.errors.InvalidCursorName("cursor does not exist")
            return Mock(fetchall=list)
        if statement.startswith(("EXECUTE", "FETCH")):
            rows = self.responses.pop(0)
            return Mock(fetchall=lambda: rows)
        return Mock()


@pytest.fixture
def protocol(monkeypatch):
    schedule = Schedule()
    monkeypatch.setattr(h, "_execute", schedule.execute)
    monkeypatch.setattr(h, "_connect", lambda *args, **kwargs: nullcontext(Mock()))
    monkeypatch.setattr(h, "_cleanup", Mock())
    monkeypatch.setattr(h.ddl_hold, "active", lambda: False)
    monkeypatch.setattr(h, "always", Mock())
    monkeypatch.setattr(h, "sometimes", Mock())
    monkeypatch.setattr(h.telemetry, "comparison", Mock())
    return schedule


def run_protocol(db):
    return h.run_round(
        "host", db, inputs=(A, B), parameters=PARAMETERS, first_page=2, page_size=2
    )


def test_protocol_interleaves_acknowledged_replacement_with_unread_server_portal(
    db, protocol
):
    assert run_protocol(db) == 1
    assert not protocol.responses
    first_fetch = next(
        i for i, sql in enumerate(protocol.statements) if sql.startswith("FETCH 2")
    )
    replacement = next(
        i for i, sql in enumerate(protocol.statements) if "CREATE OR REPLACE" in sql
    )
    remaining_fetch = next(
        i
        for i, sql in enumerate(protocol.statements[first_fetch + 1 :], first_fetch + 1)
        if sql.startswith("FETCH 2")
    )
    assert first_fetch < replacement < remaining_fetch
    assert db.execute("SELECT admin_session,started,settled FROM runs").fetchone() == (
        "admin-session",
        1,
        1,
    )
    assert h.sometimes.call_count == 3
    assert all(call.args[0] for call in h.always.call_args_list)
    assert all(
        call.args[2]["portal_timestamp"] == "100" for call in h.sometimes.call_args_list
    )


@pytest.mark.parametrize(
    "position,replacement",
    [
        (3, stamped(PA, "200")),  # stale prepared dependency after acknowledged replace
        (4, stamped(PB[2:], "100")),  # cursor rebinds its unread suffix
        (4, stamped(PA[2:], "101")),  # cursor switches snapshots between FETCH pages
        (4, stamped(PA[2:-1], "100")),  # dropped row
        (4, stamped(PA[2:] + PA[2:], "100")),  # repeated row
        (5, stamped(PA, "100")),  # exhausted cursor re-executes
        (8, stamped(PA, "200")),  # another session's temp leaks into resolution
        (13, stamped(PA, "300")),  # durable replacement defeats peer's own shadow
        (14, stamped(PB, "300")),  # qualified prepared statement follows own temp
    ],
)
def test_wrong_result_mutants_never_credit_a_completed_round(
    db, protocol, position, replacement
):
    protocol.responses[position] = replacement
    with pytest.raises(h.Incomplete, match="violated"):
        run_protocol(db)
    assert any(not call.args[0] for call in h.always.call_args_list)
    h.sometimes.assert_not_called()
    assert db.execute("SELECT settled FROM runs").fetchone() == (0,)


def test_retained_portal_after_commit_is_a_product_failure_without_guidance(
    db, protocol
):
    protocol.return_dead_portal = True
    with pytest.raises(h.Incomplete, match="retained a portal"):
        run_protocol(db)
    assert any(not call.args[0] for call in h.always.call_args_list)
    h.sometimes.assert_not_called()


def test_lost_replace_response_retains_session_fenced_cleanup_and_no_oracle_credit(
    db, protocol
):
    protocol.interrupt_at = "CREATE OR REPLACE"
    with pytest.raises(psycopg.OperationalError):
        run_protocol(db)
    assert db.execute("SELECT admin_session,started,settled FROM runs").fetchone() == (
        "admin-session",
        1,
        0,
    )
    h.sometimes.assert_not_called()
    assert h._cleanup.call_count == 1


def test_cleanup_fences_live_admin_before_any_drop(monkeypatch, db):
    run = h._reserve(db)
    with db:
        db.execute(
            "UPDATE runs SET started=1,admin_session='writer' WHERE id=?", (run,)
        )
    monkeypatch.setattr(h, "_connect", lambda *args, **kwargs: nullcontext(Mock()))
    monkeypatch.setattr(h.ddl_hold, "active", lambda: False)
    execute = Mock(return_value=Mock(fetchone=lambda: (1,)))
    monkeypatch.setattr(h, "_execute", execute)
    h._cleanup("host", db, run, float("inf"))
    assert len(execute.call_args_list) == 1
    assert execute.call_args.args[-1] == ("writer",)
    assert db.execute("SELECT cleanup_complete FROM runs").fetchone() == (0,)
    execute.return_value.fetchone = lambda: None
    h._cleanup("host", db, run, float("inf"))
    assert any(
        "DROP SCHEMA IF EXISTS session_rebinding_r1 CASCADE" in call.args
        for call in execute.call_args_list
    )
    assert db.execute("SELECT cleanup_complete FROM runs").fetchone() == (1,)


def test_ddl_hold_prevents_reservation_and_sql(monkeypatch, db):
    monkeypatch.setattr(h.ddl_hold, "active", lambda: True)
    connect = Mock()
    monkeypatch.setattr(h, "_connect", connect)
    with pytest.raises(h.Incomplete, match="DDL is held"):
        run_protocol(db)
    connect.assert_not_called()
    assert db.execute("SELECT count(*) FROM runs").fetchone() == (0,)


@pytest.mark.parametrize("timestamp", [None, "not-a-timestamp", "-1"])
def test_invalid_snapshot_timestamp_cannot_pass(monkeypatch, timestamp):
    monkeypatch.setattr(h, "always", Mock())
    monkeypatch.setattr(h.telemetry, "comparison", Mock())
    with pytest.raises(h.Incomplete):
        h._check(stamped(PA, timestamp), PA, "portal", {})
