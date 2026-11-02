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
    monkeypatch.setattr(h, "_catalog_transactions", Mock())
    monkeypatch.setattr(h, "_catalog_ids", lambda *args: {"cancel_idx": "u-cancel"})
    monkeypatch.setattr(h.copy_recovery, "run_round", Mock())
    monkeypatch.setattr(h.cancel_recovery, "run", Mock())
    monkeypatch.setattr(h.ddl_hold, "active", lambda: False)
    monkeypatch.setattr(h, "always", Mock())
    monkeypatch.setattr(h, "sometimes", Mock())
    monkeypatch.setattr(h.telemetry, "comparison", Mock())
    return schedule


def run_protocol(db, *, large_copy=False):
    return h.run_round(
        "host",
        db,
        inputs=(A, B),
        parameters=PARAMETERS,
        first_page=2,
        page_size=2,
        large_copy=large_copy,
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
    assert h.sometimes.call_count == 6
    h._catalog_transactions.assert_called_once()
    assert all(call.args[0] for call in h.always.call_args_list)
    assert all(
        call.args[2]["portal_timestamp"] == "100" for call in h.sometimes.call_args_list
    )


@pytest.mark.parametrize("large_copy", [False, True])
def test_protocol_reuses_fenced_sessions_and_generated_inputs_for_recovery(
    db, protocol, large_copy
):
    run_protocol(db, large_copy=large_copy)
    copy = h.copy_recovery.run_round.call_args
    cancel = h.cancel_recovery.run.call_args
    assert len({id(connection) for connection in copy.args}) == 3
    assert cancel.args == (copy.args[1], copy.args[2], copy.args[0])
    assert copy.kwargs["rows"] == cancel.kwargs["expected_rows"] == A
    assert copy.kwargs["schema"] == "session_rebinding_r1"
    assert cancel.kwargs["table"] == "session_rebinding_r1.t0"
    assert cancel.kwargs["index_id"] == "u-cancel"
    assert copy.kwargs["abort_bytes"] == (
        h.copy_recovery.LARGE_ABORT_BYTES
        if large_copy
        else h.copy_recovery.DEFAULT_ABORT_BYTES
    )


@pytest.mark.parametrize("helper", ["copy", "cancel"])
def test_incomplete_recovery_cannot_credit_the_combined_round(db, protocol, helper):
    if helper == "copy":
        h.copy_recovery.run_round.side_effect = h.copy_recovery.Incomplete(
            "lost boundary"
        )
    else:
        h.cancel_recovery.run.side_effect = h.cancel_recovery.Incomplete(
            "lost boundary"
        )
    with pytest.raises((h.copy_recovery.Incomplete, h.cancel_recovery.Incomplete)):
        run_protocol(db)
    h.sometimes.assert_not_called()
    assert db.execute("SELECT settled FROM runs").fetchone() == (0,)
    assert h._cleanup.call_count == 1
    if helper == "copy":
        h.cancel_recovery.run.assert_not_called()


@pytest.mark.parametrize(
    "error", [h.copy_recovery.Incomplete, h.cancel_recovery.Incomplete]
)
def test_main_treats_missing_recovery_boundaries_as_uncredited_work(
    monkeypatch, tmp_path, db, protocol, error
):
    environment = Mock()
    environment.endpoints.state_dir = tmp_path
    environment.sql_host.return_value = "host"
    monkeypatch.setattr(h, "Environment", lambda: environment)
    monkeypatch.setattr(h, "exclusive", lambda _: nullcontext(True))
    monkeypatch.setattr(h, "open_state", lambda: db)
    monkeypatch.setattr(h.history, "start_watchdog", Mock())
    monkeypatch.setattr(h, "run_round", Mock(side_effect=error("incomplete recovery")))
    assert h.main() == 0
    assert h.telemetry.comparison.call_args.args == (h.FAMILY, "portal", None)
    h.sometimes.assert_not_called()


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


class CatalogSchedule:
    value = PARAMETERS.factor * 997 + PARAMETERS.offset

    def __init__(self):
        self.statements = []
        self.snapshots = []
        for ending in ("commit", "rollback"):
            prefix = f"txn_{ending}_"
            before = {prefix + "original": "u1", prefix + "dependent": "u2"}
            peer = {**before, prefix + "late": "u3", prefix + "unrelated": "u4"}
            after = dict(peer)
            if ending == "commit":
                after[prefix + "final"] = after.pop(prefix + "original")
            self.snapshots.extend([before, peer, after])
        conflict = {"txn_conflict_original": "u5", "txn_conflict_unrelated": "u6"}
        self.snapshots.extend([conflict, conflict])
        self.commit_count = 0
        self.conflict = True
        self.rows = None
        self.definition = None
        self.interrupt_at = None

    def execute(self, conn, deadline, statement, params=None):
        self.statements.append((conn, statement))
        if self.interrupt_at is not None and self.interrupt_at in statement:
            raise psycopg.OperationalError("connection lost")
        if statement.startswith("SELECT o.name,o.id"):
            snapshot = self.snapshots.pop(0)
            return Mock(fetchall=lambda: list(snapshot.items()))
        if statement.startswith("SELECT value FROM"):
            rows = self.rows
            if rows is None:
                rows = [(self.value + int(statement.endswith("unrelated")),)]
            return Mock(fetchall=lambda: rows)
        if statement.startswith("SELECT * FROM"):
            return Mock(fetchall=lambda: [(self.value, None)])
        if statement.startswith("SHOW CREATE VIEW"):
            target = "final" if "txn_commit_" in statement else "original"
            prefix = "txn_commit_" if "txn_commit_" in statement else "txn_rollback_"
            definition = (
                self.definition
                or f"CREATE VIEW s.{prefix}late AS SELECT value FROM s.{prefix}{target}"
            )
            return Mock(fetchone=lambda: ("late", definition))
        if statement.startswith("SHOW CREATE TABLE"):
            return Mock(
                fetchone=lambda: ("original", "CREATE TABLE original(value bigint)")
            )
        if statement == "COMMIT":
            self.commit_count += 1
            if self.commit_count == 2 and self.conflict:
                raise psycopg.errors.SerializationFailure("catalog changed")
        return Mock()


def test_catalog_identity_ignores_same_schema_and_object_in_another_database(
    monkeypatch,
):
    with sqlite3.connect(":memory:") as conn:
        conn.execute("ATTACH DATABASE ':memory:' AS mz_catalog")
        conn.executescript(
            "CREATE TABLE mz_catalog.mz_databases(id TEXT,name TEXT);"
            "CREATE TABLE mz_catalog.mz_schemas(id TEXT,database_id TEXT,name TEXT);"
            "CREATE TABLE mz_catalog.mz_objects(id TEXT,schema_id TEXT,name TEXT);"
            "INSERT INTO mz_catalog.mz_databases VALUES ('d1','materialize'),('d2','other');"
            "INSERT INTO mz_catalog.mz_schemas VALUES ('s1','d1','s'),('s2','d2','s');"
            "INSERT INTO mz_catalog.mz_objects VALUES"
            " ('u1','s1','txn_original'),('u2','s2','txn_original');"
        )
        conn.create_function("current_database", 0, lambda: "materialize")
        conn.create_function("left", 2, lambda text, count: text[:count])

        def execute(_conn, _deadline, statement, params):
            statement = statement.replace("%s", "?").replace("left(", '"left"(')
            return conn.execute(statement, params)

        monkeypatch.setattr(h, "_execute", execute)
        assert h._catalog_ids(Mock(), float("inf"), "s", "txn_") == {
            "txn_original": "u1"
        }


@pytest.fixture
def catalog(monkeypatch):
    schedule = CatalogSchedule()
    monkeypatch.setattr(h, "_execute", schedule.execute)
    monkeypatch.setattr(h.ddl_hold, "active", lambda: False)
    monkeypatch.setattr(h, "always", Mock())
    monkeypatch.setattr(h.telemetry, "comparison", Mock())
    return schedule


@pytest.mark.parametrize("descending", [False, True])
def test_catalog_protocol_places_acknowledged_peer_commits_inside_staged_transaction(
    catalog, descending
):
    txn, other = Mock(), Mock()
    parameters = model.Parameters(PARAMETERS.factor, PARAMETERS.offset, descending)
    h._catalog_transactions(txn, other, float("inf"), "s", parameters, {})
    assert not catalog.snapshots
    for ending in ("commit", "rollback"):
        operations = [
            (conn, statement)
            for conn, statement in catalog.statements
            if f"txn_{ending}_" in statement
            or statement in ("BEGIN", "COMMIT", "ROLLBACK")
        ]
        rename = next(
            i
            for i, (conn, text) in enumerate(operations)
            if conn is txn and "RENAME TO" in text and "original" in text
        )
        peer_create = next(
            i
            for i, (conn, text) in enumerate(operations)
            if conn is other and "CREATE VIEW" in text and "late" in text
        )
        finish = next(
            i
            for i, (conn, text) in enumerate(operations[rename + 1 :], rename + 1)
            if conn is txn and text == ending.upper()
        )
        assert rename < peer_create < finish
    assert all(call.args[0] for call in h.always.call_args_list)
    assert {call.args[1] for call in h.telemetry.comparison.call_args_list} == {
        "view",
        "table",
    }


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_peer",
        "changed_id",
        "partial_rollback",
        "lost_conflict_peer",
        "stale_binding",
        "wrong_rows",
        "conflict_committed",
    ],
)
def test_catalog_wrong_result_mutants_fail(catalog, mutation):
    if mutation == "missing_peer":
        catalog.snapshots[2].pop("txn_commit_late")
    elif mutation == "changed_id":
        catalog.snapshots[2]["txn_commit_final"] = "u99"
    elif mutation == "partial_rollback":
        catalog.snapshots[5]["txn_rollback_pending"] = catalog.snapshots[5].pop(
            "txn_rollback_original"
        )
    elif mutation == "lost_conflict_peer":
        catalog.snapshots[7] = {"txn_conflict_original": "u5"}
    elif mutation == "stale_binding":
        catalog.definition = (
            "CREATE VIEW s.txn_commit_late AS SELECT value FROM s.txn_commit_original"
        )
    elif mutation == "wrong_rows":
        catalog.rows = [(catalog.value + 100,)]
    elif mutation == "conflict_committed":
        catalog.conflict = False
    with pytest.raises(h.Incomplete, match="catalog transaction violated"):
        h._catalog_transactions(Mock(), Mock(), float("inf"), "s", PARAMETERS, {})
    assert any(not call.args[0] for call in h.always.call_args_list)


def test_unknown_catalog_peer_response_cannot_complete_round(db, protocol, monkeypatch):
    monkeypatch.setattr(
        h,
        "_catalog_transactions",
        Mock(side_effect=psycopg.OperationalError("connection lost")),
    )
    with pytest.raises(psycopg.OperationalError):
        run_protocol(db)
    h.sometimes.assert_not_called()
    assert db.execute("SELECT settled FROM runs").fetchone() == (0,)


def test_cleanup_waits_for_every_original_mutating_session(monkeypatch, db):
    run = h._reserve(db)
    with db:
        db.execute("UPDATE runs SET started=1,admin_session='a' WHERE id=?", (run,))
        db.executemany(
            "INSERT INTO run_sessions VALUES (?,?)", [(run, "a"), (run, "b")]
        )
    monkeypatch.setattr(h, "_connect", lambda *args, **kwargs: nullcontext(Mock()))
    monkeypatch.setattr(h.ddl_hold, "active", lambda: False)
    execute = Mock(
        side_effect=[Mock(fetchone=lambda: None), Mock(fetchone=lambda: (1,))]
    )
    monkeypatch.setattr(h, "_execute", execute)
    h._cleanup("host", db, run, float("inf"))
    assert [call.args[-1] for call in execute.call_args_list] == [("a",), ("b",)]
    assert db.execute("SELECT cleanup_complete FROM runs").fetchone() == (0,)


def test_rollback_reaches_server_without_set_in_failed_transaction():
    conn = Mock()
    h._execute(conn, h.time.monotonic() + 10, "ROLLBACK")
    conn.execute.assert_called_once_with(b"ROLLBACK", None)


def test_normal_statements_use_the_timeout_configured_at_connection_startup(
    monkeypatch,
):
    monkeypatch.setattr(h.time, "monotonic", lambda: 0)
    conn = Mock()
    h._execute(conn, 60, "SELECT 1")
    conn.execute.assert_called_once_with(b"SELECT 1", None)


def test_statement_timeout_is_shortened_before_the_round_deadline(monkeypatch):
    monkeypatch.setattr(h.time, "monotonic", lambda: 0)
    conn = Mock()
    h._execute(conn, 5, "SELECT 1")
    assert [call.args for call in conn.execute.call_args_list] == [
        (b"SET statement_timeout='5000ms'",),
        (b"SELECT 1", None),
    ]
