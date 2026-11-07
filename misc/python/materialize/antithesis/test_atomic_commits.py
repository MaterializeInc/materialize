# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sqlite3
from contextlib import contextmanager
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import atomic_commits as a


@pytest.fixture
def ledger(monkeypatch):
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(a.state, "open_db", lambda *args: db)
    a.open_state()
    with db:
        db.execute(
            "INSERT INTO plans (shards,value,outcome) VALUES ('[0,1]',7,'indeterminate')"
        )
    monkeypatch.setattr(a.telemetry, "comparison", Mock())
    monkeypatch.setattr(a, "sometimes", Mock())
    monkeypatch.setattr(a, "always", lambda ok, *args: ok or pytest.fail(str(args)))
    yield db
    db.close()


def test_acknowledgement_after_read_start_is_not_a_snapshot_obligation(
    ledger, monkeypatch
):
    def execute(*args):
        with ledger:
            ledger.execute("UPDATE plans SET outcome='ok'")
        return Mock(fetchall=lambda: [])

    monkeypatch.setattr(a, "_execute", execute)
    assert a.check(ledger, Mock(), 100)


def test_snapshot_overlapping_the_held_writer_deadline_is_not_forced_empty(
    ledger, monkeypatch
):
    held = [True]

    def execute(*args):
        held[0] = False
        return Mock(fetchall=lambda: [(0, 1, 7), (1, 1, 7)])

    monkeypatch.setattr(a, "_execute", execute)
    assert a.check(ledger, Mock(), 100, uncommitted=1, still_held=lambda: held[0])
    assert ledger.execute("SELECT outcome FROM plans").fetchone() == ("observed",)


def test_still_held_transaction_cannot_be_visible(ledger, monkeypatch):
    monkeypatch.setattr(
        a, "_execute", Mock(return_value=Mock(fetchall=lambda: [(0, 1, 7), (1, 1, 7)]))
    )
    with pytest.raises(pytest.fail.Exception, match="uncommitted"):
        a.check(ledger, Mock(), 100, uncommitted=1, still_held=lambda: True)


def test_rejection_during_a_read_cannot_hide_visible_commit_evidence(
    ledger, monkeypatch
):
    def execute(*args):
        with ledger:
            ledger.execute("UPDATE plans SET outcome='rejected',finalized=1")
        return Mock(fetchall=lambda: [(0, 1, 7), (1, 1, 7)])

    monkeypatch.setattr(a, "_execute", execute)
    assertions = Mock()
    coverage = Mock()
    monkeypatch.setattr(a, "always", assertions)
    monkeypatch.setattr(a, "sometimes", coverage)
    assert not a.check(ledger, Mock(), 100)
    assert any(call.args[0] is False for call in assertions.call_args_list)
    coverage.assert_not_called()


@pytest.mark.parametrize("finalized", [0, 1])
def test_cleanup_keeps_unknown_commit_evidence_and_applies_backpressure(
    ledger, monkeypatch, finalized
):
    with ledger:
        ledger.execute("UPDATE plans SET finalized=?", (finalized,))
    monkeypatch.setattr(a, "MAX_PLANS", 1)
    execute = Mock()
    monkeypatch.setattr(a, "_execute", execute)
    assert not a._prune(ledger, Mock(), 100)
    execute.assert_not_called()
    assert ledger.execute("SELECT retired FROM plans").fetchone() == (0,)


def test_interrupted_cleanup_cannot_erase_future_markers(ledger, monkeypatch):
    monkeypatch.setattr(a, "MAX_PLANS", 1)
    with ledger:
        ledger.execute("UPDATE plans SET outcome='ok',finalized=1")
    calls = []

    def execute(conn, deadline, query, params):
        calls.append((query, params))
        assert "WHERE id = ANY(%s::bigint[])" in query and params == ([1],)
        raise OSError("lost cleanup response")

    monkeypatch.setattr(a, "_execute", execute)
    with pytest.raises(OSError):
        a._prune(ledger, Mock(), 100)
    assert len(calls) == 1
    assert ledger.execute("SELECT retired FROM plans").fetchone() == (1,)
    with ledger:
        ledger.execute(
            "INSERT INTO plans (shards,value,outcome) VALUES ('[0,2]',9,'ok')"
        )
    assert ledger.execute("SELECT max(id) FROM plans").fetchone() == (2,)


def test_replica_discovery_failure_does_not_invent_an_acknowledged_fault(monkeypatch):
    monkeypatch.setattr(a.telemetry, "resolve_cluster_id", lambda *args: "u2")
    monkeypatch.setattr(a.rollouts, "Kube", Mock())
    monkeypatch.setattr(a.quiet, "active_quiet_period", lambda: False)
    monkeypatch.setattr(
        a.pod_restarts,
        "pick_victim",
        Mock(side_effect=a.pod_restarts.ApiException(status=503)),
    )
    acknowledged = [False]
    action = a._fault(Mock(), "host", 100, acknowledged)
    assert action is not None
    action.run(lambda: True, Mock())
    assert acknowledged == [False]


@pytest.mark.parametrize(
    "observed,error,expected,contradiction",
    [
        (True, psycopg.OperationalError("COMMIT response lost"), "observed", False),
        (
            True,
            psycopg.errors.SerializationFailure("COMMIT rejected"),
            "observed",
            True,
        ),
        (
            False,
            psycopg.OperationalError("COMMIT response lost"),
            "indeterminate",
            False,
        ),
        (
            False,
            psycopg.errors.SerializationFailure("COMMIT rejected"),
            "rejected",
            False,
        ),
    ],
)
@pytest.mark.parametrize("cleanup_before_response", [False, True])
def test_late_commit_response_cannot_erase_an_observed_transaction(
    monkeypatch,
    tmp_path,
    observed,
    error,
    expected,
    contradiction,
    cleanup_before_response,
):
    database = tmp_path / "atomic.sqlite"
    monkeypatch.setattr(a.state, "open_db", lambda *args: sqlite3.connect(database))
    journal = a.open_state()
    with journal:
        marker = journal.execute(
            "INSERT INTO plans (shards,value,outcome) VALUES ('[0,1]',7,'indeterminate')"
        ).lastrowid
    assert marker is not None

    @contextmanager
    def transaction():
        yield
        if observed:
            # Another snapshot sees the committed rows before the writer gets
            # its response and records the statement's local outcome.
            with journal:
                journal.execute(
                    "UPDATE plans SET outcome='observed' WHERE id=?", (marker,)
                )
        if cleanup_before_response:
            with journal:
                journal.execute(
                    "INSERT INTO plans (shards,value,outcome,finalized) VALUES ('[0,2]',9,'ok',1)"
                )
            monkeypatch.setattr(a, "MAX_PLANS", 2)
            assert a._prune(journal, Mock(), 100)
            assert journal.execute(
                "SELECT retired FROM plans WHERE id=?", (marker,)
            ).fetchone() == (0,)
        raise error

    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    connection.transaction = transaction
    monkeypatch.setattr(a, "_connect", lambda host: connection)
    monkeypatch.setattr(a, "_execute", Mock())
    assertions = Mock()
    monkeypatch.setattr(a, "always", assertions)
    a.write("host", marker, 100, False, Mock())
    assert journal.execute(
        "SELECT outcome FROM plans WHERE id=?", (marker,)
    ).fetchone() == (expected,)
    assert (
        any(call.args[0] is False for call in assertions.call_args_list)
        is contradiction
    )
    assert journal.execute(
        "SELECT finalized FROM plans WHERE id=?", (marker,)
    ).fetchone() == (1,)
    journal.close()


def test_resolved_cleanup_reclaims_capacity_without_retiring_unknowns(
    ledger, monkeypatch
):
    with ledger:
        ledger.execute(
            "INSERT INTO plans (shards,value,outcome,finalized) VALUES ('[0,2]',9,'ok',1)"
        )
        ledger.execute(
            "INSERT INTO plans (shards,value,outcome,finalized) VALUES ('[1,2]',11,'observed',1)"
        )
        ledger.execute(
            "INSERT INTO plans (shards,value,outcome,finalized) VALUES ('[0,1]',13,'rejected',1)"
        )
    monkeypatch.setattr(a, "MAX_PLANS", 4)
    execute = Mock()
    monkeypatch.setattr(a, "_execute", execute)
    assert a._prune(ledger, Mock(), 100)
    assert ledger.execute("SELECT id,outcome,retired FROM plans").fetchall() == [
        (1, "indeterminate", 0)
    ]
    assert execute.call_count == a.TABLES
    assert all(call.args[3] == ([2, 3, 4],) for call in execute.call_args_list)


def test_delayed_resolved_cleanup_preserves_unknown_and_future_transactions(
    ledger, monkeypatch
):
    with ledger:
        ledger.execute(
            "INSERT INTO plans (shards,value,outcome,finalized) VALUES ('[0,2]',9,'observed',1)"
        )
    monkeypatch.setattr(a, "MAX_PLANS", 2)
    sut = sqlite3.connect(":memory:")
    for shard in range(a.TABLES):
        sut.execute(f"CREATE TABLE t{shard} (id bigint,v bigint)")
    for shard in (0, 1):
        sut.execute(f"INSERT INTO t{shard} VALUES (1,7)")
    for shard in (0, 2):
        sut.execute(f"INSERT INTO t{shard} VALUES (2,9)")
    delayed = []

    def execute(conn, deadline, query, params):
        delayed.append((query, params))
        raise OSError("cleanup response lost")

    monkeypatch.setattr(a, "_execute", execute)
    with pytest.raises(OSError):
        a._prune(ledger, Mock(), 100)
    with ledger:
        ledger.execute(
            "INSERT INTO plans (shards,value,outcome,finalized) VALUES ('[0,1]',17,'ok',1)"
        )
    for shard in (0, 1):
        sut.execute(f"INSERT INTO t{shard} VALUES (3,17)")
    for query, (markers,) in delayed:
        marks = ",".join("?" for _ in markers)
        translated = query.replace(f"{a.SCHEMA}.", "").replace(
            "= ANY(%s::bigint[])", f"IN ({marks})"
        )
        sut.execute(translated, markers)
    assert ledger.execute(
        "SELECT id,outcome,retired FROM plans ORDER BY id"
    ).fetchall() == [(1, "indeterminate", 0), (2, "observed", 1), (3, "ok", 0)]
    rows = [
        (shard, marker, value)
        for shard in range(a.TABLES)
        for marker, value in sut.execute(f"SELECT id,v FROM t{shard}")
    ]
    assert not a.violations(a.load_plans(ledger), rows)
    assert all(
        sut.execute(f"SELECT id FROM t{shard} WHERE id IN (1,3) ORDER BY id").fetchall()
        == [(1,), (3,)]
        for shard in (0, 1)
    )
    sut.close()


@pytest.fixture
def rollback_writer(monkeypatch, tmp_path):
    database = tmp_path / "rollback.sqlite"
    monkeypatch.setattr(a.state, "open_db", lambda *args: sqlite3.connect(database))
    journal = a.open_state()
    with journal:
        marker = journal.execute(
            "INSERT INTO plans (shards,value,outcome) VALUES ('[0,1]',7,'indeterminate')"
        ).lastrowid
    assert marker is not None
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(a, "_connect", lambda host: connection)
    monkeypatch.setattr(a, "_execute", Mock())
    monkeypatch.setattr(a, "always", Mock())
    yield journal, marker, connection
    journal.close()


@pytest.mark.parametrize("acknowledged", [False, True])
def test_requested_rollback_lost_connection_retains_uncertainty(
    rollback_writer, monkeypatch, acknowledged
):
    journal, marker, connection = rollback_writer

    def execute(conn, deadline, query, params=None):
        if query == "ROLLBACK" and not acknowledged:
            raise psycopg.OperationalError("connection lost during ROLLBACK")
        return Mock()

    monkeypatch.setattr(a, "_execute", execute)
    a.write("host", marker, 100, True, Mock())
    assert journal.execute(
        "SELECT outcome,finalized FROM plans WHERE id=?", (marker,)
    ).fetchone() == ("rejected" if acknowledged else "indeterminate", 1)


@pytest.mark.parametrize("rollback", [False, True])
def test_escaped_rollback_control_exception_still_propagates(rollback_writer, rollback):
    journal, marker, connection = rollback_writer

    @contextmanager
    def transaction():
        yield

    connection.transaction = transaction
    unexpected = psycopg.Rollback()
    with pytest.raises(psycopg.Rollback) as raised:
        a.write("host", marker, 100, rollback, Mock(side_effect=unexpected))
    assert raised.value is unexpected
    assert journal.execute(
        "SELECT outcome,finalized FROM plans WHERE id=?", (marker,)
    ).fetchone() == ("indeterminate", 0)


@pytest.mark.parametrize(
    "error,violation",
    [
        (psycopg.errors.InternalError_("unexpected ROLLBACK failure"), True),
        (psycopg.OperationalError("connection lost during ROLLBACK"), False),
        (psycopg.errors.QueryCanceled("ROLLBACK interrupted"), False),
    ],
)
def test_rollback_sql_errors_reach_the_product_classifier(
    rollback_writer, monkeypatch, error, violation
):
    journal, marker, connection = rollback_writer

    def execute(conn, deadline, query, params=None):
        if query == "ROLLBACK":
            raise error
        return Mock()

    @contextmanager
    def transaction():
        try:
            yield
        except psycopg.Rollback:
            # psycopg preserves its original control exception and logs any
            # SQL failure from its own rollback attempt.
            try:
                execute(connection, 100, "ROLLBACK")
            except psycopg.Error:
                pass
            raise

    connection.transaction = transaction
    monkeypatch.setattr(a, "_execute", execute)
    assertions = Mock()
    monkeypatch.setattr(a, "always", assertions)
    a.write("host", marker, 100, True, Mock())
    assert any(call.args[0] is False for call in assertions.call_args_list) is violation
    assert journal.execute(
        "SELECT outcome,finalized FROM plans WHERE id=?", (marker,)
    ).fetchone() == ("indeterminate", 1)
