# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import sqlite3
from collections import Counter
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import window_retractions_model as m
from materialize.antithesis.drivers import window_retractions as w


@pytest.fixture
def db(monkeypatch):
    connection = sqlite3.connect(":memory:")
    monkeypatch.setattr(w.state, "open_db", lambda _: connection)
    w.open_state()
    yield connection
    connection.close()


def assert_product(condition, message, details):
    assert condition, (message, details)


@pytest.mark.parametrize("path", ["table", "index", "mv", "recompute", "subscribe"])
def test_wrong_result_control_records_product_failure(monkeypatch, path):
    monkeypatch.setattr(w, "always", assert_product)
    monkeypatch.setattr(w.telemetry, "comparison", Mock())
    with pytest.raises(AssertionError):
        w.check_rows([(1, 7)], Counter({(1, 8): 1}), path)


def test_false_sdk_return_still_stops_the_round(monkeypatch):
    assertion = Mock()
    monkeypatch.setattr(w, "always", assertion)
    monkeypatch.setattr(w.telemetry, "comparison", Mock())
    with pytest.raises(w.Incomplete):
        w.check_rows([], Counter({(1, 8): 1}), "index")
    assert assertion.call_args.args[0] is False


def test_journal_intent_precedes_sql_and_unknown_commit_aborts_oracle(monkeypatch, db):
    run = w.reserve(db)
    conn = Mock()
    conn.transaction = lambda: nullcontext()

    def execute(_conn, _deadline, _query, _params=None):
        assert db.execute("SELECT acknowledged FROM writes").fetchall() == [(0,)]
        raise psycopg.OperationalError("commit response lost")

    monkeypatch.setattr(w, "_execute", execute)
    with pytest.raises(psycopg.OperationalError):
        w._write(conn, db, run, [m.Mutation(1, (1, 0, 1, 7, 1, 9))], 100)
    with pytest.raises(w.Incomplete, match="unknown"):
        w.expected_rows(db, run)


def test_acknowledged_journal_replays_replacements_and_deletes(monkeypatch, db):
    run = w.reserve(db)
    conn = Mock()
    conn.transaction = lambda: nullcontext()
    monkeypatch.setattr(w, "_execute", Mock())
    w._write(
        conn,
        db,
        run,
        [
            m.Mutation(1, (1, 0, 1, 7, 1, 9)),
            m.Mutation(2, (2, None, None, None, -1, 10)),
        ],
        100,
    )
    w._write(
        conn, db, run, [m.Mutation(1, (1, None, 0, 4, 2, 9)), m.Mutation(2, None)], 100
    )
    assert w.expected_rows(db, run) == ((1, None, 0, 4, 2, 9),)


def test_cleanup_waits_original_writer_and_late_drop_names_only_old_epoch(
    monkeypatch, db
):
    first = w.reserve(db)
    second = w.reserve(db)
    db.execute(
        "UPDATE runs SET started=1,writer_session=? WHERE id=?", ("writer-first", first)
    )
    db.commit()
    monkeypatch.setattr(w.ddl_hold, "active", lambda: False)
    conn = Mock()
    monkeypatch.setattr(w, "_connect", lambda *_args, **_kwargs: nullcontext(conn))
    active = True
    calls = []

    def execute(_conn, _deadline, query, params=None):
        calls.append((query, params))
        return SimpleNamespace(fetchone=lambda: (1,) if active else None)

    monkeypatch.setattr(w, "_execute", execute)
    assert not w._cleanup("host", db, first, 1e20)
    assert all("DROP SCHEMA" not in query for query, _ in calls)
    active = False
    assert w._cleanup("host", db, first, 1e20)
    assert calls[-1][0] == f"DROP SCHEMA IF EXISTS {w.schema_name(first)} CASCADE"
    assert w.schema_name(second) not in calls[-1][0]
    third = w.reserve(db)
    assert first < second < third


def test_cleanup_backpressure_survives_interrupted_drop(monkeypatch, db):
    first = w.reserve(db)
    w.reserve(db)
    with pytest.raises(w.Incomplete, match="backpressure"):
        w.reserve(db)
    db.execute("UPDATE runs SET started=1,settled=1 WHERE id=?", (first,))
    db.commit()
    monkeypatch.setattr(w.ddl_hold, "active", lambda: False)
    monkeypatch.setattr(w, "_connect", lambda *_args, **_kwargs: nullcontext(Mock()))
    monkeypatch.setattr(
        w, "_execute", Mock(side_effect=psycopg.OperationalError("response lost"))
    )
    monkeypatch.setattr(w, "always", assert_product)
    assert not w._cleanup("host", db, first, 1e20)
    with pytest.raises(w.Incomplete, match="backpressure"):
        w.reserve(db)


def test_protocol_checks_each_acknowledged_state_and_requires_real_import_for_witness(
    monkeypatch, db
):
    plan = m.generate(random.Random(4))
    writer, reader, subscriber, meta = [
        Mock(name=name) for name in ("writer", "reader", "subscriber", "meta")
    ]
    subscriber.transaction = lambda: nullcontext()
    writer.transaction = lambda: nullcontext()
    connections = iter([writer, reader, subscriber, meta])
    monkeypatch.setattr(
        w, "_connect", lambda *_args, **_kwargs: nullcontext(next(connections))
    )
    monkeypatch.setattr(w.ddl_hold, "active", lambda: False)
    monkeypatch.setattr(
        w,
        "_session",
        lambda conn, deadline: "writer" if conn is writer else "subscriber",
    )
    statements = []
    monkeypatch.setattr(
        w,
        "_execute",
        lambda conn, deadline, text, params=None: statements.append((conn, text)),
    )
    monkeypatch.setattr(w, "_physical_import", lambda *_args: False)
    monkeypatch.setattr(w, "_drain", lambda *_args: None)
    states = []
    monkeypatch.setattr(
        w,
        "_checkpoint",
        lambda _reader, _sub, _stream, _db, run, _plan, _deadline, _phase: states.append(
            w.expected_rows(db, run)
        ),
    )
    monkeypatch.setattr(w, "_cleanup", lambda *_args: True)
    witnesses = Mock()
    monkeypatch.setattr(w, "sometimes", witnesses)
    monkeypatch.setattr(w.telemetry, "comparison", Mock())
    assert w.run_round("host", db, plan) == 1
    assert len(states) == 6
    assert states[0] == states[-1] == plan.seed
    assert not any(row[1] is None for row in states[3])
    assert any(row[1] is None for row in states[4])
    physical = [
        call
        for call in witnesses.call_args_list
        if "physical subscriber" in call.args[1]
    ]
    assert physical[0].args[0] is False
    assert any("CREATE INDEX" in text for _, text in statements)
    assert any("CREATE MATERIALIZED VIEW" in text for _, text in statements)


def test_recompute_uses_only_typed_journal_values():
    query = m.query(
        w.recompute_relation([(1, None, 2, 3, -1, 9)]), m.Spec(False, True, -2, -1)
    )
    assert "(VALUES (1::bigint,NULL::int,2::int,3::int,-1::int,9::int))" in query
    assert "window_retractions" not in query
    assert "WHERE FALSE" in w.recompute_relation([])


def test_writes_use_autocommit_updates_and_deletes_outside_transaction_blocks(
    monkeypatch, db
):
    run = w.reserve(db)
    conn = Mock()
    conn.transaction.side_effect = AssertionError(
        "Materialize rejects DELETE in an explicit transaction"
    )
    queries = []
    monkeypatch.setattr(
        w,
        "_execute",
        lambda _conn, _deadline, query, params=None: queries.append(query),
    )
    w._write(conn, db, run, [m.Mutation(1, (1, 0, 1, 7, 1, 9))], 100)
    w._write(conn, db, run, [m.Mutation(1, (1, None, -1, None, 2, 9))], 100)
    w._write(conn, db, run, [m.Mutation(1, None)], 100)
    assert [query.split()[0] for query in queries] == ["INSERT", "UPDATE", "DELETE"]
    assert w.expected_rows(db, run) == ()
