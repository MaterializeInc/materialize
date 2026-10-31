# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import bounded_subscribe as b


@pytest.mark.parametrize(
    "bad",
    [
        [(10, True, None, None, None)],
        [(10, True, None, None, None), (15, False, 2, 1, 7)],
        [(10, True, None, None, None), (15, False, 1, 1, 7), (20, False, 1, 2, 7)],
        [
            (10, True, None, None, None),
            (15, False, 1, 1, 7),
            (16, False, -1, 2, 7),
            (17, False, 1, 2, 7),
        ],
        [
            (10, True, None, None, None),
            (18, True, None, None, None),
            (15, False, 1, 1, 7),
        ],
        [
            (10, True, None, None, None),
            (9, True, None, None, None),
            (15, False, 1, 1, 7),
        ],
        [(15, False, 1, 1, 7)],
    ],
)
def test_oracle_rejects_tail_loss_duplicates_canaries_and_progress_errors(bad) -> None:
    assert b.validate_rows(bad, 10, 20, Counter({(1, 7): 1}))


def test_progress_may_overshoot_exclusive_data_bound() -> None:
    rows = [
        (10, True, None, None, None),
        (15, False, 1, 1, 7),
        (30, True, None, None, None),
    ]
    assert b.validate_rows(rows, 10, 20, Counter({(1, 7): 1})) == []
    assert b.validate_rows([(20, True, None, None, None)], 20, 20, Counter()) == []


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch) -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(b.state, "open_db", lambda _: db)
    return b.open_state()


def test_expected_input_excludes_canaries_and_unacknowledged_writes(journal) -> None:
    for phase, row, acknowledged in [
        ("seed", [0, 7], 1),
        ("tail", [1, 7], 1),
        ("canary", [2, 7], 1),
        ("tail", [3, 7], 0),
    ]:
        journal.execute(
            "INSERT INTO writes VALUES (1, ?, ?, ?)",
            (phase, json.dumps([row]), acknowledged),
        )
    assert b.expected_rows(journal, 1, True) == Counter({(0, 7): 1, (1, 7): 1})
    assert b.expected_rows(journal, 1, False) == Counter({(1, 7): 1})


@pytest.mark.parametrize("snapshot", [False, True])
@pytest.mark.parametrize("empty_at_max", [False, True])
def test_protocol_keeps_cold_barrier_and_canary_out_of_sparse_read(
    monkeypatch: pytest.MonkeyPatch, journal, snapshot: bool, empty_at_max: bool
) -> None:
    calls = []
    connections = []
    for name in ("writer", "hold", "barrier", "reader"):
        conn = Mock(name=name)
        conn.name = name
        conn.__enter__ = Mock(return_value=conn)
        conn.__exit__ = Mock(return_value=None)
        conn.transaction = lambda: nullcontext()
        connections.append(conn)
    monkeypatch.setattr(b, "_connect", Mock(side_effect=connections))
    monkeypatch.setattr(b.ddl_hold, "active", lambda: False)

    def execute(conn, deadline, text, params=None):
        calls.append((conn.name, text))
        return SimpleNamespace(fetchone=lambda: (100 if conn.name == "hold" else 200,))

    monkeypatch.setattr(b, "_execute", execute)
    checks = []

    def read(conn, lower, upper, snap, deadline, fetch_size, run):
        phases = [row[0] for row in journal.execute("SELECT phase FROM writes")]
        checks.append((lower, upper, phases))
        rows: list[tuple] = [(lower, True, None, None, None)]
        if lower < upper:
            if snap:
                rows.append((100, False, 1, 0, 1))
            rows.extend((150, False, 1, i, 1) for i in (1, 2))
        return rows

    monkeypatch.setattr(b, "read_window", read)
    assertions = Mock()
    coverage = Mock()
    monkeypatch.setattr(b, "always", assertions)
    monkeypatch.setattr(b, "sometimes", coverage)
    monkeypatch.setattr(b.telemetry, "comparison", Mock())
    assert (
        b.run_round(
            "host",
            journal,
            seed_count=1,
            tail_count=2,
            hot_count=2,
            snapshot=snapshot,
            fetch_size=1,
            empty_at_max=empty_at_max,
        )
        == 3
    )
    assert all(call.args[0] is True for call in assertions.call_args_list)
    assert all(call.args[0] is True for call in coverage.call_args_list)
    assert checks[0] == (100, 201, ["seed", "tail"])
    assert checks[1] == (100, 201, ["seed", "tail", "canary"])
    assert checks[2][:2] == (
        (b.MAX_TIMESTAMP, b.MAX_TIMESTAMP) if empty_at_max else (201, 201)
    )
    timestamp_reads = [(name, text) for name, text in calls if "mz_now()" in text]
    assert "cold" in timestamp_reads[0][1]
    assert timestamp_reads[1][0] == "barrier" and "hot" in timestamp_reads[1][1]
    assert "cold" not in timestamp_reads[1][1]


def test_interrupted_insert_is_never_acknowledged(monkeypatch, journal) -> None:
    execute = Mock(side_effect=psycopg.errors.QueryCanceled("cancelled"))
    monkeypatch.setattr(b, "_execute", execute)
    with pytest.raises(psycopg.errors.QueryCanceled):
        b._insert(Mock(), journal, 1, "tail", [(1, 7)], 10)
    assert b.expected_rows(journal, 1, True) == Counter()


def test_fetch_requires_eof_and_propagates_interruption(monkeypatch) -> None:
    conn = Mock()
    conn.transaction = lambda: nullcontext()
    statements = []

    def execute(conn, deadline, text, params=None):
        statements.append(text)
        if text.startswith("FETCH"):
            raise psycopg.errors.QueryCanceled("cancelled before EOF")
        return Mock()

    monkeypatch.setattr(b, "_execute", execute)
    with pytest.raises(psycopg.errors.QueryCanceled):
        b.read_window(conn, 10, 20, True, 30, 4, 1)
    assert statements[-1] == "FETCH 4 bounded_cursor"
    assert all("timeout" not in statement for statement in statements)


def test_progress_only_output_is_bounded_without_claiming_completion(
    monkeypatch,
) -> None:
    conn = Mock()
    conn.transaction = lambda: nullcontext()
    monkeypatch.setattr(b, "MAX_RESULT_ROWS", 1)
    response = SimpleNamespace(fetchall=lambda: [(10, True, None, None, None)] * 2)
    execute = Mock(return_value=response)
    monkeypatch.setattr(b, "_execute", execute)
    with pytest.raises(TimeoutError, match="capture budget"):
        b.read_window(conn, 10, 20, True, 30, 4, 1)
    assert execute.call_count == 2


def test_classified_interruption_is_inconclusive_without_positive_coverage(
    monkeypatch, tmp_path, journal
) -> None:
    monkeypatch.setattr(
        b,
        "Environment",
        lambda: SimpleNamespace(
            endpoints=SimpleNamespace(state_dir=tmp_path), sql_host=lambda: "host"
        ),
    )
    monkeypatch.setattr(b.history, "start_watchdog", Mock())
    monkeypatch.setattr(b, "exclusive", lambda _: nullcontext(True))
    monkeypatch.setattr(b, "open_state", lambda: journal)
    monkeypatch.setattr(
        b, "run_round", Mock(side_effect=psycopg.errors.QueryCanceled("interrupted"))
    )
    assertions, coverage, comparison = Mock(), Mock(), Mock()
    monkeypatch.setattr(b, "always", assertions)
    monkeypatch.setattr(b, "sometimes", coverage)
    monkeypatch.setattr(b.telemetry, "comparison", comparison)
    assert b.main() == 0
    comparison.assert_called_once_with("bounded_subscribe", "subscribe", None)
    assert assertions.call_args.args[0] is True
    coverage.assert_not_called()


@pytest.mark.parametrize("column", [0, 2, 3, 4])
def test_null_update_fields_are_product_errors_instead_of_parser_crashes(column):
    malformed = [15, False, 1, 1, 7]
    malformed[column] = None
    assert b.validate_rows(
        [(10, True, None, None, None), malformed], 10, 20, Counter({(1, 7): 1})
    )


def test_differential_encoding_is_checked_only_at_closed_timestamps():
    rows = [
        (10, True, 99, "ignored", "ignored"),
        (15, False, 2, 1, 7),
        (15, False, -1, 1, 7),
        (15, False, 1, 99, 7),
        (15, False, -1, 99, 7),
        (30, True, None, None, None),
    ]
    assert b.validate_rows(rows, 10, 20, Counter({(1, 7): 1})) == []


@pytest.fixture
def sql_tables():
    db = sqlite3.connect(":memory:")
    db.execute(f"ATTACH DATABASE ':memory:' AS {b.SCHEMA}")
    for name in ("cold", "hot"):
        db.execute(f"CREATE TABLE {b.SCHEMA}.{name} (k bigint,v bigint)")
    yield db
    db.close()


def test_late_cleanup_cannot_delete_a_newer_round(monkeypatch, sql_tables):
    queued = []
    monkeypatch.setattr(
        b,
        "_execute",
        lambda conn, deadline, query, params=None: queued.append((query, params)),
    )
    b._cleanup_old_rows(Mock(), 100, 2)
    for name in ("cold", "hot"):
        sql_tables.executemany(
            f"INSERT INTO {b.SCHEMA}.{name} VALUES (?,?)", [(1, 1), (2, 2), (3, 3)]
        )
    for query, params in queued:
        sql_tables.execute(query.replace("%s", "?"), params)
    for name in ("cold", "hot"):
        assert sql_tables.execute(
            f"SELECT k,v FROM {b.SCHEMA}.{name} ORDER BY v"
        ).fetchall() == [(2, 2), (3, 3)]


def test_late_old_insert_does_not_enter_the_new_round_subscription(
    monkeypatch, sql_tables
):
    conn = Mock()
    conn.transaction = lambda: nullcontext()
    monkeypatch.setattr(
        b,
        "_execute",
        lambda conn, deadline, query, params=None: sql_tables.execute(
            query.replace("%s", "?"), params or ()
        ),
    )
    sql_tables.execute(f"INSERT INTO {b.SCHEMA}.cold VALUES (1,2),(9,1)")
    b._cleanup_old_rows(conn, 100, 2)
    sql_tables.execute(f"INSERT INTO {b.SCHEMA}.cold VALUES (99,1)")
    batches = []

    def execute(conn, deadline, query, params=None):
        if query.startswith("DECLARE"):
            select = query.split("SUBSCRIBE (", 1)[1].split(") WITH", 1)[0]
            selected = sql_tables.execute(select).fetchall()
            batches.extend(
                [
                    [(10, True, None, None, None)]
                    + [(10, False, 1, *row) for row in selected],
                    [],
                ]
            )
        return SimpleNamespace(fetchall=lambda: batches.pop(0))

    monkeypatch.setattr(b, "_execute", execute)
    rows = b.read_window(conn, 10, 20, True, 30, 4, 2)
    assert b.validate_rows(rows, 10, 20, Counter({(1, 2): 1})) == []
    assert sql_tables.execute(
        f"SELECT k FROM {b.SCHEMA}.cold WHERE v=1"
    ).fetchall() == [(99,)]
