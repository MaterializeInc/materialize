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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import pg_checkpoint as c
from materialize.antithesis.drivers import postgres_sources as p
from materialize.antithesis.test_pg_checkpoint import database, record


@pytest.mark.parametrize(
    "corruption", [None, "aborted", "swapped_sequence", "wrong_kind"]
)
def test_ledger_rejects_corrupted_transaction_metadata(
    monkeypatch: pytest.MonkeyPatch, corruption: str | None
) -> None:
    with sqlite3.connect(":memory:") as db:
        db.execute(
            "CREATE TABLE txns (txn_id TEXT, slot INTEGER, lsn_before INTEGER, lsn_after INTEGER, status TEXT)"
        )
        db.execute("CREATE TABLE ops (txn_id TEXT, tbl TEXT, seq INTEGER, kind TEXT)")
        db.executemany(
            "INSERT INTO txns VALUES (?, 0, 0, NULL, ?)",
            [("a", "aborted" if corruption == "aborted" else "acked"), ("b", "acked")],
        )
        db.executemany(
            "INSERT INTO ops VALUES (?, 's0.t0', ?, 'dml')", [("a", 1), ("b", 2)]
        )
        rows = [("a", "s0.t0", 1, "dml"), ("b", "s0.t0", 2, "dml")]
        if corruption == "swapped_sequence":
            rows = [("a", "s0.t0", 2, "dml"), ("b", "s0.t0", 1, "dml")]
        elif corruption == "wrong_kind":
            rows[0] = ("a", "s0.t0", 1, "truncate")
        assertions: list[bool] = []
        monkeypatch.setattr(p, "always", lambda ok, *args: assertions.append(ok))
        monkeypatch.setattr(p, "sometimes", lambda *args: None)

        ledger = p._check_ledger(db, 0, 10, rows, {})
        assert (ledger is not None) == (corruption is None)
        assert all(assertions) == (corruption is None)


@pytest.mark.parametrize("problem", ["missing", "early"])
def test_ledger_cannot_checkpoint_a_missing_or_early_transaction(
    monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    db = database()
    rows = record(db, "a", {"s0.t0": (1, "dml", [])})
    if problem == "missing":
        rows = []
    else:
        db.execute("UPDATE txns SET lsn_before = 20")
    monkeypatch.setattr(p, "always", lambda *args: None)
    monkeypatch.setattr(p, "sometimes", lambda *args: None)
    assert p._check_ledger(db, 0, 10, rows, {}) is None


def test_retirement_retries_without_losing_checkpoint_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = database()
    rows = record(db, "a", {"s0.t0": (1, "dml", [["upsert", 1, "x"]])})
    cp = c.prepare(db, c.Checkpoint(0), 5, 10, rows, {})
    up = Mock()
    up.commit.side_effect = [p.psycopg.OperationalError("commit outcome unknown"), None]
    up.execute.return_value.fetchone.return_value = ("0/100",)
    p._retire_ledger(db, up, cp)
    restored = c.load(db, 0)
    assert restored.retiring == rows
    assert restored.delete_lsn is None
    assert restored.delete_before == 256
    assert restored.tables["s0.t0"].rows == {1: "x"}
    up.execute.return_value.fetchone.return_value = ("0/200",)
    p._retire_ledger(db, up, restored)
    assert c.load(db, 0).delete_lsn == 512
    assert c.load(db, 0).delete_before == 256
    assert up.commit.call_count == 2
    assert db.execute("SELECT count(*) FROM ops").fetchone()[0] == 0


def test_delete_intent_is_durable_before_any_upstream_mutation() -> None:
    db = database()
    rows = record(db, "a", {"s0.t0": (1, "dml", [])})
    cp = c.prepare(db, c.Checkpoint(0), 5, 10, rows, {})
    up = Mock()

    def execute(query: str | bytes, params: tuple = ()) -> SimpleNamespace:
        statement = query.decode() if isinstance(query, bytes) else query
        if statement.startswith("DELETE"):
            assert c.load(db, 0).delete_before == 256
            raise p.psycopg.OperationalError("response lost during deletion")
        return SimpleNamespace(fetchone=lambda: ("0/100",))

    up.execute.side_effect = execute
    p._retire_ledger(db, up, cp)
    assert c.load(db, 0).delete_before == 256
    assert c.load(db, 0).delete_lsn is None
    assert c.load(db, 0).retiring == rows


@pytest.mark.parametrize("resolved", ["acked", "aborted", "pending"])
def test_checkpoint_requires_an_independent_upstream_outcome(
    monkeypatch: pytest.MonkeyPatch, resolved: str
) -> None:
    db = database()
    rows = record(db, "a", {"s0.t0": (1, "dml", [])}, "pending")

    def fence(*args):
        with db:
            db.execute("UPDATE txns SET status = ? WHERE txn_id = 'a'", (resolved,))

    monkeypatch.setattr(p, "_resolve_transactions", fence)
    monkeypatch.setattr(p, "always", lambda *args: None)
    monkeypatch.setattr(p, "sometimes", lambda *args: None)
    assert p._checkpoint_outcomes_resolved(
        db, Mock(), 0, 10, rows, {}, c.Checkpoint(0)
    ) is (resolved == "acked")
    assert db.execute("SELECT count(*) FROM ops").fetchone()[0] == 1


def test_retained_history_cap_stops_new_writes_without_forgetting_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = database()
    record(db, "pending", {"s0.t0": (1, "dml", [])}, "pending")
    monkeypatch.setattr(p, "HISTORY_TXNS_CAP", 1)
    up = Mock()
    _, status = p.run_txn(up, db, 0, {"s0.t0": ("dml", [["upsert", 1, "x"]])})
    assert status == "deferred"
    up.execute.assert_not_called()
    assert db.execute("SELECT txn_id FROM txns").fetchall() == [("pending",)]


def test_admission_reserves_space_and_reclaims_only_dead_processes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = database()
    monkeypatch.setattr(p, "HISTORY_TXNS_CAP", 1)
    marker = p._process_marker(p.os.getpid())
    assert marker is not None
    assert p._claim_writer(db, 0, "preparing")
    assert not p._claim_writer(db, 0, "overflow")
    db.execute("UPDATE pg_writers SET incarnation = 'reused-pid'")
    db.commit()
    assert p._claim_writer(db, 0, "replacement")
    assert db.execute("SELECT txn_id FROM pg_writers").fetchall() == [("replacement",)]


def test_counter_lock_fence_resolves_unknown_commits_and_rollbacks() -> None:
    db = database()
    record(db, "committed", {"s0.t0": (1, "dml", [])}, "unknown")
    record(db, "rolled-back", {"s0.t1": (1, "truncate", [])}, "pending")
    tables = [p._tbl(0, j) for j in range(p.TABLES_PER_SLOT)] + [p._hb(0)]
    up = Mock()
    up.execute.side_effect = [
        SimpleNamespace(fetchall=lambda: [(tbl,) for tbl in tables]),
        SimpleNamespace(fetchall=lambda: [("committed",)]),
        SimpleNamespace(fetchone=lambda: ("0/100",)),
    ]
    p._resolve_transactions(db, up, 0)
    assert db.execute(
        "SELECT txn_id, status, lsn_after FROM txns ORDER BY txn_id"
    ).fetchall() == [("committed", "acked", 256), ("rolled-back", "aborted", None)]
    assert "FOR UPDATE" in up.execute.call_args_list[0].args[0]


def test_liveness_heartbeat_has_one_reserved_slot_under_backpressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = database()
    monkeypatch.setattr(p, "HISTORY_TXNS_CAP", 1)
    assert p._claim_writer(db, 0, "normal")
    assert not p._claim_writer(db, 0, "more-normal")
    assert p._claim_writer(db, 0, "heartbeat", heartbeat=True)
    assert not p._claim_writer(db, 0, "more-heartbeat", heartbeat=True)
    assert db.execute("SELECT count(*) FROM pg_writers").fetchone()[0] == 2


def test_retiring_history_does_not_allow_reused_transaction_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = database()
    monkeypatch.setattr(p.rng, "getrandbits", lambda _: 0)

    def run(up, state_db, slot, plan, txn):
        record(state_db, txn, {"s0.t0": (1, "dml", [])})
        return txn, "acked"

    monkeypatch.setattr(p, "_run_txn", run)
    first, _ = p.run_txn(Mock(), db, 0, {"s0.t0": ("dml", [])})
    c.prepare(db, c.Checkpoint(0), 5, 10, [(first, "s0.t0", 1, "dml")], {})
    assert not db.execute("SELECT 1 FROM txns").fetchall()
    second, _ = p.run_txn(Mock(), db, 0, {"s0.t0": ("dml", [])})
    assert first != second


def test_concurrent_admission_never_exceeds_the_history_cap(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "state.sqlite"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE txns (txn_id TEXT PRIMARY KEY, slot INTEGER)")
    c.ensure_tables(db)
    db.close()
    monkeypatch.setattr(p, "HISTORY_TXNS_CAP", 2)

    def claim(i: int) -> bool:
        conn = sqlite3.connect(path, timeout=10)
        try:
            return p._claim_writer(conn, 0, str(i))
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        admitted = list(pool.map(claim, range(8)))
    assert sum(admitted) == 2


def test_checkpoint_keeps_terminal_attribution_after_payloads_retire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = database()
    db.execute(
        "CREATE TABLE exports (name TEXT, slot INTEGER, j INTEGER, s1 INTEGER, s3 INTEGER, state TEXT, created_epoch REAL)"
    )
    db.execute(
        "CREATE INDEX exports_current ON exports (slot, created_epoch) WHERE state IN ('creating','live','ambiguous')"
    )
    db.execute("INSERT INTO exports VALUES ('pg_s0_t0_e0', 0, 0, 0, 0, 'live', 0)")
    db.commit()
    rows = record(db, "terminal", {"s0.t0": (1, "truncate", [])})
    c.prepare(db, c.Checkpoint(0), 5, 10, rows, {})
    assert p._terminal_in(db, "s0.t0", 0, None) == 1
    assert db.execute(p.TERMINAL_EXPORTS_QUERY).fetchone()[0] == 1
    monkeypatch.setattr(p, "restart_events_since", lambda *args: 1)
    assert (
        p.terminal_before_restart(
            db, Mock(), "s0.t0", 0, 0, "pg_s0_t0_e0", "export", "src"
        )
        == "truncate"
    )


@pytest.mark.parametrize("corrupt", [False, True])
def test_checkpointed_reference_still_detects_export_row_corruption(
    monkeypatch: pytest.MonkeyPatch, corrupt: bool
) -> None:
    db = database()
    ledger = record(db, "a", {"s0.t0": (1, "dml", [["upsert", 1, "expected"]])})
    cp = c.prepare(db, c.Checkpoint(0), 5, 10, ledger, {})
    cp.retiring = []
    assert not db.execute("SELECT 1 FROM ops").fetchall()
    conn = Mock()
    conn.execute.side_effect = [
        SimpleNamespace(fetchall=lambda: [(1, "corrupt" if corrupt else "expected")]),
        SimpleNamespace(fetchall=lambda: []),
    ]
    monkeypatch.setattr(p, "terminal_before_restart", lambda *args: None)
    monkeypatch.setattr(p, "_check_terminal_permanence", lambda *args: None)
    monkeypatch.setattr(p.remap_frontiers, "seen_past_upper", lambda *args: False)
    checked = []
    monkeypatch.setattr(
        p, "always", lambda ok, message, details: checked.append((ok, message))
    )
    monkeypatch.setattr(p, "sometimes", lambda *args: None)
    e = p.ExportRow("pg_s0_t0_e0", 0, 0, 0, 1, "live", 0)
    p._check_export(db, conn, Mock(), e, "export", "src", 10, p._Ledger({}, {}, cp), {})
    comparisons = [
        ok for ok, message in checked if "equals its upstream table" in message
    ]
    assert comparisons == [not corrupt]


@pytest.mark.parametrize("cold_rows", [100, 10000])
def test_live_queries_and_export_allocation_do_not_scan_cold_history(
    monkeypatch: pytest.MonkeyPatch, cold_rows: int
) -> None:
    db = database()
    db.executescript("""
        CREATE TABLE exports (name TEXT PRIMARY KEY, slot INTEGER, j INTEGER,
            s1 INTEGER, s3 INTEGER, state TEXT, created_epoch REAL);
        CREATE INDEX exports_current ON exports (slot, created_epoch)
            WHERE state IN ('creating','live','ambiguous');
        CREATE INDEX exports_slot_created ON exports (slot, created_epoch);
    """)
    db.executemany(
        "INSERT INTO exports VALUES (?, 0, 0, 0, 0, 'dropped', 0)",
        [(f"pg_s0_t0_e{i}",) for i in range(cold_rows)],
    )
    db.execute("INSERT INTO exports VALUES ('active', 0, 0, 0, 0, 'live', 0)")
    db.executemany(
        "INSERT INTO terminal_archive VALUES (?, 's0.t0', ?, 'truncate', ?, ?)",
        [
            (str(i), i, "aborted" if i == 1 else "acked", float(cold_rows - i))
            for i in range(1, cold_rows + 1)
        ],
    )
    db.commit()
    c.prepare(
        db,
        c.Checkpoint(0, tables={"s0.t0": c.Table(cold_rows)}),
        5,
        10,
        [],
        {},
        {"s0.t0": {0}},
    )
    first_id = p._next_export_id(db, 0, 0)
    assert first_id == cold_rows
    steps = [0]

    def budget() -> int:
        steps[0] += 1
        return int(steps[0] > 1000)

    db.set_progress_handler(budget, 1)
    assert [e.name for e in p._exports(db, 0)] == ["active"]
    assert p._terminal_in(db, "s0.t0", 0, None) == 1
    assert p._terminal_in(db, "s0.t0", 0, None, include_aborted=False) == 2
    monkeypatch.setattr(p, "restart_events_since", lambda *args: 1)
    assert (
        p.terminal_before_restart(db, Mock(), "s0.t0", 0, 0, "active", "export", "src")
        == "truncate"
    )
    assert p._next_export_id(db, 0, 0) == cold_rows + 1
    assert steps[0] <= 1000


def _exports_table(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE exports (name TEXT PRIMARY KEY, slot INTEGER, j INTEGER,
            s1 INTEGER, s3 INTEGER, state TEXT, created_epoch REAL);
        CREATE INDEX exports_current ON exports (slot, created_epoch)
            WHERE state IN ('creating','live','ambiguous');
    """)


@pytest.mark.parametrize("existing_state", ["live", "creating"])
def test_failed_export_cleanup_cannot_admit_more_exports(
    monkeypatch: pytest.MonkeyPatch, existing_state: str
) -> None:
    db = database()
    _exports_table(db)
    db.executemany(
        "INSERT INTO exports VALUES (?, 0, 0, 0, 0, ?, 0)",
        [(f"pg_s0_t0_e{i}", existing_state) for i in range(p.MAX_EXPORTS_PER_TABLE)],
    )
    db.commit()
    monkeypatch.setattr(p.ddl_hold, "active", lambda: False)
    drop = Mock(return_value=False)
    monkeypatch.setattr(p, "_drop_export", drop)
    counter = Mock(side_effect=AssertionError("full export slots must stop creation"))
    monkeypatch.setattr(p, "_counter", counter)
    p._create_export(Mock(), "host", db, Mock(), 0, 0)
    assert len(p._exports(db, 0)) == p.MAX_EXPORTS_PER_TABLE
    counter.assert_not_called()


def test_export_reservations_are_bounded_under_concurrent_creators(
    tmp_path: Path,
) -> None:
    path = tmp_path / "exports.sqlite"
    db = sqlite3.connect(path)
    c.ensure_tables(db)
    _exports_table(db)
    db.close()

    def reserve(i: int) -> str | None:
        conn = sqlite3.connect(path, timeout=10)
        try:
            return p._reserve_export(conn, 0, 0, 10)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        names = [name for name in pool.map(reserve, range(8)) if name is not None]
    assert len(names) == len(set(names)) == p.MAX_EXPORTS_PER_TABLE


def test_extra_export_replaces_an_old_export_before_reserving_another(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = database()
    _exports_table(db)
    db.executemany(
        "INSERT INTO exports VALUES (?, 0, 0, 0, 0, 'live', 0)",
        [(f"pg_s0_t0_e{i}",) for i in range(p.MAX_EXPORTS_PER_TABLE)],
    )
    db.commit()
    monkeypatch.setattr(p.ddl_hold, "active", lambda: False)

    def drop(host, state_db, export):
        with state_db:
            state_db.execute(
                "UPDATE exports SET state = 'dropped' WHERE name = ?", (export.name,)
            )
        return True

    monkeypatch.setattr(p, "_drop_export", drop)
    monkeypatch.setattr(p, "_counter", lambda *args: 10)
    conn = Mock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    conn.execute.side_effect = p.psycopg.OperationalError("create response lost")
    monkeypatch.setattr(p, "mz_connect", lambda *args: conn)
    p._create_export(Mock(), "host", db, Mock(), 0, 0)
    active = p._exports(db, 0)
    assert len(active) == p.MAX_EXPORTS_PER_TABLE
    assert any(e.state == "creating" and e.s1 == 10 for e in active)


@pytest.mark.parametrize("trigger", ["periodic", "checkpoint"])
def test_full_ledger_sweep_finds_corruption_below_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    trigger: str,
) -> None:
    db = database()
    db.execute(
        "CREATE TABLE exports (name TEXT, slot INTEGER, j INTEGER, s1 INTEGER, s3 INTEGER, state TEXT, created_epoch REAL)"
    )
    db.execute(
        "CREATE INDEX exports_current ON exports (slot, created_epoch) WHERE state IN ('creating','live','ambiguous')"
    )
    record(db, "new", {"s0.t0": (2, "dml", [])})
    cp = c.Checkpoint(0, tables={"s0.t0": c.Table(1)})
    with db:
        c.save(db, cp)
    monkeypatch.setattr(
        p, "lookup_ids", lambda *args: {"pg_src_0": "src", "pg_ledger_0": "ledger"}
    )
    monkeypatch.setattr(p, "observe", lambda *args: None)
    monkeypatch.setattr(
        p, "frontiers", lambda *args: {"src": (0, 200), "ledger": (0, 200)}
    )
    monkeypatch.setattr(p, "pick_as_of", lambda *args: 100)
    monkeypatch.setattr(p, "record_progress_observation", lambda *args: [])
    monkeypatch.setattr(p.remap_frontiers, "seen_past_upper", lambda *args: False)
    checked = []
    monkeypatch.setattr(
        p, "always", lambda ok, message, details: checked.append((ok, message))
    )
    monkeypatch.setattr(p, "sometimes", lambda *args: None)
    queries = []

    def execute(query: bytes, params: tuple = ()) -> SimpleNamespace:
        statement = query.decode()
        queries.append(statement)
        if "SELECT lsn" in statement:
            rows = [(10,)]
        elif "count(*)" in statement:
            rows = []
        else:
            rows = [("new", "s0.t0", 2, "dml")]
            if "WHERE" not in statement:
                rows.append(("phantom-old", "s0.t0", 1, "dml"))
        return SimpleNamespace(fetchall=lambda: rows)

    conn = Mock(execute=execute)
    p._check(db, conn, Mock(), 0, p.Deadline(10))
    assert all(ok for ok, _ in checked)
    assert any("WHERE NOT" in q for q in queries)
    cp = c.load(db, 0)
    if trigger == "checkpoint":
        monkeypatch.setattr(p, "CHECKPOINT_TXNS", 1)
    else:
        cp.rounds = p.FULL_LEDGER_EVERY - 1
    with db:
        c.save(db, cp)
    checked.clear()
    p._check(db, conn, Mock(), 0, p.Deadline(10))
    assert [message for ok, message in checked if not ok] == [
        "postgres source: checkpointed ledger rows change only by the acknowledged atomic retirement"
    ]
    assert c.load(db, 0).rounds == cp.rounds
