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
import random
import sqlite3

import pytest

from materialize.antithesis import pg_checkpoint as c


def database() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE txns (
            txn_id TEXT PRIMARY KEY, slot INTEGER, lsn_before INTEGER,
            lsn_after INTEGER, status TEXT, started_epoch REAL
        );
        CREATE TABLE ops (
            txn_id TEXT, tbl TEXT, seq INTEGER, kind TEXT, ops TEXT,
            PRIMARY KEY (txn_id, tbl)
        );
    """)
    c.ensure_tables(db)
    return db


def record(
    db: sqlite3.Connection, txn: str, entries: dict[str, c.Entry], status: str = "acked"
) -> list[c.LedgerRow]:
    with db:
        db.execute("INSERT INTO txns VALUES (?, 0, 1, 2, ?, 1)", (txn, status))
        db.executemany(
            "INSERT INTO ops VALUES (?, ?, ?, ?, ?)",
            [
                (txn, tbl, seq, kind, json.dumps(ops))
                for tbl, (seq, kind, ops) in entries.items()
            ],
        )
    return [(txn, tbl, seq, kind) for tbl, (seq, kind, _) in entries.items()]


def test_checkpoints_equal_full_replay_across_disruptions_and_multiple_exports() -> (
    None
):
    db = database()
    cp = c.Checkpoint(0)
    full: dict[str, list[c.Entry]] = {"s0.t0": [], "s0.t1": []}
    draw = random.Random(948)
    for generation in range(12):
        ledger: list[c.LedgerRow] = []
        plans = {}
        by_tbl: dict[str, list[tuple[int, str, str]]] = {}
        for i in range(20):
            txn = f"g{generation}-{i}"
            plan = {}
            for tbl in draw.sample(list(full), draw.choice([1, 2])):
                seq = len(full[tbl]) + 1
                kind = draw.choice(["dml", "dml", "truncate", "drop", "add_column"])
                ops = (
                    []
                    if kind != "dml"
                    else [
                        draw.choice(
                            [
                                [
                                    "upsert",
                                    draw.randrange(-2, 4),
                                    draw.choice([None, "", "ü€", "x"]),
                                ],
                                ["update", draw.randrange(-2, 4), "updated"],
                                ["delete", draw.randrange(-2, 4)],
                                ["delete_range", -1, 2],
                            ]
                        )
                    ]
                )
                full[tbl].append((seq, kind, ops))
                plan[tbl] = (seq, kind, ops)
                plans[(txn, tbl)] = ops
                by_tbl.setdefault(tbl, []).append((seq, txn, kind))
            ledger.extend(record(db, txn, plan))
        refs = {}
        for tbl in full:
            # One old export can remain terminal while a later snapshot is live.
            for name, s3 in [
                (tbl + "-old", 0),
                (tbl + f"-g{generation}", len(full[tbl])),
            ]:
                ref = c.reference(cp, name, tbl, s3, by_tbl.get(tbl, []), plans)
                first = next(
                    ((s, k) for s, k, _ in full[tbl] if s > s3 and k in c.TERMINAL),
                    None,
                )
                expected = c.replay(
                    [
                        entry
                        for entry in full[tbl]
                        if first is None or entry[0] < first[0]
                    ]
                )
                assert ref.rows == expected
                assert ref.terminal == first
                refs[name] = ref
        cp = c.prepare(db, cp, generation + 1, generation + 10, ledger, refs)
        cp = c.load(db, 0)
        assert all(
            cp.tables[tbl].rows == c.replay(entries) for tbl, entries in full.items()
        )
        assert db.execute("SELECT count(*) FROM ops").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM txns").fetchone()[0] == 0
        assert c.partition(cp, ledger, cp.lsn, True) == ([], [], False)
        cp.delete_before, cp.delete_lsn = cp.lsn, cp.lsn + 1
        assert c.partition(cp, [], cp.lsn + 2, True) == ([], [], True)
        cp.retiring = []
        cp.delete_before = cp.delete_lsn = None
        with db:
            c.save(db, cp)


def test_checkpoint_retains_unresolved_and_resolves_reused_counter_sequences() -> None:
    db = database()
    visible = record(
        db,
        "committed-with-lost-ack",
        {"s0.t0": (1, "dml", [["upsert", 1, "x"]]), "s0.t1": (1, "dml", [])},
        "pending",
    )
    record(db, "waiting", {"s0.t0": (2, "dml", [["upsert", 2, "later"]])}, "unknown")
    record(db, "rolled-back", {"s0.t0": (1, "truncate", [])}, "unknown")
    record(db, "aborted", {"s0.t1": (2, "drop", [])}, "aborted")
    with pytest.raises(ValueError, match="upstream outcome is unresolved"):
        c.prepare(db, c.Checkpoint(0), 5, 10, visible, {})
    with db:
        db.execute(
            "UPDATE txns SET status = 'acked' WHERE txn_id = 'committed-with-lost-ack'"
        )
    cp = c.prepare(db, c.Checkpoint(0), 5, 10, visible, {})
    assert cp.tables["s0.t0"].rows == {1: "x"}
    assert db.execute("SELECT txn_id FROM txns").fetchall() == [("waiting",)]
    assert db.execute(
        "SELECT txn_id, status FROM terminal_archive ORDER BY txn_id"
    ).fetchall() == [("aborted", "aborted"), ("rolled-back", "aborted")]


def test_live_terminal_summaries_fold_exact_minima_and_stay_bounded() -> None:
    db = database()
    db.executemany(
        "INSERT INTO terminal_archive VALUES (?, 's0.t0', ?, 'truncate', 'acked', ?)",
        [(f"old-{i}", i, float(10000 - i)) for i in range(1, 10001)],
    )
    db.commit()
    cp = c.Checkpoint(0, tables={"s0.t0": c.Table(10000)})
    cp = c.prepare(db, cp, 1, 1, [], {}, {"s0.t0": {0, 10000}})
    assert c.terminal_summary(db, "s0.t0", 0) == (1, 1, 0.0, None)
    rows = record(db, "next", {"s0.t0": (10001, "drop", [])})
    with db:
        db.execute("UPDATE txns SET started_epoch = -5 WHERE txn_id = 'next'")
    cp = c.prepare(db, cp, 2, 2, rows, {}, {"s0.t0": {0, 10000}})
    assert c.terminal_summary(db, "s0.t0", 0) == (1, 1, 0.0, -5.0)
    assert c.terminal_summary(db, "s0.t0", 10000) == (10001, 10001, None, -5.0)
    cp.retiring = []
    c.prepare(db, cp, 3, 3, [], {}, {"s0.t0": {10001}})
    assert db.execute("SELECT count(*) FROM pg_terminal_summaries").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM terminal_archive").fetchone()[0] == 10001


def test_checkpoint_and_payload_retirement_are_atomic() -> None:
    db = database()
    ledger = record(db, "a", {"s0.t0": (1, "dml", [["upsert", 1, "x"]])})
    db.execute(
        "CREATE TRIGGER interrupted BEFORE DELETE ON ops BEGIN SELECT RAISE(ABORT, 'interrupted'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="interrupted"):
        c.prepare(db, c.Checkpoint(0), 5, 10, ledger, {})
    assert not c.load(db, 0).tables
    assert db.execute("SELECT count(*) FROM ops").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM txns").fetchone()[0] == 1


@pytest.mark.parametrize(
    "corruption", ["changed", "partial", "duplicate", "extra", "late_delete"]
)
def test_full_sweeps_detect_retiring_prefix_corruption(corruption: str) -> None:
    rows = [("a", "s0.t0", 1, "dml"), ("b", "s0.t0", 2, "dml")]
    cp = c.Checkpoint(0, tables={"s0.t0": c.Table(2)}, retiring=list(rows))
    f = 5
    if corruption == "changed":
        rows[0] = ("a", "s0.t0", 1, "truncate")
    elif corruption == "partial":
        rows.pop()
    elif corruption == "duplicate":
        rows.append(rows[0])
    elif corruption == "extra":
        rows.append(("phantom", "s0.t0", 1, "dml"))
    else:
        cp.delete_lsn = 4
    _, errors, retired = c.partition(cp, rows, f, True)
    assert errors
    assert not retired


def test_a_completed_retirement_does_not_allow_old_rows_to_reappear() -> None:
    cp = c.Checkpoint(0, tables={"s0.t0": c.Table(1)})
    _, errors, _ = c.partition(cp, [("old", "s0.t0", 1, "dml")], 5, True)
    assert errors


def test_pending_retirement_survives_a_workload_restart() -> None:
    db = database()
    ledger = record(db, "a", {"s0.t0": (1, "dml", [["upsert", 1, None]])})
    c.prepare(db, c.Checkpoint(0), 5, 10, ledger, {})
    restored = c.load(db, 0)
    assert restored.retiring == ledger
    assert restored.tables["s0.t0"].rows == {1: None}
    assert c.partition(restored, ledger, 10, True) == ([], [], False)
    assert c.partition(restored, [], 11, True)[1]
    restored.delete_before, restored.delete_lsn = 11, 12
    with db:
        c.save(db, restored)
    assert c.partition(c.load(db, 0), [], 13, True) == ([], [], True)


@pytest.mark.parametrize(
    "before,after,frontier,invalid,retired",
    [
        (None, None, 100, True, False),
        (10, 20, 9, True, False),
        (10, 20, 10, True, False),
        (10, None, 100, False, False),
        (10, 20, 11, False, False),
        (10, 20, 20, False, False),
        (10, 20, 21, False, True),
    ],
)
def test_retirement_requires_a_recorded_intent_and_a_confirmed_lsn(
    before: int | None, after: int | None, frontier: int, invalid: bool, retired: bool
) -> None:
    cp = c.Checkpoint(
        0,
        tables={"s0.t0": c.Table(1)},
        retiring=[("a", "s0.t0", 1, "dml")],
        delete_before=before,
        delete_lsn=after,
    )
    _, errors, confirmed = c.partition(cp, [], frontier, True)
    assert bool(errors) is invalid
    assert confirmed is retired
