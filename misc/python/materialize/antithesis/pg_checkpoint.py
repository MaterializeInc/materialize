# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Independent Postgres reference checkpoints and bounded ledger retirement.

A checkpoint advances only across a ledger prefix already validated against
the workload's transaction records. Its contents come from replaying those
records, never from adopting a Materialize query result. Checkpointed commits
are confirmed independently upstream. The exact retiring batch survives until
a full ledger read passes an acknowledged deletion's upper LSN.
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

LedgerRow = tuple[str, str, int, str]
Entry = tuple[int, str, list[list[Any]]]
Rows = dict[int, str | None]
TERMINAL = ("truncate", "drop")


def replay(entries: list[Entry], initial: Rows | None = None) -> Rows:
    rows = dict(initial or {})
    for _, kind, ops in sorted(entries, key=lambda e: e[0]):
        if kind in TERMINAL:
            rows.clear()
            continue
        for op in ops:
            if op[0] == "upsert":
                rows[op[1]] = op[2]
            elif op[0] == "update":
                if op[1] in rows:
                    rows[op[1]] = op[2]
            elif op[0] == "delete":
                rows.pop(op[1], None)
            elif op[0] == "delete_range":
                for k in [k for k in rows if op[1] <= k < op[2]]:
                    del rows[k]
    return rows


@dataclass
class Table:
    seq: int = 0
    rows: Rows = field(default_factory=dict)


@dataclass
class Export:
    seq: int
    rows: Rows
    terminal: tuple[int, str] | None = None
    added_column: bool = False


@dataclass
class Checkpoint:
    slot: int
    as_of: int = 0
    lsn: int = 0
    tables: dict[str, Table] = field(default_factory=dict)
    exports: dict[str, Export] = field(default_factory=dict)
    retiring: list[LedgerRow] = field(default_factory=list)
    delete_lsn: int | None = None
    """Post-commit upper LSN, absent while the deletion outcome is unknown."""
    delete_before: int | None = None
    """Earliest pre-delete lower LSN, persisted before any upstream mutation."""
    rounds: int = 0

    def floor(self, tbl: str) -> int:
        return self.tables.get(tbl, Table()).seq


def ensure_tables(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE IF NOT EXISTS pg_checkpoints (
            slot INTEGER PRIMARY KEY, body TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS terminal_archive (
            txn_id TEXT NOT NULL, tbl TEXT NOT NULL, seq INTEGER NOT NULL,
            kind TEXT NOT NULL, status TEXT NOT NULL, started_epoch REAL,
            PRIMARY KEY (txn_id, tbl)
        );
        CREATE INDEX IF NOT EXISTS terminal_archive_tbl_seq
            ON terminal_archive (tbl, seq, kind);
        CREATE INDEX IF NOT EXISTS terminal_archive_status_seq
            ON terminal_archive (tbl, status, seq);
        CREATE INDEX IF NOT EXISTS terminal_archive_live_seq
            ON terminal_archive (tbl, seq) WHERE status != 'aborted';
        CREATE TABLE IF NOT EXISTS pg_terminal_summaries (
            tbl TEXT NOT NULL, lo INTEGER NOT NULL, first_any INTEGER,
            first_live INTEGER, truncate_epoch REAL, drop_epoch REAL,
            PRIMARY KEY (tbl, lo)
        );
        CREATE TABLE IF NOT EXISTS pg_export_ids (
            slot INTEGER NOT NULL, j INTEGER NOT NULL, next_id INTEGER NOT NULL,
            PRIMARY KEY (slot, j)
        );
        CREATE TABLE IF NOT EXISTS pg_history_summary (
            tbl TEXT PRIMARY KEY, last_acked_dml_seq INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pg_writers (
            txn_id TEXT PRIMARY KEY, slot INTEGER NOT NULL,
            pid INTEGER NOT NULL, incarnation TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS pg_writers_slot ON pg_writers (slot);
        CREATE TABLE IF NOT EXISTS pg_txn_sequence (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 0),
            next_id INTEGER NOT NULL
        );
        INSERT OR IGNORE INTO pg_txn_sequence VALUES (0, 0);
    """)


def encode(cp: Checkpoint) -> str:
    return json.dumps(
        {
            "slot": cp.slot,
            "as_of": cp.as_of,
            "lsn": cp.lsn,
            "tables": {
                k: [v.seq, sorted(v.rows.items())] for k, v in cp.tables.items()
            },
            "exports": {
                k: [v.seq, sorted(v.rows.items()), v.terminal, v.added_column]
                for k, v in cp.exports.items()
            },
            "retiring": cp.retiring,
            "delete_lsn": cp.delete_lsn,
            "delete_before": cp.delete_before,
            "rounds": cp.rounds,
        }
    )


def load(db: sqlite3.Connection, slot: int) -> Checkpoint:
    row = db.execute(
        "SELECT body FROM pg_checkpoints WHERE slot = ?", (slot,)
    ).fetchone()
    if row is None:
        return Checkpoint(slot)
    doc = json.loads(row[0])
    return Checkpoint(
        slot=slot,
        as_of=int(doc["as_of"]),
        lsn=int(doc["lsn"]),
        tables={k: Table(int(v[0]), dict(v[1])) for k, v in doc["tables"].items()},
        exports={
            k: Export(
                int(v[0]), dict(v[1]), None if v[2] is None else tuple(v[2]), bool(v[3])
            )
            for k, v in doc["exports"].items()
        },
        retiring=[tuple(v) for v in doc["retiring"]],
        delete_lsn=doc["delete_lsn"],
        delete_before=doc.get("delete_before"),
        rounds=int(doc["rounds"]),
    )


def save(db: sqlite3.Connection, cp: Checkpoint) -> None:
    db.execute(
        "INSERT INTO pg_checkpoints VALUES (?, ?)"
        " ON CONFLICT(slot) DO UPDATE SET body = excluded.body",
        (cp.slot, encode(cp)),
    )


def partition(
    cp: Checkpoint, rows: list[LedgerRow], f: int, full: bool
) -> tuple[list[LedgerRow], list[str], bool]:
    """Return the suffix, prefix errors, and whether retirement was observed."""
    prefix = [r for r in rows if r[2] <= cp.floor(r[1])]
    suffix = [r for r in rows if r[2] > cp.floor(r[1])]
    errors: list[str] = []
    if not full:
        if prefix:
            errors.append("incremental read returned rows below its checkpoint")
        return suffix, errors, False
    if prefix and Counter(prefix) != Counter(cp.retiring):
        errors.append("retiring ledger rows changed or were only partially deleted")
    if prefix and cp.delete_lsn is not None and cp.delete_lsn < f:
        errors.append("acknowledged ledger deletion is missing below the mapped LSN")
    if cp.retiring and not prefix:
        if cp.delete_before is None:
            errors.append("ledger rows disappeared without a recorded deletion intent")
        elif f <= cp.delete_before:
            errors.append("ledger rows disappeared before their deletion could commit")
    retired = (
        bool(cp.retiring)
        and not prefix
        and not errors
        and cp.delete_lsn is not None
        and cp.delete_lsn < f
    )
    return suffix, errors, retired


def reference(
    cp: Checkpoint,
    name: str,
    tbl: str,
    s3: int,
    entries: list[tuple[int, str, str]],
    plans: dict[tuple[str, str], list[list[Any]]],
) -> Export:
    """Reference contents and the first terminal op after an export's snapshot."""
    prior = cp.exports.get(name)
    if prior is not None and prior.terminal is not None:
        return prior
    table = cp.tables.get(tbl, Table())
    base_seq = prior.seq if prior is not None else table.seq
    initial = prior.rows if prior is not None else table.rows
    terminal = next(((s, k) for s, _, k in entries if s > s3 and k in TERMINAL), None)
    kept = [
        (s, k, plans[(txn, tbl)])
        for s, txn, k in entries
        if s > base_seq and (terminal is None or s < terminal[0])
    ]
    last = max([base_seq, *(s for s, _, _ in kept)])
    return Export(
        last,
        replay(kept, initial),
        terminal,
        bool(prior and prior.added_column)
        or any(s > s3 and k == "add_column" for s, _, k in entries),
    )


def prepare(
    db: sqlite3.Connection,
    prior: Checkpoint,
    as_of: int,
    lsn: int,
    ledger: list[LedgerRow],
    exports: dict[str, Export],
    bounds: dict[str, set[int]] | None = None,
) -> Checkpoint:
    """Atomically checkpoint an independently replayed prefix and retire resolved plans."""
    if prior.retiring:
        raise ValueError("a ledger batch is already retiring")
    by_tbl: dict[str, list[Entry]] = {}
    visible = {r[0] for r in ledger}
    for txn, tbl, seq, kind in ledger:
        row = db.execute(
            "SELECT o.ops, t.status FROM ops o JOIN txns t ON o.txn_id = t.txn_id WHERE o.txn_id = ? AND o.tbl = ?",
            (txn, tbl),
        ).fetchone()
        if row is None:
            raise ValueError("a visible ledger row has no workload plan")
        if row[1] != "acked":
            raise ValueError("a visible transaction's upstream outcome is unresolved")
        by_tbl.setdefault(tbl, []).append((seq, kind, json.loads(row[0])))
    tables = dict(prior.tables)
    for tbl, entries in by_tbl.items():
        old = tables.get(tbl, Table())
        tables[tbl] = Table(max(s for s, _, _ in entries), replay(entries, old.rows))
    cp = Checkpoint(
        prior.slot, as_of, lsn, tables, exports, ledger, rounds=prior.rounds
    )
    with db:
        save(db, cp)
        terminal_batch: list[tuple[str, int, str, str, float | None]] = []
        txns = list(
            db.execute("SELECT txn_id, status FROM txns WHERE slot = ?", (cp.slot,))
        )
        for txn, status in txns:
            ops = list(db.execute("SELECT tbl, seq FROM ops WHERE txn_id = ?", (txn,)))
            # A successor holding a reused sequence proves the old transaction
            # rolled back. An unresolved transaction ahead of every prefix stays.
            excluded = (
                any(seq <= cp.floor(tbl) for tbl, seq in ops) and txn not in visible
            )
            if txn not in visible and status != "aborted" and not excluded:
                continue
            archived_status = "aborted" if excluded else status
            terminal_batch.extend(
                (tbl, int(seq), str(kind), str(archived_status), epoch)
                for tbl, seq, kind, epoch in db.execute(
                    "SELECT o.tbl, o.seq, o.kind, t.started_epoch"
                    " FROM ops o JOIN txns t ON o.txn_id = t.txn_id"
                    " WHERE o.txn_id = ? AND o.kind IN ('truncate', 'drop')",
                    (txn,),
                )
            )
            db.execute(
                "INSERT OR IGNORE INTO terminal_archive"
                " SELECT o.txn_id, o.tbl, o.seq, o.kind, ?, t.started_epoch"
                " FROM ops o JOIN txns t ON o.txn_id = t.txn_id"
                " WHERE o.txn_id = ? AND o.kind IN ('truncate', 'drop')",
                (archived_status, txn),
            )
            if status == "acked":
                for tbl, seq in db.execute(
                    "SELECT tbl, seq FROM ops WHERE txn_id = ? AND kind = 'dml'", (txn,)
                ):
                    db.execute(
                        "INSERT INTO pg_history_summary VALUES (?, ?)"
                        " ON CONFLICT(tbl) DO UPDATE SET last_acked_dml_seq = max(last_acked_dml_seq, excluded.last_acked_dml_seq)",
                        (tbl, seq),
                    )
            db.execute("DELETE FROM ops WHERE txn_id = ?", (txn,))
            db.execute("DELETE FROM txns WHERE txn_id = ?", (txn,))
        _summarize_terminals(db, bounds or {}, terminal_batch)
    return cp


def terminal_summary(db: sqlite3.Connection, tbl: str, lo: int) -> tuple | None:
    """The archived prefix's first terminal facts for one live export bound."""
    return db.execute(
        "SELECT first_any, first_live, truncate_epoch, drop_epoch"
        " FROM pg_terminal_summaries WHERE tbl = ? AND lo = ?",
        (tbl, lo),
    ).fetchone()


def _summarize_terminals(
    db: sqlite3.Connection,
    bounds: dict[str, set[int]],
    batch: list[tuple[str, int, str, str, float | None]],
) -> None:
    wanted = {(tbl, lo) for tbl, values in bounds.items() for lo in values}
    for key in list(db.execute("SELECT tbl, lo FROM pg_terminal_summaries")):
        if key not in wanted:
            db.execute(
                "DELETE FROM pg_terminal_summaries WHERE tbl = ? AND lo = ?", key
            )
    for tbl, lo in sorted(wanted):
        prior = terminal_summary(db, tbl, lo)
        if prior is None:
            # New exports bind at the current upstream sequence, so normal
            # creation has no archived suffix to scan. Existing state bootstraps once.
            prior = db.execute(
                "SELECT min(seq), min(CASE WHEN status != 'aborted' THEN seq END),"
                " min(CASE WHEN status != 'aborted' AND kind = 'truncate' THEN started_epoch END),"
                " min(CASE WHEN status != 'aborted' AND kind = 'drop' THEN started_epoch END)"
                " FROM terminal_archive WHERE tbl = ? AND seq > ?",
                (tbl, lo),
            ).fetchone()
            assert prior is not None
        values = list(prior)
        for name, seq, kind, status, epoch in batch:
            if name != tbl or seq <= lo:
                continue
            candidates = [
                seq,
                seq if status != "aborted" else None,
                epoch if kind == "truncate" and status != "aborted" else None,
                epoch if kind == "drop" and status != "aborted" else None,
            ]
            for i, value in enumerate(candidates):
                if value is not None:
                    values[i] = value if values[i] is None else min(values[i], value)
        db.execute(
            "INSERT INTO pg_terminal_summaries VALUES (?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(tbl, lo) DO UPDATE SET first_any = excluded.first_any,"
            " first_live = excluded.first_live, truncate_epoch = excluded.truncate_epoch,"
            " drop_epoch = excluded.drop_epoch",
            (tbl, lo, *values),
        )
