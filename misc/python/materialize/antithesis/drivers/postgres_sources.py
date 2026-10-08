# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Postgres CDC sources checked against a ledger of upstream transactions at the mapped LSN.

Properties: `source-matches-upstream-at-mapped-position` (A3, Postgres part),
`terminal-source-errors-are-permanent` (SQL-observable part),
`source-shards-respect-declared-key-uniqueness` (SQL `GROUP BY` fallback), and
anchors (1), (2), and (6) of `source-restarts-reach-dangerous-mid-states`.

Upstream layout per slot `i`: schema `s{i}` with data tables `t{j} (k int
PRIMARY KEY, v text)` and a `ledger`, all in publication `antithesis_pub_{i}`
(`FOR TABLES IN SCHEMA`, so a re-created table joins it). Materialize has one
source `pg_src_{i}` per slot, a table `pg_ledger_{i}` for the ledger, and one
or more exports `pg_s{i}_t{j}_e{n}` per upstream table. `first_configure`
creates all of it through `setup_main`; the driver and the check repair
whatever is missing, retrying within a bounded budget.

Every upstream transaction, including the disruptive ones (TRUNCATE, DROP and
re-CREATE, ADD COLUMN), first bumps a per-table row in
`antithesis_meta.counters`, which is outside the publication. The row lock
serializes all transactions on a table, so the returned `seq` is the table's
commit order, and committed seqs are gap-free (a rolled-back bump is reused).
Each transaction inserts `(txn_id, tbl, seq, kind)` into the ledger and is
recorded in the state database, with its operations, before COMMIT.

The oracle at a sampled mz time T with progress LSN F:

- The ledger rows visible in `pg_ledger_{i}` `AS OF T` are exactly the
  transactions Materialize claims are below F. Per table they must be a
  gap-free prefix of seq order, a transaction's rows appear together, and they
  must agree with the LSNs bracketing each transaction: committed with
  `pg_current_wal_lsn()` after COMMIT below F means visible, and
  `pg_current_wal_insert_lsn()` at the start of the transaction above F means
  not visible.
- Replaying the visible transactions of a table in seq order gives the
  expected contents of each export of it, and whether the export must be
  errored.

An export binds to the upstream table as of its creation. `s1` is the table's
committed seq read before `CREATE TABLE .. FROM SOURCE`, and `s3` the seq read
after the export's snapshot was first seen committed. A terminal operation
with seq in `(s1, s3]` may land on either side of the snapshot, so such an
export is marked ambiguous and only its error permanence is checked. For the
rest, the first terminal operation after `s3` is the one that kills it:
a visible TRUNCATE must error it, a visible DROP may (the schema validator
reports a drop asynchronously), and nothing else may.

An errored export's ok updates are invisible to SQL, so "no ok data after the
terminal error" is checked through `updates_committed` in
`mz_internal.mz_source_statistics`, a never-resetting counter.

Every per-export oracle has two messages, chosen by `terminal_before_restart`:
whether a TRUNCATE or DROP of the table was followed by an ingestion restart.
A known unfixed bug class lives entirely in the first bucket, so the second
stays a clean signal for other mechanisms. Timelines where `first_configure`
set `pg_terminal_upstream_ops` to `off` issue no terminal ops at all.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    sometimes,
)

from materialize.antithesis import sql, state
from materialize.antithesis.drivers import configure
from materialize.antithesis.drivers.recovery import mark_expected_unhealthy
from materialize.antithesis.drivers.source_status import (
    Export,
    observe,
    restart_events_since,
)
from materialize.antithesis.drivers.sources_common import (
    LAZY_SETUP_BUDGET_S,
    RETAIN_HISTORY_CHOICES,
    SOURCES_CLUSTER,
    Deadline,
    Heartbeat,
    bag_diff,
    ensure_cluster,
    ensure_retain_history,
    frontiers,
    lookup_ids,
    mz_connect,
    mz_host,
    pick_as_of,
    record_progress_observation,
    source_error_text,
    timeline_choice,
    with_retry,
)
from materialize.antithesis.drivers.sources_common import log as _log
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.rng import rng

STATE_DB = "postgres_sources"
PG_CONNECTION = "antithesis_pg"
PG_SECRET = "antithesis_pg_password"
SLOTS = 2
TABLES_PER_SLOT = 3

KEYSPACE_CHOICES = (1, 8, 64, 512)
BOUNDARY_KEYS = (-(2**31), -1, 0, 2**31 - 1)
TXNS_PER_RUN_CHOICES = (1, 5, 20, 50)
OPS_PER_TXN_CHOICES = (1, 2, 5, 20)
DISRUPT_P_CHOICES = (0.0, 0.02, 0.1, 0.3)
DISRUPT_KINDS = ("truncate", "drop", "add_column")
TERMINAL_KINDS = ("truncate", "drop")
REEXPORT_P_CHOICES = (0.2, 1.0)
"""Chance of re-exporting a table right after a terminal op. Otherwise a later
invocation re-exports it, after more DML has landed on the dead export."""
VALUE_MENU: tuple[str | None, ...] = (
    None,
    "",
    "x",
    "ü€𝄞",
    "'quoted' \\ back",
    "NULL",
    "L" * 3000,
)
DML_KINDS = ("upsert", "update", "delete", "delete_range", "churn")

MAX_EXPORTS_PER_TABLE = 3
MAX_EXTRA_COLUMNS = 8

DRIVER_BUDGET_S = 90.0
CHECK_BUDGET_S = 120.0
SNAPSHOT_WAIT_S = 20.0
# Calibration: statistics are reported on an interval and aggregated by the
# controller, so the counter of an export keeps catching up on pre-error
# commits for a while after the error is visible. Measure the reporting lag
# in a fault-free run; this must exceed it.
TERMINAL_STATS_SETTLE_S = 60.0
# A `CREATE TABLE .. FROM SOURCE` whose outcome was unknown and that is still
# not in the catalog after this long is treated as never created.
CREATING_GIVE_UP_S = 600.0
UPSTREAM_OPTIONS = "-c statement_timeout=30000 -c lock_timeout=10000"
SETUP_LOCK_KEY = 727_274
# Budget of `setup_main` in `first_configure`, for setup and seeding together.
# It runs without faults, so this only has to cover a slow environmentd.
FIRST_SETUP_BUDGET_S = 180.0
SEED_TXNS_PER_TABLE = 2
# Upstream Postgres `lock_not_available`, raised by `lock_timeout` when
# concurrent setups or transactions hold the advisory lock or a table lock.
LOCK_NOT_AVAILABLE = "55P03"
TERMINAL_EXPORTS_QUERY = (
    "SELECT count(*) FROM exports e"
    " JOIN ops o ON o.tbl = 's' || e.slot || '.t' || e.j"
    " JOIN txns t ON t.txn_id = o.txn_id"
    " WHERE e.state != 'dropped' AND o.seq > e.s1"
    " AND o.kind IN ('truncate', 'drop') AND t.status = 'acked'"
)
"""Live exports whose upstream table had an acknowledged TRUNCATE or DROP after
the export bound to it. Read-only, for other drivers' triggers."""


def log(message: str) -> None:
    _log("postgres sources", message)


def _tbl(slot: int, j: int) -> str:
    return f"s{slot}.t{j}"


def _hb(slot: int) -> str:
    return f"s{slot}.hb"


def _source(slot: int) -> str:
    return f"pg_src_{slot}"


def _ledger(slot: int) -> str:
    return f"pg_ledger_{slot}"


def parse_lsn(text: str) -> int:
    hi, lo = text.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


@dataclass(frozen=True)
class ExportRow:
    name: str
    slot: int
    j: int
    s1: int
    s3: int | None
    state: str
    created_epoch: float

    @property
    def tbl(self) -> str:
        return _tbl(self.slot, self.j)


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS txns ("
            " txn_id TEXT PRIMARY KEY, slot INTEGER NOT NULL,"
            " lsn_before INTEGER NOT NULL, lsn_after INTEGER,"
            # 'pending' until COMMIT returns, then 'acked', 'aborted', or
            # 'unknown' (the connection broke during COMMIT).
            " status TEXT NOT NULL, started_epoch REAL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS ops ("
            " txn_id TEXT NOT NULL, tbl TEXT NOT NULL, seq INTEGER NOT NULL,"
            " kind TEXT NOT NULL, ops TEXT NOT NULL, PRIMARY KEY (txn_id, tbl))"
        )
        db.execute("CREATE INDEX IF NOT EXISTS ops_tbl_seq ON ops (tbl, seq)")
        db.execute(
            "CREATE TABLE IF NOT EXISTS exports ("
            " name TEXT PRIMARY KEY, slot INTEGER NOT NULL, j INTEGER NOT NULL,"
            " s1 INTEGER NOT NULL, s3 INTEGER,"
            # 'creating', 'live', 'ambiguous', or 'dropped'.
            " state TEXT NOT NULL, created_epoch REAL NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS terminal ("
            " name TEXT PRIMARY KEY, first_t INTEGER NOT NULL,"
            " first_epoch REAL NOT NULL, baseline INTEGER)"
        )
        db.execute("CREATE TABLE IF NOT EXISTS ready (name TEXT PRIMARY KEY)")
    return db


def _is_ready(db: sqlite3.Connection, name: str) -> bool:
    return (
        db.execute("SELECT 1 FROM ready WHERE name = ?", (name,)).fetchone() is not None
    )


def _mark_ready(db: sqlite3.Connection, name: str) -> None:
    with db:
        db.execute("INSERT OR IGNORE INTO ready VALUES (?)", (name,))


def _upstream(endpoints: Endpoints) -> psycopg.Connection:
    return psycopg.connect(
        endpoints.upstream_postgres_url,
        connect_timeout=10,
        autocommit=False,
        options=UPSTREAM_OPTIONS,
    )


def _create_table_sql(slot: int, j: int) -> str:
    t = _tbl(slot, j)
    return (
        f"CREATE TABLE {t} (k int PRIMARY KEY, v text);"
        f" ALTER TABLE {t} REPLICA IDENTITY FULL"
    )


def _setup_retryable(e: BaseException) -> bool:
    # `sql.classify` describes Materialize, which never raises 55P03, so it
    # calls the upstream lock timeout a violation.
    if isinstance(e, psycopg.Error) and e.sqlstate == LOCK_NOT_AVAILABLE:
        return True
    return sql.classify(e).outcome is not sql.Outcome.VIOLATION


def _with_retry(step: str, attempt: Callable[[], None], deadline: Deadline) -> bool:
    failure = with_retry("postgres sources", step, attempt, deadline, _setup_retryable)
    if failure is None:
        return True
    always_or_unreachable(
        failure.retryable,
        "postgres source: setup fails only with retryable errors",
        failure.details(),
    )
    return False


def _ensure_upstream(
    endpoints: Endpoints, db: sqlite3.Connection, deadline: Deadline
) -> bool:
    if _is_ready(db, "upstream"):
        return True
    if not _with_retry("upstream setup", lambda: _setup_upstream(endpoints), deadline):
        return False
    _mark_ready(db, "upstream")
    return True


def _setup_upstream(endpoints: Endpoints) -> None:
    # The advisory lock serializes concurrent setups, which would otherwise
    # race on `CREATE PUBLICATION` (it has no `IF NOT EXISTS`).
    with _upstream(endpoints) as up:
        up.execute("SELECT pg_advisory_xact_lock(%s)", (SETUP_LOCK_KEY,))
        up.execute("CREATE SCHEMA IF NOT EXISTS antithesis_meta")
        up.execute(
            "CREATE TABLE IF NOT EXISTS antithesis_meta.counters"
            " (tbl text PRIMARY KEY, seq bigint NOT NULL, gen int NOT NULL)"
        )
        for slot in range(SLOTS):
            up.execute(f"CREATE SCHEMA IF NOT EXISTS s{slot}".encode())
            ledger = up.execute(
                "SELECT to_regclass(%s)", (f"s{slot}.ledger",)
            ).fetchone()
            assert ledger is not None
            if ledger[0] is None:
                up.execute(
                    f"CREATE TABLE s{slot}.ledger (txn_id text NOT NULL,"
                    " tbl text NOT NULL, seq bigint NOT NULL, kind text NOT NULL,"
                    " PRIMARY KEY (txn_id, tbl));"
                    f" ALTER TABLE s{slot}.ledger REPLICA IDENTITY FULL".encode()
                )
            tables = [_hb(slot)]
            for j in range(TABLES_PER_SLOT):
                tables.append(_tbl(slot, j))
                existing = up.execute(
                    "SELECT to_regclass(%s)", (_tbl(slot, j),)
                ).fetchone()
                assert existing is not None
                if existing[0] is None:
                    up.execute(_create_table_sql(slot, j).encode())
            for t in tables:
                up.execute(
                    "INSERT INTO antithesis_meta.counters VALUES (%s, 0, 0)"
                    " ON CONFLICT DO NOTHING",
                    (t,),
                )
            if (
                up.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s",
                    (f"antithesis_pub_{slot}",),
                ).fetchone()
                is None
            ):
                up.execute(
                    f"CREATE PUBLICATION antithesis_pub_{slot}"
                    f" FOR TABLES IN SCHEMA s{slot}".encode()
                )
        up.commit()


def _ddl(conn: psycopg.Connection, statement: str) -> None:
    try:
        conn.execute(statement.encode())
    except psycopg.Error as e:
        if sql.classify(e).race is not sql.CatalogRace.EXISTS:
            raise


def _retain(db: sqlite3.Connection, slot: int) -> str:
    return timeline_choice(db, f"retain_history_{slot}", RETAIN_HISTORY_CHOICES)


def _ensure_mz(
    endpoints: Endpoints,
    host: str,
    db: sqlite3.Connection,
    slot: int,
    deadline: Deadline,
) -> bool:
    if _is_ready(db, f"mz_slot{slot}"):
        return True
    url = urlparse(endpoints.upstream_postgres_url)
    password = unquote(url.password or "").replace("'", "''")
    retain = f"WITH (RETAIN HISTORY = FOR '{_retain(db, slot)}')"

    def attempt() -> None:
        ensure_retain_history(host, endpoints)
        with mz_connect(host) as conn:
            ensure_cluster(conn, db)
            _ddl(conn, f"CREATE SECRET IF NOT EXISTS {PG_SECRET} AS '{password}'")
            _ddl(
                conn,
                f"CREATE CONNECTION IF NOT EXISTS {PG_CONNECTION} TO POSTGRES"
                f" (HOST '{url.hostname}', PORT {url.port or 5432},"
                f" USER '{unquote(url.username or 'postgres')}',"
                f" PASSWORD SECRET {PG_SECRET},"
                f" DATABASE '{url.path.lstrip('/')}')"
                " WITH (VALIDATE = false)",
            )
            _ddl(
                conn,
                f"CREATE SOURCE IF NOT EXISTS {_source(slot)}"
                f" IN CLUSTER {SOURCES_CLUSTER}"
                f" FROM POSTGRES CONNECTION {PG_CONNECTION}"
                f" (PUBLICATION 'antithesis_pub_{slot}') {retain}",
            )
            _ddl(
                conn,
                f"CREATE TABLE IF NOT EXISTS {_ledger(slot)}"
                f' FROM SOURCE {_source(slot)} (REFERENCE "s{slot}"."ledger")'
                f" {retain}",
            )

    if not _with_retry(f"Materialize setup of slot {slot}", attempt, deadline):
        return False
    _mark_ready(db, f"mz_slot{slot}")
    return True


def _ensure_slot(
    endpoints: Endpoints,
    host: str,
    db: sqlite3.Connection,
    slot: int,
    deadline: Deadline,
) -> bool:
    return _ensure_upstream(endpoints, db, deadline) and _ensure_mz(
        endpoints, host, db, slot, deadline
    )


def _counter(up: psycopg.Connection, tbl: str) -> int:
    """The committed seq of `tbl`, read in its own transaction."""
    row = up.execute(
        "SELECT seq FROM antithesis_meta.counters WHERE tbl = %s", (tbl,)
    ).fetchone()
    up.rollback()
    assert row is not None
    return int(row[0])


def _exports(db: sqlite3.Connection, slot: int | None = None) -> list[ExportRow]:
    query = (
        "SELECT name, slot, j, s1, s3, state, created_epoch FROM exports"
        " WHERE state != 'dropped'"
    )
    params: tuple = ()
    if slot is not None:
        query += " AND slot = ?"
        params = (slot,)
    return [
        ExportRow(*r) for r in db.execute(query + " ORDER BY created_epoch", params)
    ]


def _terminal_in(
    db: sqlite3.Connection,
    tbl: str,
    lo: int,
    hi: int | None,
    include_aborted: bool = True,
) -> int | None:
    """Seq of the first terminal op recorded for `tbl` with `lo < seq <= hi`.

    Including aborted transactions over-approximates, which is the safe side
    for ambiguity; a reused seq makes an aborted op indistinguishable by seq.
    """
    query = (
        "SELECT min(o.seq) FROM ops o JOIN txns t ON o.txn_id = t.txn_id"
        " WHERE o.tbl = ? AND o.seq > ? AND o.kind IN ('truncate', 'drop')"
    )
    params: list[Any] = [tbl, lo]
    if hi is not None:
        query += " AND o.seq <= ?"
        params.append(hi)
    if not include_aborted:
        query += " AND t.status != 'aborted'"
    row = db.execute(query, params).fetchone()
    return int(row[0]) if row[0] is not None else None


def terminal_before_restart(
    db: sqlite3.Connection,
    conn: psycopg.Connection,
    tbl: str,
    s1: int,
    slot: int,
    export_name: str,
    export_id: str,
    src: str,
) -> bool:
    """Whether a terminal op on `tbl` after the export bound to it (seq above
    `s1`) was followed by an ingestion restart.

    That is the precondition of the known terminal-error-resurrection bug
    class, so oracles report it under separate messages and keep the
    general messages for other mechanisms. Restarts count as in
    `_check_terminal_permanence`: status events after the op, or a sibling
    export created after it (which reissues the ingestion).
    """
    row = db.execute(
        "SELECT min(t.started_epoch) FROM ops o JOIN txns t ON o.txn_id = t.txn_id"
        " WHERE o.tbl = ? AND o.seq > ? AND o.kind IN ('truncate', 'drop')"
        " AND t.status != 'aborted'",
        (tbl, s1),
    ).fetchone()
    if row is None or row[0] is None:
        return False
    first = float(row[0])
    if (
        db.execute(
            "SELECT 1 FROM exports WHERE slot = ? AND name != ? AND created_epoch > ?",
            (slot, export_name, first),
        ).fetchone()
        is not None
    ):
        return True
    return restart_events_since(conn, [export_id, src], first) > 0


def export_terminal_history(
    conn: psycopg.Connection, export_name: str, export_id: str
) -> bool | None:
    """`terminal_before_restart` for an export by name, reading this driver's
    state database without writing it. None if the export is not one of
    this driver's, or its state cannot be read.
    """
    path = Endpoints.from_env().state_dir / f"{STATE_DB}.sqlite"
    if not path.exists():
        return None
    try:
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=60)
    except sqlite3.Error:
        return None
    try:
        row = db.execute(
            "SELECT slot, j, s1 FROM exports WHERE name = ?", (export_name,)
        ).fetchone()
        if row is None:
            return None
        slot, j, s1 = int(row[0]), int(row[1]), int(row[2])
        src = lookup_ids(conn, [_source(slot)]).get(_source(slot))
        if src is None:
            return None
        return terminal_before_restart(
            db, conn, _tbl(slot, j), s1, slot, export_name, export_id, src
        )
    except sqlite3.Error:
        return None
    finally:
        db.close()


def _record_s3(
    db: sqlite3.Connection, up: psycopg.Connection, e: ExportRow
) -> ExportRow:
    """Record `s3` for an export whose snapshot was just seen committed."""
    s3 = _counter(up, e.tbl)
    new_state = e.state
    if _terminal_in(db, e.tbl, e.s1, s3) is not None:
        new_state = "ambiguous"
        # The terminal op may have listed exports before this one existed.
        mark_expected_unhealthy([e.name], "upstream terminal op during snapshot")
    elif e.state == "creating":
        new_state = "live"
    with db:
        db.execute(
            "UPDATE exports SET s3 = ?, state = ? WHERE name = ? AND s3 IS NULL",
            (s3, new_state, e.name),
        )
    row = db.execute(
        "SELECT name, slot, j, s1, s3, state, created_epoch FROM exports WHERE name = ?",
        (e.name,),
    ).fetchone()
    return ExportRow(*row)


def _snapshot_committed(conn: psycopg.Connection, export_id: str) -> bool:
    row = conn.execute(
        "SELECT bool_or(snapshot_committed) FROM mz_internal.mz_source_statistics"
        " WHERE id = %s",
        (export_id,),
    ).fetchone()
    return bool(row and row[0])


def _create_export(
    endpoints: Endpoints,
    host: str,
    db: sqlite3.Connection,
    up: psycopg.Connection,
    slot: int,
    j: int,
) -> None:
    n = int(
        db.execute(
            "SELECT count(*) FROM exports WHERE slot = ? AND j = ?", (slot, j)
        ).fetchone()[0]
    )
    s1 = _counter(up, _tbl(slot, j))
    while True:
        name = f"pg_s{slot}_t{j}_e{n}"
        with db:
            cur = db.execute(
                "INSERT OR IGNORE INTO exports VALUES (?, ?, ?, ?, NULL, 'creating', ?)",
                (name, slot, j, s1, time.time()),
            )
        if cur.rowcount:
            break
        n += 1
    try:
        with mz_connect(host) as conn:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {name} FROM SOURCE {_source(slot)}"
                f' (REFERENCE "s{slot}"."t{j}")'
                f" WITH (RETAIN HISTORY = FOR '{_retain(db, slot)}')".encode()
            )
            with db:
                db.execute(
                    "UPDATE exports SET state = 'live' WHERE name = ? AND state = 'creating'",
                    (name,),
                )
            export_id = lookup_ids(conn, [name]).get(name)
            deadline = Deadline(SNAPSHOT_WAIT_S)
            while export_id is not None and not deadline.expired():
                if _snapshot_committed(conn, export_id):
                    row = db.execute(
                        "SELECT name, slot, j, s1, s3, state, created_epoch"
                        " FROM exports WHERE name = ?",
                        (name,),
                    ).fetchone()
                    _record_s3(db, up, ExportRow(*row))
                    break
                time.sleep(1)
        log(f"created export {name} (s1={s1})")
    except (psycopg.Error, OSError) as e:
        log(f"creating export {name} did not complete: {e}")


def _drop_export(host: str, db: sqlite3.Connection, e: ExportRow) -> None:
    try:
        with mz_connect(host) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {e.name}".encode())
    except (psycopg.Error, OSError) as err:
        log(f"dropping export {e.name} failed: {err}")
        return
    with db:
        db.execute("UPDATE exports SET state = 'dropped' WHERE name = ?", (e.name,))
        db.execute("DELETE FROM terminal WHERE name = ?", (e.name,))


def _maintain_exports(
    endpoints: Endpoints,
    host: str,
    db: sqlite3.Connection,
    up: psycopg.Connection,
    slot: int,
) -> None:
    """Keep at least one export per table that no recorded terminal op has killed,
    and at most `MAX_EXPORTS_PER_TABLE` exports per table."""
    for j in range(TABLES_PER_SLOT):
        exports = [e for e in _exports(db, slot) if e.j == j]
        live = [
            e
            for e in exports
            if _terminal_in(db, e.tbl, e.s1, None, include_aborted=False) is None
        ]
        if not live:
            _create_export(endpoints, host, db, up, slot, j)
            exports = [e for e in _exports(db, slot) if e.j == j]
        while len(exports) > MAX_EXPORTS_PER_TABLE:
            _drop_export(host, db, exports.pop(0))


def _value() -> str | None:
    if rng.random() < 0.5:
        return f"v{rng.getrandbits(32):08x}"
    return rng.choice(VALUE_MENU)


def _key(keyspace: int) -> int:
    if rng.random() < 0.05:
        return rng.choice(BOUNDARY_KEYS)
    return rng.randrange(keyspace)


def _dml_ops(keyspace: int, n: int) -> list[list[Any]]:
    ops: list[list[Any]] = []
    for _ in range(n):
        kind = rng.choice(DML_KINDS)
        k = _key(keyspace)
        if kind == "upsert":
            ops.append(["upsert", k, _value()])
        elif kind == "update":
            ops.append(["update", k, _value()])
        elif kind == "delete":
            ops.append(["delete", k])
        elif kind == "delete_range":
            ops.append(["delete_range", k, k + rng.choice((1, 2, 10, keyspace))])
        else:
            ops.append(["delete", k])
            ops.append(["upsert", k, _value()])
    return ops


def _apply_upstream(up: psycopg.Connection, tbl: str, op: list[Any]) -> None:
    kind = op[0]
    if kind == "upsert":
        up.execute(
            f"INSERT INTO {tbl} (k, v) VALUES (%s, %s)"
            " ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v".encode(),
            (op[1], op[2]),
        )
    elif kind == "update":
        up.execute(f"UPDATE {tbl} SET v = %s WHERE k = %s".encode(), (op[2], op[1]))
    elif kind == "delete":
        up.execute(f"DELETE FROM {tbl} WHERE k = %s".encode(), (op[1],))
    elif kind == "delete_range":
        up.execute(
            f"DELETE FROM {tbl} WHERE k >= %s AND k < %s".encode(), (op[1], op[2])
        )
    else:
        raise ValueError(f"unknown op {op}")


def replay(entries: list[tuple[int, str, list[list[Any]]]]) -> dict[int, str | None]:
    """Table contents after `(seq, kind, ops)` entries applied in seq order."""
    rows: dict[int, str | None] = {}
    for _, kind, ops in sorted(entries, key=lambda e: e[0]):
        if kind in TERMINAL_KINDS:
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


def _is_ambiguous_commit(e: psycopg.Error) -> bool:
    return e.sqlstate is None or e.sqlstate.startswith(("08", "57"))


def run_txn(
    up: psycopg.Connection,
    db: sqlite3.Connection,
    slot: int,
    plan: dict[str, tuple[str, list[list[Any]]]],
) -> tuple[str, str]:
    """Run one upstream transaction. Returns `(txn_id, status)`.

    `plan` maps each table to `(kind, ops)`. The transaction and its operations
    are recorded before COMMIT so that a commit with a lost acknowledgment can
    still be replayed once Materialize shows it in the ledger.
    """
    txn_id = f"{rng.getrandbits(64):016x}"
    recorded = False
    try:
        lsn_row = up.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()
        assert lsn_row is not None
        lsn_before = parse_lsn(lsn_row[0])
        seqs: dict[str, int] = {}
        for tbl in sorted(plan):
            kind = plan[tbl][0]
            row = up.execute(
                "UPDATE antithesis_meta.counters SET seq = seq + 1, gen = gen + %s"
                " WHERE tbl = %s RETURNING seq",
                (1 if kind == "drop" else 0, tbl),
            ).fetchone()
            assert row is not None
            seqs[tbl] = int(row[0])
        for tbl in sorted(plan):
            kind, ops = plan[tbl]
            if kind == "truncate":
                up.execute(f"TRUNCATE {tbl}".encode())
            elif kind == "drop":
                slot_j = tbl.split(".t")
                up.execute(f"DROP TABLE {tbl}".encode())
                up.execute(
                    _create_table_sql(int(slot_j[0][1:]), int(slot_j[1])).encode()
                )
            elif kind == "add_column":
                schema, name = tbl.split(".")
                count_row = up.execute(
                    "SELECT count(*) FROM information_schema.columns"
                    " WHERE table_schema = %s AND table_name = %s",
                    (schema, name),
                ).fetchone()
                assert count_row is not None
                if int(count_row[0]) >= 2 + MAX_EXTRA_COLUMNS:
                    up.rollback()
                    return txn_id, "aborted"
                up.execute(f"ALTER TABLE {tbl} ADD COLUMN c{seqs[tbl]} text".encode())
            for op in ops:
                _apply_upstream(up, tbl, op)
            up.execute(
                f"INSERT INTO s{slot}.ledger VALUES (%s, %s, %s, %s)".encode(),
                (txn_id, tbl, seqs[tbl], kind),
            )
        with db:
            db.execute(
                "INSERT INTO txns (txn_id, slot, lsn_before, lsn_after, status,"
                " started_epoch) VALUES (?, ?, ?, NULL, 'pending', ?)",
                (txn_id, slot, lsn_before, time.time()),
            )
            db.executemany(
                "INSERT INTO ops VALUES (?, ?, ?, ?, ?)",
                [
                    (txn_id, tbl, seqs[tbl], plan[tbl][0], json.dumps(plan[tbl][1]))
                    for tbl in plan
                ],
            )
        recorded = True
        # Before COMMIT, so recovery never sees a killed export unexempted.
        # An aborted transaction leaves its exports exempt, which only loosens
        # recovery's checks.
        terminal = {tbl for tbl, (kind, _) in plan.items() if kind in TERMINAL_KINDS}
        mark_expected_unhealthy(
            [e.name for e in _exports(db, slot) if e.tbl in terminal],
            "upstream table truncated or dropped",
        )
    except (psycopg.Error, sqlite3.Error) as e:
        log(f"transaction {txn_id} rolled back: {e}")
        try:
            up.rollback()
        except psycopg.Error:
            pass
        if recorded:
            with db:
                db.execute(
                    "UPDATE txns SET status = 'aborted' WHERE txn_id = ?", (txn_id,)
                )
        return txn_id, "aborted"

    try:
        up.commit()
    except psycopg.Error as e:
        status = "unknown" if _is_ambiguous_commit(e) else "aborted"
        with db:
            db.execute("UPDATE txns SET status = ? WHERE txn_id = ?", (status, txn_id))
        return txn_id, status

    lsn_after = None
    try:
        lsn_row = up.execute("SELECT pg_current_wal_lsn()::text").fetchone()
        assert lsn_row is not None
        lsn_after = parse_lsn(lsn_row[0])
        up.rollback()
    except psycopg.Error:
        pass
    with db:
        db.execute(
            "UPDATE txns SET status = 'acked', lsn_after = ? WHERE txn_id = ?",
            (lsn_after, txn_id),
        )
    return txn_id, "acked"


def _dml_txn(
    up: psycopg.Connection, db: sqlite3.Connection, slot: int, tables: list[int]
) -> str:
    """Run one DML transaction over `tables` of `slot` with this timeline's
    keyspace and transaction size. Returns its status."""
    keyspace = timeline_choice(db, "keyspace", KEYSPACE_CHOICES)
    n_ops = timeline_choice(db, "ops_per_txn", OPS_PER_TXN_CHOICES)
    plan = {_tbl(slot, j): ("dml", _dml_ops(keyspace, n_ops)) for j in tables}
    return run_txn(up, db, slot, plan)[1]


def setup_main() -> int:
    """Set up every slot, seed its tables, and export them. Run from
    `first_configure`; the driver and the check repair setup lazily if this
    did not finish.

    Seeding before the first exports makes their snapshots non-empty, so the
    first checks already compare rows.
    """
    endpoints = Endpoints.from_env()
    db = open_state()
    deadline = Deadline(FIRST_SETUP_BUDGET_S)
    try:
        host = mz_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    ready = [s for s in range(SLOTS) if _ensure_slot(endpoints, host, db, s, deadline)]
    sometimes(
        len(ready) == SLOTS,
        "postgres source: first_configure set up every slot",
        {"ready_slots": ready},
    )
    if not ready:
        return 0
    try:
        up = _upstream(endpoints)
    except (psycopg.Error, OSError) as e:
        log(f"no upstream connection: {e}")
        return 0
    try:
        for slot in ready:
            if deadline.expired():
                log(f"setup budget spent; slot {slot} left for lazy seeding")
                break
            for _ in range(SEED_TXNS_PER_TABLE):
                _dml_txn(up, db, slot, list(range(TABLES_PER_SLOT)))
            _maintain_exports(endpoints, host, db, up, slot)
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        log(f"seeding stopped early: {c.outcome.value} {c.sqlstate} {c.template}")
    finally:
        try:
            up.close()
        except psycopg.Error:
            pass
    return 0


def upstream_main() -> int:
    endpoints = Endpoints.from_env()
    db = open_state()
    try:
        host = mz_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    setup_deadline = Deadline(LAZY_SETUP_BUDGET_S)
    if not _ensure_upstream(endpoints, db, setup_deadline):
        return 0
    ready_slots = [
        s for s in range(SLOTS) if _ensure_mz(endpoints, host, db, s, setup_deadline)
    ]

    keyspace = timeline_choice(db, "keyspace", KEYSPACE_CHOICES)
    n_txns = timeline_choice(db, "txns_per_run", TXNS_PER_RUN_CHOICES)
    n_ops = timeline_choice(db, "ops_per_txn", OPS_PER_TXN_CHOICES)
    disrupt_p = timeline_choice(db, "disrupt_p", DISRUPT_P_CHOICES)
    reexport_p = timeline_choice(db, "reexport_p", REEXPORT_P_CHOICES)
    # Swarm: each disruptive kind may be left out of this timeline entirely.
    kinds = [
        k
        for k in DISRUPT_KINDS
        if timeline_choice(db, f"disrupt_{k}", (True, True, False))
    ]
    if configure.profile_value(configure.PG_TERMINAL_OPS_KEY) == "off":
        kinds = [k for k in kinds if k not in TERMINAL_KINDS]

    deadline = Deadline(DRIVER_BUDGET_S)
    try:
        up = _upstream(endpoints)
    except (psycopg.Error, OSError) as e:
        log(f"no upstream connection: {e}")
        return 0
    try:
        for slot in ready_slots:
            _maintain_exports(endpoints, host, db, up, slot)
        done = 0
        while done < n_txns and not deadline.expired():
            slot = rng.randrange(SLOTS)
            if kinds and rng.random() < disrupt_p:
                j = rng.randrange(TABLES_PER_SLOT)
                kind = rng.choice(kinds)
                _, status = run_txn(up, db, slot, {_tbl(slot, j): (kind, [])})
                log(f"{kind} on {_tbl(slot, j)}: {status}")
                if (
                    status == "acked"
                    and kind in TERMINAL_KINDS
                    and slot in ready_slots
                    and rng.random() < reexport_p
                ):
                    _create_export(endpoints, host, db, up, slot, j)
            else:
                tables = rng.sample(range(TABLES_PER_SLOT), rng.choice((1, 1, 2)))
                plan = {
                    _tbl(slot, j): ("dml", _dml_ops(keyspace, n_ops)) for j in tables
                }
                run_txn(up, db, slot, plan)
            done += 1
        for slot in ready_slots:
            _maintain_exports(endpoints, host, db, up, slot)
    except (psycopg.Error, OSError) as e:
        log(f"driver stopped early: {e}")
    finally:
        try:
            up.close()
        except psycopg.Error:
            pass
    return 0


def send_heartbeats(host: str) -> list[Heartbeat]:
    """Commit one ledger-only transaction per ready slot for the liveness check."""
    endpoints = Endpoints.from_env()
    db = open_state()
    out: list[Heartbeat] = []
    with _upstream(endpoints) as up:
        for slot in range(SLOTS):
            if not _is_ready(db, f"mz_slot{slot}"):
                continue
            txn_id, status = run_txn(up, db, slot, {_hb(slot): ("heartbeat", [])})
            if status != "acked":
                continue
            out.append(
                Heartbeat(
                    _ledger(slot),
                    f"SELECT count(*) FROM {_ledger(slot)} WHERE txn_id = %s",
                    (txn_id,),
                )
            )
    return out


def _report_query_error(e: BaseException, details: dict[str, Any]) -> None:
    c = sql.classify_as_of_read(e)
    if c.outcome == sql.Outcome.VIOLATION:
        always_or_unreachable(
            False,
            "postgres source: check queries return only classified errors",
            {**details, "sqlstate": c.sqlstate, "template": c.template},
        )


def _updates_committed(conn: psycopg.Connection, export_id: str) -> int | None:
    row = conn.execute(
        "SELECT sum(updates_committed)::text FROM mz_internal.mz_source_statistics"
        " WHERE id = %s",
        (export_id,),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def check_main() -> int:
    endpoints = Endpoints.from_env()
    db = open_state()
    slot = rng.randrange(SLOTS)
    try:
        host = mz_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    if not _ensure_slot(endpoints, host, db, slot, Deadline(LAZY_SETUP_BUDGET_S)):
        return 0
    try:
        conn = mz_connect(host)
    except (psycopg.Error, OSError) as e:
        log(f"no connection: {e}")
        return 0
    try:
        up = _upstream(endpoints)
    except (psycopg.Error, OSError) as e:
        log(f"no upstream connection: {e}")
        conn.close()
        return 0
    try:
        if not _exports(db, slot):
            _maintain_exports(endpoints, host, db, up, slot)
        # Test Composer often leaves `parallel_driver_pg_upstream` unscheduled
        # for long stretches, and without new upstream transactions every
        # check compares the same, possibly empty, history. One transaction
        # per check keeps the ledger moving; it is visible to later checks.
        _dml_txn(up, db, slot, [rng.randrange(TABLES_PER_SLOT)])
        _check(db, conn, up, slot, Deadline(CHECK_BUDGET_S))
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        log(f"check abandoned: {c.outcome.value} {c.sqlstate} {c.template}")
    finally:
        conn.close()
        try:
            up.close()
        except psycopg.Error:
            pass
    return 0


@dataclass
class _Ledger:
    """The ledger of one slot as Materialize shows it at T."""

    by_tbl: dict[str, list[tuple[int, str, str]]]
    """Per table, `(seq, txn_id, kind)` in seq order."""
    tables_of: dict[str, set[str]]


def _check(
    db: sqlite3.Connection,
    conn: psycopg.Connection,
    up: psycopg.Connection,
    slot: int,
    deadline: Deadline,
) -> None:
    src_name, ledger_name = _source(slot), _ledger(slot)
    exports = _exports(db, slot)
    ids = lookup_ids(conn, [src_name, ledger_name] + [e.name for e in exports])
    if src_name not in ids or ledger_name not in ids:
        return
    src, ledger = ids[src_name], ids[ledger_name]
    now = time.time()
    for e in exports:
        if (
            e.name not in ids
            and e.state == "creating"
            and now - e.created_epoch > CREATING_GIVE_UP_S
        ):
            with db:
                db.execute(
                    "UPDATE exports SET state = 'dropped' WHERE name = ?", (e.name,)
                )
    exports = [e for e in exports if e.name in ids]
    observe(
        conn,
        [Export(ledger, src, "cdc")]
        + [Export(ids[e.name], src, "cdc") for e in exports],
    )

    fronts = frontiers(conn, [src, ledger] + [ids[e.name] for e in exports])
    if src not in fronts or ledger not in fronts:
        return
    t = pick_as_of(
        (fronts[src][0], fronts[ledger][0]), (fronts[src][1], fronts[ledger][1])
    )
    if t is None:
        return
    details: dict[str, Any] = {"slot": slot, "as_of": t}

    try:
        lsn_rows = conn.execute(
            f"SELECT lsn::text FROM {src_name} AS OF {t}".encode()
        ).fetchall()
        ledger_rows = conn.execute(
            f"SELECT txn_id, tbl, seq, kind FROM {ledger_name} AS OF {t}".encode()
        ).fetchall()
        ledger_dups = conn.execute(
            f"SELECT txn_id, tbl, count(*) FROM {ledger_name}"
            f" GROUP BY txn_id, tbl HAVING count(*) > 1 LIMIT 10 AS OF {t}".encode()
        ).fetchall()
    except psycopg.Error as e:
        _report_query_error(e, details)
        if source_error_text(e) is not None:
            always_or_unreachable(
                False,
                "postgres source: the ledger export reads without a source error",
                {**details, "error": source_error_text(e)},
            )
        return
    if len(lsn_rows) != 1:
        return
    f = int(lsn_rows[0][0])
    details["lsn"] = f

    regressions = record_progress_observation(db, f"pg_slot{slot}", t, {0: f})
    always(
        not regressions,
        "postgres source: the mapped LSN is monotone in mz time",
        {**details, "conflicting": regressions},
    )
    always(
        not ledger_dups,
        "postgres source: the ledger export holds at most one row per primary key",
        {**details, "duplicates": [list(r) for r in ledger_dups]},
    )

    led = _check_ledger(db, slot, f, ledger_rows, details)
    if led is None:
        return

    for e in exports:
        if deadline.expired():
            break
        since, upper = fronts.get(ids[e.name], (None, None))
        if since is None or upper is None or not since <= t < upper:
            continue
        _check_export(db, conn, up, e, ids[e.name], src, t, led, details)


def _check_ledger(
    db: sqlite3.Connection,
    slot: int,
    f: int,
    ledger_rows: list[tuple[Any, ...]],
    details: dict[str, Any],
) -> _Ledger | None:
    # A whole-source definite error writes at `u64::MAX`, beyond SQLite's
    # signed 64-bit integers. Every recorded LSN is far below the clamp.
    f_db = min(f, 2**63 - 1)
    by_tbl: dict[str, list[tuple[int, str, str]]] = {}
    tables_of: dict[str, set[str]] = {}
    for txn_id, tbl, seq, kind in ledger_rows:
        by_tbl.setdefault(str(tbl), []).append((int(seq), str(txn_id), str(kind)))
        tables_of.setdefault(str(txn_id), set()).add(str(tbl))
    for entries in by_tbl.values():
        entries.sort()

    recorded: dict[str, set[str]] = {}
    for txn_id, tbl in db.execute(
        "SELECT o.txn_id, o.tbl FROM ops o JOIN txns t ON o.txn_id = t.txn_id"
        " WHERE t.slot = ?",
        (slot,),
    ):
        recorded.setdefault(txn_id, set()).add(tbl)

    phantoms = sorted(t for t in tables_of if t not in recorded)
    always(
        not phantoms,
        "postgres source: every ledger row visible in Materialize was written by the workload",
        {**details, "phantoms": phantoms[:10]},
    )
    torn = sorted(
        t for t, tbls in tables_of.items() if t in recorded and tbls != recorded[t]
    )
    always(
        not torn,
        "postgres source: a transaction's ledger rows are visible together or not at all",
        {**details, "torn": torn[:10]},
    )
    gaps = {
        tbl: [s for s, _, _ in entries][:20]
        for tbl, entries in by_tbl.items()
        if [s for s, _, _ in entries] != list(range(1, len(entries) + 1))
    }
    always(
        not gaps,
        "postgres source: visible ledger rows form a gap-free prefix of each table's commit order",
        {**details, "tables": gaps},
    )

    committed_below = [
        r[0]
        for r in db.execute(
            "SELECT txn_id FROM txns WHERE slot = ? AND status = 'acked'"
            " AND lsn_after IS NOT NULL AND lsn_after < ?",
            (slot, f_db),
        )
    ]
    missing = [t for t in committed_below if t not in tables_of]
    always(
        not missing,
        "postgres source: transactions committed below the mapped LSN are visible in the ledger",
        {**details, "missing": missing[:10], "missing_count": len(missing)},
    )
    sometimes(
        len(committed_below) > len(missing),
        "postgres source: a ledger check found an acknowledged transaction committed below the mapped LSN",
        {**details, "committed_below": len(committed_below)},
    )
    early = [
        r[0]
        for r in db.execute(
            "SELECT txn_id FROM txns WHERE slot = ? AND lsn_before > ?", (slot, f_db)
        )
        if r[0] in tables_of
    ]
    always(
        not early,
        "postgres source: transactions begun above the mapped LSN are not visible in the ledger",
        {**details, "early": early[:10], "early_count": len(early)},
    )
    if phantoms or torn or gaps:
        return None
    return _Ledger(by_tbl, tables_of)


def _check_export(
    db: sqlite3.Connection,
    conn: psycopg.Connection,
    up: psycopg.Connection,
    e: ExportRow,
    export_id: str,
    src: str,
    t: int,
    led: _Ledger,
    details: dict[str, Any],
) -> None:
    details = {**details, "export": e.name, "s1": e.s1, "s3": e.s3}
    if e.s3 is None and _snapshot_committed(conn, export_id):
        e = _record_s3(db, up, e)
        details["s3"] = e.s3

    errored: str | None = None
    rows: list[tuple[Any, ...]] = []
    try:
        rows = conn.execute(f"SELECT k, v FROM {e.name} AS OF {t}".encode()).fetchall()
    except psycopg.Error as err:
        errored = source_error_text(err)
        if errored is None:
            _report_query_error(err, details)
            return
    details["errored"] = errored
    tagged = terminal_before_restart(
        db, conn, e.tbl, e.s1, e.slot, e.name, export_id, src
    )
    details["terminal_before_restart"] = tagged

    _check_terminal_permanence(db, conn, e, export_id, src, t, errored, tagged, details)

    if errored is None:
        try:
            dups = conn.execute(
                f"SELECT k, count(*) FROM {e.name}"
                f" GROUP BY k HAVING count(*) > 1 LIMIT 10 AS OF {t}".encode()
            ).fetchall()
        except psycopg.Error as err:
            _report_query_error(err, details)
            dups = []
        dup_details = {**details, "duplicates": [list(r) for r in dups]}
        if tagged:
            always_or_unreachable(
                not dups,
                "postgres source: an export holds at most one row per upstream primary key, ingestion restarted after a terminal upstream op",
                dup_details,
            )
        else:
            always(
                not dups,
                "postgres source: an export holds at most one row per upstream primary key, with no terminal upstream op before the last ingestion restart",
                dup_details,
            )

    if e.state == "ambiguous" or e.s3 is None:
        return
    entries = led.by_tbl.get(e.tbl, [])
    if not entries or entries[-1][0] < e.s3:
        return

    terminal = next(
        ((s, k) for s, _, k in entries if s > e.s3 and k in TERMINAL_KINDS), None
    )
    details["terminal"] = terminal
    if terminal is None:
        if tagged:
            always_or_unreachable(
                errored is None,
                "postgres source: an export is errored only if a terminal upstream op precedes the mapped LSN, ingestion restarted after a terminal upstream op",
                details,
            )
        else:
            always(
                errored is None,
                "postgres source: an export is errored only if a terminal upstream op precedes the mapped LSN, with no terminal upstream op before the last ingestion restart",
                details,
            )
    elif terminal[1] == "truncate":
        if tagged:
            always_or_unreachable(
                errored is not None,
                "postgres source: an export whose TRUNCATE precedes the mapped LSN is errored, ingestion restarted after a terminal upstream op",
                details,
            )
        else:
            always(
                errored is not None,
                "postgres source: an export whose TRUNCATE precedes the mapped LSN is errored, with no terminal upstream op before the last ingestion restart",
                details,
            )
    else:
        sometimes(
            errored is not None,
            "postgres source: a check observed an export errored by a DROP TABLE below the mapped LSN",
            details,
        )
    if errored is not None:
        return

    cutoff = terminal[0] if terminal is not None else None
    replayed = []
    for seq, txn_id, kind in entries:
        if cutoff is not None and seq >= cutoff:
            break
        row = db.execute(
            "SELECT ops FROM ops WHERE txn_id = ? AND tbl = ?", (txn_id, e.tbl)
        ).fetchone()
        if row is None:
            return
        replayed.append((seq, kind, json.loads(row[0])))
    expected = replay(replayed)
    diff = bag_diff(expected.items(), [(int(k), v) for k, v in rows])
    matched = diff["missing_count"] == 0 and diff["extra_count"] == 0
    sometimes(
        bool(expected),
        "postgres source: a check compared an export with a non-empty replayed upstream table",
        {**details, "replayed_txns": len(replayed), "expected_rows": len(expected)},
    )
    if tagged:
        always_or_unreachable(
            matched,
            "postgres source: an export equals its upstream table replayed to the mapped LSN, ingestion restarted after a terminal upstream op",
            {**details, **diff},
        )
    else:
        always(
            matched,
            "postgres source: an export equals its upstream table replayed to the mapped LSN, with no terminal upstream op before the last ingestion restart",
            {**details, **diff},
        )
    sometimes(
        matched and any(s > e.s3 and k == "add_column" for s, _, k in entries),
        "postgres source: an export matched upstream across an ADD COLUMN below the mapped LSN",
        details,
    )
    sometimes(
        matched and not e.name.endswith("_e0") and bool(expected),
        "postgres source: a re-created export of a table matched a non-empty upstream",
        details,
    )


def _check_terminal_permanence(
    db: sqlite3.Connection,
    conn: psycopg.Connection,
    e: ExportRow,
    export_id: str,
    src: str,
    t: int,
    errored: str | None,
    tagged: bool,
    details: dict[str, Any],
) -> None:
    row = db.execute(
        "SELECT first_t, first_epoch, baseline FROM terminal WHERE name = ?", (e.name,)
    ).fetchone()
    if errored is None:
        retract_details = {**details, "first_errored_t": row[0] if row else None}
        if tagged:
            always_or_unreachable(
                row is None or t < row[0],
                "postgres source: a terminal export error is never retracted at a later time, ingestion restarted after a terminal upstream op",
                retract_details,
            )
        else:
            always(
                row is None or t < row[0],
                "postgres source: a terminal export error is never retracted at a later time, with no terminal upstream op before the last ingestion restart",
                retract_details,
            )
        return

    now = time.time()
    if row is None:
        with db:
            db.execute(
                "INSERT OR IGNORE INTO terminal VALUES (?, ?, ?, NULL)",
                (e.name, t, now),
            )
        return
    if t < row[0]:
        with db:
            db.execute("UPDATE terminal SET first_t = ? WHERE name = ?", (t, e.name))
    first_epoch, baseline = float(row[1]), row[2]

    committed = _updates_committed(conn, export_id)
    if committed is not None:
        if baseline is None:
            if now - first_epoch >= TERMINAL_STATS_SETTLE_S:
                with db:
                    db.execute(
                        "UPDATE terminal SET baseline = ? WHERE name = ?",
                        (committed, e.name),
                    )
        else:
            stats_details = {
                **details,
                "baseline": int(baseline),
                "updates_committed": committed,
            }
            if tagged:
                always_or_unreachable(
                    committed <= int(baseline),
                    "postgres source: an export with a terminal error commits no further updates, ingestion restarted after a terminal upstream op",
                    stats_details,
                )
            else:
                always(
                    committed <= int(baseline),
                    "postgres source: an export with a terminal error commits no further updates, with no terminal upstream op before the last ingestion restart",
                    stats_details,
                )

    restarted = restart_events_since(conn, [export_id, src], first_epoch) > 0
    if not restarted:
        # Adding or dropping a sibling export reissues the ingestion.
        restarted = (
            db.execute(
                "SELECT 1 FROM exports WHERE slot = ? AND name != ? AND created_epoch > ?",
                (e.slot, e.name, first_epoch),
            ).fetchone()
            is not None
        )
    first_terminal = _terminal_in(
        db, e.tbl, e.s3 if e.s3 is not None else e.s1, None, include_aborted=False
    )
    later_dml = (
        first_terminal is not None
        and db.execute(
            "SELECT 1 FROM ops o JOIN txns t ON o.txn_id = t.txn_id"
            " WHERE o.tbl = ? AND o.seq > ? AND o.kind = 'dml' AND t.status = 'acked'",
            (e.tbl, first_terminal),
        ).fetchone()
        is not None
    )
    sometimes(
        restarted,
        "postgres source: an ingestion restarted while an export held a terminal error",
        details,
    )
    sometimes(
        restarted and later_dml,
        "postgres source: an ingestion restarted while an export held a terminal error and its table had later DML",
        details,
    )
