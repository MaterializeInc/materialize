# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Remap since against export upper, the invariant behind SS-346 and SQL-603.

Every source export (a table `FROM SOURCE`) depends on its source's remap
collection, which for the current source syntax is the source item's own
shard. The storage controller makes each export hold the remap shard back at
the export's since, so the remap since never reaches the export's upper. Both
bootstrap paths assume this of durable state:

* `StorageCollectionsImpl::create_collections_for_bootstrap` soft-asserts
  "dependency since has advanced past dependent upper" when the export's since
  is behind the remap since and the remap since is not below the export
  upper. This is the SS-346 panic.
* The storage controller's `create_collections_for_bootstrap` soft-asserts the
  same comparison unconditionally, and halts when the remap since is empty
  while the export upper is not (SQL-603).

Both skip an export whose upper is `[0]` (never written) or empty (sealed).
If the remap since reaches the export upper, a restarting ingestion cannot
tell which remapped updates the export already holds and may write them again.

The checked invariant, per live (remap, export) pair whose export upper is
neither `[0]` nor empty: `remap_since < export_upper`, where `remap_since` is
the controller's critical since of the remap shard (what a read-write
controller bootstraps from) or, when no controller critical reader is
registered, the shard since. The shard since is never above the critical
since, so the critical since is the stricter read.

Concurrency: the remap since and the export upper both only advance. The
auditor reads the remap shard first and the export shard second. If the remap
since read at t1 is at or above the export upper read at t2 > t1, then at t1
the export upper was at most that value, so the violation held at t1. The
opposite order can report a violation that never existed.

This module holds the pure classification and the violation ledger. It
imports no driver, so the source oracles can read the ledger without an
import cycle through the shard auditor.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from materialize.antithesis.endpoints import Endpoints

STATE_DB = "remap_audit"

PAIRS_SQL = """
SELECT t.id, t.name, src.id, src.name, ts.shard_id, ss.shard_id
FROM mz_tables t
JOIN mz_sources src ON src.id = t.source_id
JOIN mz_internal.mz_object_global_ids tg ON tg.id = t.id
JOIN mz_internal.mz_storage_shards ts ON ts.object_id = tg.global_id
JOIN mz_internal.mz_object_global_ids sg ON sg.id = src.id
JOIN mz_internal.mz_storage_shards ss ON ss.object_id = sg.global_id
WHERE t.id LIKE 'u%'
"""
"""Every user table fed by a source, with its own shard and the source's shard.

For a source created without subsources the source item's shard is its remap
collection. The harness creates no `ENVELOPE MATERIALIZE` (CDCv2) exports,
the only envelope without a remap dependency."""


@dataclass(frozen=True)
class Pair:
    data_id: str
    data_name: str
    remap_id: str
    remap_name: str
    data_shard: str
    remap_shard: str


@dataclass(frozen=True)
class Frontiers:
    """One shard's frontiers from a single `persistcli inspect audit-frontiers` state version."""

    initialized: bool
    tombstone: bool
    since: tuple[int, ...]
    upper: tuple[int, ...]
    controller_since: tuple[int, ...] | None

    @classmethod
    def parse(cls, report: dict[str, Any]) -> Frontiers | None:
        """None if the report is malformed."""
        try:
            initialized = bool(report["initialized"])
            if not initialized:
                return cls(False, False, (), (), None)
            controller = report.get("controller_critical_since")
            return cls(
                True,
                bool(report["tombstone"]),
                tuple(int(t) for t in report["since"]),
                tuple(int(t) for t in report["upper"]),
                tuple(int(t) for t in controller) if controller is not None else None,
            )
        except (KeyError, TypeError, ValueError):
            return None

    @property
    def effective_since(self) -> tuple[int, ...]:
        return (
            self.controller_since if self.controller_since is not None else self.since
        )


class Verdict(Enum):
    SKIP = "skip"
    """Not comparable: a shard is uninitialized or tombstoned, or the export
    upper is `[0]` or empty."""
    OK = "ok"
    PAST_UPPER_BEHIND = "past_upper_export_since_behind"
    """Remap since at or past the export upper, export since behind the remap
    since: the SS-346 panic condition."""
    PAST_UPPER_AHEAD = "past_upper_export_since_not_behind"
    """Remap since at or past the export upper, export since at or past the
    remap since: only the storage controller's unconditional check fires."""
    EMPTY_SINCE = "remap_since_empty"
    """Remap since empty while the export can still be written: the SQL-603
    halt condition."""


def _less(a: tuple[int, ...], b: tuple[int, ...]) -> bool:
    """`a < b` for totally ordered antichains, where the empty antichain is the top."""
    if not b:
        return bool(a)
    if not a:
        return False
    return a[0] < b[0]


@dataclass(frozen=True)
class Check:
    verdict: Verdict
    remap_since: tuple[int, ...]
    data_since: tuple[int, ...]
    data_upper: tuple[int, ...]

    @property
    def violated(self) -> bool:
        return self.verdict in (
            Verdict.PAST_UPPER_BEHIND,
            Verdict.PAST_UPPER_AHEAD,
            Verdict.EMPTY_SINCE,
        )

    @property
    def export_since_behind(self) -> bool:
        """The export since is behind the remap since, the precondition of the
        storage-collections bootstrap check."""
        return _less(self.data_since, self.remap_since)

    @property
    def headroom_ms(self) -> int | None:
        """`export_upper - remap_since`, or None when either is empty."""
        if not self.remap_since or not self.data_upper:
            return None
        return self.data_upper[0] - self.remap_since[0]


def classify(remap: Frontiers, data: Frontiers) -> Check:
    """Classify a pair whose remap shard was read before its export shard."""
    remap_since = remap.effective_since
    data_since = data.effective_since

    def check(verdict: Verdict) -> Check:
        return Check(verdict, remap_since, data_since, data.upper)

    if not (remap.initialized and data.initialized):
        return check(Verdict.SKIP)
    if remap.tombstone or data.tombstone:
        return check(Verdict.SKIP)
    if data.upper in ((), (0,)):
        return check(Verdict.SKIP)
    if not remap_since:
        return check(Verdict.EMPTY_SINCE)
    if _less(remap_since, data.upper):
        return check(Verdict.OK)
    if _less(data_since, remap_since):
        return check(Verdict.PAST_UPPER_BEHIND)
    return check(Verdict.PAST_UPPER_AHEAD)


def open_ledger(endpoints: Endpoints | None = None) -> sqlite3.Connection:
    # `state` imports the Antithesis SDK, which the unit tests run without.
    from materialize.antithesis import state

    db = state.open_db(STATE_DB, endpoints)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS violations ("
            " data_id TEXT PRIMARY KEY, data_name TEXT NOT NULL,"
            " remap_id TEXT NOT NULL, verdict TEXT NOT NULL,"
            " first_epoch REAL NOT NULL)"
        )
    return db


def record_violation(db: sqlite3.Connection, pair: Pair, check: Check) -> None:
    with db:
        db.execute(
            "INSERT OR IGNORE INTO violations VALUES (?, ?, ?, ?, ?)",
            (
                pair.data_id,
                pair.data_name,
                pair.remap_id,
                check.verdict.value,
                time.time(),
            ),
        )


def seen_past_upper(data_id: str | None, endpoints: Endpoints | None = None) -> bool:
    """Whether the shard auditor ever saw the remap since at or past this
    export's upper. Read-only. A missing or unreadable ledger counts as not seen."""
    if data_id is None:
        return False
    path = (
        (endpoints or Endpoints.from_env()).state_dir / f"{STATE_DB}.sqlite"
    ).resolve()
    if not path.exists():
        return False
    try:
        conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=10)
    except sqlite3.Error:
        return False
    try:
        row = conn.execute(
            "SELECT 1 FROM violations WHERE data_id = ?", (data_id,)
        ).fetchone()
        return row is not None
    except sqlite3.Error:
        return False
    finally:
        conn.close()
