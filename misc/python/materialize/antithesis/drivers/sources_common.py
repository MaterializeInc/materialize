# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Materialize-side objects and query helpers shared by the source drivers.

Setup is idempotent: `first_configure` runs each source driver's `setup_main`
before any fault, and drivers and checks repair whatever is missing through
`with_retry` before they need an object. Concurrent creators race on the
catalog, and a duplicate-object rejection means another driver won.

Every source table is created with `RETAIN HISTORY`, which needs
`enable_logical_compaction_window`. Without retained history the since of a
source trails its upper by about a second and there is no past `AS OF T` to
sample.
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import psycopg

from materialize.antithesis import sql
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

SOURCES_CLUSTER = "antithesis_sources"
CLUSTER_SIZES = ("antithesis-1", "antithesis-2", "antithesis-4")
REPLICATION_FACTORS = (1, 2)

RETAIN_HISTORY_CHOICES = ("30s", "2m", "10m")
"""Per-source retained history. Short windows put sampled `AS OF` times close
to the since, where compaction races the read; long ones exercise reads deep in
history."""

# Calibration: compaction advances the since roughly once per second, so a
# read at exactly the since loses the race most of the time. Two seconds of
# margin keeps the "oldest" draw usable without measuring the cadence.
SINCE_MARGIN_MS = 2_000

CONNECT_DEADLINE_S = 30.0
STATEMENT_TIMEOUT_MS = 30_000

SETUP_BACKOFF_BASE_S = 1.0
SETUP_BACKOFF_MAX_S = 8.0
# Lazy setup repair in drivers and checks runs under faults and must leave
# most of their budget for their own work.
LAZY_SETUP_BUDGET_S = 45.0

_ID = re.compile(r"^[us]\d+$")
_NAME = re.compile(r"^[a-z0-9_]+$")

SOURCE_ERROR_MARKERS = (
    "source must be dropped and recreated",
    "table was truncated",
    "table was dropped",
    "incompatible schema change",
)
"""Substrings of the persisted definite source errors (`SourceErrorDetails`
display and the Postgres `DefiniteError` variants the workload provokes)."""


def log(prefix: str, message: str) -> None:
    print(f"{prefix}: {message}", flush=True)


def mz_host() -> str:
    return Environment().sql_host()


def mz_connect(host: str, **kwargs: Any) -> psycopg.Connection:
    kwargs.setdefault("statement_timeout_ms", STATEMENT_TIMEOUT_MS)
    return sql.connect_with_retry(host, CONNECT_DEADLINE_S, **kwargs)


def id_list(ids: Iterable[str]) -> str:
    """A SQL `IN` list of catalog ids, validated so they can be inlined."""
    items = sorted(set(ids))
    for i in items:
        if not _ID.match(i):
            raise ValueError(f"not a catalog id: {i!r}")
    return ", ".join(f"'{i}'" for i in items) or "NULL"


def name_list(names: Iterable[str]) -> str:
    items = sorted(set(names))
    for n in items:
        if not _NAME.match(n):
            raise ValueError(f"not a workload object name: {n!r}")
    return ", ".join(f"'{n}'" for n in items) or "NULL"


def timeline_choice(db: sqlite3.Connection, name: str, choices: Sequence[Any]) -> Any:
    """A value drawn once per timeline and stored, so every invocation agrees."""
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS timeline_params"
            " (name TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        db.execute(
            "INSERT OR IGNORE INTO timeline_params VALUES (?, ?)",
            (name, json.dumps(rng.choice(list(choices)))),
        )
    row = db.execute(
        "SELECT value FROM timeline_params WHERE name = ?", (name,)
    ).fetchone()
    return json.loads(row[0])


def ensure_retain_history(host: str, endpoints: Endpoints | None = None) -> None:
    """Enable `RETAIN HISTORY` once per timeline. Failures are retried by the next caller."""
    marker = (endpoints or Endpoints.from_env()).state_dir / "sources-retain-history"
    if marker.exists():
        return
    try:
        with sql.connection(host, internal=True, connect_timeout=10) as conn:
            conn.execute("ALTER SYSTEM SET enable_logical_compaction_window = true")
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("on")
    except (psycopg.Error, OSError) as e:
        log("sources", f"enabling RETAIN HISTORY failed: {e}")


def ensure_cluster(conn: psycopg.Connection, db: sqlite3.Connection) -> None:
    """Create the shared source cluster with a per-timeline size and replication factor."""
    row = conn.execute(
        "SELECT 1 FROM mz_clusters WHERE name = %s", (SOURCES_CLUSTER,)
    ).fetchone()
    if row is not None:
        return
    size = timeline_choice(db, "cluster_size", CLUSTER_SIZES)
    rf = timeline_choice(db, "cluster_replication_factor", REPLICATION_FACTORS)
    assert size in CLUSTER_SIZES and rf in REPLICATION_FACTORS
    try:
        conn.execute(
            f"CREATE CLUSTER {SOURCES_CLUSTER}"
            f" (SIZE = '{size}', REPLICATION FACTOR = {int(rf)})".encode()
        )
    except psycopg.Error as e:
        if sql.classify(e).race is not sql.CatalogRace.EXISTS:
            raise


def lookup_ids(conn: psycopg.Connection, names: Iterable[str]) -> dict[str, str]:
    """Catalog ids of workload objects in `materialize.public`, by name."""
    names = list(names)
    if not names:
        return {}
    rows = conn.execute(
        "SELECT o.name, o.id FROM mz_objects o"
        " JOIN mz_schemas s ON o.schema_id = s.id"
        " JOIN mz_databases d ON s.database_id = d.id"
        f" WHERE d.name = 'materialize' AND s.name = 'public' AND o.name IN ({name_list(names)})".encode()
    ).fetchall()
    return {str(n): str(i) for n, i in rows}


def frontiers(
    conn: psycopg.Connection, ids: Iterable[str]
) -> dict[str, tuple[int | None, int | None]]:
    """`(since, upper)` per collection. `None` is the empty frontier."""
    rows = conn.execute(
        "SELECT object_id, read_frontier::text, write_frontier::text"
        f" FROM mz_internal.mz_frontiers WHERE object_id IN ({id_list(ids)})".encode()
    ).fetchall()
    return {
        str(i): (int(r) if r is not None else None, int(w) if w is not None else None)
        for i, r, w in rows
    }


def pick_as_of(
    sinces: Iterable[int | None], uppers: Iterable[int | None]
) -> int | None:
    """A timestamp every collection can serve: at or above each since, below each upper.

    Menu: the newest readable time, the oldest time with a margin above the
    since, and a uniform draw between them.
    """
    sinces = list(sinces)
    uppers = list(uppers)
    if not sinces or any(s is None for s in sinces) or any(u is None for u in uppers):
        return None
    since = max(s for s in sinces if s is not None)
    upper = min(u for u in uppers if u is not None)
    hi = upper - 1
    if hi < since:
        return None
    lo = min(since + SINCE_MARGIN_MS, hi)
    return rng.choice([hi, lo, rng.randint(lo, hi)])


def is_since_race(error: BaseException) -> bool:
    """The sampled `AS OF` fell below a since that compaction advanced meanwhile."""
    c = sql.classify_as_of_read(error)
    return c.outcome is sql.Outcome.REJECTED and c.sqlstate == "22000"


def source_error_text(error: BaseException) -> str | None:
    """The message if `error` is a definite source error read out of a collection."""
    if not isinstance(error, psycopg.Error) or error.sqlstate != "XX000":
        return None
    message = str(error)
    if any(m in message for m in SOURCE_ERROR_MARKERS):
        return message.strip().splitlines()[0]
    return None


def mz_now_ms(conn: psycopg.Connection) -> int:
    row = conn.execute("SELECT (extract(epoch FROM now()) * 1000)::int8").fetchone()
    assert row is not None
    return int(row[0])


def record_progress_observation(
    db: sqlite3.Connection, key: str, t: int, frontier: dict[int, int]
) -> list[dict[str, Any]]:
    """Store `frontier` as the progress at mz time `t` and return earlier
    observations of `key` it is not ordered with.

    The upstream frontier recorded in a progress collection must be monotone in
    mz time: componentwise, F(t1) <= F(t2) whenever t1 <= t2. Missing
    components count as zero.
    """
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS progress_observations"
            " (key TEXT NOT NULL, t INTEGER NOT NULL, frontier TEXT NOT NULL)"
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS progress_observations_key"
            " ON progress_observations (key, t)"
        )
    rows = db.execute(
        "SELECT t, frontier FROM progress_observations WHERE key = ?", (key,)
    ).fetchall()
    violations = []
    for other_t, other_json in rows:
        other = {int(p): int(o) for p, o in json.loads(other_json).items()}
        parts = set(other) | set(frontier)
        if other_t <= t:
            ok = all(other.get(p, 0) <= frontier.get(p, 0) for p in parts)
        else:
            ok = all(frontier.get(p, 0) <= other.get(p, 0) for p in parts)
        if not ok:
            violations.append({"t": other_t, "frontier": other})
    with db:
        db.execute(
            "INSERT INTO progress_observations VALUES (?, ?, ?)",
            (key, t, json.dumps({str(p): o for p, o in frontier.items()})),
        )
        # Bound the table: keep the most recent observations per key.
        db.execute(
            "DELETE FROM progress_observations WHERE key = ? AND rowid NOT IN"
            " (SELECT rowid FROM progress_observations WHERE key = ?"
            "  ORDER BY rowid DESC LIMIT 500)",
            (key, key),
        )
    return violations[:5]


def bag_diff(
    expected: Iterable[tuple], observed: Iterable[tuple], limit: int = 10
) -> dict[str, int | list]:
    """Multiset difference, truncated for assertion details."""
    e = Counter(expected)
    o = Counter(observed)
    missing = list((e - o).elements())
    extra = list((o - e).elements())
    return {
        "missing_count": len(missing),
        "extra_count": len(extra),
        "missing": [list(x) for x in missing[:limit]],
        "extra": [list(x) for x in extra[:limit]],
    }


@dataclass(frozen=True)
class Heartbeat:
    """An upstream change written for the liveness check, and how to see it.

    `query` returns one row whose first column is positive once the change is
    visible in `export_name`.
    """

    export_name: str
    query: str
    params: tuple


class Deadline:
    def __init__(self, seconds: float) -> None:
        self.end = time.monotonic() + seconds

    def remaining(self) -> float:
        return self.end - time.monotonic()

    def expired(self) -> bool:
        return self.remaining() <= 0


@dataclass(frozen=True)
class SetupFailure:
    """The last error of a setup step that `with_retry` gave up on."""

    step: str
    classified: sql.Classified
    retryable: bool
    """False if the error was one retrying cannot fix, so it ended the retries."""

    def details(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "outcome": self.classified.outcome.value,
            "sqlstate": self.classified.sqlstate,
            "template": self.classified.template,
        }


def with_retry(
    prefix: str,
    step: str,
    attempt: Callable[[], None],
    deadline: Deadline,
    retryable: Callable[[BaseException], bool],
) -> SetupFailure | None:
    """Run an idempotent setup step until it succeeds, fails with an error
    `retryable` rejects, or `deadline` leaves no room for another try.

    Returns None on success. `deadline` is checked between attempts only, so
    the step can overrun it by the length of one attempt.
    """
    delay = SETUP_BACKOFF_BASE_S
    while True:
        try:
            attempt()
            return None
        except Exception as e:
            c = sql.classify(e)
            failure = f"{step} failed: {c.sqlstate} {c.template}"
            if not retryable(e):
                log(prefix, f"{failure}; not retryable ({c.outcome.value})")
                return SetupFailure(step, c, False)
            wait = delay * rng.uniform(0.5, 1.0)
            if deadline.remaining() <= wait:
                log(prefix, f"{failure}; giving up at the setup deadline")
                return SetupFailure(step, c, True)
            log(prefix, f"{failure}; retrying in {wait:.1f}s")
            time.sleep(wait)
            delay = min(delay * 2, SETUP_BACKOFF_MAX_S)
