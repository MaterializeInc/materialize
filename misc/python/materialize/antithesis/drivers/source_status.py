# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Source status honesty (A5) and restart reachability anchors.

Properties: `running-source-status-implies-progress` and anchors (2) and (3)
of `source-restarts-reach-dangerous-mid-states`.

`eventually_main` runs once faults have stopped. For every source data export
(tables from a source and old-syntax subsources, never a progress collection)
it waits for recovery, then takes `t0` from environmentd's clock and asserts:

- an export reported `running` by some replica for the whole window, and
  hydrated at `t0`, has a write frontier past `t0` within `PROGRESS_WEDGE_S`
  (`always`, wedged) and within `PROGRESS_GRACE_S` (`sometimes`, performance);
- a heartbeat written upstream at `t0` by the Kafka and Postgres drivers is
  visible in that export within the same window;
- every replica that reported a source `running` for the whole window commits
  the upstream offset it knew of at `t0`. The shard upper is shared between
  replicas, so this per-replica check is what catches a wedged replica while
  another keeps writing.

Status is read per replica from `mz_internal.mz_source_status_history`
because `mz_source_statuses` rolls replicas up with `running` taking
precedence over `stalled`.

`observe` is called by the anytime source checks while faults run. It feeds
the exploration-only anchors: an export that stalled while running and later
advanced, and an ingestion restart observed while an export was mid-snapshot
or mid-rehydration.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)

from materialize.antithesis import sql, state
from materialize.antithesis.drivers.sources_common import (
    Deadline,
    Heartbeat,
    frontiers,
    id_list,
    lookup_ids,
    mz_connect,
    mz_host,
    mz_now_ms,
)
from materialize.antithesis.drivers.sources_common import log as _log

STATE_DB = "source_status"

# Calibration: the grace period G must be measured on one simulated core during
# a fault-free run (p99 of `write_frontier` lag under the planned load) and set
# to a multiple of it. Too small fails healthy runs; too large hides short
# wedges. `discovery/sources.md` section 5 suggests 30 to 60 s plus slack.
PROGRESS_GRACE_S = 120.0
# Generous bound for the `always` (wedged) form of each progress check; the
# tight grace period above is checked with `sometimes`.
PROGRESS_WEDGE_S = 600.0

# Calibration: time for killed clusterd and environmentd pods to come back and
# rehydrate after faults stop. Exports that are not running and hydrated by
# then are excluded rather than failed.
RECOVERY_BUDGET_S = 300.0

POLL_INTERVAL_S = 2.0

# Calibration: an anytime sample counts as stalled when a running export's
# frontier has not moved for this long. Exploration only, so loose is fine.
STALL_S = 30.0

DATA_EXPORTS_SQL = """
SELECT t.id, t.source_id, s.type
FROM mz_tables t JOIN mz_sources s ON t.source_id = s.id
WHERE t.id LIKE 'u%' AND s.type <> 'webhook'
UNION ALL
SELECT sub.id, d.referenced_object_id, 'subsource'
FROM mz_sources sub
JOIN mz_internal.mz_object_dependencies d ON d.object_id = sub.id
JOIN mz_sources parent ON parent.id = d.referenced_object_id
WHERE sub.id LIKE 'u%' AND sub.type = 'subsource'
  AND parent.type IN ('kafka', 'postgres', 'mysql', 'sql-server', 'load-generator')
"""


def log(message: str) -> None:
    _log("source status", message)


@dataclass(frozen=True)
class Export:
    id: str
    source_id: str
    kind: str
    """`cdc`, `upsert`, or `other`; decides which mid-state anchor applies."""


def latest_statuses(
    conn: psycopg.Connection, ids: Iterable[str]
) -> dict[str, dict[str, tuple[str, str]]]:
    """Latest `(status, occurred_at)` per collection and live replica."""
    rows = conn.execute(
        "SELECT DISTINCT ON (h.source_id, h.replica_id)"
        " h.source_id, h.replica_id, h.status, h.occurred_at::text"
        " FROM mz_internal.mz_source_status_history h"
        " JOIN mz_cluster_replicas r ON r.id = h.replica_id"
        f" WHERE h.source_id IN ({id_list(ids)})"
        " ORDER BY h.source_id, h.replica_id, h.occurred_at DESC".encode()
    ).fetchall()
    out: dict[str, dict[str, tuple[str, str]]] = {}
    for sid, rid, status, at in rows:
        out.setdefault(str(sid), {})[str(rid)] = (str(status), str(at))
    return out


def statistics(
    conn: psycopg.Connection, ids: Iterable[str]
) -> dict[tuple[str, str], dict[str, Any]]:
    rows = conn.execute(
        "SELECT st.id, st.replica_id, st.snapshot_committed,"
        " st.rehydration_latency IS NOT NULL,"
        " st.offset_known::text, st.offset_committed::text"
        " FROM mz_internal.mz_source_statistics st"
        " JOIN mz_cluster_replicas r ON r.id = st.replica_id"
        f" WHERE st.id IN ({id_list(ids)})".encode()
    ).fetchall()
    return {
        (str(i), str(r)): {
            "snapshot_committed": bool(sc),
            "rehydrated": bool(rh),
            "offset_known": int(ok) if ok is not None else None,
            "offset_committed": int(oc) if oc is not None else None,
        }
        for i, r, sc, rh, ok, oc in rows
    }


def restart_events_since(
    conn: psycopg.Connection, ids: Iterable[str], since_epoch_s: float
) -> int:
    """Status events for `ids`, and offline events of replicas running them,
    after `since_epoch_s`.

    The health operator only deduplicates statuses within one incarnation, so
    any new event marks a status change or a restarted ingestion.
    """
    ids = list(ids)
    row = conn.execute(
        "SELECT"
        " (SELECT count(*) FROM mz_internal.mz_source_status_history"
        f"  WHERE source_id IN ({id_list(ids)}) AND occurred_at > to_timestamp(%s))"
        " + (SELECT count(*) FROM mz_internal.mz_cluster_replica_status_history h"
        "    JOIN mz_cluster_replicas r ON r.id = h.replica_id"
        "    JOIN mz_sources s ON s.cluster_id = r.cluster_id"
        f"   WHERE s.id IN ({id_list(ids)}) AND h.status = 'offline'"
        "    AND h.occurred_at > to_timestamp(%s))".encode(),
        (since_epoch_s, since_epoch_s),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS stall ("
            " export TEXT PRIMARY KEY, frontier INTEGER NOT NULL,"
            " since_rt REAL NOT NULL, stalled INTEGER NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS mid_state ("
            " export TEXT NOT NULL, kind TEXT NOT NULL, since_epoch REAL NOT NULL,"
            " PRIMARY KEY (export, kind))"
        )
    return db


def observe(conn: psycopg.Connection, exports: list[Export]) -> None:
    """Sample status, frontier, and hydration for `exports` and feed the anchors.

    Errors are swallowed: this is exploration guidance, and a failed sample
    only loses one observation.
    """
    if not exports:
        return
    try:
        _observe(conn, exports)
    except (psycopg.Error, OSError, sqlite3.Error) as e:
        log(f"observation skipped: {e}")


def _observe(conn: psycopg.Connection, exports: list[Export]) -> None:
    ids = [e.id for e in exports]
    statuses = latest_statuses(conn, ids)
    fronts = frontiers(conn, ids)
    stats = statistics(conn, ids)
    db = _open_state()
    try:
        now_rt = time.monotonic()
        now_epoch = time.time()
        for e in exports:
            running = any(s == "running" for s, _ in statuses.get(e.id, {}).values())
            upper = fronts.get(e.id, (None, None))[1]
            if running and upper is not None:
                row = db.execute(
                    "SELECT frontier, since_rt, stalled FROM stall WHERE export = ?",
                    (e.id,),
                ).fetchone()
                recovered = False
                if row is None or upper != row[0]:
                    recovered = row is not None and upper > row[0] and bool(row[2])
                    with db:
                        db.execute(
                            "INSERT OR REPLACE INTO stall VALUES (?, ?, ?, 0)",
                            (e.id, upper, now_rt),
                        )
                elif now_rt - row[1] >= STALL_S:
                    with db:
                        db.execute(
                            "UPDATE stall SET stalled = 1 WHERE export = ?", (e.id,)
                        )
                sometimes(
                    recovered,
                    "source progress: a running export stalled during faults and later advanced",
                    {"export": e.id, "upper": upper},
                )

            replica_stats = [v for (i, _), v in stats.items() if i == e.id]
            mids = []
            if e.kind == "cdc":
                mids.append(
                    (
                        "snapshot",
                        any(not v["snapshot_committed"] for v in replica_stats),
                    )
                )
            if e.kind == "upsert":
                mids.append(
                    ("rehydration", any(not v["rehydrated"] for v in replica_stats))
                )
            for kind, mid in mids:
                if not mid:
                    with db:
                        db.execute(
                            "DELETE FROM mid_state WHERE export = ? AND kind = ?",
                            (e.id, kind),
                        )
                    continue
                row = db.execute(
                    "SELECT since_epoch FROM mid_state WHERE export = ? AND kind = ?",
                    (e.id, kind),
                ).fetchone()
                if row is None:
                    with db:
                        db.execute(
                            "INSERT INTO mid_state VALUES (?, ?, ?)",
                            (e.id, kind, now_epoch),
                        )
                    continue
                restarted = restart_events_since(conn, [e.id, e.source_id], row[0]) > 0
                details = {"export": e.id, "source": e.source_id}
                if kind == "snapshot":
                    sometimes(
                        restarted,
                        "source restarts: an ingestion restarted while a CDC export was mid-snapshot",
                        details,
                    )
                else:
                    sometimes(
                        restarted,
                        "source restarts: an ingestion restarted while an upsert export was mid-rehydration",
                        details,
                    )
    finally:
        db.close()


def _data_exports(conn: psycopg.Connection) -> list[tuple[str, str, str]]:
    return [(str(i), str(s), str(t)) for i, s, t in conn.execute(DATA_EXPORTS_SQL)]


def _running_replicas(
    before: dict[str, tuple[str, str]], after: dict[str, tuple[str, str]]
) -> list[str]:
    """Replicas whose latest event was the same `running` event at both samples."""
    return [r for r, ev in before.items() if ev[0] == "running" and after.get(r) == ev]


def _hydrated(stats: dict[tuple[str, str], dict[str, Any]], export: str) -> bool:
    return any(
        v["snapshot_committed"] and v["rehydrated"]
        for (i, _), v in stats.items()
        if i == export
    )


def eventually_main() -> int:
    deadline = Deadline(RECOVERY_BUDGET_S + PROGRESS_WEDGE_S + 120.0)
    try:
        host = mz_host()
        conn = mz_connect(host)
    except (psycopg.Error, OSError, RuntimeError) as e:
        log(f"no SQL connection after faults stopped: {e}")
        return 0
    try:
        return _eventually(host, conn, deadline)
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        log(f"liveness check abandoned: {c.outcome.value} {c.sqlstate} {c.template}")
        return 0
    finally:
        conn.close()


def _eventually(host: str, conn: psycopg.Connection, deadline: Deadline) -> int:
    conn.execute("SET transaction_isolation = 'serializable'")
    exports = _data_exports(conn)
    if not exports:
        log("no source data exports")
        return 0
    export_ids = [e for e, _, _ in exports]
    source_of = {e: s for e, s, _ in exports}
    source_ids = sorted(set(source_of.values()))
    all_ids = export_ids + source_ids

    recovery = Deadline(RECOVERY_BUDGET_S)
    while not recovery.expired():
        st = latest_statuses(conn, export_ids)
        stats = statistics(conn, export_ids)
        ready = [
            e
            for e in export_ids
            if any(s == "running" for s, _ in st.get(e, {}).values())
            and _hydrated(stats, e)
        ]
        if len(ready) == len(export_ids):
            break
        time.sleep(5)

    t0 = mz_now_ms(conn)
    status0 = latest_statuses(conn, all_ids)
    stats0 = statistics(conn, all_ids)
    hydrated0 = {e: _hydrated(stats0, e) for e in export_ids}

    # The source drivers import this module for `observe`.
    from materialize.antithesis.drivers import kafka_sources, postgres_sources

    heartbeats: list[Heartbeat] = []
    try:
        heartbeats += kafka_sources.send_heartbeats(host)
    except Exception as e:
        log(f"kafka heartbeats failed: {e}")
    try:
        heartbeats += postgres_sources.send_heartbeats(host)
    except Exception as e:
        log(f"postgres heartbeats failed: {e}")

    # Replicas that had not yet committed what they knew of at t0.
    lagging = {
        key
        for key, v0 in stats0.items()
        if v0["offset_known"] is not None
        and v0["offset_committed"] is not None
        and v0["offset_committed"] < v0["offset_known"]
    }
    # Seconds from the window start until each condition was first seen.
    advanced: dict[str, float] = {}
    visible: dict[int, float] = {}
    committed: dict[tuple[str, str], float] = {}
    start = time.monotonic()
    window = Deadline(min(PROGRESS_WEDGE_S, max(deadline.remaining(), 0.0)))
    final_fronts: dict[str, tuple[int | None, int | None]] = {}
    while True:
        final_fronts = frontiers(conn, export_ids)
        for e, (_, upper) in final_fronts.items():
            if upper is not None and upper > t0:
                advanced.setdefault(e, time.monotonic() - start)
        for i, hb in enumerate(heartbeats):
            if i in visible:
                continue
            try:
                row = conn.execute(hb.query.encode(), hb.params).fetchone()
                if row is not None and int(row[0]) > 0:
                    visible[i] = time.monotonic() - start
            except psycopg.Error:
                pass
        stats = statistics(conn, all_ids)
        for key in lagging - committed.keys():
            v = stats.get(key)
            if (
                v is not None
                and v["offset_committed"] is not None
                and v["offset_committed"] >= stats0[key]["offset_known"]
            ):
                committed[key] = time.monotonic() - start
        if (
            len(advanced) == len(export_ids)
            and len(visible) == len(heartbeats)
            and lagging <= committed.keys()
        ) or window.expired():
            break
        time.sleep(POLL_INTERVAL_S)

    def in_time(took: float | None) -> bool:
        return took is not None and took <= PROGRESS_GRACE_S

    status1 = latest_statuses(conn, all_ids)
    qualified: set[str] = set()
    for e in export_ids:
        if not hydrated0[e] or final_fronts.get(e, (None, None))[1] is None:
            continue
        if not _running_replicas(status0.get(e, {}), status1.get(e, {})):
            continue
        qualified.add(e)
        details = {
            "export": e,
            "source": source_of[e],
            "t0": t0,
            "upper": final_fronts.get(e, (None, None))[1],
            "grace_s": PROGRESS_GRACE_S,
            "wedge_bound_s": PROGRESS_WEDGE_S,
            "took_s": advanced.get(e),
        }
        always(
            e in advanced,
            "source progress: a continuously running export advances its write frontier past the wall clock within the wedge bound",
            details,
        )
        sometimes(
            in_time(advanced.get(e)),
            "source progress: a continuously running export advanced its write frontier past the wall clock within the tight grace period",
            details,
        )

    names = lookup_ids(conn, [hb.export_name for hb in heartbeats])
    for i, hb in enumerate(heartbeats):
        eid = names.get(hb.export_name)
        if eid is None or eid not in qualified:
            continue
        details = {
            "export": hb.export_name,
            "t0": t0,
            "grace_s": PROGRESS_GRACE_S,
            "wedge_bound_s": PROGRESS_WEDGE_S,
            "took_s": visible.get(i),
        }
        always(
            i in visible,
            "source progress: a continuously running export reflects a fresh upstream change within the wedge bound",
            details,
        )
        sometimes(
            in_time(visible.get(i)),
            "source progress: a continuously running export reflected a fresh upstream change within the tight grace period",
            details,
        )

    for cid, rid in sorted(lagging):
        v0 = stats0[(cid, rid)]
        before = status0.get(cid, {}).get(rid)
        if before is None or before[0] != "running":
            continue
        if status1.get(cid, {}).get(rid) != before:
            continue
        details = {
            "collection": cid,
            "replica": rid,
            "offset_known_t0": v0["offset_known"],
            "offset_committed_t0": v0["offset_committed"],
            "grace_s": PROGRESS_GRACE_S,
            "wedge_bound_s": PROGRESS_WEDGE_S,
            "took_s": committed.get((cid, rid)),
        }
        always(
            (cid, rid) in committed,
            "source progress: each replica running a source commits the upstream offset it knew of within the wedge bound",
            details,
        )
        sometimes(
            in_time(committed.get((cid, rid))),
            "source progress: a replica running a source committed the upstream offset it knew of within the tight grace period",
            details,
        )

    sometimes(
        len(qualified) > 0,
        "source progress: the liveness check evaluated a continuously running export",
        {"exports": len(export_ids), "qualified": len(qualified)},
    )
    log(
        f"{len(qualified)}/{len(export_ids)} exports qualified,"
        f" {len(advanced)} advanced, {len(visible)}/{len(heartbeats)} heartbeats visible"
    )
    return 0
