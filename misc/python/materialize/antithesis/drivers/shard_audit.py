# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Shard auditor: checks persist shards directly, below SQL.

Oracle for live-collection-shard-never-tombstoned,
live-state-references-only-existing-blobs,
source-shards-never-hold-negative-multiplicities, and the shard-level part of
historical-reads-are-stable-and-valid. SQL cannot show these: a peek over a
collection with a negative multiplicity errors (or returns an unrelated source
error first), and a missing blob surfaces only when some reader happens to
fetch it. So each audit runs two read-only `persistcli inspect` subcommands
against consensus and blob:

* `audit-blobs`: tombstone flag, since, upper, and every blob referenced by a
  state version at or above `seqno_since` that is absent from blob.
* `audit-multiplicities`: every (key, val, time) in `[since, upper)` whose
  accumulated diff is negative, comparing encoded keys and values.

Neither registers a reader, so the auditor does not hold back compaction or
GC. Both retry internally on GC races and fail (non-zero exit) when they cannot
reach a conclusion.

Live set: `mz_internal.mz_storage_shards` (keyed by global id) joined through
`mz_object_global_ids` to `mz_objects`. Every shard ever seen is recorded in
state database `shard_audit` (table `shards`) with its object id and name. A
shard missing from a later non-empty live set is marked dropped. A tombstoned
or empty-since shard is a violation only if it is in the live set both before
and after its audit, since a collection dropped in between legitimately looks
live and then tombstoned.

Consensus set: the live set is the SUT's own claim, so every audit also lists
the shard keys of the `consensus` table (`SELECT DISTINCT shard`, the same
query as persist's `PostgresConsensus::list_keys`) and audits a sample of the
shards no live collection maps: the catalog, txns and other system shards,
dropped collections awaiting finalization, and leaks. Table
`consensus_shards` records when each was first seen; the shards present at the
first enumeration are the baseline (system shards). An unmapped, untombstoned
shard that is outside the baseline and older than `FINALIZE_SETTLE_S` is
reported under its own `sometimes`: drops are finalized asynchronously, so it
is an observation for triage, not a failure.

Negative multiplicities are reported under two messages, split by whether the
shard belongs to a Postgres source export whose table had a terminal upstream
op followed by an ingestion restart (`postgres_sources.terminal_before_restart`,
read from that driver's state). That bucket holds a known unfixed bug class.

Fault handling: a persistcli failure (non-zero exit, timeout, unparsable
output) means consensus or blob was unreachable or the shard moved under the
read, and the audit is skipped. Violations are asserted only from parsed
persistcli output, so a skip never hides one.

Endpoints: consensus is the metadata Postgres with
`options=--search_path=consensus`, as environmentd derives it from
`--metadata-backend-url`. Blob is the environment's `persist_backend_url`.
`MZ_ANTITHESIS_PERSIST_CONSENSUS_URL` and `MZ_ANTITHESIS_PERSIST_BLOB_URL`
override the derived values.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    reachable,
    sometimes,
)
from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import sql, state
from materialize.antithesis.drivers import lifecycle, postgres_sources
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

STATE_DB = "shard_audit"
PERSISTCLI = os.environ.get("MZ_ANTITHESIS_PERSISTCLI", "persistcli")

# Must match `persist_backend_url` in test/antithesis/manifests/20-environment.yaml.
DEFAULT_BLOB_URL = (
    "s3://minio:minio123@persist/antithesis"
    "?endpoint=http%3A%2F%2Fminio.materialize.svc.cluster.local%3A9000&region=minio"
)

ANYTIME_SHARDS = 3
ANYTIME_UNMAPPED_SHARDS = 1
ANYTIME_TIMEOUT_S = 60
FINALLY_USER_SHARDS = 25
FINALLY_SYSTEM_SHARDS = 5
FINALLY_UNMAPPED_SHARDS = 10
FINALLY_TIMEOUT_S = 300
# Calibration: a dropped collection's shard is finalized by the storage
# controller's background task; ten minutes covers several retries of it
# under faults on one simulated core. Not yet measured.
FINALIZE_SETTLE_S = 600.0
CONSENSUS_TIMEOUT_S = 10
PG_EXPORT_NAME = re.compile(r"^pg_s\d+_t\d+_e\d+$")
MAX_REPORTED_NEGATIVES = 20
STDERR_TAIL_CHARS = 2000

LIVE_SHARDS_SQL = """
SELECT s.shard_id, o.id, o.type, o.name
FROM mz_internal.mz_storage_shards s
JOIN mz_internal.mz_object_global_ids g ON g.global_id = s.object_id
JOIN mz_objects o ON o.id = g.id
"""
TXNS_SHARD_SQL = """
SELECT data->'value'->>'shard' FROM mz_internal.mz_catalog_raw
WHERE data->>'kind' = 'TxnWalShard'
"""


def log(message: str) -> None:
    print(f"shard_audit[{os.getpid()}]: {message}", flush=True)


def consensus_url(endpoints: Endpoints) -> str:
    override = os.environ.get("MZ_ANTITHESIS_PERSIST_CONSENSUS_URL")
    if override:
        return override
    parts = urlsplit(endpoints.metadata_postgres_url)
    query = parse_qsl(parts.query)
    if not any(k == "sslmode" for k, _ in query):
        query.append(("sslmode", "disable"))
    query.append(("options", "--search_path=consensus"))
    return urlunsplit(parts._replace(query=urlencode(query)))


def blob_url() -> str:
    return os.environ.get("MZ_ANTITHESIS_PERSIST_BLOB_URL") or DEFAULT_BLOB_URL


@dataclass(frozen=True)
class LiveShard:
    shard_id: str
    object_id: str
    object_type: str
    name: str


def live_shards(host: str) -> dict[str, LiveShard] | None:
    """The live shard mapping, or None if it cannot be read right now."""
    try:
        with sql.connection(host, internal=True) as conn:
            rows = conn.execute(LIVE_SHARDS_SQL).fetchall()
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        log(f"cannot read live shards ({c.outcome.value}, {c.sqlstate}): {c.template}")
        return None
    return {
        row[0]: LiveShard(
            shard_id=row[0], object_id=row[1], object_type=row[2], name=row[3]
        )
        for row in rows
    }


def txns_shard(host: str) -> str | None:
    """The txn-wal txns shard named by the catalog, or None if it cannot be read."""
    try:
        with sql.connection(host, internal=True) as conn:
            row = conn.execute(TXNS_SHARD_SQL).fetchone()
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        log(
            f"cannot read the txns shard ({c.outcome.value}, {c.sqlstate}): {c.template}"
        )
        return None
    return row[0] if row else None


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS shards (
            shard_id TEXT PRIMARY KEY,
            object_id TEXT NOT NULL,
            object_type TEXT NOT NULL,
            first_seen REAL NOT NULL,
            first_signature TEXT,
            last_live REAL NOT NULL,
            dropped_at REAL,
            name TEXT
        );
        CREATE TABLE IF NOT EXISTS consensus_shards (
            shard_id TEXT PRIMARY KEY,
            first_seen_monotonic REAL NOT NULL,
            baseline INTEGER NOT NULL,
            finalized INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS audits (
            shard_id TEXT NOT NULL,
            at REAL NOT NULL,
            origin TEXT NOT NULL,
            outcome TEXT NOT NULL,
            missing_blobs INTEGER,
            negative_count INTEGER,
            checked_updates INTEGER
        );
        """)
    return db


def record_live(
    db: sqlite3.Connection, live: dict[str, LiveShard], signature: str | None
) -> None:
    now = time.time()
    with db:
        for shard in live.values():
            reappeared = db.execute(
                "SELECT 1 FROM shards WHERE shard_id = ? AND dropped_at IS NOT NULL",
                (shard.shard_id,),
            ).fetchone()
            if reappeared:
                log(
                    f"shard {shard.shard_id} ({shard.object_id}) is live again after being marked dropped"
                )
            db.execute(
                "INSERT INTO shards (shard_id, object_id, object_type, first_seen,"
                " first_signature, last_live, name) VALUES (?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT (shard_id) DO UPDATE SET last_live = excluded.last_live,"
                " dropped_at = NULL",
                (
                    shard.shard_id,
                    shard.object_id,
                    shard.object_type,
                    now,
                    signature,
                    now,
                    shard.name,
                ),
            )
        # An empty mapping means introspection is not populated yet, not that
        # every collection was dropped.
        if live:
            known = db.execute(
                "SELECT shard_id FROM shards WHERE dropped_at IS NULL"
            ).fetchall()
            for (shard_id,) in known:
                if shard_id not in live:
                    db.execute(
                        "UPDATE shards SET dropped_at = ? WHERE shard_id = ?",
                        (now, shard_id),
                    )


def consensus_shards(endpoints: Endpoints) -> set[str] | None:
    """Every shard key in consensus, or None if the metadata store is unreachable."""
    try:
        with psycopg.connect(
            consensus_url(endpoints),
            connect_timeout=CONSENSUS_TIMEOUT_S,
            autocommit=True,
        ) as conn:
            conn.execute(f"SET statement_timeout = '{CONSENSUS_TIMEOUT_S}s'".encode())
            rows = conn.execute("SELECT DISTINCT shard FROM consensus").fetchall()
    except (psycopg.Error, OSError) as e:
        log(f"cannot list consensus shards: {e}")
        return None
    return {str(r[0]) for r in rows}


def record_consensus(db: sqlite3.Connection, shards: set[str]) -> None:
    """Record first sightings. The first non-empty enumeration is the baseline."""
    now = time.monotonic()
    with db:
        baseline = (
            db.execute("SELECT 1 FROM consensus_shards LIMIT 1").fetchone() is None
        )
        db.executemany(
            "INSERT OR IGNORE INTO consensus_shards"
            " (shard_id, first_seen_monotonic, baseline) VALUES (?, ?, ?)",
            [(s, now, int(baseline)) for s in shards],
        )


def unmapped_candidates(
    db: sqlite3.Connection, shards: set[str], live: dict[str, LiveShard]
) -> list[str]:
    """Consensus shards no live collection maps and no audit saw tombstoned."""
    finalized = {
        r[0]
        for r in db.execute("SELECT shard_id FROM consensus_shards WHERE finalized")
    }
    return sorted(s for s in shards if s not in live and s not in finalized)


def first_signature(db: sqlite3.Connection, shard_id: str) -> str | None:
    row = db.execute(
        "SELECT first_signature FROM shards WHERE shard_id = ?", (shard_id,)
    ).fetchone()
    return row[0] if row else None


def restarted_between(before: str | None, after: str | None) -> bool:
    """Whether a pod present in both restart signatures restarted in between."""
    if before is None or after is None:
        return False
    old = {name: (uid, restarts) for name, uid, restarts in json.loads(before)}
    for name, uid, restarts in json.loads(after):
        if name in old and (old[name][0] != uid or old[name][1] < restarts):
            return True
    return False


def run_persistcli(
    subcommand: str,
    shard_id: str,
    endpoints: Endpoints,
    timeout_s: int,
    extra: list[str],
) -> dict[str, Any] | None:
    """Run one audit subcommand. None means it reached no conclusion."""
    env = {
        **os.environ,
        # Passed through the environment so credentials stay out of argv.
        "CONSENSUS_URI": consensus_url(endpoints),
        "BLOB_URI": blob_url(),
    }
    cmd = [PERSISTCLI, "inspect", subcommand, "--shard-id", shard_id, *extra]
    try:
        proc = subprocess.run(
            cmd, env=env, capture_output=True, text=True, timeout=timeout_s, check=False
        )
    except subprocess.TimeoutExpired:
        log(f"{subcommand} {shard_id}: timed out after {timeout_s}s, skipping")
        return None
    except OSError as e:
        log(f"{subcommand} {shard_id}: cannot run {PERSISTCLI}: {e}")
        return None
    if proc.returncode != 0:
        log(
            f"{subcommand} {shard_id}: exit {proc.returncode}, skipping:"
            f" {proc.stderr[-STDERR_TAIL_CHARS:]}"
        )
        return None
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    try:
        report = json.loads(lines[-1]) if lines else None
    except json.JSONDecodeError:
        report = None
    if not isinstance(report, dict) or report.get("shard_id") != shard_id:
        log(
            f"{subcommand} {shard_id}: unparsable output, skipping: {proc.stdout[-STDERR_TAIL_CHARS:]}"
        )
        return None
    return report


def choose(
    live: dict[str, LiveShard], user_count: int, system_count: int
) -> list[LiveShard]:
    user = sorted(
        (s for s in live.values() if s.object_id.startswith("u")),
        key=lambda s: s.shard_id,
    )
    other = sorted(
        (s for s in live.values() if not s.object_id.startswith("u")),
        key=lambda s: s.shard_id,
    )
    picked = rng.sample(user, min(user_count, len(user)))
    picked += rng.sample(other, min(system_count, len(other)))
    return picked


class Auditor:
    def __init__(self, origin: str, timeout_s: int) -> None:
        self.origin = origin
        self.timeout_s = timeout_s
        self.env = Environment()
        self.host = self.env.sql_host()
        self.db = open_state()
        self.txns_shard = txns_shard(self.host)

    def signature(self) -> str | None:
        return lifecycle.safe_signature(self.env)

    def record(
        self,
        shard_id: str,
        outcome: str,
        blobs: dict[str, Any] | None,
        mult: dict[str, Any] | None,
    ) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO audits VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    shard_id,
                    time.time(),
                    self.origin,
                    outcome,
                    len(blobs["missing"]) if blobs else None,
                    mult["negative_count"] if mult else None,
                    mult["checked_updates"] if mult else None,
                ),
            )

    def run(self, user_count: int, system_count: int, unmapped_count: int) -> int:
        live = live_shards(self.host)
        if live is None:
            return 0
        signature = self.signature()
        record_live(self.db, live, signature)
        targets = choose(live, user_count, system_count)
        log(f"{self.origin}: auditing {len(targets)} of {len(live)} live shards")
        for shard in targets:
            self.audit(shard, signature)

        shards = consensus_shards(self.env.endpoints)
        if shards is None:
            return 0
        record_consensus(self.db, shards)
        unmapped = unmapped_candidates(self.db, shards, live)
        picked = rng.sample(unmapped, min(unmapped_count, len(unmapped)))
        log(
            f"{self.origin}: auditing {len(picked)} of {len(unmapped)} unmapped"
            f" shards ({len(shards)} in consensus)"
        )
        for shard_id in picked:
            self.audit_unmapped(shard_id)
        return 0

    def terminal_tag(self, name: str | None, object_id: str | None) -> bool:
        """Whether the shard's Postgres export had a terminal upstream op
        followed by an ingestion restart. Unknown history counts as tagged,
        so it never pollutes the clean message."""
        if name is None or object_id is None or not PG_EXPORT_NAME.match(name):
            return False
        try:
            with sql.connection(self.host) as conn:
                tagged = postgres_sources.export_terminal_history(conn, name, object_id)
        except (psycopg.Error, OSError) as e:
            log(f"cannot read terminal history of {name}: {e}")
            return True
        return True if tagged is None else tagged

    def audit_unmapped(self, shard_id: str) -> None:
        """Audit a consensus shard that no live collection maps."""
        endpoints = self.env.endpoints
        blobs = run_persistcli("audit-blobs", shard_id, endpoints, self.timeout_s, [])
        mult = run_persistcli(
            "audit-multiplicities",
            shard_id,
            endpoints,
            self.timeout_s,
            ["--max-reported", str(MAX_REPORTED_NEGATIVES)],
        )
        known = self.db.execute(
            "SELECT object_id, object_type, name, dropped_at FROM shards"
            " WHERE shard_id = ?",
            (shard_id,),
        ).fetchone()
        seen = self.db.execute(
            "SELECT first_seen_monotonic, baseline FROM consensus_shards"
            " WHERE shard_id = ?",
            (shard_id,),
        ).fetchone()
        details: dict[str, Any] = {
            "origin": self.origin,
            "shard_id": shard_id,
            "object_id": known[0] if known else None,
            "object_type": known[1] if known else None,
            "object_name": known[2] if known else None,
            "baseline": bool(seen[1]) if seen else None,
        }
        reachable(
            "shard audit: audited a shard in consensus that no live collection maps",
            details,
        )

        if blobs is not None:
            details.update(
                tombstone=blobs.get("tombstone"),
                since=blobs.get("since"),
                upper=blobs.get("upper"),
                seqno=blobs.get("seqno"),
            )
            always(
                not blobs["missing"],
                "shard audit: every blob referenced by an unmapped shard's persist state exists",
                {
                    **details,
                    "missing": blobs["missing"][:20],
                    "missing_count": len(blobs["missing"]),
                },
            )
            if blobs.get("tombstone"):
                with self.db:
                    self.db.execute(
                        "UPDATE consensus_shards SET finalized = 1 WHERE shard_id = ?",
                        (shard_id,),
                    )
            else:
                self.report_unfinalized(known, seen, details)

        if mult is not None and mult.get("initialized"):
            tagged = self.terminal_tag(
                known[2] if known else None, known[0] if known else None
            )
            self.check_multiplicities(mult, tagged, details)

        outcome = "complete" if blobs is not None and mult is not None else "partial"
        if blobs is None and mult is None:
            outcome = "skipped"
        self.record(shard_id, outcome, blobs, mult)

    def report_unfinalized(
        self,
        known: tuple[Any, ...] | None,
        seen: tuple[Any, ...] | None,
        details: dict[str, Any],
    ) -> None:
        if seen is None or seen[1]:
            return
        if known is not None and known[3] is not None:
            unmapped_for = time.time() - float(known[3])
        elif known is None:
            unmapped_for = time.monotonic() - float(seen[0])
        else:
            # Mapped at its last sighting; the live set read now has not
            # been recorded as a drop yet.
            return
        sometimes(
            unmapped_for >= FINALIZE_SETTLE_S,
            "shard audit: a shard in consensus is neither mapped to a live collection nor finalized after the settle bound",
            {
                **details,
                "unmapped_for_s": unmapped_for,
                "settle_s": FINALIZE_SETTLE_S,
            },
        )

    def audit(self, shard: LiveShard, signature: str | None) -> None:
        endpoints = self.env.endpoints
        blobs = run_persistcli(
            "audit-blobs", shard.shard_id, endpoints, self.timeout_s, []
        )
        mult = run_persistcli(
            "audit-multiplicities",
            shard.shard_id,
            endpoints,
            self.timeout_s,
            ["--max-reported", str(MAX_REPORTED_NEGATIVES)],
        )
        details: dict[str, Any] = {
            "origin": self.origin,
            "shard_id": shard.shard_id,
            "object_id": shard.object_id,
            "object_type": shard.object_type,
            "object_name": shard.name,
        }

        if blobs is not None:
            details.update(
                tombstone=blobs.get("tombstone"),
                since=blobs.get("since"),
                upper=blobs.get("upper"),
                seqno=blobs.get("seqno"),
                seqno_since=blobs.get("seqno_since"),
            )
            always(
                not blobs["missing"],
                "shard audit: every blob referenced by live persist state exists",
                {
                    **details,
                    "missing": blobs["missing"][:20],
                    "missing_count": len(blobs["missing"]),
                },
            )
            sometimes(
                blobs.get("versions_audited", 0) > 1,
                "shard audit: blob audit covered more than one live state version",
                details,
            )
            if blobs.get("initialized"):
                self.check_not_tombstoned(shard, blobs, details)

        if mult is not None and mult.get("initialized"):
            tagged = self.terminal_tag(shard.name, shard.object_id)
            self.check_multiplicities(mult, tagged, details)
            sometimes(
                mult["checked_updates"] > 0,
                "shard audit: audited a shard with at least one update",
                details,
            )
            sometimes(
                mult["checked_updates"] > 0 and shard.object_type == "source",
                "shard audit: audited a source shard with at least one update",
                details,
            )
            sometimes(
                mult["checked_updates"] > 0
                and restarted_between(
                    first_signature(self.db, shard.shard_id), signature
                ),
                "shard audit: audited a shard with updates that existed before a Materialize pod restart",
                details,
            )

        outcome = "complete" if blobs is not None and mult is not None else "partial"
        if blobs is None and mult is None:
            outcome = "skipped"
        self.record(shard.shard_id, outcome, blobs, mult)

    def check_not_tombstoned(
        self, shard: LiveShard, blobs: dict[str, Any], details: dict[str, Any]
    ) -> None:
        bad = bool(blobs.get("tombstone")) or blobs.get("since") == []
        if bad:
            # The collection may have been dropped between enumeration and the
            # audit. Only a shard live in both reads is a violation.
            live = live_shards(self.host)
            if live is None:
                log(
                    f"{shard.shard_id}: tombstoned or empty since, cannot re-read the live set, skipping"
                )
                return
            if shard.shard_id not in live:
                log(
                    f"{shard.shard_id}: tombstoned or empty since, dropped during the audit"
                )
                return
        always(
            not bad,
            "shard audit: live collection's shard is not a tombstone and has a non-empty since",
            details,
        )

    def check_multiplicities(
        self, mult: dict[str, Any], tagged: bool, details: dict[str, Any]
    ) -> None:
        clean = mult["negative_count"] == 0
        # The txn-wal txns shard is not a multiset: `forget` retracts a
        # registration under the forget timestamp, not the registration
        # timestamp, so its accumulations go negative by design.
        if details["shard_id"] == self.txns_shard:
            return
        # Rows written under different schemas compare unequal, so a negative
        # there can be a retraction of a row inserted under the older schema.
        if not clean and len(mult.get("schema_ids", [])) > 1:
            log(
                f"{details['shard_id']}: {mult['negative_count']} negative accumulations"
                f" across schemas {mult['schema_ids']}, not asserting:"
                f" {mult['negative'][:3]}"
            )
            return
        report = {
            **details,
            "terminal_before_restart": tagged,
            "since": mult.get("since"),
            "upper": mult.get("upper"),
            "format": mult.get("format"),
            "schema_ids": mult.get("schema_ids"),
            "checked_updates": mult["checked_updates"],
            "negative_count": mult["negative_count"],
            "negative": mult["negative"],
        }
        if tagged:
            always_or_unreachable(
                clean,
                "shard audit: no (key, val) in a persist shard accumulates to a negative count at any retained time, ingestion restarted after a terminal upstream op",
                report,
            )
        else:
            always(
                clean,
                "shard audit: no (key, val) in a persist shard accumulates to a negative count at any retained time, with no terminal upstream op before the last ingestion restart",
                report,
            )


def _main(
    origin: str,
    timeout_s: int,
    user_count: int,
    system_count: int,
    unmapped_count: int,
) -> int:
    try:
        auditor = Auditor(origin, timeout_s)
    except (ApiException, OSError, RuntimeError) as e:
        log(f"cannot locate the environment: {e}")
        return 0
    return auditor.run(user_count, system_count, unmapped_count)


def anytime_main() -> int:
    """Audit a few randomly chosen shards while faults may be active."""
    user = rng.randint(1, ANYTIME_SHARDS)
    return _main(
        "anytime",
        ANYTIME_TIMEOUT_S,
        user,
        ANYTIME_SHARDS - user,
        ANYTIME_UNMAPPED_SHARDS,
    )


def finally_main() -> int:
    """Audit up to `FINALLY_USER_SHARDS` user shards, a sample of system
    shards, and a sample of unmapped consensus shards."""
    return _main(
        "finally",
        FINALLY_TIMEOUT_S,
        FINALLY_USER_SHARDS,
        FINALLY_SYSTEM_SHARDS,
        FINALLY_UNMAPPED_SHARDS,
    )
