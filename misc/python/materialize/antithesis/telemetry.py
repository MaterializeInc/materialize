# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Bounded workload measurements and semantic coverage shared across commands."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import resource
import runpy
import sqlite3
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import psycopg

from materialize.antithesis import (
    exit_audit,
    ivm_model,
    observations,
    quiet,
    sql,
    state,
)

PROFILE_ENV = "MZ_ANTITHESIS_TEMPLATE_PROFILE"
STATE_DB = "telemetry"
EVENT_CAP = 4096
EVENT_RETENTION_S = 3600.0
PHASE_SAMPLE_S = 5.0
ACTIVE_CAP = 256
HEALTH_DIAGNOSTIC_CAPACITY = 128

_FAMILIES = frozenset(
    {
        "history",
        "aggregate_retractions",
        "atomic_commits",
        "bounded_subscribe",
        "index_handover",
        "counters",
        "read_paths",
        "ivm",
        "postgres",
        "postgres_sources",
        "kafka_sources",
        "kafka_sinks",
        "schema_evolution",
        "access_control",
        "canaries",
        "recovery",
        "shard_audit",
        "lifecycle",
        "append",
        "rmw",
        "other",
    }
)
_PATHS = frozenset(
    {
        "",
        "history",
        "counter",
        "table",
        "log",
        "ledger",
        "export",
        "full",
        "point",
        "incremental",
        "full_ledger",
        "incremental_ledger",
        "subscribe",
        "snapshot",
        "index",
        "persist",
        "recompute",
        "replica",
        "mv",
        "view",
        "join_mv",
        "other",
        *(f"P{i}" for i in range(1, 8)),
        *ivm_model.VIEW_TAGS,
    }
)
_PHASES = frozenset(
    {
        "",
        "Applying",
        "ReadyToPromote",
        "Promoting",
        "Applied",
        "WaitingForApproval",
        "FailedDeploy",
        "RolloutTimeout",
        "other",
    }
)
_ROLES = frozenset(
    {
        "",
        "environmentd",
        "environmentd_active",
        "environmentd_candidate",
        "clusterd",
        "orchestratord",
        "other",
    }
)
_OUTCOMES = frozenset({"acked", "indeterminate", "other"})
_T = TypeVar("_T")


@dataclass
class Recorder:
    profile: str
    command: str
    lock: threading.Lock = field(default_factory=threading.Lock)
    counts: Counter[tuple[str, str, str]] = field(default_factory=Counter)
    observations: Counter[tuple[str, bool]] = field(default_factory=Counter)
    harness_errors: int = 0
    health_diagnostics: Counter[tuple[str, str, str]] = field(default_factory=Counter)
    calls: int = 0
    metric_errors: int = 0
    phase_sample: tuple[tuple[str, int | None, str | None], float] | None = None
    cluster_ids: dict[tuple[str, str], str] = field(default_factory=dict)
    interleaving_round: str | None = None
    interleaving_details: dict[str, Any] = field(default_factory=dict)


_recorder: Recorder | None = None
_command_lock = threading.Lock()


@dataclass(frozen=True)
class Event:
    kind: str
    started: float
    completed: float
    phase: str = ""
    role: str = ""
    cluster_id: str = ""


def enabled() -> bool:
    """Whether this process is running a measured test command."""
    return _recorder is not None


@contextmanager
def interleaving(mode: str, **details: Any) -> Iterator[str | None]:
    """Correlate protocol diagnostics within the current measured command."""
    recorder = _recorder
    if recorder is None:
        yield None
        return
    token = f"{os.getpid()}:{time.monotonic_ns()}"
    with recorder.lock:
        previous = recorder.interleaving_round
        previous_details = recorder.interleaving_details
        recorder.interleaving_round = token
        recorder.interleaving_details = {"mode": mode, **copy.deepcopy(details)}
    try:
        interleaving_progress("start", mode=mode, **details)
        yield token
    finally:
        with recorder.lock:
            recorder.interleaving_round = previous
            recorder.interleaving_details = previous_details


def interleaving_progress(
    stage: str, *, expected_round: str | None = None, **details: Any
) -> None:
    recorder = _recorder
    if recorder is None:
        return
    with recorder.lock:
        token = recorder.interleaving_round
        metadata = copy.deepcopy(recorder.interleaving_details)
    if token is None or (expected_round is not None and token != expected_round):
        return
    observations.send_event(
        "antithesis_interleaving",
        {
            "profile": recorder.profile,
            "command": recorder.command,
            "round": token,
            "stage": stage,
            **metadata,
            **details,
        },
    )


def _label(value: str, allowed: frozenset[str]) -> str:
    return value if value in allowed else "other"


def _path(value: str) -> str:
    if value.startswith("replica:"):
        return "replica"
    if value.startswith("canary:"):
        value = value.removeprefix("canary:")
    return _label(value, _PATHS)


def _failed(operation: str, error: Exception) -> None:
    recorder = _recorder
    if recorder is not None:
        with recorder.lock:
            recorder.metric_errors += 1
    print(
        f"telemetry: {operation}: {type(error).__name__}", file=sys.stderr, flush=True
    )
    observations.harness_error(
        "telemetry",
        operation,
        {"error": type(error).__name__},
    )


def _open() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS commands (
            profile TEXT, command TEXT,
            started INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0, interrupted INTEGER NOT NULL DEFAULT 0,
            no_comparison INTEGER NOT NULL DEFAULT 0, uninstrumented INTEGER NOT NULL DEFAULT 0,
            wall_s REAL NOT NULL DEFAULT 0, cpu_s REAL NOT NULL DEFAULT 0,
            child_cpu_s REAL NOT NULL DEFAULT 0, metric_errors INTEGER NOT NULL DEFAULT 0,
            harness_errors INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (profile, command));
        CREATE TABLE IF NOT EXISTS active_commands (
            invocation TEXT PRIMARY KEY, profile TEXT, command TEXT,
            pid INTEGER NOT NULL, token TEXT NOT NULL, started REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS comparisons (
            profile TEXT, command TEXT, family TEXT, path TEXT, outcome TEXT,
            units INTEGER NOT NULL, PRIMARY KEY (profile, command, family, path, outcome));
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY, profile TEXT, kind TEXT, started REAL, completed REAL,
            phase TEXT NOT NULL, role TEXT NOT NULL, event_key TEXT NOT NULL,
            cluster_id TEXT NOT NULL DEFAULT '',
            UNIQUE (profile, kind, event_key));
        CREATE INDEX IF NOT EXISTS event_times ON events (profile, completed, started);
        CREATE TABLE IF NOT EXISTS coverage (
            profile TEXT, phase TEXT, role TEXT, family TEXT, path TEXT, outcome TEXT,
            hits INTEGER NOT NULL, PRIMARY KEY (profile, phase, role, family, path, outcome));
        CREATE TABLE IF NOT EXISTS observations (
            profile TEXT, command TEXT, message TEXT, observed INTEGER,
            evaluations INTEGER NOT NULL,
            PRIMARY KEY (profile, command, message, observed));
        CREATE TABLE IF NOT EXISTS harness_diagnostics (
            profile TEXT, command TEXT, component TEXT, message TEXT, details TEXT,
            occurrences INTEGER NOT NULL,
            PRIMARY KEY (profile, command, component, message, details));
    """)
    if "cluster_id" not in {row[1] for row in db.execute("PRAGMA table_info(events)")}:
        db.execute("BEGIN IMMEDIATE")
        if "cluster_id" not in {
            row[1] for row in db.execute("PRAGMA table_info(events)")
        }:
            db.execute(
                "ALTER TABLE events ADD COLUMN cluster_id TEXT NOT NULL DEFAULT ''"
            )
        db.commit()
    return db


def _db_call(
    operation: str, fn: Callable[[sqlite3.Connection], _T], fallback: _T
) -> _T:
    db = None
    try:
        db = _open()
        result = fn(db)
        db.commit()
        return result
    except Exception as e:
        _failed(operation, e)
        return fallback
    finally:
        if db is not None:
            try:
                db.close()
            except Exception as e:
                _failed(f"{operation} cleanup", e)


def _process_token(pid: int) -> str | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except (FileNotFoundError, ProcessLookupError):
        return None
    if fields[0] == "Z":
        return None
    return fields[19]


def _reap(db: sqlite3.Connection) -> None:
    for invocation, profile, command, pid, token in db.execute(
        "SELECT invocation, profile, command, pid, token FROM active_commands"
    ).fetchall():
        if _process_token(pid) == token:
            continue
        db.execute(
            "UPDATE commands SET interrupted = interrupted + 1 WHERE profile = ? AND command = ?",
            (profile, command),
        )
        db.execute("DELETE FROM active_commands WHERE invocation = ?", (invocation,))


def _prune_events(db: sqlite3.Connection, now: float) -> None:
    removed = db.execute(
        "DELETE FROM events WHERE completed < ?", (now - EVENT_RETENTION_S,)
    ).rowcount
    removed += db.execute(
        "DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT ?)",
        (EVENT_CAP,),
    ).rowcount
    if removed:
        db.execute(
            "INSERT INTO metadata VALUES ('events_pruned', ?)"
            " ON CONFLICT (key) DO UPDATE SET value = value + excluded.value",
            (removed,),
        )


def comparison(family: str, path: str, passed: bool | None, units: int = 1) -> None:
    """Count evaluated and skipped comparison units within the current command."""
    recorder = _recorder
    if recorder is None:
        return
    if type(units) is not int or units < 0:
        _failed(
            "comparison", ValueError("comparison units must be nonnegative integers")
        )
        return
    outcome = "skipped" if passed is None else "passed" if passed else "failed"
    key = (_label(family, _FAMILIES), _path(path), outcome)
    with recorder.lock:
        recorder.counts[key] += units
        recorder.calls += 1


def phase_observed(
    phase: str | None,
    generation: int | None,
    request: str | None,
    *,
    started: float | None = None,
    force: bool = False,
) -> None:
    """Record an observed rollout phase, throttling repeated samples of one state."""
    recorder = _recorder
    if recorder is None:
        return
    phase = _label(phase or "", _PHASES)
    now = time.monotonic()
    observed_start = now if started is None else started
    if observed_start > now:
        _failed(
            "phase observation",
            ValueError("phase observation completed before it started"),
        )
        return
    key = (phase, generation, request)
    with recorder.lock:
        previous = recorder.phase_sample
        if (
            not force
            and previous is not None
            and previous[0] == key
            and now - previous[1] < PHASE_SAMPLE_S
        ):
            return
        recorder.phase_sample = key, now
    digest = hashlib.sha256(repr(key).encode()).hexdigest()

    def record(db: sqlite3.Connection) -> None:
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute(
            "SELECT event_key, completed FROM events WHERE profile = ? AND kind = 'phase' ORDER BY id DESC LIMIT 1",
            (recorder.profile,),
        ).fetchone()
        if (
            force
            or previous is None
            or not (
                previous[0].startswith(digest) and now - previous[1] < PHASE_SAMPLE_S
            )
        ):
            db.execute(
                "INSERT OR IGNORE INTO events (profile, kind, started, completed, phase, role, event_key) VALUES (?, 'phase', ?, ?, ?, '', ?)",
                (recorder.profile, observed_start, now, phase, f"{digest}:{now.hex()}"),
            )
        _prune_events(db, now)

    _db_call("phase observation", record, None)


def fault_observed(
    role: str, started: float, completed: float, key: str, *, cluster_id: str = ""
) -> None:
    """Record one acknowledged harness fault, deduplicated by its durable key."""
    recorder = _recorder
    if recorder is None:
        return
    if completed < started:
        _failed("fault observation", ValueError("fault completed before it started"))
        return
    digest = hashlib.sha256(key.encode()).hexdigest()

    def record(db: sqlite3.Connection) -> None:
        db.execute(
            "INSERT OR IGNORE INTO events (profile, kind, started, completed, phase, role, event_key, cluster_id) VALUES (?, 'fault', ?, ?, '', ?, ?, ?)",
            (
                recorder.profile,
                started,
                completed,
                _label(role, _ROLES),
                digest,
                cluster_id,
            ),
        )
        _prune_events(db, time.monotonic())

    _db_call("fault observation", record, None)


def resolve_cluster_id(host: str, name: str) -> str | None:
    """Resolve an oracle's cluster identity, caching successful lookups for this command."""
    recorder = _recorder
    if recorder is None:
        return None
    key = (host, name)
    with recorder.lock:
        known = recorder.cluster_ids.get(key)
    if known is not None:
        return known
    try:
        with sql.connection(
            host, connect_timeout=3, statement_timeout_ms=3_000
        ) as conn:
            row = conn.execute(
                "SELECT id FROM mz_catalog.mz_clusters WHERE name = %s", (name,)
            ).fetchone()
    except (psycopg.Error, OSError):
        return None
    if row is None:
        return None
    result = str(row[0])
    with recorder.lock:
        recorder.cluster_ids[key] = result
    return result


def events_between(start: float, end: float) -> list[Event]:
    """Return retained observations that overlap the requested interval."""
    recorder = _recorder
    if recorder is None:
        return []

    def read(db: sqlite3.Connection) -> list[Event]:
        _prune_events(db, time.monotonic())
        return [
            Event(*row)
            for row in db.execute(
                "SELECT kind, started, completed, phase, role, cluster_id FROM events"
                " WHERE profile = ? AND started <= ? AND completed >= ? ORDER BY started, id",
                (recorder.profile, end, start),
            )
        ]

    return _db_call("read observations", read, [])


def observation(message: str, condition: bool) -> None:
    """Count every evaluated coverage target, including false conditions."""
    recorder = _recorder
    if recorder is not None:
        with recorder.lock:
            recorder.observations[(message, condition)] += 1


def record_harness_error(record: Mapping[str, Any]) -> None:
    """Count invalidity in memory without depending on the failing database."""
    recorder = _recorder
    if recorder is not None:
        with recorder.lock:
            recorder.harness_errors += 1
            key = (
                str(record["component"]),
                str(record["message"]),
                json.dumps(record["details"], sort_keys=True),
            )
            if (
                key in recorder.health_diagnostics
                or len(recorder.health_diagnostics) < HEALTH_DIAGNOSTIC_CAPACITY
            ):
                recorder.health_diagnostics[key] += 1


def record_coverage(cases: Iterable[tuple[str, str, str, str, str]]) -> None:
    """Aggregate verified combinations without retaining raw operation histories."""
    recorder = _recorder
    if recorder is None:
        return
    counts = Counter(
        (
            recorder.profile,
            _label(phase, _PHASES),
            _label(role, _ROLES),
            _label(family, _FAMILIES),
            _path(path),
            _label(outcome, _OUTCOMES),
        )
        for phase, role, family, path, outcome in cases
    )
    if not counts:
        return

    def record(db: sqlite3.Connection) -> None:
        db.executemany(
            "INSERT INTO coverage VALUES (?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT (profile, phase, role, family, path, outcome)"
            " DO UPDATE SET hits = hits + excluded.hits",
            ((*key, n) for key, n in counts.items()),
        )

    _db_call("record coverage", record, None)


def _begin(recorder: Recorder, started: float) -> str | None:
    pid = os.getpid()
    try:
        token = _process_token(pid)
    except Exception as e:
        _failed("start command", e)
        return None
    if token is None:
        _failed("start command", RuntimeError("current process has no start token"))
        return None
    invocation = f"{pid}:{token}:{started.hex()}"

    def record(db: sqlite3.Connection) -> str:
        db.execute("BEGIN IMMEDIATE")
        _reap(db)
        if (
            db.execute("SELECT count(*) FROM active_commands").fetchone()[0]
            >= ACTIVE_CAP
        ):
            raise RuntimeError("too many active measured commands")
        db.execute(
            "INSERT INTO metadata VALUES ('started', ?) ON CONFLICT (key) DO UPDATE SET value = min(value, excluded.value)",
            (started,),
        )
        db.execute(
            "INSERT INTO commands (profile, command, started) VALUES (?, ?, 1)"
            " ON CONFLICT (profile, command) DO UPDATE SET started = started + 1",
            (recorder.profile, recorder.command),
        )
        db.execute(
            "INSERT INTO active_commands VALUES (?, ?, ?, ?, ?, ?)",
            (invocation, recorder.profile, recorder.command, pid, token, started),
        )
        _prune_events(db, started)
        return invocation

    return _db_call("start command", record, None)


def _finish(
    recorder: Recorder,
    invocation: str | None,
    status: str,
    wall_s: float,
    cpu_s: float,
    child_cpu_s: float,
) -> None:
    if invocation is None:
        return
    with recorder.lock:
        counts = dict(recorder.counts)
        calls = recorder.calls
        metric_errors = recorder.metric_errors
        observation_counts = dict(recorder.observations)
        harness_errors = recorder.harness_errors
        health_diagnostics = dict(recorder.health_diagnostics)
    checked = sum(n for (_, _, outcome), n in counts.items() if outcome != "skipped")
    checker = (
        recorder.command.startswith(("anytime_", "eventually_", "finally_"))
        and "workload_efficiency" not in recorder.command
    )

    def record(db: sqlite3.Connection) -> None:
        db.execute("BEGIN IMMEDIATE")
        db.execute("DELETE FROM active_commands WHERE invocation = ?", (invocation,))
        db.execute(
            "UPDATE commands SET completed = completed + ?, errors = errors + ?, interrupted = interrupted + ?,"
            " no_comparison = no_comparison + ?, uninstrumented = uninstrumented + ?,"
            " wall_s = wall_s + ?, cpu_s = cpu_s + ?, child_cpu_s = child_cpu_s + ?, metric_errors = metric_errors + ?, harness_errors = harness_errors + ?"
            " WHERE profile = ? AND command = ?",
            (
                int(status == "completed"),
                int(status == "error"),
                int(status == "interrupted"),
                int(checker and status == "completed" and checked == 0),
                int(checker and status == "completed" and calls == 0),
                wall_s,
                cpu_s,
                child_cpu_s,
                metric_errors,
                harness_errors,
                recorder.profile,
                recorder.command,
            ),
        )
        db.executemany(
            "INSERT INTO comparisons VALUES (?, ?, ?, ?, ?, ?)"
            " ON CONFLICT (profile, command, family, path, outcome) DO UPDATE SET units = units + excluded.units",
            (
                (recorder.profile, recorder.command, *key, n)
                for key, n in counts.items()
            ),
        )
        db.executemany(
            "INSERT INTO observations VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT (profile, command, message, observed)"
            " DO UPDATE SET evaluations = evaluations + excluded.evaluations",
            (
                (recorder.profile, recorder.command, message, condition, count)
                for (message, condition), count in observation_counts.items()
            ),
        )
        diagnostic_count = db.execute(
            "SELECT count(*) FROM harness_diagnostics"
        ).fetchone()[0]
        for (component, message, details), count in health_diagnostics.items():
            key = (recorder.profile, recorder.command, component, message, details)
            updated = db.execute(
                "UPDATE harness_diagnostics SET occurrences = occurrences + ?"
                " WHERE profile = ? AND command = ? AND component = ? AND message = ? AND details = ?",
                (count, *key),
            ).rowcount
            if not updated and diagnostic_count < HEALTH_DIAGNOSTIC_CAPACITY:
                db.execute(
                    "INSERT INTO harness_diagnostics VALUES (?, ?, ?, ?, ?, ?)",
                    (*key, count),
                )
                diagnostic_count += 1

    _db_call("finish command", record, None)


def _child_cpu() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def _audit_exit_observer() -> None:
    try:
        exit_audit.main()
    except Exception as error:
        _failed("exit observer audit", error)
        observations.send_event(
            "harness_health", _unavailable_health("exit observer audit failed")
        )


def run_command(profile: str, source: str | Path) -> int:
    """Run an authoritative command, preserving its result and measuring its work."""
    global _recorder
    if not _command_lock.acquire(blocking=False):
        raise RuntimeError("measured commands cannot nest within one process")
    recorder = Recorder(profile, Path(source).name)
    previous_profile = os.environ.get(PROFILE_ENV)
    _recorder = recorder
    os.environ[PROFILE_ENV] = profile
    started, cpu, child_cpu = time.monotonic(), time.process_time(), _child_cpu()
    invocation = _begin(recorder, started)
    status = "error"
    try:
        _audit_exit_observer()
        try:
            runpy.run_path(str(source), run_name="__main__")
            code = 0
        except SystemExit as e:
            if e.code is None:
                code = 0
            elif isinstance(e.code, int):
                code = e.code
            else:
                print(e.code, file=sys.stderr)
                code = 1
        except (KeyboardInterrupt, GeneratorExit):
            status = "interrupted"
            raise
        status = "completed" if code == 0 else "error"
        return code
    finally:
        try:
            _audit_exit_observer()
            _finish(
                recorder,
                invocation,
                status,
                max(0.0, time.monotonic() - started),
                max(0.0, time.process_time() - cpu),
                max(0.0, _child_cpu() - child_cpu),
            )
        finally:
            _recorder = None
            if previous_profile is None:
                os.environ.pop(PROFILE_ENV, None)
            else:
                os.environ[PROFILE_ENV] = previous_profile
            _command_lock.release()


def _unavailable_health(reason: str) -> dict[str, Any]:
    try:
        first_error = observations.durable_health()
    except Exception as error:
        first_error = {
            "valid": False,
            "component": "telemetry",
            "message": "durable harness health is unavailable",
            "details": {"error": type(error).__name__},
        }
    return {
        "valid": False,
        "errors": None,
        "diagnostics": [],
        "diagnostics_omitted": None,
        "first_error": first_error,
        "reason": reason,
    }


def summary() -> dict[str, Any]:
    """Summarize observed timeline work, excluding unmeasured SUT CPU usage."""
    now = time.monotonic()

    def read(db: sqlite3.Connection) -> dict[str, Any]:
        db.execute("BEGIN IMMEDIATE")
        _reap(db)
        _prune_events(db, now)
        start = db.execute(
            "SELECT value FROM metadata WHERE key = 'started'"
        ).fetchone()
        elapsed = max(0.0, now - start[0]) if start is not None else 0.0

        def rows(table: str) -> list[dict[str, Any]]:
            cursor = db.execute(f"SELECT * FROM {table}")
            assert cursor.description is not None
            names = [c[0] for c in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]

        commands, comparisons, coverage = (
            rows("commands"),
            rows("comparisons"),
            rows("coverage"),
        )
        evaluated = sum(r["units"] for r in comparisons if r["outcome"] != "skipped")
        successful = sum(r["units"] for r in comparisons if r["outcome"] == "passed")
        pruned = db.execute(
            "SELECT value FROM metadata WHERE key = 'events_pruned'"
        ).fetchone()
        catalog = observations.catalog()
        observed = rows("observations")
        targets = {}
        if catalog is not None:
            targets = {
                target["message"]: {**target, "observed": 0, "not_observed": 0}
                for target in catalog["targets"]
            }
        for row in observed:
            target = targets.setdefault(
                row["message"],
                {"message": row["message"], "observed": 0, "not_observed": 0},
            )
            key = "observed" if row["observed"] else "not_observed"
            target[key] += row["evaluations"]
        diagnostics = rows("harness_diagnostics")
        for diagnostic in diagnostics:
            diagnostic["details"] = json.loads(diagnostic["details"])
        health_errors = sum(command["harness_errors"] for command in commands)
        durable_error = observations.durable_health()
        return {
            "telemetry_available": True,
            "observed_timeline_s": elapsed,
            "workload_self_cpu_s": sum(r["cpu_s"] for r in commands),
            "workload_child_cpu_s": sum(r["child_cpu_s"] for r in commands),
            "sut_cpu_measured": False,
            "interrupted_cpu_measured": False,
            "interrupted_comparisons_complete": False,
            "comparison_units_per_timeline_hour": (
                evaluated * 3600 / elapsed if elapsed else 0.0
            ),
            "successful_comparison_units_per_timeline_hour": (
                successful * 3600 / elapsed if elapsed else 0.0
            ),
            "active_commands": db.execute(
                "SELECT count(*) FROM active_commands"
            ).fetchone()[0],
            "retained_events": db.execute("SELECT count(*) FROM events").fetchone()[0],
            "pruned_events": int(pruned[0]) if pruned is not None else 0,
            "event_capacity": EVENT_CAP,
            "event_retention_s": EVENT_RETENTION_S,
            "commands": commands,
            "comparisons": comparisons,
            "coverage": coverage,
            "observations": observed,
            "coverage_targets": [targets[key] for key in sorted(targets)],
            "coverage_catalog_available": catalog is not None,
            "coverage_catalog_unresolved": (
                catalog["unresolved"] if catalog is not None else []
            ),
            "harness_health": {
                "valid": health_errors == 0 and durable_error is None,
                "errors": health_errors,
                "diagnostics": diagnostics,
                "diagnostics_omitted": health_errors
                - sum(row["occurrences"] for row in diagnostics),
                "first_error": durable_error,
            },
        }

    result = _db_call("summarize", read, {})
    if not result:
        return {
            "telemetry_available": False,
            "harness_health": _unavailable_health("telemetry summary is unavailable"),
        }
    try:
        result["quiet_fraction"] = quiet.recent_quiet_fraction(
            result["observed_timeline_s"]
        )
    except Exception as e:
        _failed("quiet fraction", e)
        result["quiet_fraction"] = None
        health = result["harness_health"]
        invalid = _unavailable_health("quiet fraction is unavailable")
        invalid["diagnostics"] = health["diagnostics"]
        invalid["first_error"] = health["first_error"] or invalid["first_error"]
        result["harness_health"] = invalid
    return result


def report_main() -> int:
    print(json.dumps(summary(), sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(report_main())
