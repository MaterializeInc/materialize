# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Recovery checks after faults stop, and the pod-exit second channel.

`eventually_main` and `finally_main` run the checks of
transient-faults-leave-no-permanent-damage once fault injection has stopped.
Each check polls until it passes or `WEDGE_FACTOR` times its budget runs out,
then asserts twice: an `always` that it passed at all (a failure means the
system stayed wedged), and a `sometimes` that it passed within the tight
budget. The checks run in dependency order (availability first), and each gets
its own budget so one slow check does not turn every later one into a false
failure.

From construction on, a background thread samples every Materialize
container's restart history, and the last check asserts that no container
crash-looped (see `materialize.antithesis.crash_loops`) in a window after
faults stopped. The SUT reports each halt's verdict in process; this check
catches designed halts and panics that repeat.

`anytime_main` samples every Materialize pod's container statuses and
classifies each new termination by exit code (sut-anomalies-are-explained,
workload side). It sees exits that bypass the SUT's panic hook.

Contracts for other drivers, in state database `recovery`:

* `retry_ledger(family, probe_sql, recorded_at)`: a driver records the
  statement family of every statement that failed indeterminately during
  faults. After quiet, one probe per family must succeed: `probe_sql` if
  given (self-contained and idempotent), else this module's canary for a
  known family (`FAMILY_PROBES`).
* `expected_unhealthy(object_name, reason)`: objects a driver broke on
  purpose (for example a source whose upstream it truncated). They are exempt
  from the hydration, frontier, and readability checks.
* `terminations`: every container termination `anytime_main` classified.
"""

from __future__ import annotations

import calendar
import json
import re
import sqlite3
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg
import requests
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable,
)
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable as always,
)
from antithesis.lifecycle import (  # pyright: ignore[reportMissingModuleSource]
    send_event,
)
from kubernetes import client  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import crash_loops, sql, state
from materialize.antithesis.drivers import configure, lifecycle, rollouts
from materialize.antithesis.drivers.sources_common import source_error_text
from materialize.antithesis.endpoints import INTERNAL_HTTP_PORT
from materialize.antithesis.environment import Environment
from materialize.antithesis.observations import reachable, sometimes
from materialize.antithesis.rng import rng

STATE_DB = "recovery"
SCHEMA = "materialize.recovery"

# environmentd boot plus hydration of every object on one simulated core.
# Not yet measured; triage of the first runs should log time-to-healthy and
# tighten this. Too short gives false wedges, too long wastes dead timelines.
RECOVERY_BUDGET_S = 900
# Each check after the availability gate. Covers a second envd boot (a crash
# loop must show up as a restart within it) and one hydration round.
CHECK_BUDGET_S = 300
# `cr_up_to_date` may request a fresh rollout, which must then complete.
CR_BUDGET_S = CHECK_BUDGET_S + rollouts.T_ROLLOUT_SECONDS
# The budgets above are tight bounds, checked with `sometimes` (performance).
# Each `always` (wedged) polls for this many times its tight budget, so a slow
# schedule on one simulated core does not read as a wedge.
WEDGE_FACTOR = 3
POLL_S = 5
# A crash loop must show `crash_loops.LOOP_MIN_CRASHES` (2) crashes within
# this window. The kubelet's restart backoff caps at 300 s, so a loop that
# halts during boot crashes about every 300 s plus one boot; two such cycles
# fit.
CRASHLOOP_WINDOW_S = 720
CRASHLOOP_SAMPLE_S = 5
# Log lines of a crashed container that name the cause, shown in details.
EXIT_LOG_MARKERS = ("halting process:", "panicked at")
EXIT_LOG_TAIL = 200
# Ten default timestamp intervals; any live collection ticks within it.
FRONTIER_WINDOW_S = 10
# A reconfiguration past its deadline must settle within one controller tick;
# this allows a few ticks plus a slow coordinator.
RECONFIG_GRACE_MS = 120_000
APPLY_DRAIN_TIMEOUT_MS = 120_000
READ_TIMEOUT_MS = 60_000
HTTP_TIMEOUT_S = 30
RESTART_COMPARE_P = 0.5

ANYTIME_DURATION_S = 120
ANYTIME_POLL_S = 10
HALT_EVIDENCE_BUDGET_S = 10.0
HALT_EVIDENCE_ATTEMPTS = 3
HALT_EVIDENCE_REQUEST_S = 3.0
HALT_EVIDENCE_RETRY_S = 30.0
HALT_EVIDENCE_MAX_BYTES = 16_384

ROLE_ENVIRONMENTD = "environmentd"
ROLE_CLUSTERD = "clusterd"
ROLE_ORCHESTRATORD = "orchestratord"
OPERATOR_APP_NAME = "materialize-operator"

# A probe per statement family that drivers record in `retry_ledger`. Each is
# self-contained: it creates what it needs under `SCHEMA` and cleans up.
FAMILY_PROBES: dict[str, list[str]] = {
    "read": ["SELECT 1"],
    "dml_insert": [
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.probe_rows (x int)",
        f"INSERT INTO {SCHEMA}.probe_rows VALUES (1)",
    ],
    "ddl_create_table": [
        f"CREATE TABLE {SCHEMA}.probe_table (x int)",
        f"DROP TABLE {SCHEMA}.probe_table",
    ],
    "ddl_alter_table": [
        f"CREATE TABLE {SCHEMA}.probe_alter (x int)",
        f"ALTER TABLE {SCHEMA}.probe_alter ADD COLUMN y text",
        f"DROP TABLE {SCHEMA}.probe_alter",
    ],
    "ddl_create_mv": [
        f"CREATE TABLE {SCHEMA}.probe_mv_t (x int)",
        f"CREATE MATERIALIZED VIEW {SCHEMA}.probe_mv IN CLUSTER {configure.SHARED_CLUSTER}"
        f" AS SELECT x FROM {SCHEMA}.probe_mv_t",
        f"SELECT count(*) FROM {SCHEMA}.probe_mv",
        f"DROP TABLE {SCHEMA}.probe_mv_t CASCADE",
    ],
    "ddl_replacement": [
        f"CREATE TABLE {SCHEMA}.probe_rp_t (x int)",
        f"CREATE MATERIALIZED VIEW {SCHEMA}.probe_rp_mv IN CLUSTER {configure.SHARED_CLUSTER}"
        f" AS SELECT x FROM {SCHEMA}.probe_rp_t",
        f"CREATE REPLACEMENT MATERIALIZED VIEW {SCHEMA}.probe_rp FOR {SCHEMA}.probe_rp_mv"
        f" IN CLUSTER {configure.SHARED_CLUSTER} AS SELECT x FROM {SCHEMA}.probe_rp_t",
        f"DROP TABLE {SCHEMA}.probe_rp_t CASCADE",
    ],
    "ddl_apply_replacement": [
        f"CREATE TABLE {SCHEMA}.probe_ap_t (x int)",
        f"CREATE MATERIALIZED VIEW {SCHEMA}.probe_ap_mv IN CLUSTER {configure.SHARED_CLUSTER}"
        f" AS SELECT x FROM {SCHEMA}.probe_ap_t",
        f"CREATE REPLACEMENT MATERIALIZED VIEW {SCHEMA}.probe_ap FOR {SCHEMA}.probe_ap_mv"
        f" IN CLUSTER {configure.SHARED_CLUSTER} AS SELECT x FROM {SCHEMA}.probe_ap_t",
        f"ALTER MATERIALIZED VIEW {SCHEMA}.probe_ap_mv APPLY REPLACEMENT {SCHEMA}.probe_ap",
        f"DROP TABLE {SCHEMA}.probe_ap_t CASCADE",
    ],
    "ddl_index": [
        f"CREATE TABLE {SCHEMA}.probe_ix_t (x int)",
        f"CREATE INDEX probe_ix IN CLUSTER {configure.SHARED_CLUSTER} ON {SCHEMA}.probe_ix_t (x)",
        f"DROP TABLE {SCHEMA}.probe_ix_t CASCADE",
    ],
    "ddl_drop": [
        f"CREATE TABLE {SCHEMA}.probe_drop (x int)",
        f"DROP TABLE {SCHEMA}.probe_drop",
    ],
    "ddl_cluster": [
        "CREATE CLUSTER recovery_probe_c (SIZE 'antithesis-1', REPLICATION FACTOR 0)",
        "DROP CLUSTER recovery_probe_c",
    ],
    "ddl_alter_cluster": [
        "CREATE CLUSTER recovery_probe_a (SIZE 'antithesis-1', REPLICATION FACTOR 0)",
        "ALTER CLUSTER recovery_probe_a SET (SIZE 'antithesis-2')"
        " WITH (WAIT UNTIL READY (TIMEOUT '0s', ON TIMEOUT 'COMMIT'))",
        "DROP CLUSTER recovery_probe_a",
    ],
    "ddl_replica": [
        "CREATE CLUSTER recovery_probe_r REPLICAS ()",
        "CREATE CLUSTER REPLICA recovery_probe_r.r1 SIZE 'antithesis-1'",
        "DROP CLUSTER recovery_probe_r CASCADE",
    ],
}
# Leftovers of an interrupted earlier probe, dropped before every probe.
PROBE_CLEANUP = [
    f"DROP TABLE IF EXISTS {SCHEMA}.{t} CASCADE"
    for t in (
        "probe_table",
        "probe_alter",
        "probe_mv_t",
        "probe_rp_t",
        "probe_ap_t",
        "probe_ix_t",
        "probe_drop",
    )
] + [
    f"DROP CLUSTER IF EXISTS {c} CASCADE"
    for c in ("recovery_probe_c", "recovery_probe_a", "recovery_probe_r")
]


def log(message: str) -> None:
    print(f"recovery: {message}", flush=True)


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS retry_ledger (
            family TEXT, probe_sql TEXT, recorded_at REAL);
        CREATE TABLE IF NOT EXISTS expected_unhealthy (
            object_name TEXT PRIMARY KEY, reason TEXT);
        CREATE TABLE IF NOT EXISTS terminations (
            pod_uid TEXT, container TEXT, restart_count INTEGER, role TEXT,
            pod TEXT, exit_code INTEGER, reason TEXT, signal INTEGER,
            finished_at TEXT, classification TEXT, observed_at REAL,
            PRIMARY KEY (pod_uid, container, restart_count));
        CREATE TABLE IF NOT EXISTS halt_evidence (
            namespace TEXT, pod_uid TEXT, container TEXT,
            termination_count INTEGER, finished_at TEXT,
            payload TEXT NOT NULL, emitted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (namespace, pod_uid, container, termination_count, finished_at));
        """)
    db.commit()
    return db


def poll(
    budget_s: float, check: Callable[[], tuple[bool, Any]], what: str
) -> tuple[bool, Any, float]:
    """Run `check` until it passes or `budget_s` elapses.

    Returns (passed, last details, seconds taken). Exceptions from transient
    faults count as a failed attempt.
    """
    start = time.monotonic()
    details: Any = None
    while True:
        try:
            ok, details = check()
        except rollouts.TRANSIENT_ERRORS as e:
            ok, details = False, {"error": f"{type(e).__name__}: {e}"}
        elapsed = time.monotonic() - start
        if ok:
            log(f"{what}: passed after {elapsed:.0f}s")
            return True, details, elapsed
        if elapsed >= budget_s:
            log(f"{what}: failed after {elapsed:.0f}s: {details}")
            return False, details, elapsed
        time.sleep(POLL_S)


def rows(
    host: str, text: str, params: tuple[Any, ...] = (), **kwargs: Any
) -> list[tuple[Any, ...]]:
    with sql.connection(host, **kwargs) as conn:
        # Without parameters psycopg leaves `%` alone, so LIKE patterns need
        # no escaping.
        cur = conn.execute(text.encode(), params or None)
        return cur.fetchall() if cur.description else []


def exempt_names(db: sqlite3.Connection) -> set[str]:
    return {r[0] for r in db.execute("SELECT object_name FROM expected_unhealthy")}


def mark_expected_unhealthy(names: list[str], reason: str) -> None:
    """Record objects a driver broke on purpose, see `expected_unhealthy`."""
    if not names:
        return
    db = open_state()
    try:
        with db:
            db.executemany(
                "INSERT OR REPLACE INTO expected_unhealthy VALUES (?, ?)",
                [(name, reason) for name in names],
            )
    finally:
        db.close()


@dataclass(frozen=True)
class ContainerObservation:
    role: str
    namespace: str
    pod: str
    pod_uid: str
    container: str
    restart_count: int
    exit_code: int | None
    reason: str | None
    signal: int | None
    finished_at: str | None
    waiting_reason: str | None
    deleting: bool = False
    current_terminated: bool = False

    @property
    def termination_count(self) -> int:
        # The current incarnation has not incremented restart_count yet.
        # Use its eventual ordinal to deduplicate it after the kubelet restarts it.
        return self.restart_count + int(self.current_terminated)


def pod_role(
    namespace: str, operator_namespace: str, labels: dict[str, str]
) -> str | None:
    if namespace == operator_namespace:
        if labels.get("app.kubernetes.io/name") == OPERATOR_APP_NAME:
            return ROLE_ORCHESTRATORD
        return None
    if labels.get("materialize.cloud/app") == "environmentd":
        return ROLE_ENVIRONMENTD
    if any(k.endswith("/cluster-id") for k in labels):
        return ROLE_CLUSTERD
    return None


def observe_pods(env: Environment) -> list[ContainerObservation]:
    core = client.CoreV1Api()
    result = []
    for namespace in (env.endpoints.namespace, env.endpoints.operator_namespace):
        for pod in core.list_namespaced_pod(
            namespace,
            _request_timeout=rollouts.K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        ).items:
            metadata = pod.metadata
            assert metadata is not None
            role = pod_role(
                namespace, env.endpoints.operator_namespace, metadata.labels or {}
            )
            if role is None:
                continue
            assert metadata.name is not None and metadata.uid is not None
            assert pod.status is not None
            for cs in pod.status.container_statuses or []:
                current = cs.state.terminated if cs.state else None
                term = current or (cs.last_state.terminated if cs.last_state else None)
                waiting = cs.state.waiting if cs.state else None
                result.append(
                    ContainerObservation(
                        role=role,
                        namespace=namespace,
                        pod=metadata.name,
                        pod_uid=metadata.uid,
                        container=cs.name,
                        restart_count=cs.restart_count,
                        exit_code=term.exit_code if term else None,
                        reason=term.reason if term else None,
                        signal=term.signal if term else None,
                        finished_at=str(term.finished_at) if term else None,
                        waiting_reason=waiting.reason if waiting else None,
                        deleting=metadata.deletion_timestamp is not None,
                        current_terminated=current is not None,
                    )
                )
    return result


def classify_exit(exit_code: int, reason: str | None) -> str:
    """Map a container exit to the reason class that explains it.

    `halt!` exits 166 and `exit!` (fenced deployment, graceful termination)
    exits 0; both are designed, and which halt message is acceptable in which
    fault context is the SUT-side classifier's job. 137 and 143 are SIGKILL
    and SIGTERM from outside (fault injection, eviction, generation teardown).
    The panic hook aborts the process (134); 101 is an unwinding Rust panic.
    """
    if reason == "OOMKilled":
        return "oom"
    if exit_code == 0:
        return "clean"
    if exit_code == 166:
        return "halt"
    if exit_code in (137, 143):
        return "killed"
    if exit_code in (101, 134):
        return "panic"
    return "unknown"


@dataclass
class HaltEvidenceBudget:
    remaining_s: float = HALT_EVIDENCE_BUDGET_S
    attempts: int = HALT_EVIDENCE_ATTEMPTS
    retry_at: dict[tuple[Any, ...], float] = field(default_factory=dict)


def _termination_identity(o: ContainerObservation) -> dict[str, Any]:
    return {
        "namespace": o.namespace,
        "pod_uid": o.pod_uid,
        "container": o.container,
        "termination_count": o.termination_count,
        "finished_at": o.finished_at,
        "exit_code": o.exit_code,
    }


@dataclass(frozen=True)
class _LogSelector:
    pod_uid: str
    pod_name: str
    restart_count: int
    current_terminated: bool
    exit_code: int
    signal: int | None
    finished_at: str | None
    container_id: str | None
    started_at: str | None
    prior_finished_at: str | None
    next_started_at: str | None


def _time_text(value: Any) -> str | None:
    return str(value) if value is not None else None


def _log_selector(pod: Any, container: str) -> _LogSelector | None:
    metadata, status = getattr(pod, "metadata", None), getattr(pod, "status", None)
    if metadata is None or status is None:
        return None
    for cs in status.container_statuses or []:
        if cs.name != container:
            continue
        current = getattr(getattr(cs, "state", None), "terminated", None)
        previous = getattr(getattr(cs, "last_state", None), "terminated", None)
        term = current or previous
        if term is None:
            return None
        running = getattr(getattr(cs, "state", None), "running", None)
        return _LogSelector(
            metadata.uid,
            metadata.name,
            cs.restart_count,
            current is not None,
            term.exit_code,
            term.signal,
            _time_text(term.finished_at),
            getattr(cs, "container_id", None),
            _time_text(getattr(term, "started_at", None)),
            (
                _time_text(previous.finished_at)
                if current is not None and previous
                else None
            ),
            _time_text(running.started_at) if current is None and running else None,
        )
    return None


def _timestamp_interval(value: str | None) -> tuple[int, int] | None:
    if value is None:
        return None
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
        value,
    )
    if match is None:
        return None
    try:
        seconds = datetime.fromisoformat(match[1] + match[3]).astimezone(UTC)
    except ValueError:
        return None
    fraction = match[2] or ""
    start = calendar.timegm(seconds.utctimetuple()) * 1_000_000_000
    start += int(fraction.ljust(9, "0"))
    # metav1.Time drops fractional seconds. Its precision is an interval, not
    # evidence that a nanosecond log timestamp is after the process exited.
    return start, start + 10 ** (9 - len(fraction))


def _correlated_log_time(stamp: str, selector: _LogSelector) -> bool:
    logged = _timestamp_interval(stamp)
    started = _timestamp_interval(selector.started_at)
    finished = _timestamp_interval(selector.finished_at)
    if logged is None or started is None or finished is None:
        return False
    if logged[0] < started[0] or logged[1] > finished[1] or started[0] >= finished[1]:
        return False
    prior = _timestamp_interval(selector.prior_finished_at)
    following = _timestamp_interval(selector.next_started_at)
    if prior is not None and logged[0] < prior[1]:
        return False
    if following is not None and logged[1] > following[0]:
        return False
    ordinal = selector.restart_count + int(selector.current_terminated)
    if logged[0] < started[1] and ordinal != 1 and prior is None:
        return False
    # In the uncertain final bucket the new incarnation must be known to
    # start later. Otherwise a stale pod status can name the wrong log stream.
    if logged[1] > finished[0] and following is None:
        return False
    return True


def _halt_message(text: str, selector: _LogSelector) -> tuple[str, str] | None:
    try:
        encoded_len = len(text.encode())
    except UnicodeError:
        return None
    if encoded_len >= HALT_EVIDENCE_MAX_BYTES or not text.endswith("\n"):
        return None
    messages = []
    prefix = "halting process: "
    for raw in text.splitlines():
        stamp, _, line = raw.partition(" ")
        try:
            record = json.loads(line)
            message = record.get("fields", {}).get("message")
        except (ValueError, AttributeError, RecursionError):
            message = line
        if isinstance(message, str) and message.startswith(prefix):
            if not _correlated_log_time(stamp, selector):
                return None
            messages.append((message[len(prefix) :], stamp))
    return messages[0] if len(messages) == 1 and messages[0][0] else None


def capture_halt_evidence(
    o: ContainerObservation, deadline: float
) -> dict[str, Any] | None:
    """Read one complete halt message only while its log selector stays unchanged."""
    if (
        o.exit_code != 166
        or o.signal not in (None, 0)
        or not o.finished_at
        or not o.pod_uid
    ):
        return None
    core = client.CoreV1Api()

    def timeout() -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("halt evidence capture budget elapsed")
        return min(HALT_EVIDENCE_REQUEST_S, remaining)

    try:
        before = core.read_namespaced_pod(
            o.pod,
            o.namespace,
            _request_timeout=timeout(),  # pyright: ignore[reportCallIssue]
        )
        selector = _log_selector(before, o.container)
        expected = (
            o.pod_uid,
            o.pod,
            o.restart_count,
            o.current_terminated,
            o.exit_code,
            o.signal,
            o.finished_at,
        )
        if (
            selector is None
            or (
                selector.pod_uid,
                selector.pod_name,
                selector.restart_count,
                selector.current_terminated,
                selector.exit_code,
                selector.signal,
                selector.finished_at,
            )
            != expected
        ):
            return None
        text = core.read_namespaced_pod_log(
            o.pod,
            o.namespace,
            container=o.container,
            previous=not o.current_terminated,
            timestamps=True,
            tail_lines=EXIT_LOG_TAIL,
            limit_bytes=HALT_EVIDENCE_MAX_BYTES + 1,
            _request_timeout=timeout(),  # pyright: ignore[reportCallIssue]
        )
        after = core.read_namespaced_pod(
            o.pod,
            o.namespace,
            _request_timeout=timeout(),  # pyright: ignore[reportCallIssue]
        )
        # A current termination can become the previous run while reading logs.
        # Its ordinal stays the same, but the API's current/previous routing changes.
        if _log_selector(after, o.container) != selector or not isinstance(text, str):
            return None
        halt = _halt_message(text, selector)
        if halt is None:
            return None
    except (*rollouts.TRANSIENT_ERRORS, UnicodeError):
        return None
    identity = _termination_identity(o)
    return {
        "termination": identity,
        "halt": {"termination": dict(identity), "message": halt[0], "complete": True},
        "log_capture": {
            "message_at": halt[1],
            "started_at": selector.started_at,
            "finished_at": selector.finished_at,
            "prior_finished_at": selector.prior_finished_at,
            "next_started_at": selector.next_started_at,
        },
    }


def record_halt_evidence(
    db: sqlite3.Connection,
    observations: list[ContainerObservation],
    budget: HaltEvidenceBudget,
    deadline: float | None = None,
) -> None:
    """Cache and emit correlated halt evidence within one monitor's shared budget."""
    for o in observations:
        if o.exit_code != 166 or not o.finished_at:
            continue
        key = (o.namespace, o.pod_uid, o.container, o.termination_count, o.finished_at)
        row = db.execute(
            "SELECT payload, emitted FROM halt_evidence WHERE namespace = ? AND pod_uid = ? "
            "AND container = ? AND termination_count = ? AND finished_at = ?",
            key,
        ).fetchone()
        if row is None:
            now = time.monotonic()
            if (
                budget.attempts <= 0
                or budget.remaining_s <= 0
                or now < budget.retry_at.get(key, 0)
                or (deadline is not None and now >= deadline)
            ):
                continue
            budget.attempts -= 1
            budget.retry_at[key] = now + HALT_EVIDENCE_RETRY_S
            end = now + budget.remaining_s
            if deadline is not None:
                end = min(end, deadline)
            evidence = capture_halt_evidence(o, end)
            spent = max(0.0, time.monotonic() - now)
            budget.remaining_s = max(0.0, budget.remaining_s - spent)
            if evidence is None:
                continue
            payload = json.dumps(evidence, sort_keys=True)
            with db:
                db.execute(
                    "INSERT OR IGNORE INTO halt_evidence VALUES (?, ?, ?, ?, ?, ?, 0)",
                    (*key, payload),
                )
            row = (payload, 0)
        if row[1]:
            continue
        send_event("antithesis_exit_evidence", json.loads(row[0]))
        with db:
            db.execute(
                "UPDATE halt_evidence SET emitted = 1 WHERE namespace = ? AND pod_uid = ? "
                "AND container = ? AND termination_count = ? AND finished_at = ?",
                key,
            )


def record_terminations(
    db: sqlite3.Connection,
    observations: list[ContainerObservation],
    *,
    evidence_budget: HaltEvidenceBudget | None = None,
    deadline: float | None = None,
) -> int:
    """Classify and assert on every termination not seen before. Returns how many."""
    new = 0
    for o in observations:
        if o.exit_code is None:
            continue
        with db:
            previous = db.execute(
                "SELECT max(restart_count) FROM terminations WHERE pod_uid = ? AND container = ?",
                (o.pod_uid, o.container),
            ).fetchone()[0]
            cls = classify_exit(o.exit_code, o.reason)
            inserted = db.execute(
                "INSERT OR IGNORE INTO terminations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    o.pod_uid,
                    o.container,
                    o.termination_count,
                    o.role,
                    o.pod,
                    o.exit_code,
                    o.reason,
                    o.signal,
                    o.finished_at,
                    cls,
                    time.time(),
                ),
            ).rowcount
        if not inserted:
            continue
        new += 1
        details = {
            "role": o.role,
            "pod": o.pod,
            "container": o.container,
            "restart_count": o.restart_count,
            "exit_code": o.exit_code,
            "reason": o.reason,
            "signal": o.signal,
            "finished_at": o.finished_at,
            "waiting_reason": o.waiting_reason,
            # Kubernetes keeps only the last termination; restarts between two
            # samples are counted but not classified.
            "unobserved_terminations": (
                max(0, o.termination_count - previous - 1)
                if previous is not None
                else None
            ),
        }
        log(f"termination {cls}: {details}")
        # OOM kills are asserted by `resource_kills`, which can tell a
        # container's own limit from node-wide memory exhaustion.
        # Whether a panic is a bug is decided in the process, by the panic
        # hook (some panics are designed, such as a fetch after losing a
        # persist lease). An exit code cannot tell the two apart, so this side
        # only records that the path was reached.
        if cls == "panic":
            reachable("a Materialize pod exited by panic or abort", details)
        always_or_unreachable(
            cls != "unknown",
            "every Materialize pod exit code has an explanation",
            details,
        )
        sometimes(
            cls == "killed",
            "a Materialize pod restarted after an external kill",
            details,
        )
        sometimes(cls == "halt", "a Materialize pod restarted after a halt", details)
    record_halt_evidence(
        db, observations, evidence_budget or HaltEvidenceBudget(), deadline
    )
    return new


def anytime_main() -> int:
    try:
        env = Environment()
    except (ApiException, OSError) as e:
        log(f"cannot reach the Kubernetes API: {e}")
        return 0
    db = open_state()
    deadline = time.monotonic() + ANYTIME_DURATION_S
    evidence_budget = HaltEvidenceBudget()
    while time.monotonic() < deadline:
        try:
            record_terminations(
                db,
                observe_pods(env),
                evidence_budget=evidence_budget,
                deadline=deadline,
            )
        except (ApiException, OSError) as e:
            log(f"cannot read pods: {e}")
        time.sleep(ANYTIME_POLL_S)
    return 0


class Recovery:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.env = Environment()
        self.host = self.env.sql_host()
        self.db = open_state()
        self.exempt = exempt_names(self.db)
        self.timings: dict[str, float] = {}
        self.unstick = rollouts.OperatorUnstick(
            rollouts.operator_ctx(self.env), "recovery"
        )
        # Restart history from the moment faults stopped, read by
        # `no_crash_loop` after the other checks.
        self.restarts = crash_loops.Tracker()
        self.restarts_lock = threading.Lock()
        self.containers: dict[str, ContainerObservation] = {}
        self.sampling_done = threading.Event()
        threading.Thread(
            target=self.sample_restarts, name="crash-loop-sampler", daemon=True
        ).start()

    def details(self, **extra: Any) -> dict[str, Any]:
        return {"mode": self.mode, "timings": dict(self.timings), **extra}

    def canary(self) -> tuple[bool, Any]:
        rows(self.host, "SELECT 1")
        rows(self.host, "SELECT 1", internal=True)
        with sql.connection(
            self.host, options={"cluster": configure.SHARED_CLUSTER}
        ) as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            conn.execute(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.canary (x int)")
            before_row = conn.execute(
                f"SELECT count(*) FROM {SCHEMA}.canary"
            ).fetchone()
            conn.execute(f"INSERT INTO {SCHEMA}.canary VALUES (1)")
            after_row = conn.execute(f"SELECT count(*) FROM {SCHEMA}.canary").fetchone()
        assert before_row is not None and after_row is not None
        before, after = before_row[0], after_row[0]
        return after == before + 1, {"before": before, "after": after}

    def cr_up_to_date(self) -> tuple[bool, Any]:
        snap, _ = self.unstick.step()
        ok = snap.condition_status == "True" and snap.last_completed == snap.request
        return ok, {**snap.summary(), "fresh_request": self.unstick.fresh_requested}

    def blocked_ids(self) -> set[str]:
        """User objects on a cluster without replicas, or depending on one
        through `mz_object_dependencies`, transitively. They cannot hydrate
        or advance."""
        blocked = {
            r[0]
            for r in rows(
                self.host,
                "SELECT o.id FROM mz_objects o WHERE o.cluster_id IS NOT NULL"
                " AND NOT EXISTS ("
                "  SELECT 1 FROM mz_cluster_replicas r WHERE r.cluster_id = o.cluster_id)",
            )
        }
        deps = rows(
            self.host,
            "SELECT object_id, referenced_object_id"
            " FROM mz_internal.mz_object_dependencies WHERE object_id LIKE 'u%'",
        )
        grew = True
        while grew:
            grew = False
            for obj, ref in deps:
                if ref in blocked and obj not in blocked:
                    blocked.add(obj)
                    grew = True
        return blocked

    def replicas_online(self) -> tuple[bool, Any]:
        offline = rows(
            self.host,
            "SELECT c.name, r.name, s.process_id, s.status, s.reason"
            " FROM mz_cluster_replicas r"
            " JOIN mz_clusters c ON r.cluster_id = c.id"
            " JOIN mz_internal.mz_cluster_replica_statuses s ON s.replica_id = r.id"
            " WHERE r.id LIKE 'u%' AND s.status <> 'online'",
        )
        return not offline, {"offline": [list(map(str, r)) for r in offline]}

    def hydrated(self) -> tuple[bool, Any]:
        pending = rows(
            self.host,
            "SELECT o.id, o.name, r.name FROM mz_internal.mz_hydration_statuses h"
            " JOIN mz_objects o ON o.id = h.object_id"
            " JOIN mz_cluster_replicas r ON r.id = h.replica_id"
            " WHERE NOT h.hydrated AND h.object_id LIKE 'u%'",
        )
        blocked = self.blocked_ids() if pending else set()
        pending = [
            p for p in pending if p[1] not in self.exempt and p[0] not in blocked
        ]
        return not pending, {"not_hydrated": [list(map(str, p)) for p in pending]}

    def reconfigurations_settled(self) -> tuple[bool, Any]:
        overdue = rows(
            self.host,
            "SELECT c.name, rc.deadline::text, rc.on_timeout"
            " FROM mz_internal.mz_cluster_reconfigurations rc"
            " JOIN mz_clusters c ON c.id = rc.cluster_id"
            " WHERE rc.status = 'in-progress'"
            " AND rc.deadline::text::numeric + %s < extract(epoch FROM now()) * 1000",
            (RECONFIG_GRACE_MS,),
        )
        return not overdue, {"overdue": [list(map(str, r)) for r in overdue]}

    def drain_replacements(self) -> None:
        """Apply or drop every pending lifecycle replacement; each must finish."""
        found: list[tuple[Any, ...]] = []

        def list_pending() -> tuple[bool, Any]:
            found[:] = rows(
                self.host,
                "SELECT o.name, t.name FROM mz_internal.mz_replacements r"
                " JOIN mz_objects o ON o.id = r.id JOIN mz_objects t ON t.id = r.target_id"
                " JOIN mz_schemas s ON s.id = o.schema_id WHERE s.name = %s",
                (lifecycle.SCHEMA,),
            )
            return True, {}

        listed, info, _ = poll(CHECK_BUDGET_S, list_pending, "list replacements")
        if not listed:
            log(f"cannot list pending replacements: {info}")
            return
        for replacement, target in found:
            apply = rng.random() < 0.7
            text = (
                f"ALTER MATERIALIZED VIEW {lifecycle.QUALIFIED}.{target}"
                f" APPLY REPLACEMENT {lifecycle.QUALIFIED}.{replacement}"
                if apply
                else f"DROP MATERIALIZED VIEW {lifecycle.QUALIFIED}.{replacement}"
            )

            def attempt(text: str = text) -> tuple[bool, Any]:
                try:
                    rows(self.host, text, statement_timeout_ms=APPLY_DRAIN_TIMEOUT_MS)
                    return True, {"sql": text}
                except psycopg.Error as e:
                    c = lifecycle.outcome_of(e)
                    if c.outcome is sql.Outcome.REJECTED:
                        return True, {"sql": text, "rejected": c.template}
                    return False, {
                        "sql": text,
                        "sqlstate": c.sqlstate,
                        "template": c.template,
                    }

            ok, info, took = poll(
                CHECK_BUDGET_S * WEDGE_FACTOR, attempt, f"drain {replacement}"
            )
            details = self.details(
                replacement=replacement, target=target, apply=apply, info=info
            )
            always_or_unreachable(
                ok,
                "a pending replacement MV can be applied or dropped after faults stop",
                details,
            )
            sometimes(
                ok and took <= CHECK_BUDGET_S,
                "a pending replacement MV was applied or dropped within the tight budget after faults stop",
                details,
            )

    def frontier_sample(self) -> tuple[float, dict[str, tuple[bool, int | None]]]:
        """Wall-clock ms, and per user collection whether it must advance and
        its max write frontier. Any global id of a versioned table advancing
        counts for the table. A `blocked_ids` collection cannot advance; a
        sealed collection has no write frontier."""
        sample = rows(
            self.host,
            "SELECT o.id, o.name, o.type,"
            " max(f.write_frontier)::text,"
            " (extract(epoch FROM now()) * 1000)::float8"
            " FROM mz_internal.mz_frontiers f"
            " JOIN mz_internal.mz_object_global_ids g ON g.global_id = f.object_id"
            " JOIN mz_objects o ON o.id = g.id"
            " WHERE o.id LIKE 'u%'"
            " GROUP BY o.id, o.name, o.type",
        )
        blocked = self.blocked_ids()
        # A multi-output source with no exported tables or subsources runs no
        # ingestion, so its progress frontier legitimately stays put.
        blocked |= {
            r[0]
            for r in rows(
                self.host,
                "SELECT s.id FROM mz_sources s"
                " WHERE s.type IN ('postgres', 'mysql', 'sql-server')"
                " AND NOT EXISTS (SELECT 1 FROM mz_internal.mz_object_dependencies d"
                " WHERE d.referenced_object_id = s.id)",
            )
        }
        now_ms = float(sample[0][4]) if sample else time.time() * 1000
        return now_ms, {
            f"{name} ({id_}, {typ})": (
                id_ not in blocked,
                int(frontier) if frontier is not None else None,
            )
            for id_, name, typ, frontier, _ in sample
            if name not in self.exempt
        }

    def frontiers_advance(self) -> tuple[bool, Any]:
        now_ms, first = self.frontier_sample()
        time.sleep(FRONTIER_WINDOW_S)
        _, second = self.frontier_sample()
        stalled = [
            key
            for key, (must_advance, f1) in first.items()
            if key in second and must_advance and f1 is not None
            # A frontier ahead of wall-clock time (a REFRESH schedule) has
            # nothing to advance to yet.
            and f1 <= now_ms and (f2 := second[key][1]) is not None and f2 <= f1
        ]
        return not stalled, {"stalled": stalled, "checked": len(first)}

    def hung_alters(self) -> tuple[bool, Any]:
        """`APPLY REPLACEMENT` and `ALTER SINK ... SET FROM` statements that
        never finished. Both wait on a frontier; if their target is dropped
        during the wait they hang forever (database-issues#9820), so those
        are reported separately from hangs with a live target.

        Statements of sessions that no longer exist are excluded: a session
        lost to an environmentd restart never logs its statement's end."""
        # The activity log is only readable with monitoring privileges, so
        # read it as mz_system.
        rates = rows(
            self.host,
            "SELECT current_setting('statement_logging_max_sample_rate'),"
            " current_setting('statement_logging_default_sample_rate')",
            internal=True,
        )
        if any(float(r) == 0 for r in rates[0]):
            return True, {"skipped": "statement logging is off", "rates": rates[0]}
        hung = rows(
            self.host,
            "SELECT a.sql, a.began_at::text"
            " FROM mz_internal.mz_recent_activity_log a"
            " JOIN mz_internal.mz_sessions s ON s.id = a.session_id"
            " WHERE a.finished_at IS NULL"
            " AND (a.sql ILIKE '%%APPLY REPLACEMENT%%'"
            "  OR (a.sql ILIKE 'ALTER SINK%%' AND a.sql ILIKE '%%SET FROM%%'))"
            " AND a.began_at < now() - %s * INTERVAL '1 second'",
            (CHECK_BUDGET_S,),
            internal=True,
        )
        live = {
            r[0]
            for r in rows(self.host, "SELECT name FROM mz_objects WHERE id LIKE 'u%'")
        }
        known, other = [], []
        for text, began in hung:
            m = re.search(
                r"ALTER\s+(?:MATERIALIZED\s+VIEW|SINK)\s+(?:IF\s+EXISTS\s+)?(\S+)",
                text,
                re.IGNORECASE,
            )
            target = m[1].split(".")[-1].strip('"') if m else None
            (known if target is not None and target not in live else other).append(
                [text, began, target]
            )
        return not other, {"target_dropped": known, "target_live": other}

    def collections_readable(self) -> tuple[bool, Any]:
        objects = rows(
            self.host,
            "SELECT d.name, s.name, o.name, o.type,"
            " (SELECT count(*) FROM mz_cluster_replicas r WHERE r.cluster_id = o.cluster_id)"
            " FROM mz_objects o JOIN mz_schemas s ON s.id = o.schema_id"
            " JOIN mz_databases d ON d.id = s.database_id"
            " LEFT JOIN mz_internal.mz_replacements rp ON rp.id = o.id"
            " WHERE o.id LIKE 'u%' AND o.type IN ('table', 'materialized-view')"
            " AND rp.id IS NULL",
        )
        failures = []
        errored = []
        for db_name, schema, name, typ, replicas in objects:
            if name in self.exempt or (typ == "materialized-view" and replicas == 0):
                continue
            try:
                rows(
                    self.host,
                    f'SELECT count(*) FROM "{db_name}"."{schema}"."{name}"',
                    statement_timeout_ms=READ_TIMEOUT_MS,
                    options={"cluster": configure.SHARED_CLUSTER},
                )
            except psycopg.Error as e:
                c = sql.classify(e)
                if c.race is sql.CatalogRace.MISSING:
                    continue
                # A definite source error is the collection's content, not a
                # failure to read it. `postgres_sources` records the exports it
                # breaks in `expected_unhealthy`. This covers any it missed.
                error = source_error_text(e)
                if error is not None:
                    errored.append([f"{db_name}.{schema}.{name}", error])
                    continue
                failures.append([f"{db_name}.{schema}.{name}", c.sqlstate, c.template])
        return not failures, {
            "unreadable": failures,
            "source_errored": errored,
            "checked": len(objects),
        }

    def self_check(self, route: str) -> tuple[bool, Any]:
        response = requests.get(
            f"http://{self.host}:{INTERNAL_HTTP_PORT}{route}", timeout=HTTP_TIMEOUT_S
        )
        response.raise_for_status()
        return True, response.json()

    def sample_restarts(self) -> None:
        """Feed `self.restarts` from pod statuses until `run` ends."""
        while not self.sampling_done.is_set():
            try:
                observations = observe_pods(self.env)
            except rollouts.TRANSIENT_ERRORS as e:
                log(f"crash-loop sampler cannot read pods: {e}")
                observations = None
            if observations is not None:
                samples = []
                for o in observations:
                    name = f"{o.role}/{o.pod}/{o.container}"
                    self.containers[name] = o
                    samples.append(
                        crash_loops.Sample(
                            key=(o.pod_uid, o.container),
                            name=name,
                            restart_count=o.termination_count,
                            exit_class=(
                                classify_exit(o.exit_code, o.reason)
                                if o.exit_code is not None
                                else None
                            ),
                            exit_code=o.exit_code,
                            finished_at=o.finished_at,
                            waiting_reason=o.waiting_reason,
                            deleting=o.deleting,
                        )
                    )
                with self.restarts_lock:
                    self.restarts.observe(time.monotonic(), samples)
            self.sampling_done.wait(CRASHLOOP_SAMPLE_S)

    def exit_log_lines(self, name: str) -> list[str]:
        """Halt and panic lines from the previous run of a container, best effort."""
        o = self.containers.get(name)
        if o is None:
            return []
        try:
            text = client.CoreV1Api().read_namespaced_pod_log(
                o.pod,
                o.namespace,
                container=o.container,
                previous=True,
                tail_lines=EXIT_LOG_TAIL,
                _request_timeout=HTTP_TIMEOUT_S,  # pyright: ignore[reportCallIssue]
            )
        except rollouts.TRANSIENT_ERRORS as e:
            return [f"cannot read the previous log: {e}"]
        return [
            line[:1000]
            for line in text.splitlines()
            if any(m in line for m in EXIT_LOG_MARKERS)
        ][-5:]

    def no_crash_loop(self) -> tuple[bool, Any]:
        """No container crash-looped in the last `CRASHLOOP_WINDOW_S`, once
        the sampler has covered that long."""
        with self.restarts_lock:
            covered = self.restarts.covered(CRASHLOOP_WINDOW_S)
            loops = self.restarts.loops(CRASHLOOP_WINDOW_S)
        if not covered:
            return False, {"waiting_for_coverage_s": CRASHLOOP_WINDOW_S}
        for name, info in loops.items():
            info["exit_log"] = self.exit_log_lines(name)
        return not loops, {"crash_loops": loops}

    def retry_ledger(self) -> None:
        families = self.db.execute(
            "SELECT family, max(probe_sql) FROM retry_ledger GROUP BY family"
        ).fetchall()
        for family, probe_sql in families:
            probe = [probe_sql] if probe_sql else FAMILY_PROBES.get(family)
            if probe is None:
                log(f"no probe for statement family {family}")
                continue

            def attempt(probe: list[str] = probe) -> tuple[bool, Any]:
                with sql.connection(
                    self.host, options={"cluster": configure.SHARED_CLUSTER}
                ) as conn:
                    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
                    for text in PROBE_CLEANUP:
                        conn.execute(text.encode())
                    for text in probe:
                        conn.execute(text.encode())
                return True, {}

            ok, info, took = poll(
                CHECK_BUDGET_S * WEDGE_FACTOR, attempt, f"probe {family}"
            )
            always_or_unreachable(
                ok,
                "a statement family that failed during faults succeeds after faults stop",
                self.details(family=family, info=info),
            )
            sometimes(
                ok and took <= CHECK_BUDGET_S,
                "a statement family that failed during faults succeeded within the tight budget after faults stop",
                self.details(family=family, took_s=took),
            )

    def restart_and_compare(self) -> None:
        """Restart pods while no driver runs DDL, then compare the lifecycle
        projection (restart-preserves-derived-state) and re-read every
        lifecycle collection (live-collection-shard-never-tombstoned)."""
        from materialize.antithesis.drivers.pod_restarts import delete_recorded

        s = lifecycle.Session(self.env, lifecycle.open_state())
        before = lifecycle.quiet_projection(s)
        if before is None:
            log("no quiet lifecycle projection, skipping restart comparison")
            return
        victims = rng.choice(["environmentd", "clusterd", "both"])
        kube = rollouts.Kube(self.env)
        killed = []
        pods = kube.core.list_namespaced_pod(
            self.env.endpoints.namespace,
            _request_timeout=rollouts.K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        ).items
        metas = []
        for pod in pods:
            meta = pod.metadata
            assert meta is not None
            if meta.deletion_timestamp is not None:
                continue
            metas.append(meta)
            role = pod_role(
                self.env.endpoints.namespace,
                self.env.endpoints.operator_namespace,
                meta.labels or {},
            )
            if role == ROLE_ENVIRONMENTD and victims in ("environmentd", "both"):
                assert meta.name is not None and meta.namespace is not None
                if delete_recorded(
                    kube,
                    origin="recovery",
                    role=role,
                    namespace=meta.namespace,
                    pod=meta.name,
                    uid=meta.uid,
                    grace=None,
                    reason="restart_and_compare",
                ):
                    killed.append(meta.name)
        clusterd = [
            m
            for m in metas
            if pod_role(
                self.env.endpoints.namespace,
                self.env.endpoints.operator_namespace,
                m.labels or {},
            )
            == ROLE_CLUSTERD
        ]
        if clusterd and victims in ("clusterd", "both"):
            victim = rng.choice(clusterd)
            assert victim.name is not None and victim.namespace is not None
            if delete_recorded(
                kube,
                origin="recovery",
                role=ROLE_CLUSTERD,
                namespace=victim.namespace,
                pod=victim.name,
                uid=victim.uid,
                grace=None,
                reason="restart_and_compare",
            ):
                killed.append(victim.name)
        if not killed:
            return
        reachable(
            "recovery restarted pods to compare derived state", {"killed": killed}
        )
        s.drop_conn()

        ok, info, took = poll(
            RECOVERY_BUDGET_S * WEDGE_FACTOR, self.canary, "canary after restart"
        )
        always_or_unreachable(
            ok,
            "environmentd serves SQL again after a restart with faults stopped",
            self.details(killed=killed, info=info),
        )
        sometimes(
            ok and took <= RECOVERY_BUDGET_S,
            "environmentd served SQL again within the tight budget after a restart with faults stopped",
            self.details(killed=killed, took_s=took),
        )
        if not ok:
            return
        poll(CHECK_BUDGET_S, self.hydrated, "hydration after restart")
        after = lifecycle.quiet_projection(s)
        if after is None or after[0] != before[0]:
            log("lifecycle projection unavailable after restart")
            return
        lifecycle.compare_projections(before[1], after[1], f"recovery-{self.mode}")
        lifecycle.check_readable(
            s, f"recovery-{self.mode}-after-restart", require_success=True
        )
        s.drop_conn()

    def run(self) -> int:
        wedge = CHECK_BUDGET_S * WEDGE_FACTOR
        ok, info, took = poll(RECOVERY_BUDGET_S * WEDGE_FACTOR, self.canary, "canary")
        self.timings["canary"] = took
        always(
            ok,
            "canary queries succeed after faults stop",
            self.details(info=info, budget_s=RECOVERY_BUDGET_S),
        )
        sometimes(
            ok and took <= RECOVERY_BUDGET_S,
            "canary queries succeeded within the tight budget after faults stop",
            self.details(budget_s=RECOVERY_BUDGET_S),
        )
        if not ok:
            return 0

        ok, info, took = poll(CR_BUDGET_S * WEDGE_FACTOR, self.cr_up_to_date, "rollout")
        self.timings["cr"] = took
        always(
            ok,
            "the Materialize CR is up to date after faults stop",
            self.details(info=info),
        )
        sometimes(
            ok and took <= CR_BUDGET_S,
            "the Materialize CR was up to date within the tight budget after faults stop",
            self.details(budget_s=CR_BUDGET_S),
        )

        ok, info, took = poll(wedge, self.replicas_online, "replicas online")
        self.timings["replicas"] = took
        always(
            ok,
            "every cluster replica is online after faults stop",
            self.details(info=info),
        )
        sometimes(
            ok and took <= CHECK_BUDGET_S,
            "every cluster replica was online within the tight budget after faults stop",
            self.details(),
        )

        ok, info, took = poll(wedge, self.hydrated, "hydration")
        self.timings["hydration"] = took
        always(
            ok, "every collection hydrates after faults stop", self.details(info=info)
        )
        sometimes(
            ok and took <= CHECK_BUDGET_S,
            "every collection hydrated within the tight budget after faults stop",
            self.details(),
        )

        ok, info, took = poll(wedge, self.reconfigurations_settled, "reconfigurations")
        self.timings["reconfig"] = took
        always(
            ok,
            "no graceful cluster reconfiguration stays in progress past its deadline after faults stop",
            self.details(info=info),
        )
        sometimes(
            ok and took <= CHECK_BUDGET_S,
            "graceful cluster reconfigurations settled within the tight budget after faults stop",
            self.details(),
        )

        self.drain_replacements()

        ok, info, took = poll(wedge, self.hung_alters, "hung alters")
        self.timings["hung_alters"] = took
        info = info if isinstance(info, dict) else {}
        always(
            ok,
            "no APPLY REPLACEMENT or ALTER SINK with a live target stays unfinished after faults stop",
            self.details(info=info),
        )
        sometimes(
            ok and took <= CHECK_BUDGET_S,
            "APPLY REPLACEMENT and ALTER SINK statements finished within the tight budget after faults stop",
            self.details(),
        )
        sometimes(
            bool(info.get("target_dropped")),
            "an APPLY REPLACEMENT or ALTER SINK hung after its target was dropped (known bug database-issues#9820)",
            self.details(info=info),
        )

        ok, info, took = poll(wedge, self.frontiers_advance, "frontiers")
        self.timings["frontiers"] = took
        always(
            ok,
            "every collection frontier advances after faults stop",
            self.details(info=info),
        )
        sometimes(
            ok and took <= CHECK_BUDGET_S,
            "every collection frontier advanced within the tight budget after faults stop",
            self.details(),
        )

        ok, info, took = poll(wedge, self.collections_readable, "readable")
        self.timings["readable"] = took
        always(
            ok,
            "every live table and materialized view is readable after faults stop",
            self.details(info=info),
        )
        sometimes(
            ok and took <= CHECK_BUDGET_S,
            "every live table and materialized view was readable within the tight budget after faults stop",
            self.details(),
        )
        try:
            s = lifecycle.Session(self.env, lifecycle.open_state())
            lifecycle.check_readable(s, f"recovery-{self.mode}", require_success=True)
            s.drop_conn()
        except (psycopg.Error, OSError) as e:
            log(f"lifecycle readability check skipped: {e}")

        self.retry_ledger()

        ok, info, _ = poll(
            CHECK_BUDGET_S,
            lambda: self.self_check("/api/catalog/check"),
            "catalog check",
        )
        always(
            ok and info == "",
            "environmentd catalog consistency check passes after faults stop",
            self.details(info=info),
        )
        ok, info, _ = poll(
            CHECK_BUDGET_S,
            lambda: self.self_check("/api/coordinator/check"),
            "coordinator check",
        )
        always(
            ok and info == "",
            "environmentd coordinator consistency check passes after faults stop",
            self.details(info=info),
        )

        try:
            record_terminations(self.db, observe_pods(self.env))
        except (ApiException, OSError) as e:
            log(f"cannot read pods: {e}")
        # Recovery's own steps do no DDL while a read-only generation runs
        # (`cr_up_to_date` settles any rollout first), and eventually_ and
        # finally_ commands run without the drivers, so the designed reboot of
        # a read-only generation on leader DDL cannot repeat here.
        with self.restarts_lock:
            started = self.restarts.started
        sampled_s = time.monotonic() - started if started is not None else 0.0
        coverage_wait = max(0.0, CRASHLOOP_WINDOW_S - sampled_s)
        ok, info, took = poll(
            coverage_wait + CRASHLOOP_WINDOW_S * WEDGE_FACTOR,
            self.no_crash_loop,
            "crash loop",
        )
        took = max(0.0, took - coverage_wait)
        self.sampling_done.set()
        always(
            ok,
            "No Materialize pod crash-loops after faults stop",
            self.details(info=info, window_s=CRASHLOOP_WINDOW_S),
        )
        sometimes(
            ok and took <= CHECK_BUDGET_S,
            "No Materialize pod crash-looped within the tight budget after faults stop",
            self.details(window_s=CRASHLOOP_WINDOW_S, took_s=took),
        )

        if rng.random() < RESTART_COMPARE_P:
            try:
                self.restart_and_compare()
            except (psycopg.Error, OSError, ApiException) as e:
                log(f"restart comparison aborted: {e}")
        return 0


def start(mode: str) -> int:
    recovery: list[Recovery] = []

    def construct() -> tuple[bool, Any]:
        recovery.append(Recovery(mode))
        return True, {}

    # Resolving the SQL host needs the Kubernetes API, which also restarts
    # after faults stop.
    ok, info, _ = poll(RECOVERY_BUDGET_S, construct, "kubernetes API")
    if not ok:
        log(f"cannot reach the Kubernetes API: {info}")
        return 0
    return recovery[0].run()


def eventually_main() -> int:
    return start("eventually")


def finally_main() -> int:
    return start("finally")


if __name__ == "__main__":
    sys.exit(eventually_main())
