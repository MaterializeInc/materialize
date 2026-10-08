# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""0dt deployments driven through the real orchestratord, and their oracles.

Three commands share this module:

- `driver_main` (`parallel_driver_rollouts`) patches the Materialize CR to run
  one or two deploy actions per invocation, optionally deleting an environmentd
  or orchestratord pod at a chosen phase. It is a parallel driver so that
  client writes, DDL, and ingestion from the other drivers overlap catch-up,
  promotion, and the candidate's reboot. Only the invocation holding the CR
  lease (`CrLease`) writes the spec. Invocations that cannot take it watch the
  CR for a short while to evaluate the overlap anchors, then exit.
- `observer_main` (`anytime_rollout_observer`) watches the CR and the
  StatefulSets, samples the environmentd pods and each incarnation's leader
  status, and checks the deploy safety properties against history kept in
  SQLite. Status properties are checked on every stored CR version, in order,
  across invocations (`CrWatcher`).
- `converge_main` (`eventually_rollout_converges`) checks that, once faults
  and patching stop, the environment settles on one serving generation at the
  last requested spec. It keeps the CR and StatefulSet watches running, so
  rollouts it requests are checked too.

The workload is the only writer of the CR spec and never writes its status.
Every patch is appended to the `submitted` table, keyed by `requestRollout`,
before it is sent, and marked acknowledged, rejected, or indeterminate after.
Operator steps (`OperatorUnstick`) patch without the lease: they run only from
`eventually_` and `finally_` commands, which never overlap a driver.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import psycopg
import requests
import urllib3
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    reachable,
    sometimes,
)
from kubernetes import client  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore

from materialize import orchestratord
from materialize.antithesis import kube_watch, sql
from materialize.antithesis.drivers import counters, history, lifecycle
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.environment import Environment
from materialize.antithesis.kube_watch import ObjectChain
from materialize.antithesis.quiet import request_quiet_period
from materialize.antithesis.rng import rng
from materialize.antithesis.state import open_db
from materialize.mz_version import MzVersion

# Every duration bound below needs calibration on one simulated core, where
# both environmentd generations, every clusterd twice, orchestratord, and the
# dependencies share a single CPU.

# Per Kubernetes API call. Needs calibration on one simulated core.
# NOTE: passed as `_request_timeout`, which every generated client method
# accepts but kubernetes-stubs omits, hence the `reportCallIssue` ignores.
K8S_TIMEOUT_SECONDS = 10
# Per `/api/leader/status` probe. Needs calibration on one simulated core.
LEADER_PROBE_TIMEOUT_SECONDS = 3.0
# Poll interval while following a rollout. Short enough to see `Promoting`,
# which lasts from the status write to envd's reboot as leader. Needs
# calibration on one simulated core.
POLL_SECONDS = 0.5
# How long the driver follows one rollout before moving on. Not a liveness
# bound: faults may legitimately stall a rollout. Needs calibration on one
# simulated core.
FOLLOW_TIMEOUT_SECONDS = 15 * 60
# How long the driver waits for a rollout to reach an intermediate phase
# before acting anyway. Needs calibration on one simulated core.
PHASE_WAIT_SECONDS = 10 * 60
# One clean same-version rollout, request to `Applied`, with no faults. Needs
# calibration on one simulated core.
T_ROLLOUT_SECONDS = 10 * 60
# One read-only environmentd bootstrap, from container start to the catch-up
# loop's first DDL check. Needs calibration on one simulated core.
READ_ONLY_BOOTSTRAP_SECONDS = 2 * 60
# `with_0dt_deployment_ddl_check_interval` default, used when the live value
# cannot be read.
DEFAULT_DDL_CHECK_INTERVAL_SECONDS = 5 * 60
# Paced-DDL bound multiplier K in `K * Q + T_ROLLOUT_SECONDS`. Needs
# calibration on one simulated core.
PACED_DDL_K = 2
# DDL bursts per paced-DDL rollout. The bound is measured from the last one.
PACED_DDL_BURSTS = 3
# Time for the system to serve SQL again once a quiet period starts and
# Antithesis restarts killed containers. Needs calibration on one simulated
# core.
RECOVERY_SECONDS = 5 * 60
# Performance bound for `eventually_rollout_converges`, from the start of the
# command. Covers one fresh rollout plus catch-up, promotion, reboot, and old
# clusterd cleanup. Needs calibration on one simulated core.
CONVERGENCE_TIMEOUT_SECONDS = 30 * 60
# Wedge bound for the same command. Exceeding it means the deployment is
# stuck, not slow.
CONVERGENCE_WEDGED_SECONDS = 2 * CONVERGENCE_TIMEOUT_SECONDS
# Wedge bound for paced DDL, as a multiple of its performance bound.
PACED_DDL_WEDGED_FACTOR = 2
# How long the CR lease stays valid without renewal. The holder renews it on
# every poll, so this only needs to exceed the longest gap between polls: one
# DDL burst or SQL recovery wait, both bounded by `RECOVERY_SECONDS`.
CR_LEASE_SECONDS = 2 * RECOVERY_SECONDS
# The holder rewrites the lease row at most this often.
CR_LEASE_RENEW_SECONDS = 30.0
# How long an invocation without the lease watches the CR.
NON_HOLDER_WATCH_SECONDS = 60.0
# How many of the most recent rows of another driver's `ops` table the overlap
# anchors scan. Drivers append an op when they invoke it and complete it within
# a statement timeout, so recent acknowledgements are among the latest rows.
OVERLAP_OPS_SCAN = 5000
# Wall time of one observer invocation, and the minimum interval between its
# pod and leader samples. Needs calibration on one simulated core.
OBSERVER_DURATION_SECONDS = 2 * 60
OBSERVER_INTERVAL_SECONDS = 1.0
# Server-side timeout of one watch request. Watches resume from the last
# resourceVersion, so this only bounds how long one watch blocks the observer
# loop when nothing changes.
WATCH_WINDOW_SECONDS = 1
# The observer reads `mz_version()` whenever the watched CR's phase, active
# generation or completed image changed, and otherwise at most this often. A
# SQL round trip per loop would slow the loop whenever environmentd is
# unreachable.
VERSION_SAMPLE_SECONDS = 5.0
# Observer state older than this is not treated as the previous sample when
# computing transition reach claims.
OBSERVER_PREVIOUS_MAX_AGE_SECONDS = 15.0

DEFAULT_ROLLOUT_REQUEST_TIMEOUT = "24h"
DEFAULT_ROLLOUT_STRATEGY = "WaitUntilReady"
FORCE_ROLLOUT_ANNOTATION = "materialize.cloud/force-rollout"
LEASE_NAME = "orchestratord"
MIGRATION_ARG_PREFIX = "--unsafe-force-builtin-schema-migration="
SPEC_FIELDS = (
    "environmentdImageRef",
    "environmentdExtraArgs",
    "requestRollout",
    "forceRollout",
    "forcePromote",
    "rolloutStrategy",
    "rolloutRequestTimeout",
)

REASONS_IN_PROGRESS = ("Applying", "ReadyToPromote", "Promoting")

TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    ApiException,
    urllib3.exceptions.HTTPError,
    requests.RequestException,
    psycopg.Error,
    sqlite3.OperationalError,
    OSError,
    TimeoutError,
    RuntimeError,
)


def log(message: str) -> None:
    print(f"rollouts: {message}", flush=True)


def new_uuid() -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


@dataclass(frozen=True)
class Snapshot:
    """One read of the Materialize CR."""

    obj: dict[str, Any]

    @property
    def spec(self) -> dict[str, Any]:
        return self.obj.get("spec") or {}

    @property
    def status(self) -> dict[str, Any]:
        return self.obj.get("status") or {}

    @property
    def resource_version(self) -> str | None:
        return (self.obj.get("metadata") or {}).get("resourceVersion")

    @property
    def generation(self) -> int | None:
        return (self.obj.get("metadata") or {}).get("generation")

    @property
    def condition(self) -> dict[str, Any]:
        for c in self.status.get("conditions") or []:
            if c.get("type") == "UpToDate":
                return c
        return {}

    @property
    def reason(self) -> str | None:
        return self.condition.get("reason")

    @property
    def condition_status(self) -> str | None:
        return self.condition.get("status")

    @property
    def active(self) -> int | None:
        value = self.status.get("activeGeneration")
        return int(value) if value is not None else None

    @property
    def request(self) -> str | None:
        return self.spec.get("requestRollout")

    @property
    def last_completed(self) -> str | None:
        return self.status.get("lastCompletedRolloutRequest")

    @property
    def last_completed_image(self) -> str | None:
        return self.status.get("lastCompletedRolloutEnvironmentdImageRef")

    @property
    def resource_id(self) -> str | None:
        return self.status.get("resourceId")

    def expected_force(self) -> str:
        """The `materialize.cloud/force` value orchestratord derives from this
        spec, as `force_rollout_value` does."""
        # An absent `forceRollout` deserializes to the nil UUID.
        force = self.spec.get("forceRollout") or str(uuid.UUID(int=0))
        annotation = ((self.obj.get("metadata") or {}).get("annotations") or {}).get(
            FORCE_ROLLOUT_ANNOTATION
        )
        return f"{force}/{annotation}" if annotation is not None else str(force)

    def tracked_spec(self) -> dict[str, Any]:
        return {k: self.spec.get(k) for k in SPEC_FIELDS}

    def summary(self) -> dict[str, Any]:
        return {
            "resource_version": self.resource_version,
            "generation": self.generation,
            "active_generation": self.active,
            "reason": self.reason,
            "condition_status": self.condition_status,
            "observed_generation": self.condition.get("observedGeneration"),
            "request": self.request,
            "last_completed": self.last_completed,
            "last_completed_image": self.last_completed_image,
        }


@dataclass(frozen=True)
class EnvdStatefulSet:
    generation: int
    name: str
    uid: str
    image: str
    force: str | None
    args: tuple[str, ...]
    deleting: bool


@dataclass(frozen=True)
class EnvdPod:
    """One environmentd incarnation: a pod UID plus its container restart count."""

    generation: int
    name: str
    uid: str
    ip: str | None
    restart_count: int
    started_at: int | None
    """Container start, in seconds, from the kubelet's clock."""
    ready: bool
    last_exit_code: int | None

    def incarnation(self) -> tuple[str, int, int | None, str | None]:
        return (self.uid, self.restart_count, self.started_at, self.ip)


class Kube:
    """Kubernetes reads and writes for one Materialize CR, with request timeouts."""

    def __init__(self, env: Environment) -> None:
        self.env = env
        self.namespace = env.endpoints.namespace
        self.operator_namespace = env.endpoints.operator_namespace
        self.name = env.endpoints.environment
        self.custom = client.CustomObjectsApi()
        self.apps = client.AppsV1Api()
        self.core = client.CoreV1Api()
        self.coordination = client.CoordinationV1Api()

    def snapshot(self) -> Snapshot:
        return Snapshot(
            self.custom.get_namespaced_custom_object(
                orchestratord.GROUP,
                orchestratord.VERSION,
                self.namespace,
                orchestratord.PLURAL,
                self.name,
                _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
            )
        )

    def try_snapshot(self) -> Snapshot | None:
        try:
            return self.snapshot()
        except TRANSIENT_ERRORS as e:
            log(f"reading the CR failed: {e}")
            return None

    def patch_spec(self, spec: dict[str, Any]) -> None:
        """Merge-patch the spec. A `None` value removes the field."""
        self.custom.patch_namespaced_custom_object(
            orchestratord.GROUP,
            orchestratord.VERSION,
            self.namespace,
            orchestratord.PLURAL,
            self.name,
            {"spec": spec},
            _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        )

    def envd_statefulsets(self, resource_id: str) -> list[EnvdStatefulSet]:
        prefix = f"mz{resource_id}-environmentd-"
        result = []
        for sts in self.apps.list_namespaced_stateful_set(
            self.namespace,
            _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        ).items:
            metadata = sts.metadata
            assert metadata is not None and metadata.name is not None
            suffix = metadata.name.removeprefix(prefix)
            if suffix == metadata.name or not suffix.isdigit():
                continue
            assert metadata.uid is not None and sts.spec is not None
            template = sts.spec.template
            assert template.spec is not None
            containers = template.spec.containers
            container = next(
                (c for c in containers if c.name == "environmentd"), containers[0]
            )
            assert container.image is not None
            result.append(
                EnvdStatefulSet(
                    generation=int(suffix),
                    name=metadata.name,
                    uid=metadata.uid,
                    image=container.image,
                    force=(metadata.annotations or {}).get("materialize.cloud/force"),
                    args=tuple(container.args or ()),
                    deleting=metadata.deletion_timestamp is not None,
                )
            )
        return sorted(result, key=lambda s: s.generation)

    def envd_pods(self) -> list[EnvdPod]:
        pods = self.core.list_namespaced_pod(
            self.namespace,
            label_selector="materialize.cloud/app=environmentd",
            _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        ).items
        result = []
        for pod in pods:
            metadata = pod.metadata
            assert metadata is not None
            generation = (metadata.annotations or {}).get(
                "materialize.cloud/generation"
            )
            if generation is None or not generation.isdigit():
                continue
            assert metadata.name is not None and metadata.uid is not None
            assert pod.status is not None
            statuses = pod.status.container_statuses or []
            main = next((s for s in statuses if s.name == "environmentd"), None)
            if main is None and statuses:
                main = statuses[0]
            started_at = None
            last_exit_code = None
            if main is not None:
                if main.state and main.state.running and main.state.running.started_at:
                    started_at = int(main.state.running.started_at.timestamp())
                if main.last_state and main.last_state.terminated:
                    last_exit_code = main.last_state.terminated.exit_code
            result.append(
                EnvdPod(
                    generation=int(generation),
                    name=metadata.name,
                    uid=metadata.uid,
                    ip=pod.status.pod_ip,
                    restart_count=sum(s.restart_count for s in statuses),
                    started_at=started_at,
                    ready=bool(statuses) and all(s.ready for s in statuses),
                    last_exit_code=last_exit_code,
                )
            )
        return result

    def clusterd_statefulsets(self) -> list[str]:
        names = []
        for sts in self.apps.list_namespaced_stateful_set(
            self.namespace,
            _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        ).items:
            assert sts.metadata is not None and sts.metadata.name is not None
            names.append(sts.metadata.name)
        return names

    def lease_holder(self) -> str | None:
        lease = self.coordination.read_namespaced_lease(
            LEASE_NAME,
            self.operator_namespace,
            _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        )
        return lease.spec.holder_identity if lease.spec else None


# Matches `<prefix>cluster-<cluster>-replica-<replica>-gen-<g>`, the name
# environmentd's Kubernetes orchestrator gives each replica StatefulSet.
CLUSTERD_NAME = re.compile(
    r"cluster-(?P<cluster>[a-z]+\d+)-replica-(?P<replica>[a-z]+\d+)-gen-(?P<gen>\d+)$"
)


def leader_statuses(pods: list[EnvdPod]) -> dict[str, str | None]:
    return {
        p.uid: (
            orchestratord.leader_status(p.ip, timeout=LEADER_PROBE_TIMEOUT_SECONDS)
            if p.ip
            else None
        )
        for p in pods
    }


def consistent_leader_samples(
    kube: Kube,
) -> tuple[list[EnvdPod], list[tuple[EnvdPod, str]]]:
    """Leader status per incarnation, kept only if the pod did not change while probed.

    Also returns the pod listing taken after the probes.
    """
    before = kube.envd_pods()
    statuses = leader_statuses(before)
    after_list = kube.envd_pods()
    after = {p.uid: p for p in after_list}
    samples = []
    for pod in before:
        status = statuses.get(pod.uid)
        later = after.get(pod.uid)
        if status is None or later is None:
            continue
        if later.incarnation() != pod.incarnation():
            continue
        samples.append((pod, status))
    return after_list, samples


# Shared driver state.


def requests_db(env: Environment) -> sqlite3.Connection:
    db = open_db("rollout_requests", env.endpoints)
    db.execute(
        "CREATE TABLE IF NOT EXISTS submitted ("
        " seq INTEGER PRIMARY KEY AUTOINCREMENT,"
        " request_id TEXT,"
        " action TEXT NOT NULL,"
        " phase_before TEXT,"
        " spec TEXT NOT NULL,"
        " outcome TEXT NOT NULL,"
        " at REAL NOT NULL)"
    )
    db.execute("CREATE INDEX IF NOT EXISTS submitted_request ON submitted (request_id)")
    db.execute(
        "CREATE TABLE IF NOT EXISTS params (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS ddl_objects (name TEXT PRIMARY KEY, kind TEXT NOT NULL)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS counters (key TEXT PRIMARY KEY, value INTEGER NOT NULL)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS cr_lease ("
        " slot INTEGER PRIMARY KEY CHECK (slot = 0),"
        " holder TEXT NOT NULL, pid INTEGER NOT NULL, expires REAL NOT NULL)"
    )
    db.commit()
    return db


class LeaseLost(Exception):
    """The CR lease passed to another invocation. Not transient: the holder
    must stop writing the spec, so this escapes `TRANSIENT_ERRORS` handlers."""


class CrLease:
    """Exclusive right to write the CR spec, held by one driver invocation.

    The lease row names a holder and a `time.monotonic()` expiry, which every
    command in the workload container can compare. A lease is free when it is
    expired or its holder process is gone. Acquire and renew run under
    `BEGIN IMMEDIATE`, so two invocations never both believe they hold it.
    """

    def __init__(self, env: Environment) -> None:
        self.db = open_db("rollout_requests", env.endpoints)
        self.db.isolation_level = None
        self.holder = f"{os.getpid()}-{new_uuid()}"
        self.renewed_at: float | None = None

    def _take(self, steal: bool) -> bool:
        now = time.monotonic()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT holder, pid, expires FROM cr_lease WHERE slot = 0"
            ).fetchone()
            ours = row is not None and row[0] == self.holder
            free = row is None or row[2] <= now or not lifecycle.pid_alive(row[1])
            if not ours and not (steal and free):
                self.db.execute("ROLLBACK")
                return False
            self.db.execute(
                "INSERT INTO cr_lease (slot, holder, pid, expires) VALUES (0, ?, ?, ?)"
                " ON CONFLICT (slot) DO UPDATE"
                " SET holder = excluded.holder, pid = excluded.pid,"
                " expires = excluded.expires",
                (self.holder, os.getpid(), now + CR_LEASE_SECONDS),
            )
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.renewed_at = now
        return True

    def acquire(self) -> bool:
        return self._take(steal=True)

    def renew(self) -> None:
        """Extend the lease. Raises `LeaseLost` if another invocation took it."""
        if self.renewed_at is None:
            raise LeaseLost("the CR lease was never acquired")
        if time.monotonic() - self.renewed_at < CR_LEASE_RENEW_SECONDS:
            return
        if not self._take(steal=False):
            raise LeaseLost("the CR lease passed to another invocation")

    def release(self) -> None:
        self.db.execute(
            "DELETE FROM cr_lease WHERE slot = 0 AND holder = ?", (self.holder,)
        )
        self.renewed_at = None


def peek_db(name: str, endpoints: Endpoints) -> sqlite3.Connection | None:
    """Open another driver's state database read-only, or None if it does not exist yet."""
    path = (endpoints.state_dir / f"{name}.sqlite").resolve()
    if not path.exists():
        return None
    try:
        return sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=10)
    except sqlite3.Error:
        return None


def acked_writes_between(endpoints: Endpoints, start: float, end: float) -> int:
    """Client-history and counter writes acknowledged within `(start, end)`.

    Both drivers record completion with `time.monotonic()` in this container.
    """
    total = 0
    for name, kind_filter in (
        (history.STATE_DB, " AND kind = 'w'"),
        (counters.STATE_DB, ""),
    ):
        conn = peek_db(name, endpoints)
        if conn is None:
            continue
        try:
            row = conn.execute(
                "SELECT count(*) FROM ops"
                " WHERE op_id > (SELECT coalesce(max(op_id), 0) - ? FROM ops)"
                f" AND outcome = 'ok' AND complete_rt > ? AND complete_rt < ?{kind_filter}",
                (OVERLAP_OPS_SCAN, start, end),
            ).fetchone()
            total += int(row[0])
        except sqlite3.Error:
            pass
        finally:
            conn.close()
    return total


def lifecycle_ddl_in_flight(endpoints: Endpoints) -> int:
    """Lifecycle DDL statements running right now, by live invocations."""
    conn = peek_db(lifecycle.STATE_DB, endpoints)
    if conn is None:
        return 0
    try:
        pids = [r[0] for r in conn.execute("SELECT pid FROM inflight").fetchall()]
    except sqlite3.Error:
        return 0
    finally:
        conn.close()
    return sum(1 for pid in pids if lifecycle.pid_alive(pid))


@dataclass
class PhaseOverlap:
    """Anchors proving other drivers' load overlapped a rollout phase.

    Activity is attributed to a phase only when it was read between two
    consecutive CR snapshots that saw the same request, reason, and active
    generation. Leaving a phase and returning to it between two polls would
    need a second rollout to reach the same reason with the same request and
    active generation, which takes longer than one poll.
    """

    endpoints: Endpoints
    previous: tuple[tuple[Any, ...], float] | None = None

    def poll(self, kube: Kube) -> Snapshot | None:
        read_at = time.monotonic()
        ddl = lifecycle_ddl_in_flight(self.endpoints)
        snap = kube.try_snapshot()
        if snap is None:
            self.previous = None
            return None
        key = (snap.request, snap.reason, snap.active)
        previous, self.previous = self.previous, (key, time.monotonic())
        if previous is None or previous[0] != key:
            return snap
        if snap.reason not in ("Applying", "Promoting"):
            return snap
        writes = acked_writes_between(self.endpoints, previous[1], read_at)
        details = {**snap.summary(), "acked_writes": writes, "ddl_in_flight": ddl}
        if snap.reason == "Applying":
            sometimes(
                writes > 0,
                "A client write was acknowledged while a rollout was Applying",
                details,
            )
            sometimes(
                ddl > 0,
                "Lifecycle DDL was in flight while a rollout was Applying",
                details,
            )
        else:
            sometimes(
                writes > 0,
                "A client write was acknowledged while a rollout was Promoting",
                details,
            )
            sometimes(
                ddl > 0,
                "Lifecycle DDL was in flight while a rollout was Promoting",
                details,
            )
        return snap


def next_counter(db: sqlite3.Connection, key: str) -> int:
    db.execute(
        "INSERT INTO counters (key, value) VALUES (?, 1)"
        " ON CONFLICT (key) DO UPDATE SET value = value + 1",
        (key,),
    )
    db.commit()
    return int(
        db.execute("SELECT value FROM counters WHERE key = ?", (key,)).fetchone()[0]
    )


ACTIONS = (
    "same_version",
    "forced_migration",
    "supersede",
    "revert",
    "patch_during_promoting",
    "force_promote",
    "manual_promote",
    "short_timeout",
    "paced_ddl",
    "upgrade",
)


@dataclass(frozen=True)
class TimelineParams:
    """Swarm parameters, drawn once per timeline so timelines skew differently."""

    weights: dict[str, int]
    kill_probability: float


def timeline_params(db: sqlite3.Connection) -> TimelineParams:
    row = db.execute("SELECT value FROM params WHERE key = 'timeline'").fetchone()
    if row is not None:
        raw = json.loads(row[0])
        return TimelineParams(raw["weights"], raw["kill_probability"])
    while True:
        weights = {a: rng.choice([0, 1, 1, 4]) for a in ACTIONS}
        # paced_ddl holds a long quiet period, so it is never the dominant action.
        weights["paced_ddl"] = rng.choice([0, 0, 1])
        # Never zero: a timeline that starts on an older release must be able
        # to leave it. Without a pending upgrade the action is a same-version
        # deploy.
        weights["upgrade"] = rng.choice([1, 2, 4])
        if any(weights.values()):
            break
    params = TimelineParams(weights, rng.choice([0.0, 0.3, 0.9]))
    db.execute(
        "INSERT OR IGNORE INTO params (key, value) VALUES ('timeline', ?)",
        (
            json.dumps(
                {"weights": params.weights, "kill_probability": params.kill_probability}
            ),
        ),
    )
    db.commit()
    return params


@dataclass
class Ctx:
    env: Environment
    kube: Kube
    db: sqlite3.Connection
    params: TimelineParams
    lease: CrLease | None = None
    """Held by the driver. Operator contexts have none."""
    overlap: PhaseOverlap | None = None

    def poll(self) -> Snapshot | None:
        """Renew the lease, then read the CR, evaluating the overlap anchors."""
        if self.lease is not None:
            self.lease.renew()
        if self.overlap is not None:
            return self.overlap.poll(self.kube)
        return self.kube.try_snapshot()

    def sleep(self, seconds: float) -> None:
        """Sleep, renewing the lease."""
        deadline = time.monotonic() + seconds
        while True:
            if self.lease is not None:
                self.lease.renew()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(remaining, POLL_SECONDS))


def submit(ctx: Ctx, action: str, changes: dict[str, Any]) -> str | None:
    """Patch the spec, recording the intended spec before and the outcome after.

    Returns the `requestRollout` of the intended spec, which is the new one when
    `changes` sets it, whether or not the patch landed. Returns None, without
    patching, if the CR could not be read first. Raises `LeaseLost` before
    patching if the context's lease passed to another invocation.
    """
    if ctx.lease is not None:
        ctx.lease.renew()
    before = ctx.kube.try_snapshot()
    if before is None:
        return None
    intended = {**before.tracked_spec(), **changes}
    request = intended.get("requestRollout")
    cursor = ctx.db.execute(
        "INSERT INTO submitted (request_id, action, phase_before, spec, outcome, at)"
        " VALUES (?, ?, ?, ?, 'attempted', ?)",
        (request, action, before.reason, json.dumps(intended), time.time()),
    )
    ctx.db.commit()
    seq = cursor.lastrowid
    try:
        ctx.kube.patch_spec(changes)
        outcome = "acked"
    except ApiException as e:
        outcome = (
            "rejected"
            if e.status is not None and 400 <= e.status < 500
            else "indeterminate"
        )
        log(f"{action}: patch {outcome}: {e.status} {e.reason}")
    except TRANSIENT_ERRORS as e:
        outcome = "indeterminate"
        log(f"{action}: patch indeterminate: {e}")
    ctx.db.execute("UPDATE submitted SET outcome = ? WHERE seq = ?", (outcome, seq))
    ctx.db.commit()
    log(f"{action}: submitted {changes} ({outcome}, phase before {before.reason})")
    return request


def new_request() -> dict[str, Any]:
    request = new_uuid()
    return {"requestRollout": request, "forceRollout": request}


def toggled_migration_args(spec: dict[str, Any]) -> list[str] | None:
    """Add a forced builtin migration if none is configured, else remove it."""
    args = spec.get("environmentdExtraArgs") or []
    if any(a.startswith(MIGRATION_ARG_PREFIX) for a in args):
        return None
    mode = rng.choice(["replacement", "evolution"])
    return ["--unsafe-mode", f"{MIGRATION_ARG_PREFIX}{mode}"]


@dataclass(frozen=True)
class KillPlan:
    phase: str
    """A CR reason, or `CandidateIsLeader`."""
    target: str
    """`active`, `candidate`, or `orchestratord`."""
    grace: int | None


# Grace periods for harness pod deletes: abrupt, or the pod's default. Never 0:
# a zero grace period force-deletes the pod object before its containers stop,
# so a StatefulSet starts the replacement alongside the old incarnation, which
# no real failure does.
GRACE_MENU = (1, None)


def maybe_kill_plan(ctx: Ctx) -> KillPlan | None:
    if rng.random() >= ctx.params.kill_probability:
        return None
    return KillPlan(
        phase=rng.choice(
            ["Applying", "ReadyToPromote", "Promoting", "CandidateIsLeader"]
        ),
        target=rng.choice(["active", "candidate", "orchestratord"]),
        grace=rng.choice(GRACE_MENU),
    )


def kill_due(ctx: Ctx, plan: KillPlan, snap: Snapshot) -> bool:
    if plan.phase != "CandidateIsLeader":
        return snap.reason == plan.phase
    if snap.reason != "Promoting" or snap.active is None:
        return False
    candidates = [p for p in ctx.kube.envd_pods() if p.generation == snap.active + 1]
    return any(
        p.ip
        and orchestratord.leader_status(p.ip, timeout=LEADER_PROBE_TIMEOUT_SECONDS)
        == "IsLeader"
        for p in candidates
    )


def execute_kill(ctx: Ctx, plan: KillPlan, snap: Snapshot) -> bool:
    # `pod_restarts` imports this module for `Kube`.
    from materialize.antithesis.drivers import pod_restarts

    reason = f"rollout {snap.request} at {snap.reason}"
    if plan.target == "orchestratord":
        holder = ctx.kube.lease_holder()
        if holder is None or not pod_restarts.delete_recorded(
            ctx.kube,
            origin="rollouts",
            role="orchestratord",
            namespace=ctx.kube.operator_namespace,
            pod=holder,
            uid=None,
            grace=plan.grace,
            reason=reason,
        ):
            return False
        reachable(
            "Rollout driver deleted the orchestratord leader pod mid-rollout",
            {"phase": snap.reason, "pod": holder},
        )
    else:
        if snap.active is None:
            return False
        generation = snap.active if plan.target == "active" else snap.active + 1
        pods = [p for p in ctx.kube.envd_pods() if p.generation == generation]
        if not pods:
            return False
        deleted = [
            p.name
            for p in pods
            if pod_restarts.delete_recorded(
                ctx.kube,
                origin="rollouts",
                role=f"environmentd_{plan.target}",
                namespace=ctx.kube.namespace,
                pod=p.name,
                uid=p.uid,
                grace=plan.grace,
                reason=reason,
            )
        ]
        if not deleted:
            return False
        if plan.target == "active":
            reachable(
                "Rollout driver deleted the active environmentd pod mid-rollout",
                {"phase": snap.reason, "pods": deleted},
            )
        else:
            reachable(
                "Rollout driver deleted the candidate environmentd pod mid-rollout",
                {"phase": snap.reason, "pods": deleted},
            )
    sometimes(
        snap.reason == "Promoting",
        "Rollout driver deleted a pod while the rollout was Promoting",
        {"phase": snap.reason, "target": plan.target},
    )
    log(f"killed {plan.target} at {snap.reason}")
    return True


@dataclass
class Followed:
    phases: list[str] = field(default_factory=list)
    outcome: str | None = None
    """Terminal reason for the request, `Superseded`, or None on timeout."""
    killed_phase: str | None = None


TERMINAL_FOR_REQUEST = ("Applied", "RolloutTimeout", "WaitingForApproval")


def follow(
    ctx: Ctx,
    request: str | None,
    timeout: float = FOLLOW_TIMEOUT_SECONDS,
    kill: KillPlan | None = None,
) -> Followed:
    """Poll the CR until `request` completes, is replaced, or `timeout` passes."""
    result = Followed()
    if request is None:
        return result
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = ctx.poll()
        if snap is not None:
            if snap.request != request:
                result.outcome = "Superseded"
                return result
            if snap.reason and (not result.phases or result.phases[-1] != snap.reason):
                result.phases.append(snap.reason)
            if snap.last_completed == request and snap.reason in TERMINAL_FOR_REQUEST:
                result.outcome = snap.reason
                return result
            if kill is not None and result.killed_phase is None:
                try:
                    if kill_due(ctx, kill, snap) and execute_kill(ctx, kill, snap):
                        result.killed_phase = snap.reason
                except TRANSIENT_ERRORS as e:
                    log(f"kill attempt failed: {e}")
        time.sleep(POLL_SECONDS)
    return result


def wait_for_phase(
    ctx: Ctx, request: str | None, phases: tuple[str, ...], timeout: float
) -> Snapshot | None:
    """Poll until `request` is in one of `phases`, completes, or is replaced."""
    if request is None:
        return None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = ctx.poll()
        if snap is not None:
            if snap.request != request or snap.reason in phases:
                return snap
            if snap.last_completed == request and snap.reason in TERMINAL_FOR_REQUEST:
                return snap
        time.sleep(POLL_SECONDS)
    return ctx.kube.try_snapshot()


def note_kill_outcome(res: Followed) -> None:
    if res.killed_phase == "Promoting":
        sometimes(
            res.outcome == "Applied",
            "Rollout reached Applied after a pod kill during Promoting",
            {"phases": res.phases, "outcome": res.outcome},
        )


# Delays between reaching a phase and acting on it. Zero acts on the first
# poll that sees the phase; the longest stays well inside one catch-up.
ACT_DELAY_MENU = (0, 1, 5, 30)
# How long a ManuallyPromote rollout sits in ReadyToPromote before the force
# promote. Needs calibration on one simulated core.
MANUAL_WAIT_MENU = (0, 5, 30, 120)


def action_same_version(ctx: Ctx) -> None:
    request = submit(ctx, "same_version", new_request())
    res = follow(ctx, request, kill=maybe_kill_plan(ctx))
    if res.outcome == "Applied":
        reachable("Same-version deploy reached Applied", {"phases": res.phases})
    note_kill_outcome(res)


def action_forced_migration(ctx: Ctx) -> None:
    snap = ctx.kube.try_snapshot()
    if snap is None:
        return
    args = toggled_migration_args(snap.spec)
    request = submit(
        ctx, "forced_migration", {**new_request(), "environmentdExtraArgs": args}
    )
    res = follow(ctx, request, kill=maybe_kill_plan(ctx))
    if res.outcome == "Applied" and args:
        reachable(
            "Deploy with forced builtin schema migration reached Applied",
            {"args": args, "phases": res.phases},
        )
    note_kill_outcome(res)


def maybe_toggle(snap: Snapshot | None) -> dict[str, Any]:
    if snap is None or rng.random() < 0.5:
        return {}
    return {"environmentdExtraArgs": toggled_migration_args(snap.spec)}


def action_supersede(ctx: Ctx) -> None:
    first = submit(
        ctx,
        "supersede_first",
        {**new_request(), **maybe_toggle(ctx.kube.try_snapshot())},
    )
    wait_for_phase(ctx, first, ("Applying",), PHASE_WAIT_SECONDS)
    ctx.sleep(rng.choice(ACT_DELAY_MENU))
    snap = ctx.kube.try_snapshot()
    sometimes(
        snap is not None and snap.request == first and snap.reason == "Applying",
        "Rollout driver superseded a rollout while it was Applying",
        snap.summary() if snap else {},
    )
    second = submit(ctx, "supersede_second", {**new_request(), **maybe_toggle(snap)})
    note_kill_outcome(follow(ctx, second, kill=maybe_kill_plan(ctx)))


def action_revert(ctx: Ctx) -> None:
    prior = ctx.kube.try_snapshot()
    if prior is None:
        return
    first = submit(ctx, "revert_forward", {**new_request(), **maybe_toggle(prior)})
    wait_for_phase(ctx, first, ("Applying",), PHASE_WAIT_SECONDS)
    ctx.sleep(rng.choice(ACT_DELAY_MENU))
    snap = ctx.kube.try_snapshot()
    sometimes(
        snap is not None and snap.request == first and snap.reason == "Applying",
        "Rollout driver reverted a rollout while it was Applying",
        snap.summary() if snap else {},
    )
    changes: dict[str, Any] = {
        "forceRollout": prior.spec.get("forceRollout"),
        "environmentdExtraArgs": prior.spec.get("environmentdExtraArgs"),
    }
    # Reverting with the previous request id completes nothing new; with the
    # forward id or a fresh one, `Applied` records that id.
    variant = rng.choice(["restore_request", "keep_request", "fresh_request"])
    if variant == "restore_request" and prior.request is not None:
        changes["requestRollout"] = prior.request
    elif variant == "fresh_request":
        changes["requestRollout"] = new_uuid()
    request = submit(ctx, f"revert_{variant}", changes)
    note_kill_outcome(follow(ctx, request, kill=maybe_kill_plan(ctx)))


def action_patch_during_promoting(ctx: Ctx) -> None:
    prior = ctx.kube.try_snapshot()
    if prior is None:
        return
    first = submit(ctx, "promoting_forward", {**new_request(), **maybe_toggle(prior)})
    snap = wait_for_phase(ctx, first, ("Promoting",), FOLLOW_TIMEOUT_SECONDS)
    sometimes(
        snap is not None and snap.request == first and snap.reason == "Promoting",
        "Rollout driver patched the spec while the rollout was Promoting",
        snap.summary() if snap else {},
    )
    variant = rng.choice(["new_request", "same_request", "revert"])
    changes: dict[str, Any]
    if variant == "new_request":
        changes = {
            **new_request(),
            "environmentdExtraArgs": toggled_migration_args(
                snap.spec if snap else prior.spec
            ),
        }
    elif variant == "same_request":
        changes = rng.choice(
            [
                {"forceRollout": new_uuid()},
                {
                    "environmentdExtraArgs": toggled_migration_args(
                        snap.spec if snap else prior.spec
                    )
                },
            ]
        )
    else:
        changes = {
            "forceRollout": prior.spec.get("forceRollout"),
            "environmentdExtraArgs": prior.spec.get("environmentdExtraArgs"),
        }
    request = submit(ctx, f"promoting_{variant}", changes)
    note_kill_outcome(follow(ctx, request, kill=maybe_kill_plan(ctx)))


def action_force_promote(ctx: Ctx) -> None:
    changes = new_request()
    request = changes["requestRollout"]
    if rng.random() < 0.5:
        submit(ctx, "force_promote_same_patch", {**changes, "forcePromote": request})
    else:
        submit(ctx, "force_promote_forward", changes)
        wait_for_phase(ctx, request, ("Applying",), PHASE_WAIT_SECONDS)
        ctx.sleep(rng.choice(ACT_DELAY_MENU))
        submit(ctx, "force_promote_later", {"forcePromote": request})
    res = follow(ctx, request, kill=maybe_kill_plan(ctx))
    if res.outcome == "Applied":
        reachable("Force-promoted rollout reached Applied", {"phases": res.phases})
    note_kill_outcome(res)


def action_manual_promote(ctx: Ctx) -> None:
    request = submit(
        ctx, "manual_promote", {**new_request(), "rolloutStrategy": "ManuallyPromote"}
    )
    try:
        snap = wait_for_phase(ctx, request, ("ReadyToPromote",), FOLLOW_TIMEOUT_SECONDS)
        waited = (
            snap is not None
            and snap.request == request
            and snap.reason == "ReadyToPromote"
        )
        if waited:
            ctx.sleep(rng.choice(MANUAL_WAIT_MENU))
        submit(ctx, "manual_promote_approve", {"forcePromote": request})
        res = follow(ctx, request, kill=maybe_kill_plan(ctx))
        if waited and res.outcome == "Applied":
            reachable(
                "ManuallyPromote rollout reached Applied after a force promote from ReadyToPromote",
                {"phases": res.phases},
            )
        note_kill_outcome(res)
    finally:
        submit(
            ctx, "manual_promote_restore", {"rolloutStrategy": DEFAULT_ROLLOUT_STRATEGY}
        )


def action_upgrade(ctx: Ctx) -> None:
    """Roll out the image under test over an environment on an older release.

    Once the spec names the target image, every other action keeps it, so an
    upgrade that times out or is superseded is retried by whatever rolls out
    next. Without a pending upgrade this is a same-version deploy.
    """
    target = ctx.env.endpoints.upgrade_pending_image
    snap = ctx.kube.try_snapshot()
    if (
        target is None
        or snap is None
        or snap.spec.get("environmentdImageRef") == target
    ):
        action_same_version(ctx)
        return
    if rng.random() < 0.3:
        action_upgrade_cancelled(ctx, snap, target)
        return
    request = submit(ctx, "upgrade", {**new_request(), "environmentdImageRef": target})
    res = follow(ctx, request, kill=maybe_kill_plan(ctx))
    if res.outcome == "Applied":
        reachable(
            "Cross-version upgrade reached Applied",
            {"from": snap.spec.get("environmentdImageRef"), "phases": res.phases},
        )
    note_kill_outcome(res)


def action_upgrade_cancelled(ctx: Ctx, prior: Snapshot, target: str) -> None:
    """Start an upgrade under ManuallyPromote, then cancel it before promotion.

    ManuallyPromote never promotes without `forcePromote`, so the old release
    stays the leader throughout, and reverting the image is a cancellation
    rather than a downgrade. The old release must keep serving afterwards:
    nothing the read-only candidate did may have left state it cannot read.
    """
    prior_image = prior.spec.get("environmentdImageRef")
    request = submit(
        ctx,
        "upgrade_manual",
        {
            **new_request(),
            "environmentdImageRef": target,
            "rolloutStrategy": "ManuallyPromote",
        },
    )
    try:
        snap = wait_for_phase(ctx, request, ("ReadyToPromote",), FOLLOW_TIMEOUT_SECONDS)
        parked = (
            snap is not None
            and snap.request == request
            and snap.reason == "ReadyToPromote"
        )
        if parked:
            reachable(
                "Cross-version upgrade candidate reached ReadyToPromote",
                {"from": prior_image},
            )
            ctx.sleep(rng.choice(MANUAL_WAIT_MENU))
        # Restoring the previous request id completes nothing new; a fresh one
        # rolls the old release out again, which must not park in turn, so the
        # strategy is restored in the same patch.
        variant = rng.choice(["restore_request", "fresh_request"])
        changes: dict[str, Any] = {
            "environmentdImageRef": prior_image,
            "forceRollout": prior.spec.get("forceRollout"),
            "rolloutStrategy": DEFAULT_ROLLOUT_STRATEGY,
        }
        if variant == "restore_request" and prior.request is not None:
            changes["requestRollout"] = prior.request
        else:
            changes["requestRollout"] = new_uuid()
        cancel = submit(ctx, f"upgrade_cancel_{variant}", changes)
        res = follow(ctx, cancel, kill=maybe_kill_plan(ctx))
        after = ctx.kube.try_snapshot()
        # Settled means the cancel request completed. The condition is not
        # checked: a revert that restores the completed request id settles as
        # `WaitingForApproval` rather than `Applied`.
        if (
            after is not None
            and after.request == cancel
            and after.last_completed == cancel
            and after.last_completed_image is not None
        ):
            always(
                after.last_completed_image == prior_image
                and (variant == "fresh_request" or after.active == prior.active),
                "A cancelled cross-version upgrade leaves the old release serving",
                {
                    "prior_image": prior_image,
                    "prior_active": prior.active,
                    "variant": variant,
                    "parked": parked,
                    "phases": res.phases,
                    **after.summary(),
                },
            )
            reachable(
                "Cross-version upgrade cancelled before promotion",
                {"variant": variant, "parked": parked},
            )
    finally:
        submit(
            ctx, "upgrade_manual_restore", {"rolloutStrategy": DEFAULT_ROLLOUT_STRATEGY}
        )


# rolloutRequestTimeout values: the humantime boundary, and the family around
# one clean rollout (`T_ROLLOUT_SECONDS`), so cancellation and promotion race.
def timeout_menu() -> list[str]:
    t = T_ROLLOUT_SECONDS
    return ["1s", f"{t // 4}s", f"{t - 1}s", f"{t}s", f"{t + 1}s"]


def action_short_timeout(ctx: Ctx) -> None:
    timeout = rng.choice(timeout_menu())
    request = submit(
        ctx, "short_timeout", {**new_request(), "rolloutRequestTimeout": timeout}
    )
    try:
        res = follow(ctx, request, kill=maybe_kill_plan(ctx))
    finally:
        submit(
            ctx,
            "short_timeout_restore",
            {"rolloutRequestTimeout": DEFAULT_ROLLOUT_REQUEST_TIMEOUT},
        )
    note_kill_outcome(res)
    # A cancelled request is marked completed, and the next pass reports
    # WaitingForApproval because the spec still differs from the active one.
    if res.outcome not in ("RolloutTimeout", "WaitingForApproval"):
        return
    reachable(
        "Short rolloutRequestTimeout cancelled a rollout",
        {"timeout": timeout, "phases": res.phases},
    )
    retry = submit(ctx, "short_timeout_retry", new_request())
    res = follow(ctx, retry)
    sometimes(
        res.outcome == "Applied",
        "Rollout reached Applied after a RolloutTimeout and a fresh request",
        {"phases": res.phases, "outcome": res.outcome},
    )


def parse_duration_seconds(text: str) -> float | None:
    units = {"ms": 0.001, "s": 1, "sec": 1, "m": 60, "min": 60, "h": 3600, "d": 86400}
    total = 0.0
    matched = False
    for value, unit in re.findall(r"(\d+(?:\.\d+)?)\s*([a-z]+)", text.strip().lower()):
        if unit not in units:
            return None
        total += float(value) * units[unit]
        matched = True
    return total if matched else None


def ddl_check_interval_seconds(host: str) -> float:
    try:
        with sql.connection(host, connect_timeout=10) as conn:
            row = conn.execute("SHOW with_0dt_deployment_ddl_check_interval").fetchone()
        parsed = parse_duration_seconds(str(row[0])) if row else None
        if parsed:
            return parsed
    except TRANSIENT_ERRORS as e:
        log(f"reading the DDL check interval failed: {e}")
    return DEFAULT_DDL_CHECK_INTERVAL_SECONDS


def ddl_burst(ctx: Ctx, host: str) -> None:
    """Create and drop a few user objects on the leader, so the read-only generation must restart."""
    size = rng.choice([1, 2, 5])
    try:
        with sql.connection(host, connect_timeout=10) as conn:
            for _ in range(size):
                existing = ctx.db.execute(
                    "SELECT name, kind FROM ddl_objects ORDER BY name"
                ).fetchall()
                op = rng.choice(["create_table", "create_view", "drop", "drop"])
                if op == "drop" and existing:
                    name, kind = rng.choice(existing)
                    statement = f"DROP {kind} IF EXISTS {name}"
                    forget = name
                    remember = None
                else:
                    kind = "VIEW" if op == "create_view" else "TABLE"
                    name = f"rollout_ddl_{next_counter(ctx.db, 'ddl_object')}"
                    statement = (
                        f"CREATE VIEW {name} AS SELECT 1 AS a"
                        if kind == "VIEW"
                        else f"CREATE TABLE {name} (a int)"
                    )
                    forget = None
                    remember = (name, kind)
                try:
                    conn.execute(statement.encode())
                except psycopg.Error as e:
                    classified = sql.classify(e)
                    always_or_unreachable(
                        classified.outcome != sql.Outcome.VIOLATION,
                        "Paced DDL during a rollout fails only with expected errors",
                        {
                            "statement": statement,
                            "sqlstate": classified.sqlstate,
                            "template": classified.template,
                        },
                    )
                    if classified.outcome == sql.Outcome.INDETERMINATE:
                        return
                    continue
                if remember is not None:
                    ctx.db.execute(
                        "INSERT OR REPLACE INTO ddl_objects (name, kind) VALUES (?, ?)",
                        remember,
                    )
                if forget is not None:
                    ctx.db.execute("DELETE FROM ddl_objects WHERE name = ?", (forget,))
                ctx.db.commit()
    except TRANSIENT_ERRORS as e:
        log(f"DDL burst interrupted: {e}")


def wait_for_sql(host: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with sql.connection(host, connect_timeout=5) as conn:
                conn.execute("SELECT 1")
            return True
        except TRANSIENT_ERRORS:
            time.sleep(2)
    return False


def audit_high_water(host: str) -> int | None:
    try:
        with sql.connection(host, connect_timeout=10) as conn:
            row = conn.execute(
                "SELECT coalesce(max(id), 0) FROM mz_catalog.mz_audit_events"
            ).fetchone()
        return int(row[0]) if row else None
    except TRANSIENT_ERRORS as e:
        log(f"reading the audit log failed: {e}")
        return None


def foreign_ddl_since(host: str, after_id: int | None) -> int | None:
    """Catalog changes after audit event `after_id` not made by `ddl_burst`, or None if unknown."""
    if after_id is None:
        return None
    try:
        with sql.connection(host, connect_timeout=10) as conn:
            row = conn.execute(
                "SELECT count(*) FROM mz_catalog.mz_audit_events"
                " WHERE id > %s"
                " AND coalesce(details ->> 'name', '') NOT LIKE 'rollout_ddl_%%'",
                (after_id,),
            ).fetchone()
        return int(row[0]) if row else None
    except TRANSIENT_ERRORS as e:
        log(f"reading the audit log failed: {e}")
        return None


def action_paced_ddl(ctx: Ctx) -> None:
    """A rollout with `PACED_DDL_BURSTS` DDL bursts at least Q apart reaches
    promotion within `K * Q + T_ROLLOUT_SECONDS` of the last burst.

    Q is one read-only bootstrap plus the DDL check interval: with gaps that
    long, a candidate restarted by one burst can finish bootstrapping and pass
    a check before the next. The bound only holds without faults and without
    DDL from other drivers, which can legitimately keep restarting the
    candidate, so the action runs inside a quiet period and asserts only when
    the audit log shows no other catalog change since the request.
    """
    host = ctx.env.sql_host()
    interval = ddl_check_interval_seconds(host)
    q = READ_ONLY_BOOTSTRAP_SECONDS + interval
    bound = PACED_DDL_K * q + T_ROLLOUT_SECONDS
    wedged_bound = PACED_DDL_WEDGED_FACTOR * bound
    gap_menu = [q, q + 1, 1.5 * q, 2 * q]
    bursts_window = q + (PACED_DDL_BURSTS - 1) * max(gap_menu)
    # Covers the SQL recovery wait, the settle wait, the bursts, and the
    # performance bound. The wedge bound's tail gets its own request below,
    # only if the rollout is still in flight.
    request_quiet_period(int(2 * RECOVERY_SECONDS + bursts_window + bound + 60))
    if not wait_for_sql(host, RECOVERY_SECONDS):
        log("paced_ddl: environment did not recover; skipping")
        return
    # A promotion left in flight by an earlier invocation would complete the
    # new request without a candidate, so wait it out.
    settle_deadline = time.monotonic() + RECOVERY_SECONDS
    while True:
        settled = ctx.poll()
        if settled is not None and settled.reason != "Promoting":
            break
        if time.monotonic() >= settle_deadline:
            log("paced_ddl: a previous promotion is still in flight; skipping")
            return
        time.sleep(POLL_SECONDS)
    audit_start = audit_high_water(host)
    request = submit(
        ctx,
        "paced_ddl",
        {
            **new_request(),
            "rolloutStrategy": DEFAULT_ROLLOUT_STRATEGY,
            "rolloutRequestTimeout": DEFAULT_ROLLOUT_REQUEST_TIMEOUT,
        },
    )
    if request is None:
        return
    start = time.monotonic()
    next_burst = start + rng.choice([0.0, q / 2, q])
    restarts: dict[str, tuple[int, int]] = {}
    bursts = 0
    last_burst = start
    reached_after: float | None = None
    tail_quiet_requested = False
    last: Snapshot | None = None
    while bursts < PACED_DDL_BURSTS or time.monotonic() - last_burst < wedged_bound:
        if (
            not tail_quiet_requested
            and bursts >= PACED_DDL_BURSTS
            and time.monotonic() - last_burst >= bound
        ):
            request_quiet_period(int(wedged_bound - bound + 60))
            tail_quiet_requested = True
        snap = ctx.poll()
        if snap is not None:
            last = snap
            if snap.request != request:
                log("paced_ddl: request replaced; abandoning the check")
                return
            if snap.reason == "Promoting" or (
                snap.last_completed == request and snap.reason == "Applied"
            ):
                reached_after = max(time.monotonic() - last_burst, 0.0)
                break
            if snap.last_completed == request:
                break
            if snap.active is not None:
                try:
                    for pod in ctx.kube.envd_pods():
                        if pod.generation == snap.active + 1:
                            low, high = restarts.get(
                                pod.uid, (pod.restart_count, pod.restart_count)
                            )
                            restarts[pod.uid] = (
                                min(low, pod.restart_count),
                                max(high, pod.restart_count),
                            )
                except TRANSIENT_ERRORS:
                    pass
        if bursts < PACED_DDL_BURSTS and time.monotonic() >= next_burst:
            ddl_burst(ctx, host)
            bursts += 1
            last_burst = time.monotonic()
            next_burst = last_burst + rng.choice(gap_menu)
        time.sleep(POLL_SECONDS)
    candidate_restarts = sum(high - low for low, high in restarts.values()) + max(
        len(restarts) - 1, 0
    )
    reached = reached_after is not None
    foreign = foreign_ddl_since(host, audit_start)
    details = {
        "bound_seconds": bound,
        "wedged_bound_seconds": wedged_bound,
        "q_seconds": q,
        "elapsed_seconds": time.monotonic() - start,
        "reached_after_last_burst_seconds": reached_after,
        "bursts": bursts,
        "candidate_restarts": candidate_restarts,
        "foreign_ddl": foreign,
        "last": last.summary() if last else None,
    }
    sometimes(
        foreign == 0,
        "Rollout under paced DDL ran without DDL from other drivers",
        details,
    )
    if foreign == 0:
        always(
            reached,
            "Rollout under paced DDL without foreign DDL reached promotion within the wedge bound after the last burst",
            details,
        )
        sometimes(
            reached_after is not None and reached_after <= bound,
            "Rollout under paced DDL without foreign DDL reached promotion within the performance bound after the last burst",
            details,
        )
    sometimes(
        reached and candidate_restarts >= 2,
        "Rollout under paced DDL completed after at least two candidate restarts",
        details,
    )


ACTION_FNS = {
    "same_version": action_same_version,
    "forced_migration": action_forced_migration,
    "supersede": action_supersede,
    "revert": action_revert,
    "patch_during_promoting": action_patch_during_promoting,
    "force_promote": action_force_promote,
    "manual_promote": action_manual_promote,
    "short_timeout": action_short_timeout,
    "paced_ddl": action_paced_ddl,
    "upgrade": action_upgrade,
}


def normalize(ctx: Ctx, action: str) -> None:
    """Undo strategy and timeout overrides a killed invocation may have left behind.

    Neither field is part of the generated resources, so restoring them
    neither starts nor cancels a rollout.
    """
    snap = ctx.kube.try_snapshot()
    if snap is None:
        return
    changes: dict[str, Any] = {}
    if (
        snap.spec.get("rolloutStrategy", DEFAULT_ROLLOUT_STRATEGY)
        != DEFAULT_ROLLOUT_STRATEGY
    ):
        changes["rolloutStrategy"] = DEFAULT_ROLLOUT_STRATEGY
    if (
        snap.spec.get("rolloutRequestTimeout", DEFAULT_ROLLOUT_REQUEST_TIMEOUT)
        != DEFAULT_ROLLOUT_REQUEST_TIMEOUT
    ):
        changes["rolloutRequestTimeout"] = DEFAULT_ROLLOUT_REQUEST_TIMEOUT
    if changes:
        submit(ctx, action, changes)


def watch_overlap(ctx: Ctx, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        ctx.poll()
        time.sleep(POLL_SECONDS)


def driver_main() -> int:
    env = Environment()
    kube = Kube(env)
    db = requests_db(env)
    ctx = Ctx(env, kube, db, timeline_params(db), overlap=PhaseOverlap(env.endpoints))
    lease = CrLease(env)
    try:
        acquired = lease.acquire()
    except sqlite3.OperationalError as e:
        log(f"taking the CR lease failed: {e}")
        acquired = False
    if not acquired:
        log("another invocation holds the CR lease; watching only")
        watch_overlap(ctx, NON_HOLDER_WATCH_SECONDS)
        return 0
    ctx.lease = lease
    try:
        try:
            normalize(ctx, "normalize")
        except TRANSIENT_ERRORS as e:
            log(f"normalize failed: {e}")
        names = [a for a in ACTIONS if ctx.params.weights.get(a)]
        weights = [ctx.params.weights[a] for a in names]
        start = time.monotonic()
        for i in range(rng.choice([1, 2])):
            # Bounds the invocation: a second action starts only within one
            # follow of the start.
            if i > 0 and time.monotonic() - start > FOLLOW_TIMEOUT_SECONDS:
                break
            action = rng.choices(names, weights=weights)[0]
            log(f"action {action}")
            try:
                ACTION_FNS[action](ctx)
            except TRANSIENT_ERRORS as e:
                log(f"action {action} interrupted: {e}")
    except LeaseLost as e:
        log(f"stopping: {e}")
        return 0
    finally:
        try:
            lease.release()
        except sqlite3.Error as e:
            log(f"releasing the CR lease failed: {e}")
    return 0


# Observer.


def observer_db(env: Environment) -> sqlite3.Connection:
    db = open_db("rollout_observer", env.endpoints)
    db.isolation_level = None
    db.execute(
        "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS incarnations ("
        " pod_uid TEXT NOT NULL, restart_count INTEGER NOT NULL,"
        " generation INTEGER NOT NULL, pod TEXT NOT NULL,"
        " first_observation INTEGER NOT NULL, started_at INTEGER,"
        " PRIMARY KEY (pod_uid, restart_count))"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS leader_first_seen ("
        " generation INTEGER PRIMARY KEY, first_observation INTEGER NOT NULL)"
    )
    return db


def kv_get(db: sqlite3.Connection, key: str) -> Any:
    row = db.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def kv_set(db: sqlite3.Connection, key: str, value: Any) -> None:
    db.execute(
        "INSERT INTO kv (key, value) VALUES (?, ?)"
        " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (key, json.dumps(value)),
    )


def note_phase(reason: str | None, details: dict[str, Any]) -> None:
    sometimes(reason == "Applying", "Observer saw a rollout in Applying", details)
    sometimes(
        reason == "ReadyToPromote", "Observer saw a rollout in ReadyToPromote", details
    )
    sometimes(reason == "Promoting", "Observer saw a rollout in Promoting", details)
    sometimes(reason == "Applied", "Observer saw a rollout Applied", details)
    sometimes(
        reason == "RolloutTimeout", "Observer saw a rollout in RolloutTimeout", details
    )
    sometimes(
        reason == "WaitingForApproval",
        "Observer saw a rollout in WaitingForApproval",
        details,
    )
    sometimes(
        reason == "FailedDeploy", "Observer saw a rollout in FailedDeploy", details
    )


def protected_from(snap: Snapshot, leaders: list[int]) -> int | None:
    """Highest generation that has reached a state after which it must not be torn down."""
    candidates = list(leaders)
    if snap.active is not None:
        candidates.append(snap.active)
        if snap.reason == "Promoting":
            candidates.append(snap.active + 1)
    return max(candidates) if candidates else None


def check_status_history(db: sqlite3.Connection, snap: Snapshot) -> None:
    """activeGeneration never decreases, and Promoting is only left by advancing it."""
    details = snap.summary()
    active = snap.active
    if active is None:
        return
    max_active = kv_get(db, "max_active")
    if max_active is not None:
        always(
            active >= max_active,
            "Materialize status activeGeneration never decreases",
            {**details, "max_active_seen": max_active},
        )
    kv_set(db, "max_active", max(active, max_active or active))

    promoting_at = kv_get(db, "promoting_at")
    if promoting_at is not None:
        always_or_unreachable(
            (snap.reason == "Promoting" and active == promoting_at)
            or active >= promoting_at + 1,
            "Promoting is only left by advancing activeGeneration",
            {**details, "promoting_seen_at_active": promoting_at},
        )
    if snap.reason == "Promoting":
        kv_set(db, "promoting_at", max(active, promoting_at or active))


def check_adjacent_versions(snap: Snapshot, previous: Snapshot) -> None:
    """Checks a CR version against the version stored immediately before it.

    orchestratord's `apply` sends a CR in `Promoting` only to `promote()`,
    whose only status write is `Applied` at the next generation, and spec
    patches leave the status as it is. A version without a status is not
    checked here: whatever restores the status is covered by the
    order-independent checks in `check_status_history`.
    """
    details = {"previous": previous.summary(), "current": snap.summary()}
    if previous.reason == "Promoting" and previous.active is not None and snap.status:
        always_or_unreachable(
            (snap.reason == "Promoting" and snap.active == previous.active)
            or (snap.reason == "Applied" and snap.active == previous.active + 1),
            "The CR version after Promoting is Promoting at the same activeGeneration or Applied at the next",
            details,
        )
    sometimes(
        previous.reason == "Promoting" and snap.reason == "Applied",
        "Observer saw consecutive CR versions go from Promoting to Applied",
        details,
    )
    sometimes(
        previous.reason == "ReadyToPromote" and snap.reason == "Promoting",
        "Observer saw consecutive CR versions go from ReadyToPromote to Promoting",
        details,
    )


def check_cr_version(
    db: sqlite3.Connection, snap: Snapshot, previous: Snapshot | None
) -> None:
    """Checks one stored CR version. `previous` is the version stored
    immediately before it, or None if that is unknown."""
    note_phase(snap.reason, snap.summary())
    check_status_history(db, snap)
    raise_protected(db, protected_from(snap, []))
    if previous is not None:
        check_adjacent_versions(snap, previous)


class CrWatcher:
    """Runs `check_cr_version` on every stored version of the CR, in order.

    The chain (`kube_watch.ObjectChain`) lives in the observer database, so
    the next invocation resumes where the last one stopped, and versions
    written while no observer ran are still checked unless the API server has
    since dropped them (a relist). Each version is checked and the chain
    advanced in one transaction that first compares the stored chain with
    this watcher's, so concurrent watchers check each version once: a watcher
    that finds the chain moved adopts it and resumes from there.
    """

    def __init__(self, kube: Kube, db: sqlite3.Connection) -> None:
        self.kube = kube
        self.db = db
        self.watch = kube_watch.ResourceWatch(
            kube.custom.list_namespaced_custom_object,
            orchestratord.GROUP,
            orchestratord.VERSION,
            kube.namespace,
            orchestratord.PLURAL,
            field_selector=f"metadata.name={kube.name}",
            window_seconds=WATCH_WINDOW_SECONDS,
            request_timeout=K8S_TIMEOUT_SECONDS,
        )
        self.chain = self._stored()

    @property
    def latest(self) -> Snapshot | None:
        return Snapshot(self.chain.last) if self.chain.last is not None else None

    def _stored(self) -> ObjectChain:
        return ObjectChain.from_json(kv_get(self.db, "cr_chain"))

    def _commit(self, apply: Callable[[ObjectChain], ObjectChain]) -> bool:
        """Stores `apply(chain)` if the stored chain is still this watcher's.
        Otherwise adopts the stored one and returns False."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            stored = self._stored()
            if stored.key() != self.chain.key():
                self.db.execute("ROLLBACK")
                self.chain = stored
                return False
            chain = apply(self.chain)
            kv_set(self.db, "cr_chain", chain.to_json())
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.chain = chain
        return True

    def _advance(self, chain: ObjectChain, item: kube_watch.Item) -> ObjectChain:
        chain, step = chain.advance(item)
        if step is None:
            return chain
        if isinstance(item, kube_watch.Relist):
            log(f"CR relisted at {item.resource_version}; not adjacent to the last")
        if step.current is not None:
            check_cr_version(
                self.db,
                Snapshot(step.current),
                Snapshot(step.predecessor) if step.predecessor else None,
            )
        return chain

    def window(self) -> None:
        window = self.watch.window(self.chain.resource_version)
        if window.error is not None:
            log(f"CR watch: {window.error}")
        for item in window.items:
            if not self._commit(lambda chain: self._advance(chain, item)):
                return
        self._commit(lambda chain: chain.resume_at(window.resource_version))


def uid_of(obj: dict[str, Any]) -> str | None:
    return (obj.get("metadata") or {}).get("uid")


def statefulset_gone(item: kube_watch.Item, uid: str, seen: bool) -> str | None:
    """How `item` shows the StatefulSet with `uid` deleted, or None if it does not.

    `seen` says whether an earlier item of the same watch held `uid`. Only
    then does a relist without it prove a deletion: the protected UID may
    have been bound, by another observer, to a StatefulSet created after the
    relist was read.
    """
    if isinstance(item, kube_watch.Relist):
        objs = [o for o in item.items if uid_of(o) == uid]
        if not objs:
            return "absent from a relist" if seen else None
    else:
        if uid_of(item.obj) != uid:
            return None
        if item.type == "DELETED":
            return "deleted"
        objs = [item.obj]
    if (objs[0].get("metadata") or {}).get("deletionTimestamp") is not None:
        return "deletion requested"
    return None


class StatefulSetWatcher:
    """Checks the protected generation's StatefulSet on every StatefulSet
    change and relist in the environment namespace.

    The resume point is per invocation. A deletion made while no observer ran
    is left to the listing in `check_protected`.
    """

    def __init__(self, kube: Kube, db: sqlite3.Connection) -> None:
        self.kube = kube
        self.db = db
        self.watch = kube_watch.ResourceWatch(
            kube.apps.list_namespaced_stateful_set,
            kube.namespace,
            window_seconds=WATCH_WINDOW_SECONDS,
            request_timeout=K8S_TIMEOUT_SECONDS,
        )
        self.resource_version: str | None = None
        self.seen: set[str] = set()

    def window(self) -> None:
        window = self.watch.window(self.resource_version)
        if window.error is not None:
            log(f"StatefulSet watch: {window.error}")
        for item in window.items:
            # A failure leaves the resume point before `item`, so the next
            # window delivers it again.
            self._check(item)
            if isinstance(item, kube_watch.Relist):
                self.seen = {u for u in map(uid_of, item.items) if u is not None}
            elif (uid := uid_of(item.obj)) is not None:
                if item.type == "DELETED":
                    self.seen.discard(uid)
                else:
                    self.seen.add(uid)
            self.resource_version = item.resource_version or self.resource_version
        self.resource_version = window.resource_version

    def _check(self, item: kube_watch.Item) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            protected = protected_state(self.db)
            uid = protected["uid"]
            how = (
                statefulset_gone(item, uid, uid in self.seen)
                if uid is not None
                else None
            )
            if how is not None:
                protected = check_protected_present(
                    self.kube, protected, False, {"seen": how}
                )
                kv_set(self.db, "protected", protected)
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise


def protected_state(db: sqlite3.Connection) -> dict[str, Any]:
    return kv_get(db, "protected") or {"generation": None, "uid": None}


def raise_protected(db: sqlite3.Connection, candidate: int | None) -> None:
    """Protects `candidate` instead, if it is newer than the protected generation."""
    protected = protected_state(db)
    if candidate is not None and (
        protected["generation"] is None or candidate > protected["generation"]
    ):
        kv_set(db, "protected", {"generation": candidate, "uid": None})


def check_protected_present(
    kube: Kube, protected: dict[str, Any], present: bool, details: dict[str, Any]
) -> dict[str, Any]:
    """Asserts the protected StatefulSet is present or a newer generation is
    protected, and returns the protected state to store."""
    superseded = False
    later: Snapshot | None = None
    newer: int | None = None
    if not present:
        # A legitimate teardown of the protected generation happens only after
        # `Promoting` for its successor was written, so a CR read after the
        # deletion was observed reflects that successor if the teardown was
        # legitimate.
        later = kube.snapshot()
        newer = protected_from(later, [])
        superseded = newer is not None and newer > protected["generation"]
    always(
        present or superseded,
        "A promoted environmentd generation's StatefulSet is not deleted before a newer generation is promoted",
        {
            "protected_generation": protected["generation"],
            "protected_uid": protected["uid"],
            **details,
            "status_after_listing": later.summary() if later else None,
        },
    )
    if superseded:
        return {"generation": newer, "uid": None}
    return protected


def check_protected(
    db: sqlite3.Connection,
    kube: Kube,
    snap: Snapshot,
    leader_gens: list[int],
    statefulsets: list[EnvdStatefulSet],
) -> None:
    """The newest promoted generation's StatefulSet is not deleted until a newer one is promoted.

    Binds the protected generation to a StatefulSet UID from a listing taken
    after `snap`, so the UID is the promoted one rather than a candidate torn
    down before promotion. `StatefulSetWatcher` checks the bound UID on every
    change; this checks it against `statefulsets` too.
    """
    raise_protected(db, protected_from(snap, leader_gens))
    protected = protected_state(db)
    if protected["generation"] is None:
        return
    if protected["uid"] is None:
        match = next(
            (s for s in statefulsets if s.generation == protected["generation"]), None
        )
        if match is not None:
            protected["uid"] = match.uid
        kv_set(db, "protected", protected)
        return
    protected = check_protected_present(
        kube,
        protected,
        protected["uid"] in {s.uid for s in statefulsets},
        {
            "statefulsets": [[s.generation, s.uid] for s in statefulsets],
            "status": snap.summary(),
        },
    )
    kv_set(db, "protected", protected)


def check_applied_matches(
    kube: Kube, snap: Snapshot, statefulsets: list[EnvdStatefulSet]
) -> None:
    """An Applied status describes the StatefulSet the active generation runs.

    Only checked when the condition was written by a pass that saw the current
    spec (`observedGeneration == metadata.generation`). Otherwise a spec patch
    the controller has not reconciled yet would look like drift.
    """
    if snap.condition_status != "True" or snap.reason != "Applied":
        return
    if snap.condition.get("observedGeneration") != snap.generation:
        return
    active = next((s for s in statefulsets if s.generation == snap.active), None)
    if active is None:
        return
    # The StatefulSet listing must describe the same CR version.
    again = kube.snapshot()
    if again.resource_version != snap.resource_version:
        return
    extra_args = list(snap.spec.get("environmentdExtraArgs") or [])
    details = {
        **snap.summary(),
        "statefulset": active.name,
        "statefulset_image": active.image,
        "statefulset_force": active.force,
        "statefulset_unsafe_args": [a for a in active.args if a.startswith("--unsafe")],
        "spec_image": snap.spec.get("environmentdImageRef"),
        "spec_extra_args": extra_args,
        "expected_force": snap.expected_force(),
    }
    always(
        active.image == snap.status.get("lastCompletedRolloutEnvironmentdImageRef"),
        "Applied status image matches the active environmentd StatefulSet image",
        details,
    )
    always(
        active.image == snap.spec.get("environmentdImageRef"),
        "Applied status implies the active StatefulSet runs the spec image",
        details,
    )
    always(
        active.force == snap.expected_force(),
        "Applied status implies the active StatefulSet carries the spec force annotation",
        details,
    )
    always(
        all(a in active.args for a in extra_args)
        and all(a in extra_args for a in active.args if a.startswith("--unsafe")),
        "Applied status implies the active StatefulSet runs the spec extra args",
        details,
    )


def first_observation(db: sqlite3.Connection, pod: EnvdPod) -> int:
    return int(
        db.execute(
            "SELECT first_observation FROM incarnations"
            " WHERE pod_uid = ? AND restart_count = ?",
            (pod.uid, pod.restart_count),
        ).fetchone()[0]
    )


def check_leaders(
    db: sqlite3.Connection,
    observation: int,
    pods: list[EnvdPod],
    samples: list[tuple[EnvdPod, str]],
) -> list[int]:
    """No incarnation of a lower generation first seen after a higher generation's leader was first seen reports IsLeader.

    A promoted generation's leader incarnation always starts after the
    promotion fenced the catalog at its generation, so an older generation's
    incarnation that starts later must find itself fenced out. An old
    incarnation that started earlier may still report IsLeader until it
    touches the catalog.

    Start order comes from the observers' own sequence of pod listings, not
    kubelet `startedAt`, which clock faults move. An incarnation absent from
    the listing where a leader was first seen started after that listing,
    when the leader was already running. NOTE: this assumes the kubelet
    publishes a new incarnation's status faster than a promoted environmentd
    boots to answering leader probes.
    """
    for pod in [*pods, *(pod for pod, _ in samples)]:
        db.execute(
            "INSERT OR IGNORE INTO incarnations"
            " (pod_uid, restart_count, generation, pod, first_observation, started_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                pod.uid,
                pod.restart_count,
                pod.generation,
                pod.name,
                observation,
                pod.started_at,
            ),
        )
    leaders = [
        (pod, first_observation(db, pod))
        for pod, status in samples
        if status == "IsLeader"
    ]
    for pod, first in leaders:
        db.execute(
            "INSERT INTO leader_first_seen (generation, first_observation)"
            " VALUES (?, ?)"
            " ON CONFLICT (generation) DO UPDATE"
            " SET first_observation = MIN(first_observation, excluded.first_observation)",
            (pod.generation, first),
        )
    for pod, first in leaders:
        row = db.execute(
            "SELECT generation, first_observation FROM leader_first_seen"
            " WHERE generation > ? ORDER BY first_observation LIMIT 1",
            (pod.generation,),
        ).fetchone()
        newer = (
            db.execute(
                "SELECT pod, pod_uid, started_at FROM incarnations"
                " WHERE generation = ? AND first_observation = ? LIMIT 1",
                (row[0], row[1]),
            ).fetchone()
            if row
            else None
        )
        always(
            row is None or first <= row[1],
            "No environmentd incarnation first seen after a newer generation's leader was first seen reports IsLeader",
            {
                "generation": pod.generation,
                "pod": pod.name,
                "pod_uid": pod.uid,
                "restart_count": pod.restart_count,
                "first_observation": first,
                "started_at": pod.started_at,
                "newer_leader_generation": row[0] if row else None,
                "newer_leader_first_observation": row[1] if row else None,
                "newer_leader_incarnation": list(newer) if newer else None,
            },
        )
    leader_gens = sorted({pod.generation for pod, _ in leaders})
    sometimes(
        len(leader_gens) >= 2,
        "Two environmentd generations reported IsLeader in one observation",
        {"generations": leader_gens},
    )
    max_leader = db.execute("SELECT MAX(generation) FROM leader_first_seen").fetchone()[
        0
    ]
    if max_leader is not None:
        exited = [
            pod.name
            for pod in pods
            if pod.generation < max_leader and pod.last_exit_code == 0
        ]
        sometimes(
            bool(exited),
            "Fenced-out environmentd generation restarted and exited cleanly",
            {"pods": exited, "max_leader_generation": max_leader},
        )
    return leader_gens


def check_transitions(
    db: sqlite3.Connection, snap: Snapshot, pods: list[EnvdPod], holder: str | None
) -> None:
    # Only candidate pods: promote() tears down the old generation while the
    # status still says Promoting, so its pods vanish without any kill.
    candidate = snap.active + 1 if snap.active is not None else None
    candidate_pods = sorted(p.uid for p in pods if p.generation == candidate)
    current: dict[str, Any] = {
        "at": time.monotonic(),
        "reason": snap.reason,
        "active": snap.active,
        "condition_status": snap.condition_status,
        "holder": holder,
        "pods": candidate_pods,
    }
    previous = kv_get(db, "previous")
    kv_set(db, "previous", current)
    if (
        previous is None
        or current["at"] - previous["at"] > OBSERVER_PREVIOUS_MAX_AGE_SECONDS
    ):
        return
    holder_moved = (
        previous["holder"] is not None
        and holder is not None
        and previous["holder"] != holder
    )
    in_rollout = "Unknown" in (previous["condition_status"], snap.condition_status)
    details = {"previous": previous, "current": current}
    sometimes(
        holder_moved and in_rollout,
        "orchestratord leadership moved while a rollout was in progress",
        details,
    )
    still_promoting = (
        previous["reason"] == "Promoting"
        and snap.reason == "Promoting"
        and previous["active"] == snap.active
    )
    sometimes(
        still_promoting and holder_moved,
        "Promoting survived an orchestratord leadership change",
        details,
    )
    sometimes(
        still_promoting and bool(set(previous["pods"]) - set(candidate_pods)),
        "Candidate environmentd pod was replaced while the rollout was Promoting",
        details,
    )


def observe_once(db: sqlite3.Connection, kube: Kube) -> None:
    # One observation at a time across concurrent observers, so history is
    # updated in the order the API server answered.
    db.execute("BEGIN IMMEDIATE")
    try:
        snap = kube.snapshot()
        observation = (kv_get(db, "observations") or 0) + 1
        kv_set(db, "observations", observation)
        pods, samples = consistent_leader_samples(kube)
        leader_gens = check_leaders(db, observation, pods, samples)
        if snap.resource_id is not None:
            statefulsets = kube.envd_statefulsets(snap.resource_id)
            check_protected(db, kube, snap, leader_gens, statefulsets)
            check_applied_matches(kube, snap, statefulsets)
        try:
            holder = kube.lease_holder()
        except TRANSIENT_ERRORS:
            holder = None
        check_transitions(db, snap, pods, holder)
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def check_serving_version(
    db: sqlite3.Connection, env: Environment, cr: Snapshot | None
) -> None:
    """Once a version has served SQL, no older version serves it again.

    The query runs outside the observer lock, so concurrent observers can
    record samples out of order. A sample only counts against the highest
    version if its query started after the highest version's query finished.

    `cr` is the newest CR version the caller has watched, read before the
    query started. It only annotates the sample: the Service can still route
    to a generation the CR no longer names, so the served version is not
    asserted against it.
    """
    started = time.time()
    with sql.connection(
        env.sql_host(), connect_timeout=5, statement_timeout_ms=5_000
    ) as conn:
        row = conn.execute("SELECT mz_version()").fetchone()
    finished = time.time()
    raw = str(row[0]) if row else ""
    served = parse_mz_version(raw)
    if served is None:
        return
    details: dict[str, Any] = {
        "served": raw,
        "started": started,
        "cr": cr.summary() if cr else None,
    }
    sometimes(
        cr is not None
        and cr.reason in REASONS_IN_PROGRESS
        and cr.spec.get("environmentdImageRef") != cr.last_completed_image,
        "Observer read the serving version while a rollout to a different image was in progress",
        details,
    )
    db.execute("BEGIN IMMEDIATE")
    try:
        highest = kv_get(db, "highest_served_version")
        highest_version = MzVersion.parse_mz(highest["version"]) if highest else None
        if highest is not None and highest_version is not None:
            always(
                not (served < highest_version and started > highest["finished"]),
                "The environmentd version serving SQL never goes backwards",
                {**details, "highest": highest},
            )
            if served > highest_version:
                reachable(
                    "SQL served by a newer environmentd version than before",
                    {**details, "previous": highest["version"]},
                )
        if highest_version is None or served > highest_version:
            kv_set(
                db,
                "highest_served_version",
                {"version": raw.split(" ")[0], "finished": finished},
            )
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def run_watches(watchers: list[CrWatcher | StatefulSetWatcher]) -> None:
    """One window of each watch. Watch failures are expected under faults."""
    for watcher in watchers:
        try:
            watcher.window()
        except TRANSIENT_ERRORS as e:
            log(f"watch window interrupted: {e}")


def observer_main() -> int:
    env = Environment()
    kube = Kube(env)
    db = observer_db(env)
    cr = CrWatcher(kube, db)
    watchers = [cr, StatefulSetWatcher(kube, db)]
    deadline = time.monotonic() + OBSERVER_DURATION_SECONDS
    next_observation = 0.0
    version_key: tuple[Any, ...] | None = None
    next_version = 0.0
    while time.monotonic() < deadline:
        run_watches(watchers)
        now = time.monotonic()
        if now >= next_observation:
            next_observation = now + OBSERVER_INTERVAL_SECONDS
            try:
                observe_once(db, kube)
            except TRANSIENT_ERRORS as e:
                log(f"observation skipped: {e}")
        latest = cr.latest
        key = (
            (latest.reason, latest.active, latest.last_completed_image)
            if latest
            else None
        )
        now = time.monotonic()
        if key != version_key or now >= next_version:
            version_key = key
            next_version = now + VERSION_SAMPLE_SECONDS
            try:
                check_serving_version(db, env, latest)
            except TRANSIENT_ERRORS as e:
                log(f"version sample skipped: {e}")
    return 0


# Convergence.


def expected_version(image: str | None) -> str | None:
    """The `mz_version()` prefix an image tag implies, or None for non-semver tags."""
    if not image:
        return None
    tag = image.rpartition(":")[2]
    if not re.match(r"^v\d+\.\d+\.\d+", tag):
        return None
    return tag.replace("--", "+").split("+")[0]


def parse_mz_version(mz_version: str) -> MzVersion | None:
    """Parse the output of `mz_version()`, such as `v26.46.0-dev (8b2eecd06)`."""
    try:
        return MzVersion.parse_mz(mz_version.split(" ")[0])
    except ValueError:
        return None


def convergence_failures(
    kube: Kube, env: Environment, snap: Snapshot
) -> dict[str, Any]:
    """Which convergence conditions do not hold right now. Empty means converged."""
    failures: dict[str, Any] = {}
    if not (
        snap.condition_status == "True"
        and snap.reason == "Applied"
        and snap.last_completed == snap.request
    ):
        failures["up_to_date"] = snap.summary()
    if snap.resource_id is None or snap.active is None:
        failures["status"] = "no resourceId or activeGeneration"
        return failures

    statefulsets = kube.envd_statefulsets(snap.resource_id)
    if [(s.generation, s.deleting) for s in statefulsets] != [(snap.active, False)]:
        failures["envd_statefulsets"] = [
            [s.generation, s.deleting] for s in statefulsets
        ]

    pods = [p for p in kube.envd_pods() if p.generation == snap.active]
    statuses = leader_statuses(pods)
    if len(pods) != 1 or not pods[0].ready or statuses.get(pods[0].uid) != "IsLeader":
        failures["leader"] = [[p.name, p.ready, statuses.get(p.uid)] for p in pods]

    replicas: set[tuple[str, str]] | None = None
    try:
        with sql.connection(
            env.sql_host(), connect_timeout=10, statement_timeout_ms=30_000
        ) as conn:
            conn.execute("SELECT 1")
            version_row = conn.execute("SELECT mz_version()").fetchone()
            replicas = {
                (str(c), str(r))
                for c, r in conn.execute(
                    "SELECT cluster_id, id FROM mz_catalog.mz_cluster_replicas"
                ).fetchall()
            }
        version = str(version_row[0]) if version_row else ""
        spec_image = snap.spec.get("environmentdImageRef")
        expected = expected_version(spec_image)
        if expected is not None and version.split(" ")[0].split("+")[0] != expected:
            failures["version"] = {"mz_version": version, "expected": expected}
        # The image under test has no release in its tag, but after an upgrade
        # it must serve a newer version than the release it replaced.
        base = expected_version(env.endpoints.initial_environmentd_image)
        if (
            spec_image is not None
            and spec_image == env.endpoints.upgrade_pending_image
            and base is not None
        ):
            served = parse_mz_version(version)
            if served is None or not served > MzVersion.parse_mz(base):
                failures["upgraded_version"] = {"mz_version": version, "base": base}
    except TRANSIENT_ERRORS as e:
        failures["sql"] = str(e)

    clusterd: dict[tuple[str, str], int] = {}
    stray = []
    for name in kube.clusterd_statefulsets():
        match = CLUSTERD_NAME.search(name)
        if match is None:
            continue
        generation = int(match["gen"])
        if generation != snap.active:
            stray.append(name)
        else:
            clusterd[(match["cluster"], match["replica"])] = generation
    if stray:
        failures["clusterd_other_generations"] = stray
    if replicas is not None and set(clusterd) != replicas:
        failures["clusterd_vs_catalog"] = {
            "orphaned": sorted("/".join(k) for k in set(clusterd) - replicas),
            "missing": sorted("/".join(k) for k in replicas - set(clusterd)),
        }
    return failures


@dataclass
class OperatorUnstick:
    """What an operator does, once each, to settle the CR after faults stop.

    Call `step` on every poll. It first restores the strategy and timeout and
    force-promotes a rollout waiting in `ReadyToPromote`. A cancelled, failed,
    or unrequested rollout is not retried by orchestratord, so the first time
    the CR is seen in that state it requests one fresh rollout, which must
    then complete.
    """

    ctx: Ctx
    origin: str
    normalized: bool = False
    fresh_requested: bool = False

    def step(self) -> tuple[Snapshot, bool]:
        """A fresh snapshot, and whether this step requested a fresh rollout.

        Raises `TRANSIENT_ERRORS`.
        """
        if not self.normalized:
            normalize(self.ctx, f"{self.origin}_normalize")
            snap = self.ctx.kube.snapshot()
            if snap.reason == "ReadyToPromote" and snap.request is not None:
                submit(
                    self.ctx, f"{self.origin}_promote", {"forcePromote": snap.request}
                )
            self.normalized = True
        snap = self.ctx.kube.snapshot()
        if (
            not self.fresh_requested
            and snap.condition_status == "False"
            and snap.reason in ("RolloutTimeout", "FailedDeploy", "WaitingForApproval")
        ):
            submit(self.ctx, f"{self.origin}_fresh_request", new_request())
            self.fresh_requested = True
            return snap, True
        return snap, False


def operator_ctx(env: Environment) -> Ctx:
    db = requests_db(env)
    return Ctx(env, Kube(env), db, timeline_params(db))


def converge_main() -> int:
    env = Environment()
    ctx = operator_ctx(env)
    kube, db = ctx.kube, ctx.db
    start = time.monotonic()
    deadline = start + CONVERGENCE_WEDGED_SECONDS
    unstick = OperatorUnstick(ctx, "converge")
    observer = observer_db(env)
    watchers = [CrWatcher(kube, observer), StatefulSetWatcher(kube, observer)]
    failures: dict[str, Any] = {"never_checked": True}
    last: Snapshot | None = None
    while time.monotonic() < deadline:
        try:
            snap, requested = unstick.step()
            last = snap
            if requested:
                continue
            failures = convergence_failures(kube, env, snap)
            if not failures:
                break
        except TRANSIENT_ERRORS as e:
            failures = {"error": str(e)}
        log(f"not converged: {failures}")
        # Waits by watching, so the rollouts this command requests are checked
        # like any other.
        retry_at = time.monotonic() + 5
        while time.monotonic() < retry_at:
            run_watches(watchers)
    converged = not failures
    elapsed = time.monotonic() - start
    submitted = db.execute(
        "SELECT request_id, action, outcome FROM submitted ORDER BY seq DESC LIMIT 5"
    ).fetchall()
    details = {
        "bound_seconds": CONVERGENCE_TIMEOUT_SECONDS,
        "wedged_bound_seconds": CONVERGENCE_WEDGED_SECONDS,
        "elapsed_seconds": elapsed,
        "failures": failures,
        "fresh_request": unstick.fresh_requested,
        "last_status": last.summary() if last else None,
        "recent_submissions": [list(r) for r in submitted],
    }
    always(
        converged,
        "Deployment converges to one serving generation at the last requested spec after faults stop",
        details,
    )
    sometimes(
        converged and elapsed <= CONVERGENCE_TIMEOUT_SECONDS,
        "Deployment converged within the performance bound after faults stop",
        details,
    )
    target = env.endpoints.upgrade_pending_image
    if target is not None:
        sometimes(
            converged
            and last is not None
            and last.last_completed_image == target
            and last.spec.get("environmentdImageRef") == target,
            "Deployment converged on the upgraded release after starting on an older one",
            details,
        )
    return 0
