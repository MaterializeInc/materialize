# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""OOM kills, evictions, and node memory pressure, told apart from crashes.

A process the kernel or the kubelet kills for memory exits like a crash (137,
or a pod that vanishes), so the crash oracles cannot tell the two apart. This
module reads every pod, the memory-related events, and the node conditions
cluster-wide and sorts what it finds into three properties:

* "No Materialize container is OOM-killed at its own memory limit": an
  environmentd, clusterd or orchestratord container whose own cgroup limit
  was hit while the node as a whole had memory. A Materialize memory-use
  finding: compare the usage with the limit in the details.
* "No pod is evicted and no process is OOM-killed for lack of node memory":
  kubelet evictions and admission rejections, node-wide OOM kills,
  `EvictionThresholdMet` and `SystemOOM` node events, pods unschedulable for
  memory, and limit OOMs of non-Materialize containers. All of these are
  harness sizing problems, not Materialize bugs.
* "The node never reports memory pressure": the `MemoryPressure` node
  condition or its `NodeHasInsufficientMemory` event. The kubelet sets it at
  its eviction threshold, so it is the early warning for the previous one.

An OOM kill counts as node-wide when the container has no memory limit (its
cgroup cannot run out on its own) or when a `SystemOOM` node event falls
within `SYSTEM_OOM_WINDOW_S` of it. The kubelet records `SystemOOM` only for
OOM kills whose constraining cgroup is the root, that is the whole machine.

Kubernetes keeps only the last termination of each container and forgets a
pod once it is deleted, so a kill followed by a quick restart and second kill,
or by a generation teardown, can go unobserved. Events last an hour (the API
server's default TTL), so the event half of the check also covers that gap
within the hour.

Every finding is asserted once per timeline (table `seen` in state database
`resource_kills`).
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import urllib3
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)
from kubernetes import client  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import state
from materialize.antithesis.drivers.recovery import pod_role
from materialize.antithesis.environment import Environment

STATE_DB = "resource_kills"

ANYTIME_DURATION_S = 120
ANYTIME_POLL_S = 10
K8S_TIMEOUT_S = 10
# The kubelet records the container status and the `SystemOOM` event for one
# kill independently. Not measured; generous so a paused kubelet still lands
# both inside it.
SYSTEM_OOM_WINDOW_S = 60

MSG_LIMIT = "No Materialize container is OOM-killed at its own memory limit"
MSG_NODE = "No pod is evicted and no process is OOM-killed for lack of node memory"
MSG_PRESSURE = "The node never reports memory pressure"
MSG_COVERAGE = "resource kill check read pods, events and nodes"

# Pod `status.reason` values the kubelet sets when it evicts a pod or rejects
# it at admission for lack of memory.
POD_FAILURE_REASONS = frozenset({"Evicted", "OutOfmemory"})
NODE_EVENT_REASONS = ("Evicted", "OutOfmemory", "SystemOOM", "FailedScheduling")
PRESSURE_EVENT_REASONS = ("EvictionThresholdMet", "NodeHasInsufficientMemory")

TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    ApiException,
    urllib3.exceptions.HTTPError,
    OSError,
)


def log(message: str) -> None:
    print(f"resource kills: {message}", flush=True)


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS seen (
            key TEXT PRIMARY KEY, kind TEXT, details TEXT, observed_at REAL);
        """)
    db.commit()
    return db


@dataclass(frozen=True)
class Finding:
    kind: str
    """`limit`, `node` or `pressure`, see the module doc."""
    key: str
    details: dict[str, Any]


def _ts(value: datetime | None) -> float | None:
    return value.timestamp() if value is not None else None


def _event_interval(event: Any) -> tuple[float, float] | None:
    first = _ts(event.first_timestamp) or _ts(event.event_time)
    last = _ts(event.last_timestamp) or first
    if first is None or last is None:
        return None
    return first, last


def _event_details(event: Any) -> dict[str, Any]:
    obj = event.involved_object
    return {
        "reason": event.reason,
        "type": event.type,
        "object_kind": obj.kind if obj else None,
        "namespace": obj.namespace if obj else None,
        "name": obj.name if obj else None,
        "message": event.message,
        "count": event.count,
        "first_timestamp": str(event.first_timestamp or event.event_time),
        "last_timestamp": str(event.last_timestamp),
    }


def node_memory() -> dict[str, int]:
    """`MemTotal` and `MemAvailable` in bytes.

    The workload container shares the node's kernel, and `/proc/meminfo` is
    not namespaced, so these are the node's figures.
    """
    result = {}
    with open("/proc/meminfo") as f:
        for line in f:
            name, _, rest = line.partition(":")
            if name in ("MemTotal", "MemAvailable"):
                result[name] = int(rest.split()[0]) * 1024
    return result


class Check:
    def __init__(self, env: Environment) -> None:
        self.env = env
        self.core = client.CoreV1Api()

    def _list_events(self, reason: str) -> list[Any]:
        return self.core.list_event_for_all_namespaces(
            field_selector=f"reason={reason}",
            _request_timeout=K8S_TIMEOUT_S,  # pyright: ignore[reportCallIssue]
        ).items

    def sample(self) -> tuple[list[Finding], bool]:
        """Every current finding, and whether every read succeeded."""
        findings: list[Finding] = []
        complete = True

        system_ooms: list[tuple[float, float]] = []
        for reason in NODE_EVENT_REASONS + PRESSURE_EVENT_REASONS:
            try:
                events = self._list_events(reason)
            except TRANSIENT_ERRORS as e:
                log(f"cannot list {reason} events: {e}")
                complete = False
                continue
            for event in events:
                if reason == "FailedScheduling" and "Insufficient memory" not in (
                    event.message or ""
                ):
                    continue
                if reason == "SystemOOM":
                    interval = _event_interval(event)
                    if interval is not None:
                        system_ooms.append(interval)
                kind = "pressure" if reason in PRESSURE_EVENT_REASONS else "node"
                assert event.metadata is not None
                findings.append(
                    Finding(
                        kind,
                        f"event/{event.metadata.uid}",
                        _event_details(event),
                    )
                )

        try:
            pods = self.core.list_pod_for_all_namespaces(
                _request_timeout=K8S_TIMEOUT_S,  # pyright: ignore[reportCallIssue]
            ).items
        except TRANSIENT_ERRORS as e:
            log(f"cannot list pods: {e}")
            pods = []
            complete = False
        for pod in pods:
            findings.extend(self._pod_findings(pod, system_ooms))

        try:
            nodes = self.core.list_node(
                _request_timeout=K8S_TIMEOUT_S,  # pyright: ignore[reportCallIssue]
            ).items
        except TRANSIENT_ERRORS as e:
            log(f"cannot list nodes: {e}")
            nodes = []
            complete = False
        for node in nodes:
            assert node.metadata is not None and node.status is not None
            for c in node.status.conditions or []:
                if c.type == "MemoryPressure" and c.status == "True":
                    findings.append(
                        Finding(
                            "pressure",
                            f"condition/{node.metadata.name}/{c.last_transition_time}",
                            {
                                "node": node.metadata.name,
                                "condition": c.type,
                                "reason": c.reason,
                                "message": c.message,
                                "since": str(c.last_transition_time),
                                "allocatable_memory": (
                                    node.status.allocatable or {}
                                ).get("memory"),
                            },
                        )
                    )
        return findings, complete

    def _pod_findings(
        self, pod: Any, system_ooms: list[tuple[float, float]]
    ) -> list[Finding]:
        metadata, spec, status = pod.metadata, pod.spec, pod.status
        assert metadata is not None and spec is not None and status is not None
        namespace = metadata.namespace
        role = pod_role(
            namespace, self.env.endpoints.operator_namespace, metadata.labels or {}
        )
        if namespace not in (
            self.env.endpoints.namespace,
            self.env.endpoints.operator_namespace,
        ):
            role = None
        base = {"namespace": namespace, "pod": metadata.name, "role": role}
        result = []
        if status.reason in POD_FAILURE_REASONS:
            result.append(
                Finding(
                    "node",
                    f"pod/{metadata.uid}/{status.reason}",
                    {**base, "reason": status.reason, "message": status.message},
                )
            )
        specs = {
            c.name: c for c in (spec.containers or []) + (spec.init_containers or [])
        }
        statuses = (status.container_statuses or []) + (
            status.init_container_statuses or []
        )
        for cs in statuses:
            for term in (
                cs.state.terminated if cs.state else None,
                cs.last_state.terminated if cs.last_state else None,
            ):
                if term is None or term.reason != "OOMKilled":
                    continue
                container = specs.get(cs.name)
                resources = container.resources if container else None
                limit = ((resources.limits if resources else None) or {}).get("memory")
                request = ((resources.requests if resources else None) or {}).get(
                    "memory"
                )
                finished = _ts(term.finished_at)
                near_system_oom = finished is not None and any(
                    first - SYSTEM_OOM_WINDOW_S
                    <= finished
                    <= last + SYSTEM_OOM_WINDOW_S
                    for first, last in system_ooms
                )
                own_limit = limit is not None and not near_system_oom
                result.append(
                    Finding(
                        "limit" if own_limit and role is not None else "node",
                        f"oom/{metadata.uid}/{cs.name}/{term.finished_at}",
                        {
                            **base,
                            "container": cs.name,
                            "reason": term.reason,
                            "exit_code": term.exit_code,
                            "finished_at": str(term.finished_at),
                            "restart_count": cs.restart_count,
                            "memory_limit": limit,
                            "memory_request": request,
                            "near_system_oom": near_system_oom,
                        },
                    )
                )
        return result


MESSAGES = {"limit": MSG_LIMIT, "node": MSG_NODE, "pressure": MSG_PRESSURE}


def check_once(db: sqlite3.Connection, check: Check) -> None:
    findings, complete = check.sample()
    try:
        memory = node_memory()
    except OSError as e:
        log(f"cannot read /proc/meminfo: {e}")
        memory = {}
    summary = {"node_memory": memory, "complete": complete}
    log(f"sampled {len(findings)} findings, {summary}")
    sometimes(complete, MSG_COVERAGE, summary)
    for f in findings:
        with db:
            inserted = db.execute(
                "INSERT OR IGNORE INTO seen VALUES (?, ?, ?, ?)",
                (f.key, f.kind, repr(f.details), time.time()),
            ).rowcount
        if not inserted:
            continue
        details = {**f.details, "node_memory": memory}
        log(f"{f.kind}: {details}")
        always(False, MESSAGES[f.kind], details)
    for message in MESSAGES.values():
        always(True, message, summary)


def _run(duration_s: float) -> int:
    try:
        env = Environment()
    except TRANSIENT_ERRORS as e:
        log(f"cannot reach the Kubernetes API: {e}")
        return 0
    check = Check(env)
    db = open_state()
    deadline = time.monotonic() + duration_s
    while True:
        check_once(db, check)
        if time.monotonic() + ANYTIME_POLL_S >= deadline:
            return 0
        time.sleep(ANYTIME_POLL_S)


def anytime_main() -> int:
    return _run(ANYTIME_DURATION_S)


def finally_main() -> int:
    return _run(0)
