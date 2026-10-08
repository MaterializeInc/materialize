# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Pure logic of the generation isolation oracles, testable without the SDK.

Generation attribution. Every persist writer and critical reader records the
`HOSTNAME` of the process that registered it, followed by its build version,
and under Kubernetes `HOSTNAME` is the pod name. environmentd pods are named
`<prefix>environmentd-<generation>-<ordinal>` and clusterd pods
`<prefix>cluster-<cluster>-replica-<replica>-gen-<generation>-<ordinal>`, so a
handle, or an upstream connection by pod IP, names the deploy generation that
made it.

Never promoted. A generation is promoted only after orchestratord writes
`Promoting` and then calls the candidate's promote endpoint, and
`activeGeneration` never decreases (`promoting-is-a-point-of-no-return`). So
if two CR reads bracket an observation, both show the same active generation
A, and neither shows `Promoting`, no generation above A was ever leader up to
the second read. With `Promoting` in either read, A + 1 may be. Generation
numbers are reused after a cancelled rollout, but a reused number above A was
never promoted under any of its incarnations.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

ENVD_POD = re.compile(r"-environmentd-(?P<gen>\d+)-\d+$")
CLUSTERD_POD = re.compile(r"-gen-(?P<gen>\d+)-\d+$")
VERSION = re.compile(r"v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)")


def pod_generation(pod_name: str) -> int | None:
    """The deploy generation in an environmentd or clusterd pod name."""
    match = ENVD_POD.search(pod_name) or CLUSTERD_POD.search(pod_name)
    return int(match["gen"]) if match else None


def hostname_generation(hostname: str) -> int | None:
    """The deploy generation of a persist handle's `debug.hostname`."""
    parts = hostname.split()
    return pod_generation(parts[0]) if parts else None


def version_tuple(text: str | None) -> tuple[int, int, int] | None:
    """Major, minor and patch of a version string, ignoring any pre-release."""
    if not text:
        return None
    match = VERSION.match(text.strip())
    if match is None:
        return None
    return (int(match["major"]), int(match["minor"]), int(match["patch"]))


@dataclass(frozen=True)
class CrPoint:
    """The parts of one Materialize CR read that bound promotion."""

    active: int | None
    reason: str | None


def promotable_ceiling(before: CrPoint, after: CrPoint) -> int | None:
    """The highest generation that may have been leader at any point up to `after`.

    None when the two reads do not bracket a single active generation, in
    which case nothing between them can be attributed.
    """
    if before.active is None or before.active != after.active:
        return None
    if "Promoting" in (before.reason, after.reason):
        return before.active + 1
    return before.active


def foreign_handles(state: dict[str, Any], ceiling: int) -> list[dict[str, Any]]:
    """Writers and critical readers registered by a generation above `ceiling`.

    `state` is `persistcli inspect state` output. A writer enters persist state
    only through a successful `compare_and_append`, and a critical reader only
    by registering a critical since handle, so either from a never-promoted
    generation is a write by a read-only process. Leased readers are left out:
    a read-only process registers them by design.
    """
    found = []
    for kind in ("writers", "critical_readers"):
        for handle_id, handle in (state.get(kind) or {}).items():
            debug = handle.get("debug") or {}
            hostname = str(debug.get("hostname") or "")
            generation = hostname_generation(hostname)
            if generation is not None and generation > ceiling:
                found.append(
                    {
                        "kind": kind,
                        "id": handle_id,
                        "hostname": hostname,
                        "purpose": debug.get("purpose"),
                        "generation": generation,
                        "most_recent_write_upper": handle.get(
                            "most_recent_write_upper"
                        ),
                        "since": handle.get("since"),
                    }
                )
    return found


def data_version_exceeds(
    state: dict[str, Any], leader_version: tuple[int, int, int]
) -> bool:
    """Whether the shard's data version is newer than the leader's build.

    `applier_version` in the rollup is the shard's data version, which only
    `upgrade_version` raises; the leader halts on reading data newer than its
    code.
    """
    data = version_tuple(state.get("applier_version"))
    return data is not None and data > leader_version


@dataclass(frozen=True)
class OracleMark:
    """The highest value of one oracle column seen so far, and when the sample
    that saw it finished, in `time.monotonic()` seconds."""

    value: int
    finished: float


def oracle_regressions(
    marks: dict[tuple[str, str], OracleMark],
    rows: Iterable[tuple[str, int, int]],
    started: float,
) -> list[dict[str, Any]]:
    """Columns of a sample that went below a value already committed when it started.

    `marks` is keyed by (timeline, column). A mark only binds a sample that
    started after the mark's sample finished, since concurrent samples may
    be answered in either order.
    """
    found = []
    for timeline, read_ts, write_ts in rows:
        for column, value in (("read_ts", read_ts), ("write_ts", write_ts)):
            mark = marks.get((timeline, column))
            if mark is not None and started > mark.finished and value < mark.value:
                found.append(
                    {
                        "timeline": timeline,
                        "column": column,
                        "observed": value,
                        "previous": mark.value,
                        "previous_finished": mark.finished,
                        "started": started,
                    }
                )
    return found


def advance_marks(
    marks: dict[tuple[str, str], OracleMark],
    rows: Iterable[tuple[str, int, int]],
    finished: float,
) -> dict[tuple[str, str], OracleMark]:
    """`marks` raised by a sample. An equal value keeps the earlier finish,
    which binds more later samples."""
    out = dict(marks)
    for timeline, read_ts, write_ts in rows:
        for column, value in (("read_ts", read_ts), ("write_ts", write_ts)):
            mark = out.get((timeline, column))
            if mark is None or value > mark.value:
                out[(timeline, column)] = OracleMark(value, finished)
    return out


@dataclass(frozen=True)
class PodRef:
    name: str
    uid: str
    generation: int


@dataclass(frozen=True)
class Walsender:
    pid: int
    backend_start: str
    client_addr: str | None
    slot_name: str | None


def foreign_walsenders(
    first: list[Walsender],
    second: list[Walsender],
    pods_first: dict[str, PodRef],
    pods_second: dict[str, PodRef],
    ceiling: int,
) -> list[dict[str, Any]]:
    """Replication connections held by a pod of a generation above `ceiling`.

    `pods_*` map pod IP to pod. A connection counts only if it is the same
    backend in both samples and its client IP belonged to the same pod in
    both listings. A connection left over from an earlier owner of that IP
    would be reset once its new owner answers the server's next keepalive, so
    the samples must be taken at least one `wal_sender_timeout` apart.
    """
    later = {(w.pid, w.backend_start): w for w in second}
    found = []
    for w in first:
        if w.client_addr is None or (w.pid, w.backend_start) not in later:
            continue
        if later[(w.pid, w.backend_start)].client_addr != w.client_addr:
            continue
        pod = pods_first.get(w.client_addr)
        again = pods_second.get(w.client_addr)
        if pod is None or again is None or pod.uid != again.uid:
            continue
        if pod.generation > ceiling:
            found.append(
                {
                    "pid": w.pid,
                    "backend_start": w.backend_start,
                    "client_addr": w.client_addr,
                    "slot_name": w.slot_name,
                    "pod": pod.name,
                    "generation": pod.generation,
                }
            )
    return found


@dataclass
class Key:
    """One modelled attribute: the value last acknowledged or observed, plus
    values sent by statements whose outcome is unknown.

    A statement whose connection broke may still be applied later, so its
    value stays allowed until `settle_s` after it was sent, whatever is
    observed or acknowledged in between.
    """

    confirmed: Any
    pending: list[tuple[Any, float]] = field(default_factory=list)

    def allowed(self) -> list[Any]:
        return [self.confirmed, *(v for v, _ in self.pending)]

    def to_json(self) -> dict[str, Any]:
        return {"confirmed": self.confirmed, "pending": self.pending}

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Key:
        return cls(data["confirmed"], [(v, float(t)) for v, t in data["pending"]])


class AclModel:
    """Expected owners, privilege grants and cluster settings, keyed by a string.

    Only one invocation mutates or observes the modelled objects at a time,
    so every change to them is either acknowledged here, rejected here, or
    pending with an unknown outcome. A key absent from the model is adopted
    from the first observation.
    """

    def __init__(self, keys: dict[str, Key], settle_s: float) -> None:
        self.keys = keys
        self.settle_s = settle_s

    def sent(self, key: str, value: Any, at: float) -> None:
        """Record a statement before it is sent, as if its outcome were unknown."""
        k = self.keys.get(key)
        if k is not None:
            k.pending.append((value, at))

    def acknowledged(self, key: str, value: Any, at: float) -> None:
        k = self.keys.get(key)
        if k is None:
            return
        k.confirmed = value
        self._forget(k, value, at)

    def rejected(self, key: str, value: Any, at: float) -> None:
        k = self.keys.get(key)
        if k is not None:
            self._forget(k, value, at)

    @staticmethod
    def _forget(k: Key, value: Any, at: float) -> None:
        for i, (v, t) in enumerate(k.pending):
            if v == value and t == at:
                del k.pending[i]
                return

    def observe(self, observed: dict[str, Any], now: float) -> list[dict[str, Any]]:
        """Compare an observation of every key with the model, then adopt it.

        Returns the keys whose observed value the model does not allow.
        """
        mismatches = []
        for key, value in observed.items():
            k = self.keys.get(key)
            if k is None:
                self.keys[key] = Key(value)
                continue
            if value not in k.allowed():
                mismatches.append(
                    {
                        "key": key,
                        "observed": value,
                        "confirmed": k.confirmed,
                        "pending": k.pending,
                    }
                )
            k.confirmed = value
            k.pending = [(v, t) for v, t in k.pending if now - t < self.settle_s]
        return mismatches
