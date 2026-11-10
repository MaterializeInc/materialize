# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Crash-loop detection over sampled container restart history.

A container crash-loops in a window when it restarted after a crash (a halt
or a panic) at least `LOOP_MIN_CRASHES` times within the window and its pod
is still live at the end of it. One crash is not a loop: a fenced generation
halts once before it is deleted, and a lost lease or a dead replica peer
halts a process once. A pod that is deleted (generation teardown) is never a
loop, whatever its history.

Kubernetes keeps only a container's last termination, so restarts between
two samples are unobserved. They are attributed the class of the termination
seen at the sample that revealed them, which is exact when sampling is faster
than the kubelet's minimum restart backoff (10 s).

The kubelet delays each restart of a crashed container by a backoff that
doubles up to 300 s, so a loop's crashes can be minutes apart. The recovery
check deletes a pod that waits out that backoff (`Tracker.reset`), and the
replacement pod, which a StatefulSet creates under the same name, inherits the
restart history. A container persists in a loop when it crashes
`PERSISTING_CRASHES` times, each within the window of the previous crash: two
crashes then a quiet window is still a container that settled.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

CRASH_CLASSES = frozenset({"halt", "panic"})
LOOP_MIN_CRASHES = 2
PERSISTING_CRASHES = 3


@dataclass(frozen=True)
class Sample:
    """One container's status at one sampling time."""

    key: tuple[str, str]
    """(pod uid, container name)."""
    name: str
    """`role/pod/container`, for details."""
    restart_count: int
    exit_class: str | None
    """Class of the last termination, see `recovery.classify_exit`."""
    exit_code: int | None
    finished_at: str | None
    waiting_reason: str | None
    deleting: bool


@dataclass
class _History:
    name: str
    first_seen: float
    restart_count: int
    last_seen: float
    deleting: bool
    waiting_reason: str | None
    restarts: list[dict[str, Any]] = field(default_factory=list)


class Tracker:
    """Restart history of every container seen in samples, from the first sample on."""

    def __init__(self) -> None:
        self.started: float | None = None
        self.last_sample: float | None = None
        self.containers: dict[tuple[str, str], _History] = {}
        # Histories of reset pods by container name, until a replacement
        # inherits them. A reset pod's later samples are ignored.
        self.reset_pending: dict[str, _History] = {}
        self.retired: set[tuple[str, str]] = set()

    def reset(self, key: tuple[str, str]) -> None:
        """Record that the check deleted this container's pod to skip its
        restart backoff. The history stays live until the next new pod with
        the same container name inherits it."""
        h = self.containers.pop(key, None)
        if h is not None:
            self.retired.add(key)
            self.reset_pending[h.name] = h

    def observe(self, now: float, samples: list[Sample]) -> None:
        if self.started is None:
            self.started = now
        self.last_sample = now
        for s in samples:
            if s.key in self.retired:
                continue
            h = self.containers.get(s.key)
            if h is None:
                h = self.reset_pending.pop(s.name, None)
                if h is None:
                    # Restarts before the first sample predate the window.
                    self.containers[s.key] = _History(
                        s.name, now, s.restart_count, now, s.deleting, s.waiting_reason
                    )
                    continue
                # Every restart of the replacement followed the reset.
                h.restart_count = 0
                self.containers[s.key] = h
            if s.restart_count > h.restart_count:
                h.restarts.append(
                    {
                        "observed_at": now,
                        "restart_count": s.restart_count,
                        "restarts": s.restart_count - h.restart_count,
                        "class": s.exit_class,
                        "exit_code": s.exit_code,
                        "finished_at": s.finished_at,
                    }
                )
                h.restart_count = s.restart_count
            h.last_seen = now
            h.deleting = s.deleting
            h.waiting_reason = s.waiting_reason

    def covered(self, window_s: float) -> bool:
        """Whether sampling has run for at least `window_s`."""
        return (
            self.started is not None
            and self.last_sample is not None
            and self.last_sample - self.started >= window_s
        )

    def _live_crashes(
        self, now: float
    ) -> Iterator[tuple[_History, list[tuple[float, int]]]]:
        live = [
            h for h in self.containers.values() if h.last_seen >= now and not h.deleting
        ]
        for h in live + list(self.reset_pending.values()):
            yield h, [
                (r["observed_at"], r["restarts"])
                for r in h.restarts
                if r["class"] in CRASH_CLASSES
            ]

    def crashed(self, window_s: float) -> list[str]:
        """Live containers that crashed in the last `window_s` before the
        latest sample."""
        if self.last_sample is None:
            return []
        lo = self.last_sample - window_s
        return [
            h.name
            for h, crashes in self._live_crashes(self.last_sample)
            if any(at >= lo for at, _ in crashes)
        ]

    def loops(self, window_s: float, span_s: float = 0.0) -> dict[str, dict[str, Any]]:
        """Containers that crash-looped in some `window_s` window ending within
        the last `span_s` before the latest sample, with their restart history
        relative to that sample."""
        if self.last_sample is None:
            return {}
        now = self.last_sample
        result = {}
        for h, crashes in self._live_crashes(now):
            # A window's count rises only when its end reaches a crash, so the
            # earliest end and the crashes inside the span are the only
            # candidate maxima.
            ends = [now - span_s] + [
                at for at, _ in crashes if now - span_s < at <= now
            ]
            most = max(
                sum(n for at, n in crashes if end - window_s <= at <= end)
                for end in ends
            )
            if most < LOOP_MIN_CRASHES:
                continue
            result[h.name] = {
                "crashes_in_window": most,
                "restart_count": h.restart_count,
                "waiting_reason": h.waiting_reason,
                "history": [
                    {**r, "observed_at": round(r["observed_at"] - now, 1)}
                    for r in h.restarts
                ],
            }
        return result

    def persisting(self, window_s: float, since: float) -> dict[str, dict[str, Any]]:
        """Live containers that crashed `PERSISTING_CRASHES` times since
        `since`, each crash within `window_s` of the previous one."""
        if self.last_sample is None:
            return {}
        result = {}
        for h, crashes in self._live_crashes(self.last_sample):
            times = sorted(at for at, n in crashes if at >= since for _ in range(n))
            chain = 1
            for prev, at in zip(times, times[1:]):
                chain = chain + 1 if at - prev <= window_s else 1
                if chain >= PERSISTING_CRASHES:
                    break
            if chain < PERSISTING_CRASHES:
                continue
            result[h.name] = {
                "crashes_since": len(times),
                "restart_count": h.restart_count,
                "waiting_reason": h.waiting_reason,
                "history": [
                    {**r, "observed_at": round(r["observed_at"] - self.last_sample, 1)}
                    for r in h.restarts
                ],
            }
        return result
