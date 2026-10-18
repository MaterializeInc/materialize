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
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

CRASH_CLASSES = frozenset({"halt", "panic"})
LOOP_MIN_CRASHES = 2


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

    def observe(self, now: float, samples: list[Sample]) -> None:
        if self.started is None:
            self.started = now
        self.last_sample = now
        for s in samples:
            h = self.containers.get(s.key)
            if h is None:
                # Restarts before the first sample predate the window.
                self.containers[s.key] = _History(
                    s.name, now, s.restart_count, now, s.deleting, s.waiting_reason
                )
                continue
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

    def loops(self, window_s: float) -> dict[str, dict[str, Any]]:
        """Containers that crash-looped in the last `window_s` before the
        latest sample, with their restart history relative to that sample."""
        if self.last_sample is None:
            return {}
        now = self.last_sample
        lo = now - window_s
        result = {}
        for h in self.containers.values():
            if h.last_seen < now or h.deleting:
                continue
            in_window = [r for r in h.restarts if r["observed_at"] >= lo]
            crashes = sum(
                r["restarts"] for r in in_window if r["class"] in CRASH_CLASSES
            )
            if crashes < LOOP_MIN_CRASHES:
                continue
            result[h.name] = {
                "crashes_in_window": crashes,
                "restart_count": h.restart_count,
                "waiting_reason": h.waiting_reason,
                "history": [
                    {**r, "observed_at": round(r["observed_at"] - now, 1)}
                    for r in h.restarts
                ],
            }
        return result
