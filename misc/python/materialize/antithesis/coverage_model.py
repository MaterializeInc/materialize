# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Witnesses connecting observed disruptions to writes verified by later reads."""

from __future__ import annotations

import bisect
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields
from typing import Protocol

from materialize.antithesis import serializability as sz
from materialize.antithesis.ivm_model import WriteOp

Case = tuple[str, str, str, str, str]
"""Phase and kill exposures of one write, its family, later read path, and outcome.

A phase and a kill can occur at different points of the same unresolved interval.
"""


class Event(Protocol):
    @property
    def kind(self) -> str: ...

    @property
    def started(self) -> float: ...

    @property
    def completed(self) -> float: ...

    @property
    def phase(self) -> str: ...

    @property
    def role(self) -> str: ...


@dataclass(frozen=True)
class Exposure:
    phase: str
    role: str
    completed: float


def _exposures(
    ordered: list[Event], starts: list[float], invoke: float, end: float
) -> list[Exposure]:
    first = bisect.bisect_left(starts, invoke)
    last = bisect.bisect_right(starts, end)
    matching = []
    for event in ordered[first:last]:
        if event.completed > end:
            continue
        if event.kind == "phase" and event.phase:
            matching.append(Exposure(event.phase, "", event.completed))
        elif event.kind == "fault" and event.role:
            matching.append(Exposure("", event.role, event.completed))
    return matching


def report_passed(report: sz.Report) -> bool:
    """Whether every evaluated history invariant passed."""
    for field in fields(report):
        if field.name == "stats":
            continue
        value = getattr(report, field.name)
        failed = any(value.values()) if isinstance(value, dict) else bool(value)
        if failed:
            return False
    return True


def history_cases(
    ops: Iterable[sz.Op], events: Iterable[Event], read_ids: set[int]
) -> set[Case]:
    """Return witnessed exposure combinations for selected successful reads.

    A phase sample or the entire acknowledged delete request must lie within
    a write's unresolved interval. A later read must contain that write's id.
    An indeterminate write is resolved no later than its first observation.
    """
    operations = list(ops)
    writers = {op.op_id: op for op in operations if op.is_write}
    observed_by: dict[int, float] = {}
    for read in operations:
        if read.is_write or not read.acked:
            continue
        assert read.complete is not None
        for observation in read.observations:
            for op_id in observation.values:
                observed_by[op_id] = min(
                    observed_by.get(op_id, read.complete), read.complete
                )

    ordered = sorted(events, key=lambda e: e.started)
    starts = [e.started for e in ordered]
    exposures: dict[int, list[Exposure]] = {}
    for op_id, write in writers.items():
        if write.outcome is sz.Outcome.FAIL or op_id not in observed_by:
            continue
        end = observed_by[op_id]
        if write.acked:
            assert write.complete is not None
            end = min(end, write.complete)
        matching = _exposures(ordered, starts, write.invoke, end)
        if matching:
            exposures[op_id] = matching

    cases: set[Case] = set()
    for read in operations:
        if read.op_id not in read_ids or read.is_write or not read.acked:
            continue
        for observation in read.observations:
            for op_id in set(observation.values) & exposures.keys():
                write = writers[op_id]
                if write.write_key != observation.key:
                    continue
                before_read = [
                    e for e in exposures[op_id] if e.completed <= read.invoke
                ]
                if not before_read:
                    continue
                phases = {e.phase for e in before_read if e.phase} or {""}
                roles = {e.role for e in before_read if e.role} or {""}
                outcome = "acked" if write.acked else "indeterminate"
                for phase in phases:
                    for role in roles:
                        cases.add(
                            (phase, role, write.kind.value, observation.path, outcome)
                        )
    return cases


def stream_cases(
    writes: Iterable[WriteOp],
    observed_by: Mapping[int, tuple[float, float]],
    events: Iterable[Event],
) -> set[Case]:
    """Exposure witnesses for write ids verified at closed SUBSCRIBE timestamps."""
    ordered = sorted(events, key=lambda e: e.started)
    starts = [e.started for e in ordered]
    cases: set[Case] = set()
    for write in writes:
        if write.op_id not in observed_by or write.outcome == "rejected":
            continue
        fetched, observed = observed_by[write.op_id]
        end = observed
        acked = write.outcome == "ok" and write.complete_rt is not None
        if acked:
            assert write.complete_rt is not None
            end = min(end, write.complete_rt)
        exposed = [
            e
            for e in _exposures(ordered, starts, write.invoke_rt, end)
            if e.completed <= fetched
        ]
        if not exposed:
            continue
        phases = {e.phase for e in exposed if e.phase} or {""}
        roles = {e.role for e in exposed if e.role} or {""}
        for phase in phases:
            for role in roles:
                cases.add(
                    (
                        phase,
                        role,
                        "ivm",
                        "subscribe",
                        "acked" if acked else "indeterminate",
                    )
                )
    return cases
