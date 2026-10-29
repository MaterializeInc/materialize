# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Coverage credited only after an oracle completes a non-vacuous comparison."""

from __future__ import annotations

from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    sometimes,
)

from materialize.antithesis import coverage_model, telemetry
from materialize.antithesis import serializability as sz
from materialize.antithesis.ivm_model import WriteOp


def history_checked(ops: list[sz.Op], report: sz.Report, read_ids: set[int]) -> None:
    if not telemetry.enabled():
        return
    reads = [op for op in ops if op.op_id in read_ids and op.acked]
    passed = coverage_model.report_passed(report)
    telemetry.comparison(
        "history", "history", passed if reads else None, len(reads) or 1
    )
    writes = [op for op in ops if op.is_write]
    if not reads or not writes or not passed:
        return
    events = telemetry.events_between(
        min(op.invoke for op in writes), max(op.invoke for op in reads)
    )
    cases = coverage_model.history_cases(ops, events, read_ids)
    telemetry.record_coverage(cases)
    details = {"combinations": sorted(cases)[:20], "reads": len(reads)}
    sometimes(
        any(p == "Promoting" and f == "append" for p, _, f, _, _ in cases),
        "coverage: a later read checked an append unresolved when promotion was observed",
        details,
    )
    sometimes(
        any(p == "Promoting" and f == "rmw" for p, _, f, _, _ in cases),
        "coverage: a later read checked an RMW unresolved when promotion was observed",
        details,
    )
    sometimes(
        any(r == "environmentd_active" for _, r, _, _, _ in cases),
        "coverage: a later read checked a write unresolved throughout an active-generation delete",
        details,
    )
    sometimes(
        any(r == "environmentd_candidate" for _, r, _, _, _ in cases),
        "coverage: a later read checked a write unresolved throughout a candidate-generation delete",
        details,
    )
    sometimes(
        any(path in ("index", "mv", "join_mv") for _, _, _, path, _ in cases),
        "coverage: a maintained read path checked a write exposed to an observed disruption",
        details,
    )
    sometimes(
        any(o == "indeterminate" for _, _, _, _, o in cases),
        "coverage: a later read resolved a disrupted write with an indeterminate outcome",
        details,
    )


def ivm_checked(
    writes: list[WriteOp], observed_by: dict[int, tuple[float, float]], passed: bool
) -> None:
    if not telemetry.enabled() or not passed or not observed_by or not writes:
        return
    events = telemetry.events_between(
        min(w.invoke_rt for w in writes), max(t[0] for t in observed_by.values())
    )
    cases = coverage_model.stream_cases(writes, observed_by, events)
    telemetry.record_coverage(cases)
    details = {"combinations": sorted(cases)[:20], "writes": len(observed_by)}
    sometimes(
        any(p == "Promoting" for p, _, _, _, _ in cases),
        "coverage: SUBSCRIBE checked a write unresolved when promotion was observed",
        details,
    )
    sometimes(
        any(r == "clusterd" for _, r, _, _, _ in cases),
        "coverage: SUBSCRIBE checked a write unresolved throughout an acknowledged replica delete",
        details,
    )
