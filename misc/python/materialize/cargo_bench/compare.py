# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Comparison of criterion results between an ancestor baseline and the current run.

Reads the JSON files criterion writes under its output directory. Criterion
itself never fails a run on a regression, so the verdict logic lives here.
"""

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class Verdict(Enum):
    REGRESSION = "regression"
    IMPROVEMENT = "improvement"
    UNCHANGED = "unchanged"
    NEW = "new"


@dataclass(frozen=True)
class BenchResult:
    """Outcome for one criterion benchmark id.

    Means are in nanoseconds. Change fields are relative to the ancestor mean,
    so 0.1 is a 10% slowdown. They are `None` when there is no ancestor
    measurement to compare against.
    """

    id: str
    current_mean_ns: float
    ancestor_mean_ns: float | None
    change_mean: float | None
    change_lower: float | None
    change_upper: float | None
    verdict: Verdict


@dataclass(frozen=True)
class CompareReport:
    results: list[BenchResult]
    warnings: list[str]

    @property
    def has_regressions(self) -> bool:
        return any(r.verdict == Verdict.REGRESSION for r in self.results)


_VERDICT_ORDER = {
    Verdict.REGRESSION: 0,
    Verdict.IMPROVEMENT: 1,
    Verdict.UNCHANGED: 2,
    Verdict.NEW: 3,
}


def _load(path: Path) -> Any:
    with path.open() as f:
        return json.load(f)


def compare(criterion_dir: Path, threshold: float) -> CompareReport:
    """Compare every benchmark under `criterion_dir` against its `ancestor` baseline.

    A benchmark is a regression when the lower bound of criterion's 95%
    confidence interval on the relative mean change exceeds `threshold`, and
    an improvement when the upper bound is below `-threshold`. A benchmark
    with no ancestor baseline is reported as new.
    """
    results: list[BenchResult] = []
    warnings: list[str] = []

    # Criterion copies benchmark.json into every saved baseline directory,
    # not just "new/", so the guard on new_dir.name below is what identifies
    # the current result and prevents counting a benchmark once per baseline
    # it has been copied into.
    for benchmark_json in sorted(criterion_dir.rglob("benchmark.json")):
        new_dir = benchmark_json.parent
        if new_dir.name != "new":
            continue
        bench_dir = new_dir.parent
        full_id = _load(benchmark_json)["full_id"]
        current_mean = _load(new_dir / "estimates.json")["mean"]["point_estimate"]

        ancestor_estimates = bench_dir / "ancestor" / "estimates.json"
        change_estimates = bench_dir / "change" / "estimates.json"
        # A change file is only meaningful when the ancestor baseline it was
        # computed against is present. A stale change file from an earlier
        # run must not produce a verdict.
        if not ancestor_estimates.exists():
            results.append(
                BenchResult(full_id, current_mean, None, None, None, None, Verdict.NEW)
            )
            continue
        ancestor_mean = _load(ancestor_estimates)["mean"]["point_estimate"]
        if not change_estimates.exists():
            warnings.append(
                f"{full_id}: ancestor baseline exists but criterion wrote no change estimate, treating as new"
            )
            results.append(
                BenchResult(
                    full_id, current_mean, ancestor_mean, None, None, None, Verdict.NEW
                )
            )
            continue

        change = _load(change_estimates)["mean"]
        lower = change["confidence_interval"]["lower_bound"]
        upper = change["confidence_interval"]["upper_bound"]
        if lower > threshold:
            verdict = Verdict.REGRESSION
        elif upper < -threshold:
            verdict = Verdict.IMPROVEMENT
        else:
            verdict = Verdict.UNCHANGED
        results.append(
            BenchResult(
                full_id,
                current_mean,
                ancestor_mean,
                change["point_estimate"],
                lower,
                upper,
                verdict,
            )
        )

    def sort_key(r: BenchResult) -> tuple[int, float, str]:
        change = r.change_mean if r.change_mean is not None else 0.0
        return (_VERDICT_ORDER[r.verdict], -change, r.id)

    results.sort(key=sort_key)
    return CompareReport(results, warnings)


def format_duration(ns: float) -> str:
    """Format nanoseconds with the largest unit that keeps the value below 1000."""
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.2f} {unit}"
    return f"{ns:.2f} ns"


def _pct(value: float) -> str:
    return f"{value * 100:+.1f}%"


def render_markdown(report: CompareReport) -> str:
    """Render the results as a markdown table, one row per benchmark."""
    lines = [
        "| Benchmark | Ancestor | Current | Change | 95% CI | Verdict |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for r in report.results:
        ancestor = (
            format_duration(r.ancestor_mean_ns)
            if r.ancestor_mean_ns is not None
            else ""
        )
        change = _pct(r.change_mean) if r.change_mean is not None else ""
        ci = (
            f"[{_pct(r.change_lower)}, {_pct(r.change_upper)}]"
            if r.change_lower is not None and r.change_upper is not None
            else ""
        )
        verdict = r.verdict.value
        if r.verdict == Verdict.REGRESSION:
            verdict = f"**{verdict}**"
        lines.append(
            f"| {r.id} | {ancestor} | {format_duration(r.current_mean_ns)} | {change} | {ci} | {verdict} |"
        )
    return "\n".join(lines)
