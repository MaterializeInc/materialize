#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Summarize performance movements across the nightly, release qualification and spec
sheet pipelines.

Reads Buildkite directly when a token is configured. Given `--from-log` or `--from-csv`
it instead parses files that were fetched by other means, which keeps a single
implementation of the parsing and ranking behind both entry points.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence

from materialize.buildkite_insights.cache.cache_constants import (
    FETCH_MODE_CHOICES,
    FetchMode,
)
from materialize.buildkite_insights.perf_movements import spec_sheet
from materialize.buildkite_insights.perf_movements.collect import (
    LOG_PARSER_BY_STEP_KEY,
    PIPELINES,
    collect_pipeline_movements,
)
from materialize.buildkite_insights.perf_movements.movement import JobRef, Movement

LOCAL_BUILD_NUMBER = 0
"""Stands in for a build number when parsing a file instead of a Buildkite job."""


def _verdict(movement: Movement) -> str:
    if movement.reported_regression:
        return "WORSE!"
    if movement.is_improvement:
        return "better"
    if movement.is_deterioration:
        return "worse"
    return "same"


def select(
    movements: Sequence[Movement],
    threshold_percent: float,
    only: str,
) -> list[Movement]:
    """Rank movements worst first, dropping those under threshold.

    A step that published its own regression verdict is always kept: the team's gate
    outranks the caller's threshold in both directions.
    """
    selected = [
        movement
        for movement in movements
        if movement.reported_regression or movement.exceeds(threshold_percent)
    ]

    if only == "regressions":
        selected = [m for m in selected if m.is_deterioration]
    elif only == "improvements":
        selected = [m for m in selected if m.is_improvement]

    return sorted(
        selected,
        key=lambda m: (m.is_improvement, -m.magnitude_percent),
    )


def render_text(movements: Sequence[Movement], show_urls: bool) -> str:
    if not movements:
        return "No movements above the threshold."

    lines = [
        f"{'VERDICT':<7} | {'CHANGE':>9} | {'METRIC':<16} | {'BASELINE':>14} |"
        f" {'THIS':>14} | {'UNIT':<6} | {'BUILD':<28} | SCENARIO"
    ]
    lines.append("-" * 150)

    for movement in movements:
        lines.append(
            f"{_verdict(movement):<7} |"
            f" {movement.change_percent:>+8.1f}% |"
            f" {movement.metric:<16} |"
            f" {movement.baseline:>14.3f} |"
            f" {movement.this:>14.3f} |"
            f" {movement.unit or '':<6} |"
            f" {movement.job.describe():<28} |"
            f" {movement.scenario}"
        )

    if show_urls:
        lines.append("")
        lines.append("Jobs:")
        seen = []
        for movement in movements:
            if movement.job.source is not None:
                continue
            url = movement.job.url()
            if url not in seen:
                seen.append(url)
                lines.append(f"* {movement.job.step_key}: {url}")

    return "\n".join(lines)


def render_json(movements: Sequence[Movement]) -> str:
    payload = [
        {
            "pipeline": m.job.pipeline_slug,
            "build_number": m.job.build_number,
            "step_key": m.job.step_key,
            "commit_hash": m.job.commit_hash,
            "source": m.job.source,
            "url": m.job.url() if m.job.source is None else None,
            "scenario": m.scenario,
            "metric": m.metric,
            "unit": m.unit,
            "this": m.this,
            "baseline": m.baseline,
            "change_percent": m.change_percent,
            "direction": m.direction.value,
            "baseline_kind": m.baseline_kind.value,
            "threshold_percent": m.threshold_percent,
            "reported_regression": m.reported_regression,
            "verdict": _verdict(m),
        }
        for m in movements
    ]
    return json.dumps(payload, indent=2)


def _movements_from_log_file(path: str, step_key: str) -> list[Movement]:
    parser = LOG_PARSER_BY_STEP_KEY.get(step_key)
    if parser is None:
        raise ValueError(
            f"--step-key must be one of {sorted(LOG_PARSER_BY_STEP_KEY)}, got {step_key!r}"
        )

    with open(path) as handle:
        text = handle.read()

    job = JobRef(
        pipeline_slug="local",
        build_number=LOCAL_BUILD_NUMBER,
        job_id="",
        step_key=step_key,
        source=os.path.basename(path),
    )
    return parser(text, job)


def _movements_from_csv_files(
    latest_paths: list[str], baseline_paths: list[str], min_history: int
) -> list[Movement]:
    def medians(paths: list[str]) -> dict[tuple[spec_sheet.SampleKey, str], float]:
        per_file = []
        for path in paths:
            with open(path) as handle:
                per_file.append(
                    spec_sheet.median_per_key(
                        spec_sheet.parse_results_csv(handle.read())
                    )
                )
        return spec_sheet.merge_medians(per_file)

    job = JobRef(
        pipeline_slug="spec-sheet",
        build_number=LOCAL_BUILD_NUMBER,
        job_id="",
        step_key="cluster-spec-sheet",
        source="local CSV files",
    )
    # Each baseline file is treated as its own earlier observation, which is what
    # `min_history` counts.
    history = [medians([path]) for path in baseline_paths]

    return spec_sheet.compare_to_window(
        latest=medians(latest_paths),
        history=history,
        job=job,
        min_history=min_history,
    )


def _require_buildkite_token() -> None:
    if not (os.getenv("BUILDKITE_CI_API_KEY") or os.getenv("BUILDKITE_TOKEN")):
        raise ValueError(
            "no Buildkite token: set BUILDKITE_TOKEN (or BUILDKITE_CI_API_KEY) with read"
            " access to builds and artifacts, or pass --from-log / --from-csv with files"
            " fetched by other means"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="perf-movements",
        description="Summarize performance movements across Materialize CI pipelines.",
    )
    parser.add_argument(
        "--pipeline",
        action="append",
        choices=PIPELINES,
        help="pipeline to report on; repeatable, defaults to all",
    )
    parser.add_argument("--branch", default="main")
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="minimum movement in percent to report",
    )
    parser.add_argument(
        "--max-builds",
        type=int,
        default=6,
        help="builds to consider when reconstructing a baseline",
    )
    parser.add_argument(
        "--min-history",
        type=int,
        default=3,
        help="earlier observations a reconstructed baseline needs before it is reported",
    )
    parser.add_argument(
        "--only",
        choices=["all", "regressions", "improvements"],
        default="all",
    )
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--urls", action="store_true", help="list job URLs after the table")
    parser.add_argument(
        "--fetch",
        type=FetchMode,
        choices=FETCH_MODE_CHOICES,
        default=FetchMode.AUTO,
    )
    parser.add_argument(
        "--from-log",
        help="parse this job log instead of querying Buildkite; requires --step-key",
    )
    parser.add_argument("--step-key", help="step key that produced --from-log")
    parser.add_argument(
        "--from-csv",
        action="append",
        default=[],
        help="spec sheet result CSV of the build being reported on; repeatable",
    )
    parser.add_argument(
        "--baseline-csv",
        action="append",
        default=[],
        help="spec sheet result CSV of an earlier build; repeatable",
    )
    args = parser.parse_args()

    if args.from_log and not args.step_key:
        parser.error("--from-log requires --step-key")
    if args.baseline_csv and not args.from_csv:
        parser.error("--baseline-csv requires --from-csv")

    movements: list[Movement] = []
    try:
        if args.from_log:
            movements.extend(_movements_from_log_file(args.from_log, args.step_key))
        if args.from_csv:
            movements.extend(
                _movements_from_csv_files(
                    args.from_csv, args.baseline_csv, args.min_history
                )
            )
        if not args.from_log and not args.from_csv:
            _require_buildkite_token()
            for pipeline in args.pipeline or list(PIPELINES):
                movements.extend(
                    collect_pipeline_movements(
                        pipeline_slug=pipeline,
                        branch=args.branch,
                        fetch_mode=args.fetch,
                        max_builds=args.max_builds,
                        min_history=args.min_history,
                    )
                )
    except (ValueError, RuntimeError) as error:
        # buildkite_insights raises a bare RuntimeError for any non-200 response, so an
        # expired or under-scoped token arrives here rather than as a usage error.
        print(f"perf-movements: {error}", file=sys.stderr)
        return 1

    selected = select(movements, args.threshold, args.only)[: args.limit]

    if args.format == "json":
        print(render_json(selected))
    else:
        print(render_text(selected, show_urls=args.urls))

    return 0


if __name__ == "__main__":
    sys.exit(main())
