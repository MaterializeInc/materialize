#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from materialize.buildkite_insights.util import buildkite_api
from materialize.buildkite_insights.util.data_io import (
    ensure_temp_dir_exists,
    exists_file_with_recent_data,
    get_file_path,
    read_results_from_file,
    write_results_to_file,
)

OUTPUT_TYPE_TXT = "txt"
OUTPUT_TYPE_TXT_SHORT = "txt-short"
OUTPUT_TYPE_CSV = "csv"

FETCH_MODE_NO = "no"
FETCH_MODE_YES = "yes"
FETCH_MODE_AUTO = "auto"

BUILDKITE_BUILD_STATES = [
    "running",
    "scheduled",
    "passed",
    "failing",
    "failed",
    "blocked",
    "canceled",
    "canceling",
    "skipped",
    "not_run",
    "finished",
]

MZ_PIPELINES = [
    "cleanup",
    "coverage",
    "deploy",
    "deploy-lsp",
    "deploy-mz",
    "deploy-website",
    "license",
    "nightlies",
    "release-qualification",
    "security",
    "sql-logic-tests",
    "tests",
    "www",
]


@dataclass
class StepData:
    step_key: str
    build_number: int
    created_at: datetime
    duration_in_min: float | None
    passed: bool
    exit_status: int | None
    retry_count: int


def get_data(
    pipeline_slug: str,
    fetch_mode: str,
    max_fetches: int,
    branch: str | None,
    build_state: str | None,
    items_per_page: int = 50,
) -> list[Any]:
    ensure_temp_dir_exists()

    file_path = get_file_path(
        pipeline_slug=pipeline_slug,
        branch=branch,
        build_state=build_state,
        max_fetches=max_fetches,
        items_per_page=items_per_page,
    )
    no_fetch = fetch_mode == FETCH_MODE_NO

    if fetch_mode == FETCH_MODE_AUTO and exists_file_with_recent_data(file_path):
        no_fetch = True

    if no_fetch:
        print(f"Using existing data: {file_path}")
        return read_results_from_file(file_path)

    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds"
    params = {"include_retried_jobs": "true", "per_page": str(items_per_page)}

    if branch is not None:
        params["branch"] = branch

    if build_state is not None:
        params["state"] = build_state

    result = buildkite_api.get(request_path, params, max_fetches=max_fetches)
    write_results_to_file(result, file_path)
    return result


def get_step_infos_from_build(build: Any, build_step_keys: list[str]) -> list[StepData]:
    collected_steps = []

    for job in build["jobs"]:
        if not job.get("step_key"):
            continue

        if build_step_keys is not None and job["step_key"] not in build_step_keys:
            continue

        if job["state"] in ["canceled", "running"]:
            continue

        build_number = build["number"]
        created_at = datetime.fromisoformat(job["created_at"])
        build_step_key = job["step_key"]

        if job.get("started_at") and job.get("finished_at"):
            started_at = datetime.fromisoformat(job["started_at"])
            finished_at = datetime.fromisoformat(job["finished_at"])
            duration_in_min = (finished_at - started_at).total_seconds() / 60
        else:
            duration_in_min = None

        job_passed = job["state"] == "passed"
        exit_status = job["exit_status"]
        retry_count = job["retries_count"] or 0

        assert (
            not job_passed or duration_in_min is not None
        ), "Duration must be available for passed step"

        step_data = StepData(
            build_step_key,
            build_number,
            created_at,
            duration_in_min,
            job_passed,
            exit_status=exit_status,
            retry_count=retry_count,
        )
        collected_steps.append(step_data)

    return collected_steps


def collect_data(
    data: list[Any],
    build_step_keys: list[str],
) -> list[StepData]:
    result = []
    for build in data:
        step_infos = get_step_infos_from_build(build, build_step_keys)
        result.extend(step_infos)

    return result


def print_data(
    step_infos: list[StepData],
    pipeline_slug: str,
    build_step_keys: list[str],
    output_type: str,
) -> None:
    if output_type == OUTPUT_TYPE_CSV:
        print(
            "step_key,build_number,created_at,duration_in_min,passed,exit_status,retry_count"
        )

    for entry in step_infos:
        if output_type in [OUTPUT_TYPE_TXT, OUTPUT_TYPE_TXT_SHORT]:
            formatted_duration = (
                f"{entry.duration_in_min:.2f}"
                if entry.duration_in_min is not None
                else "None"
            )
            url = (
                ""
                if output_type == OUTPUT_TYPE_TXT_SHORT
                else f"https://buildkite.com/materialize/{pipeline_slug}/builds/{entry.build_number}, "
            )
            print(
                f"{entry.step_key}, {entry.build_number}, {entry.created_at}, {formatted_duration} min, {url}{'SUCCESS' if entry.passed else 'FAIL'}{' (RETRY)' if entry.retry_count > 0 else ''}"
            )
        elif output_type == OUTPUT_TYPE_CSV:
            print(
                f"{entry.step_key},{entry.build_number},{entry.created_at.isoformat()},{entry.duration_in_min},{1 if entry.passed else 0},{entry.exit_status},{entry.retry_count}"
            )

    if output_type in [OUTPUT_TYPE_TXT, OUTPUT_TYPE_TXT_SHORT]:
        print_stats(step_infos, build_step_keys)


def print_stats(
    step_infos: list[StepData],
    build_step_keys: list[str],
) -> None:
    if len(step_infos) == 0:
        print(f"No data for jobs with keys {build_step_keys}!")
        return

    dfs = pd.DataFrame(step_infos)
    dfs_with_success = dfs.loc[dfs["passed"]]

    number_of_builds = len(step_infos)
    number_of_builds_with_successful_step = len(dfs_with_success.index)
    success_prop = number_of_builds_with_successful_step / number_of_builds

    print()
    print(f"Statistics for jobs {build_step_keys}:")
    print(f"Number of builds: {number_of_builds}")
    print(
        f"Number of builds with job success: {number_of_builds_with_successful_step} ({100 * success_prop:.1f}%)"
    )
    print(
        f"Min duration with success: {dfs_with_success['duration_in_min'].min():.2f} min"
    )
    print(
        f"Max duration with success: {dfs_with_success['duration_in_min'].max():.2f} min"
    )
    print(
        f"Mean duration with success: {dfs_with_success['duration_in_min'].mean():.2f} min"
    )
    print(
        f"Median duration with success: {dfs_with_success['duration_in_min'].median():.2f} min"
    )


def main(
    pipeline_slug: str,
    build_step_keys: list[str],
    fetch_mode: str,
    max_fetches: int,
    branch: str | None,
    build_state: str | None,
    output_type: str,
) -> None:
    data = get_data(pipeline_slug, fetch_mode, max_fetches, branch, build_state)
    step_infos = collect_data(data, build_step_keys)
    print_data(step_infos, pipeline_slug, build_step_keys, output_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="buildkite-step-durations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--pipeline", choices=MZ_PIPELINES, default="tests", type=str)
    parser.add_argument("--build-step-key", action="append", type=str)
    parser.add_argument(
        "--fetch",
        choices=[FETCH_MODE_AUTO, FETCH_MODE_NO, FETCH_MODE_YES],
        default=FETCH_MODE_AUTO,
        type=str,
        help="Whether to fetch new data from Buildkite or reuse previously fetched, matching data.",
    )
    parser.add_argument("--max-fetches", default=3, type=int)
    parser.add_argument(
        "--branch", default="main", type=str, help="Use '*' for all branches"
    )
    parser.add_argument(
        "--build-state",
        default=None,
        type=str,
        choices=BUILDKITE_BUILD_STATES,
    )
    parser.add_argument(
        "--output-type",
        choices=[OUTPUT_TYPE_TXT, OUTPUT_TYPE_TXT_SHORT, OUTPUT_TYPE_CSV],
        default=OUTPUT_TYPE_TXT,
        type=str,
    )
    args = parser.parse_args()

    main(
        args.pipeline,
        args.build_step_key,
        args.fetch,
        args.max_fetches,
        args.branch if args.branch != "*" else None,
        args.build_state,
        args.output_type,
    )
