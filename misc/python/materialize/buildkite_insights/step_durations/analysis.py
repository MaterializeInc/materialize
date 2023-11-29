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
    read_results_from_file,
    write_results_to_file,
)

OUTPUT_TYPE_TXT = "txt"
OUTPUT_TYPE_CSV = "csv"

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


@dataclass
class StepData:
    step_key: str
    build_number: int
    created_at: datetime
    duration_in_min: float | None
    passed: bool


def get_file_name(pipeline_slug: str) -> str:
    return f"{pipeline_slug}_builds.json"


def get_data(
    pipeline_slug: str,
    no_fetch: bool,
    max_fetches: int | None,
    branch: str | None,
    build_state: str | None,
) -> list[Any]:
    if no_fetch:
        return read_results_from_file(get_file_name(pipeline_slug))

    request_path = f"organizations/materialize/pipelines/{pipeline_slug}/builds"
    params = {"include_retried_jobs": "true", "per_page": "100"}

    if branch is not None:
        params["branch"] = branch

    if build_state is not None:
        params["state"] = build_state

    result = buildkite_api.get(request_path, params, max_fetches=max_fetches)
    write_results_to_file(result, get_file_name(pipeline_slug))
    return result


def get_step_info_from_build(build: Any, build_step_key: str) -> StepData | None:
    for job in build["jobs"]:
        if not job.get("step_key"):
            continue

        if job["step_key"] != build_step_key:
            continue

        if job["state"] in ["canceled", "running"]:
            continue

        build_number = build["number"]
        created_at = datetime.fromisoformat(job["created_at"])

        if job.get("started_at") and job.get("finished_at"):
            started_at = datetime.fromisoformat(job["started_at"])
            finished_at = datetime.fromisoformat(job["finished_at"])
            duration_in_min = (finished_at - started_at).total_seconds() / 60
        else:
            duration_in_min = None

        job_passed = job["state"] == "passed"

        assert (
            not job_passed or duration_in_min is not None
        ), "Duration must be available for passed step"

        step_data = StepData(
            build_step_key, build_number, created_at, duration_in_min, job_passed
        )
        return step_data

    return None


def collect_data(data: list[Any], build_step_key: str) -> list[StepData]:
    result = []
    for build in data:
        step_info = get_step_info_from_build(build, build_step_key)

        if step_info is not None:
            result.append(step_info)

    return result


def print_data(
    step_infos: list[StepData], build_step_key: str, output_type: str
) -> None:
    if output_type == OUTPUT_TYPE_CSV:
        print("step_key,build_number,created_at,duration_in_min,passed")

    for entry in step_infos:
        if output_type == OUTPUT_TYPE_TXT:
            formatted_duration = (
                f"{entry.duration_in_min:.2f}"
                if entry.duration_in_min is not None
                else "None"
            )
            print(
                f"{entry.step_key}, {entry.build_number}, {entry.created_at}, {formatted_duration} min, {'SUCCESS' if entry.passed else 'FAIL'}"
            )
        elif output_type == OUTPUT_TYPE_CSV:
            print(
                f"{entry.step_key},{entry.build_number},{entry.created_at.isoformat()},{entry.duration_in_min}, {1 if entry.passed else 0}"
            )

    if output_type == OUTPUT_TYPE_TXT:
        print_stats(step_infos, build_step_key)


def print_stats(step_infos: list[StepData], build_step_key: str) -> None:
    if len(step_infos) == 0:
        print(f"No data for job {build_step_key}!")
        return

    dfs = pd.DataFrame(step_infos)
    dfs_with_success = dfs.loc[dfs["passed"]]

    number_of_builds = len(step_infos)
    number_of_builds_with_successful_step = len(dfs_with_success.index)
    success_prop = number_of_builds_with_successful_step / number_of_builds

    print()
    print(f"Statistics for job '{build_step_key}':")
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
    build_step_key: str,
    no_fetch: bool,
    max_fetches: int | None,
    branch: str | None,
    build_state: str | None,
    output_type: str,
) -> None:
    data = get_data(pipeline_slug, no_fetch, max_fetches, branch, build_state)
    step_infos = collect_data(data, build_step_key)
    print_data(step_infos, build_step_key, output_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="buildkite-step-durations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--pipeline", default="tests", type=str)
    parser.add_argument("--build-step-key", default=None, type=str)
    parser.add_argument("--no-fetch", action="store_true")
    parser.add_argument("--max-fetches", default=5, type=int)
    parser.add_argument("--branch", default="main", type=str)
    parser.add_argument(
        "--build-state",
        default=None,
        type=str,
        choices=BUILDKITE_BUILD_STATES,
    )
    parser.add_argument(
        "--output-type",
        choices=[OUTPUT_TYPE_TXT, OUTPUT_TYPE_CSV],
        default=OUTPUT_TYPE_TXT,
        type=str,
    )
    args = parser.parse_args()

    main(
        args.pipeline,
        args.build_step_key,
        args.no_fetch,
        args.max_fetches,
        args.branch,
        args.build_state,
        args.output_type,
    )
