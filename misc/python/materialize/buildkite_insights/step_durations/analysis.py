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
from typing import Any

import pandas as pd

from materialize.buildkite_insights.step_durations.build_step import (
    BuildStep,
    BuildStepOutcome,
    extract_build_step_data,
)
from materialize.buildkite_insights.util.buildkite_api import fetch_builds
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

    result = fetch_builds(
        pipeline_slug=pipeline_slug,
        max_fetches=max_fetches,
        branch=branch,
        build_state=build_state,
        items_per_page=items_per_page,
    )
    write_results_to_file(result, file_path)
    return result


def print_data(
    step_infos: list[BuildStepOutcome],
    pipeline_slug: str,
    build_steps: list[BuildStep],
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
            url = ("" if output_type == OUTPUT_TYPE_TXT_SHORT else entry.web_url,)
            print(
                f"{entry.step_key}, {entry.build_number}, {entry.created_at}, {formatted_duration} min, {url}{'SUCCESS' if entry.passed else 'FAIL'}{' (RETRY)' if entry.retry_count > 0 else ''}"
            )
        elif output_type == OUTPUT_TYPE_CSV:
            print(
                f"{entry.step_key},{entry.build_number},{entry.created_at.isoformat()},{entry.duration_in_min},{1 if entry.passed else 0},{entry.exit_status},{entry.retry_count}"
            )

    if output_type in [OUTPUT_TYPE_TXT, OUTPUT_TYPE_TXT_SHORT]:
        print_stats(step_infos, build_steps)


def print_stats(
    step_infos: list[BuildStepOutcome],
    build_steps: list[BuildStep],
) -> None:
    if len(step_infos) == 0:
        print(f"No data for jobs with keys {build_steps}!")
        return

    dfs = pd.DataFrame(step_infos)
    dfs_with_success = dfs.loc[dfs["passed"]]

    number_of_builds = len(step_infos)
    number_of_builds_with_successful_step = len(dfs_with_success.index)
    success_prop = number_of_builds_with_successful_step / number_of_builds

    print()
    print(f"Statistics for jobs {build_steps}:")
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
    build_steps: list[BuildStep],
    fetch_mode: str,
    max_fetches: int,
    branch: str | None,
    build_state: str | None,
    output_type: str,
) -> None:
    builds_data = get_data(pipeline_slug, fetch_mode, max_fetches, branch, build_state)
    step_infos = extract_build_step_data(
        builds_data=builds_data, selected_build_steps=build_steps
    )
    print_data(step_infos, pipeline_slug, build_steps, output_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="buildkite-step-durations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--pipeline", choices=MZ_PIPELINES, default="tests", type=str)
    # TODO: Should also take --build-parallel-job
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
        [BuildStep(build_step_key, None) for build_step_key in args.build_step_key]
        or [],
        args.fetch,
        args.max_fetches,
        args.branch if args.branch != "*" else None,
        args.build_state,
        args.output_type,
    )
