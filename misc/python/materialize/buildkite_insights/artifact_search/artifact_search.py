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

from materialize.buildkite_insights.artifact_search.artifact_search_presentation import (
    print_artifact_match,
    print_before_search_results,
    print_summary,
)
from materialize.buildkite_insights.buildkite_api.buildkite_config import MZ_PIPELINES
from materialize.buildkite_insights.buildkite_api.generic_api import RateLimitExceeded
from materialize.buildkite_insights.cache import artifacts_cache, builds_cache
from materialize.buildkite_insights.cache.cache_constants import (
    FETCH_MODE_CHOICES,
    FetchMode,
)
from materialize.buildkite_insights.util.build_step_utils import extract_build_job_ids
from materialize.buildkite_insights.util.search_utility import (
    _search_value_to_pattern,
    determine_line_number,
    determine_position_in_line,
)

ACCEPTED_FILE_ENDINGS = {"log", "txt", "xml", "zst"}


def main(
    pipeline_slug: str,
    build_number: int,
    specified_job_id: str | None,
    pattern: str,
    fetch: FetchMode,
    max_results: int,
    use_regex: bool,
    include_zst_files: bool,
) -> None:
    assert len(pattern) > 0, "pattern must not be empty"

    artifact_list_by_job_id: dict[str, list[Any]] = dict()

    if specified_job_id is not None:
        job_ids = [specified_job_id]
    else:
        build = builds_cache.get_or_query_single_build(
            pipeline_slug, fetch, build_number=build_number
        )
        job_ids = extract_build_job_ids(build)

    for job_id in job_ids:
        artifact_list_by_job_id[job_id] = (
            artifacts_cache.get_or_query_job_artifact_list(
                pipeline_slug, fetch, build_number=build_number, job_id=job_id
            )
        )

    print_before_search_results()

    count_matches = 0
    count_all_artifacts = 0
    ignored_file_names = set()
    max_search_results_hit = False

    for job_id, artifact_list in artifact_list_by_job_id.items():
        count_artifacts_of_job = len(artifact_list)

        if count_artifacts_of_job == 0:
            print(f"Skipping job {job_id} without artifacts.")
            continue

        print(f"Searching {count_artifacts_of_job} artifacts of job {job_id}.")
        count_all_artifacts = count_all_artifacts + count_artifacts_of_job

        for artifact in artifact_list:
            max_entries_to_print = max(0, max_results - count_matches)
            if max_entries_to_print == 0:
                max_search_results_hit = True
                break

            artifact_id = artifact["id"]
            artifact_file_name = artifact["filename"]

            if not _can_search_artifact(artifact_file_name, include_zst_files):
                print(f"Skipping artifact {artifact_file_name} due to file ending!")
                ignored_file_names.add(artifact_file_name)
                continue

            try:
                matches_in_artifact, max_search_results_hit = _search_artifact(
                    pipeline_slug,
                    artifact_file_name=artifact_file_name,
                    build_number=build_number,
                    job_id=job_id,
                    artifact_id=artifact_id,
                    pattern=pattern,
                    use_regex=use_regex,
                    fetch=fetch,
                    max_entries_to_print=max_entries_to_print,
                )
            except RateLimitExceeded:
                print("Aborting due to exceeded rate limit!")
                return

            count_matches = count_matches + matches_in_artifact

    print_summary(
        pipeline_slug=pipeline_slug,
        build_number=build_number,
        job_id=specified_job_id,
        count_artifacts=count_all_artifacts,
        count_matches=count_matches,
        ignored_file_names=ignored_file_names,
        max_search_results_hit=max_search_results_hit,
    )


def _can_search_artifact(artifact_file_name: str, include_zst_files: bool) -> bool:
    if not include_zst_files and is_zst_file(artifact_file_name):
        return False

    for file_ending in ACCEPTED_FILE_ENDINGS:
        if artifact_file_name.endswith(f".{file_ending}"):
            return True

    return False


def _search_artifact(
    pipeline_slug: str,
    fetch: FetchMode,
    build_number: int,
    job_id: str,
    artifact_id: str,
    artifact_file_name: str,
    pattern: str,
    use_regex: bool,
    max_entries_to_print: int,
) -> tuple[int, bool]:
    """
    :return: number of highlighted results and whether further matches exceeding max_entries_to_print exist
    """
    search_pattern = _search_value_to_pattern(pattern, use_regex)

    artifact_content = artifacts_cache.get_or_download_artifact(
        pipeline_slug,
        fetch,
        build_number=build_number,
        job_id=job_id,
        artifact_id=artifact_id,
        is_zst_compressed=is_zst_file(artifact_file_name),
    )

    search_offset = 0
    match_count = 0

    while True:
        match = search_pattern.search(artifact_content, pos=search_offset)

        if match is None:
            break

        match_count = match_count + 1

        line_number = determine_line_number(artifact_content, position=match.start())
        position_in_line = determine_position_in_line(
            artifact_content, position=match.start()
        )

        print_artifact_match(
            file_name=artifact_file_name,
            line_number=line_number,
            position_in_line=position_in_line,
            content=artifact_content,
            search_value=pattern,
            use_regex=use_regex,
            search_offset=search_offset,
        )

        search_offset = match.end()

        if match_count >= max_entries_to_print:
            return match_count, True

    return match_count, False


def is_zst_file(file_name: str) -> bool:
    return file_name.endswith(".zst")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="buildkite-artifact-search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "pipeline",
        choices=MZ_PIPELINES,
        type=str,
    )

    # no hyphen because positionals with hyphen cause issues
    parser.add_argument(
        "buildnumber",
        type=int,
    )

    parser.add_argument("pattern", type=str)

    parser.add_argument("--jobid", type=str)

    parser.add_argument("--max-results", default=50, type=int)
    parser.add_argument(
        "--use-regex",
        action="store_true",
    )
    parser.add_argument(
        "--include-zst-files", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--fetch",
        type=lambda mode: FetchMode[mode.upper()],
        choices=FETCH_MODE_CHOICES,
        default=FetchMode.AUTO,
        help="Whether to fetch fresh builds from Buildkite.",
    )

    args = parser.parse_args()

    main(
        args.pipeline,
        args.buildnumber,
        args.jobid,
        args.pattern,
        args.fetch,
        args.max_results,
        args.use_regex,
        args.include_zst_files,
    )
