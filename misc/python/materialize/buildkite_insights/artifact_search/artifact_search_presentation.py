# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.buildkite_insights.buildkite_api.builds_api import get_url_to_build
from materialize.buildkite_insights.util.search_utility import (
    highlight_match,
    trim_match,
)
from materialize.terminal import (
    COLOR_CYAN,
    with_formatting,
)

SHORT_SEPARATOR = "----------"
LONG_SEPARATOR = "-------------------------------------------------------------------------------------"


def print_before_search_results() -> None:
    print()
    print(LONG_SEPARATOR)


def print_artifact_match(
    file_name: str,
    line_number: int,
    position_in_line: int,
    content: str,
    search_value: str,
    use_regex: bool,
    search_offset: int,
) -> None:
    matched_snippet = trim_match(
        match_text=content,
        search_value=search_value,
        use_regex=use_regex,
        search_offset=search_offset,
        one_line_match_presentation=False,
    )
    matched_snippet = highlight_match(
        input=matched_snippet,
        search_value=search_value,
        use_regex=use_regex,
    )

    print(
        f"{with_formatting(file_name, COLOR_CYAN)}, line {line_number}, position {position_in_line}"
    )
    print(SHORT_SEPARATOR)
    print(matched_snippet)
    print(LONG_SEPARATOR)


def print_summary(
    pipeline_slug: str,
    build_number: int,
    job_id: str | None,
    count_artifacts: int,
    count_matches: int,
    ignored_file_names: set[str],
    max_search_results_hit: bool,
) -> None:
    job_execution_info = (
        f"job execution {job_id}" if job_id is not None else "all job executions"
    )
    url_to_build = get_url_to_build(
        pipeline_slug=pipeline_slug, build_number=build_number, job_id=job_id
    )
    search_scope = f"{job_execution_info} in build #{build_number} of pipeline {pipeline_slug} ({url_to_build})"

    if count_artifacts == 0:
        print(f"Found no artifacts for {search_scope}!")
    else:
        suppressed_results_info = (
            f"Showing only the first {count_matches} matches! "
            if max_search_results_hit
            else ""
        )
        ignored_artifacts_info = (
            f"\nOut of {count_artifacts} artifacts, {len(ignored_file_names)} were ignored due to their file extension: {', '.join(ignored_file_names)}."
            if len(ignored_file_names) > 0
            else ""
        )
        print(
            f"{count_matches} match(es) for {search_scope}. "
            f"{suppressed_results_info}"
            f"{ignored_artifacts_info}"
        )
