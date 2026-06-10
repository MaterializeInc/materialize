# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.buildkite_insights.annotation_search.buildkite_search_source import (
    ANY_BRANCH_VALUE,
)
from materialize.buildkite_insights.data.build_annotation import BuildAnnotation
from materialize.buildkite_insights.data.build_info import Build
from materialize.buildkite_insights.util.search_utility import (
    highlight_match,
    trim_match,
)
from materialize.terminal import (
    COLOR_CYAN,
    STYLE_BOLD,
    with_formatting,
)

SHORT_SEPARATOR = "----------"
LONG_SEPARATOR = "-------------------------------------------------------------------------------------"


def print_before_search_results() -> None:
    print()
    print(LONG_SEPARATOR)


def print_annotation_match(
    build: Build,
    annotation: BuildAnnotation,
    search_value: str,
    use_regex: bool,
    short_result_presentation: bool,
    one_line_match_presentation: bool,
) -> None:
    print(
        with_formatting(
            f"Match in build #{build.number} (pipeline {build.pipeline} on {build.branch}):",
            STYLE_BOLD,
        )
    )
    print(f"URL: {with_formatting(build.web_url, COLOR_CYAN)}")
    print(f"Date: {with_formatting(str(build.created_at), COLOR_CYAN)}")

    if annotation.title is not None:
        print(f"Annotation: {with_formatting(annotation.title, COLOR_CYAN)}")

    if not short_result_presentation:
        matched_snippet = trim_match(
            match_text=annotation.content,
            search_value=search_value,
            use_regex=use_regex,
            one_line_match_presentation=one_line_match_presentation,
        )
        matched_snippet = highlight_match(
            input=matched_snippet,
            search_value=search_value,
            use_regex=use_regex,
        )

        print(SHORT_SEPARATOR)
        print(matched_snippet)

    print(LONG_SEPARATOR)


def print_summary(
    pipeline_slug: str,
    branch: str | None,
    builds: list[Build],
    count_matches: int,
    max_results: int,
) -> None:
    if len(builds) == 0:
        print("Found no builds!")
    else:
        most_recent_build_number = builds[0].number
        oldest_build_number = builds[-1].number
        suppressed_results_info = (
            f"Showing only the first {max_results} matches! "
            if count_matches > max_results
            else ""
        )
        branch = branch or ANY_BRANCH_VALUE
        print(
            f"{count_matches} match(es) in {len(builds)} searched builds of pipeline '{pipeline_slug}' and branch '{branch}'. "
            f"{suppressed_results_info}"
            f"The most recent considered build was #{most_recent_build_number}, "
            f"the oldest was #{oldest_build_number}."
        )
