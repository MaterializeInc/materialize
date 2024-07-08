#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import re
import time

from materialize.buildkite_insights.annotation_search.annotation_search_presentation import (
    print_annotation_match,
    print_before_search_results,
)
from materialize.buildkite_insights.data.build_annotation import BuildAnnotation
from materialize.buildkite_insights.data.build_info import Build
from materialize.test_analytics.search.test_analytics_search_source import (
    ANY_BRANCH_VALUE,
    ANY_PIPELINE_VALUE,
    TestAnalyticsDataSource,
)


def start_search(
    search_source: TestAnalyticsDataSource,
    pipeline_slug: str,
    branch: str | None,
    build_step_keys: list[str],
    only_failed_builds: bool,
    not_newer_than_build_number: int | None,
    like_pattern: str,
    max_results: int,
    short_result_presentation: bool,
    one_line_match_presentation: bool,
) -> None:
    assert len(like_pattern) > 0, "pattern must not be empty"

    if branch == ANY_BRANCH_VALUE:
        branch = None

    if not like_pattern.startswith("%"):
        like_pattern = f"%{like_pattern}"
    if not like_pattern.endswith("%"):
        like_pattern = f"{like_pattern}%"

    start_time = time.time()
    matches: list[tuple[Build, BuildAnnotation]] = search_source.search_annotations(
        pipeline=pipeline_slug,
        branch=branch,
        build_step_keys=build_step_keys,
        not_newer_than_build_number=not_newer_than_build_number,
        like_pattern=like_pattern,
        max_entries=max_results + 1,
        only_failed_builds=only_failed_builds,
    )
    end_time = time.time()
    duration_in_sec = round(end_time - start_time, 2)

    more_results_exist = len(matches) > max_results

    if more_results_exist:
        matches = matches[:max_results]

    search_value = _like_pattern_to_regex(like_pattern)

    print("Searching test-analytics database...")

    print_before_search_results()

    if len(matches) == 0:
        print("No matches found.")
        return

    for build, build_annotation in matches:
        print_annotation_match(
            build=build,
            annotation=build_annotation,
            search_value=search_value,
            use_regex=True,
            short_result_presentation=short_result_presentation,
            one_line_match_presentation=one_line_match_presentation,
        )

    _print_summary(pipeline_slug, matches, duration_in_sec, more_results_exist)


def _print_summary(
    pipeline: str,
    matches: list[tuple[Build, BuildAnnotation]],
    duration_in_sec: float,
    more_results_exist: bool,
) -> None:
    newest_build_number_with_match = matches[0][0].number
    oldest_build_number_with_match = matches[-1][0].number
    search_scope = (
        f"builds #{oldest_build_number_with_match} to #{newest_build_number_with_match} of pipeline {pipeline }"
        if pipeline != ANY_PIPELINE_VALUE
        else "all pipelines"
    )

    print(
        f"Found {len(matches)} matches in {search_scope}. Search took {duration_in_sec}s.\n"
        "More matches exist in earlier builds."
        if more_results_exist
        else ""
    )


def _like_pattern_to_regex(like_pattern: str) -> str:
    like_pattern = like_pattern.strip("%")
    like_pattern = re.escape(like_pattern)
    return like_pattern.replace("%", ".*")
