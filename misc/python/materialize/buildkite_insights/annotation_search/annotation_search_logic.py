#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.buildkite_insights.annotation_search.annotation_search_presentation import (
    print_annotation_match,
    print_before_search_results,
    print_summary,
)
from materialize.buildkite_insights.annotation_search.buildkite_search_source import (
    ANY_BRANCH_VALUE,
    BuildkiteDataSource,
)
from materialize.buildkite_insights.buildkite_api.generic_api import RateLimitExceeded
from materialize.buildkite_insights.data.build_annotation import BuildAnnotation
from materialize.buildkite_insights.data.build_info import Build
from materialize.buildkite_insights.util.search_utility import (
    _search_value_to_pattern,
)


def search_build(
    build: Build,
    search_source: BuildkiteDataSource,
    search_value: str,
    use_regex: bool,
    max_entries_to_print: int,
    short_result_presentation: bool,
    one_line_match_presentation: bool,
    verbose: bool = False,
) -> int:
    assert max_entries_to_print >= 0

    if verbose:
        print(f"Searching build #{build.number}...")

    annotations = search_source.fetch_annotations(build)

    matched_annotations = search_annotations(
        annotations,
        search_value=search_value,
        use_regex=use_regex,
    )

    for i, annotation in enumerate(matched_annotations):
        if i >= max_entries_to_print:
            break

        print_annotation_match(
            build,
            annotation,
            search_value=search_value,
            use_regex=use_regex,
            short_result_presentation=short_result_presentation,
            one_line_match_presentation=one_line_match_presentation,
        )

    return len(matched_annotations)


def search_annotations(
    annotations: list[BuildAnnotation],
    search_value: str,
    use_regex: bool,
) -> list[BuildAnnotation]:
    matched_annotations = []

    search_pattern = _search_value_to_pattern(search_value, use_regex)
    for annotation in annotations:
        if search_pattern.search(annotation.content) is not None:
            matched_annotations.append(
                BuildAnnotation(title=annotation.title, content=annotation.content)
            )

    return matched_annotations


def start_search(
    pipeline_slug: str,
    branch: str | None,
    search_source: BuildkiteDataSource,
    max_results: int,
    only_one_result_per_build: bool,
    pattern: str,
    use_regex: bool,
    short_result_presentation: bool,
    one_line_match_presentation: bool,
    verbose: bool,
) -> None:
    assert len(pattern) > 0, "pattern must not be empty"

    if branch == ANY_BRANCH_VALUE:
        branch = None

    builds = search_source.fetch_builds(pipeline=pipeline_slug, branch=branch)

    print_before_search_results()

    count_matches = 0

    for build in builds:
        max_entries_to_print = max(0, max_results - count_matches)
        if max_entries_to_print == 0:
            break

        if only_one_result_per_build:
            max_entries_to_print = 1

        try:
            matches_in_build = search_build(
                build,
                search_source,
                pattern,
                use_regex=use_regex,
                max_entries_to_print=max_entries_to_print,
                short_result_presentation=short_result_presentation,
                one_line_match_presentation=one_line_match_presentation,
                verbose=verbose,
            )
        except RateLimitExceeded:
            print("Aborting due to exceeded rate limit!")
            return

        if only_one_result_per_build:
            matches_in_build = min(1, matches_in_build)

        count_matches = count_matches + matches_in_build

    print_summary(pipeline_slug, branch, builds, count_matches, max_results)
