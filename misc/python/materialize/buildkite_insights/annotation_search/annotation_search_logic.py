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
from typing import Any

from materialize.buildkite_insights.annotation_search.annotation_match import (
    AnnotationMatch,
)
from materialize.buildkite_insights.annotation_search.annotation_search_presentation import (
    print_annotation_match,
    print_before_search_results,
    print_summary,
)
from materialize.buildkite_insights.buildkite_api.buildkite_constants import (
    BUILDKITE_COMPLETED_BUILD_STATES,
    BUILDKITE_FAILED_BUILD_STATES,
    BUILDKITE_RELEVANT_FAILED_BUILD_STEP_STATES,
)
from materialize.buildkite_insights.buildkite_api.generic_api import RateLimitExceeded
from materialize.buildkite_insights.cache import annotations_cache, builds_cache
from materialize.buildkite_insights.cache.cache_constants import (
    FetchMode,
)
from materialize.buildkite_insights.steps.build_step import (
    BuildStepMatcher,
    extract_build_step_outcomes,
)
from materialize.buildkite_insights.util.search_utility import (
    _search_value_to_pattern,
)


def search_build(
    build: Any,
    search_value: str,
    use_regex: bool,
    fetch_mode: FetchMode,
    max_entries_to_print: int,
    short_result_presentation: bool,
    one_line_match_presentation: bool,
    verbose: bool = False,
) -> int:
    assert max_entries_to_print >= 0

    build_number = build["number"]
    build_pipeline = build["pipeline"]["slug"]
    build_state = build["state"]
    branch = build["branch"]
    web_url = build["web_url"]

    is_completed_build_state = build_state in BUILDKITE_COMPLETED_BUILD_STATES

    if verbose:
        print(f"Searching build #{build_number}...")

    annotations = annotations_cache.get_or_query_annotations(
        fetch_mode=fetch_mode,
        pipeline_slug=build_pipeline,
        build_number=build_number,
        add_to_cache_if_not_present=is_completed_build_state,
        quiet_mode=not verbose,
    )

    matched_annotations = search_annotations(
        annotations,
        search_value=search_value,
        use_regex=use_regex,
    )

    for i, annotation in enumerate(matched_annotations):
        if i >= max_entries_to_print:
            break

        print_annotation_match(
            build_number,
            build_pipeline,
            branch,
            web_url,
            annotation,
            search_value=search_value,
            use_regex=use_regex,
            short_result_presentation=short_result_presentation,
            one_line_match_presentation=one_line_match_presentation,
        )

    return len(matched_annotations)


def search_annotations(
    annotations: list[Any],
    search_value: str,
    use_regex: bool,
) -> list[AnnotationMatch]:
    matched_annotations = []

    search_pattern = _search_value_to_pattern(search_value, use_regex)
    for annotation in annotations:
        annotation_html = annotation["body_html"]
        annotation_text = clean_annotation_text(annotation_html)

        if search_pattern.search(annotation_text) is not None:
            annotation_title = try_extracting_title_from_annotation_html(
                annotation_html
            )
            matched_annotations.append(
                AnnotationMatch(annotation_title, annotation_text)
            )

    return matched_annotations


def clean_annotation_text(annotation_html: str) -> str:
    return re.sub(r"<[^>]+>", "", annotation_html)


def try_extracting_title_from_annotation_html(annotation_html: str) -> str | None:
    # match <p>...</p> header
    header_paragraph_match = re.search("<p>(.*?)</p>", annotation_html)

    if header_paragraph_match is None:
        return None

    header_paragraph = header_paragraph_match.group(1)

    build_step_name_match = re.search(
        "<a href=.*?>(.*?)</a> (failed|succeeded)", header_paragraph
    )

    if build_step_name_match:
        return build_step_name_match.group(1)
    else:
        return clean_annotation_text(header_paragraph)


def filter_builds(
    builds_data: list[Any],
    only_failed_build_step_keys: list[str],
) -> list[Any]:
    if len(only_failed_build_step_keys) == 0:
        return builds_data

    failed_build_step_matcher = [
        BuildStepMatcher(build_step_key, None)
        for build_step_key in only_failed_build_step_keys
    ]

    step_outcomes = extract_build_step_outcomes(
        builds_data=builds_data,
        selected_build_steps=failed_build_step_matcher,
        build_step_states=BUILDKITE_RELEVANT_FAILED_BUILD_STEP_STATES,
    )

    builds_containing_failed_step_keys = {
        outcome.build_number for outcome in step_outcomes
    }

    filtered_builds = [
        build
        for build in builds_data
        if int(build["number"]) in builds_containing_failed_step_keys
    ]
    return filtered_builds


def start_search(
    pipeline_slug: str,
    branch: str,
    fetch_builds_mode: FetchMode,
    fetch_annotations_mode: FetchMode,
    max_build_fetches: int,
    first_build_page_to_fetch: int,
    max_results: int,
    only_one_result_per_build: bool,
    only_failed_builds: bool,
    only_failed_build_step_keys: list[str],
    pattern: str,
    use_regex: bool,
    short_result_presentation: bool,
    one_line_match_presentation: bool,
    verbose: bool,
) -> None:
    assert len(pattern) > 0, "pattern must not be empty"

    if only_failed_builds:
        build_states = BUILDKITE_FAILED_BUILD_STATES
    else:
        build_states = []

    # do not try to continue with incomplete data in case of an exceeded rate limit because fetching the annotations
    # will anyway most likely fail
    if pipeline_slug == "*":
        builds_data = builds_cache.get_or_query_builds_for_all_pipelines(
            fetch_builds_mode,
            max_build_fetches,
            branch=branch,
            build_states=build_states,
            first_page=first_build_page_to_fetch,
        )
    else:
        builds_data = builds_cache.get_or_query_builds(
            pipeline_slug,
            fetch_builds_mode,
            max_build_fetches,
            branch=branch,
            build_states=build_states,
            first_page=first_build_page_to_fetch,
        )

    builds_data = filter_builds(builds_data, only_failed_build_step_keys)

    print_before_search_results()

    count_matches = 0

    for build in builds_data:
        max_entries_to_print = max(0, max_results - count_matches)
        if max_entries_to_print == 0:
            break

        if only_one_result_per_build:
            max_entries_to_print = 1

        try:
            matches_in_build = search_build(
                build,
                pattern,
                use_regex=use_regex,
                fetch_mode=fetch_annotations_mode,
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

    print_summary(pipeline_slug, builds_data, count_matches, max_results)
