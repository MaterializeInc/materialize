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
import re
from typing import Any

from materialize.buildkite_insights.annotation_search.search_result_presentation import (
    print_before_search_results,
    print_match,
    print_summary,
)
from materialize.buildkite_insights.annotation_search.search_utility import (
    _search_value_to_pattern,
)
from materialize.buildkite_insights.buildkite_api.buildkite_config import MZ_PIPELINES
from materialize.buildkite_insights.buildkite_api.buildkite_constants import (
    BUILDKITE_COMPLETED_BUILD_STATES,
    BUILDKITE_FAILED_BUILD_STATES,
    BUILDKITE_RELEVANT_FAILED_BUILD_STEP_STATES,
)
from materialize.buildkite_insights.cache import annotations_cache, builds_cache
from materialize.buildkite_insights.cache.cache_constants import (
    FETCH_MODE_CHOICES,
    FetchMode,
)
from materialize.buildkite_insights.steps.build_step import (
    BuildStepMatcher,
    extract_build_step_outcomes,
)


def search_build(
    build: Any,
    search_value: str,
    use_regex: bool,
    fetch_mode: FetchMode,
    max_entries_to_print: int,
) -> int:
    assert max_entries_to_print >= 0

    build_number = build["number"]
    build_pipeline = build["pipeline"]["slug"]
    build_state = build["state"]
    branch = build["branch"]
    web_url = build["web_url"]

    is_completed_build_state = build_state in BUILDKITE_COMPLETED_BUILD_STATES

    annotations = annotations_cache.get_or_query_annotations(
        fetch_mode=fetch_mode,
        pipeline_slug=build_pipeline,
        build_number=build_number,
        add_to_cache_if_not_present=is_completed_build_state,
    )

    matched_annotations = search_annotations(
        annotations,
        search_value=search_value,
        use_regex=use_regex,
    )

    for i, annotation in enumerate(matched_annotations):
        if i >= max_entries_to_print:
            break

        print_match(
            build_number,
            build_pipeline,
            branch,
            web_url,
            annotation,
            search_value=search_value,
            use_regex=use_regex,
        )

    return len(matched_annotations)


def search_annotations(
    annotations: list[Any],
    search_value: str,
    use_regex: bool,
) -> list[str]:
    matched_annotations = []

    search_pattern = _search_value_to_pattern(search_value, use_regex)
    for annotation in annotations:
        annotation_html = annotation["body_html"]
        annotation_text = clean_annotation_text(annotation_html)

        if search_pattern.search(annotation_text) is not None:
            matched_annotations.append(annotation_text)

    return matched_annotations


def clean_annotation_text(annotation_html: str) -> str:
    return re.sub(r"<[^>]+>", "", annotation_html)


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


def main(
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
) -> None:
    assert len(pattern) > 0, "pattern must not be empty"

    if only_failed_builds:
        build_states = BUILDKITE_FAILED_BUILD_STATES
    else:
        build_states = []

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

        matches_in_build = search_build(
            build,
            pattern,
            use_regex=use_regex,
            fetch_mode=fetch_annotations_mode,
            max_entries_to_print=max_entries_to_print,
        )

        if only_one_result_per_build:
            matches_in_build = min(1, matches_in_build)

        count_matches = count_matches + matches_in_build

    print_summary(pipeline_slug, builds_data, count_matches, max_results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="buildkite-annotation-search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("pattern", nargs="*", type=str)

    parser.add_argument(
        "--pipeline",
        choices=MZ_PIPELINES,
        type=str,
        required=True,
        help="Use * for all pipelines",
    )
    parser.add_argument(
        "--branch",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--fetch-builds",
        type=lambda mode: FetchMode[mode],
        choices=FETCH_MODE_CHOICES,
        default=FetchMode.AUTO,
        help="Whether to fetch fresh builds from Buildkite.",
    )
    parser.add_argument(
        "--fetch-annotations",
        type=lambda mode: FetchMode[mode.upper()],
        choices=FETCH_MODE_CHOICES,
        default=FetchMode.AUTO,
        help="Whether to fetch fresh annotations from Buildkite.",
    )
    parser.add_argument("--max-build-fetches", default=2, type=int)
    parser.add_argument("--first-build-page-to-fetch", default=1, type=int)
    parser.add_argument("--max-results", default=50, type=int)
    parser.add_argument(
        "--only-one-result-per-build",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--only-failed-builds",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--only-failed-build-step-key", action="append", default=[], type=str
    )
    parser.add_argument(
        "--use-regex",
        action="store_true",
    )
    args = parser.parse_args()

    assert len(args.pattern) == 1, "Exactly one search pattern must be provided"
    pattern = args.pattern[0]

    main(
        args.pipeline,
        args.branch,
        args.fetch_builds,
        args.fetch_annotations,
        args.max_build_fetches,
        args.first_build_page_to_fetch,
        args.max_results,
        args.only_one_result_per_build,
        args.only_failed_builds,
        args.only_failed_build_step_key,
        pattern,
        args.use_regex,
    )
