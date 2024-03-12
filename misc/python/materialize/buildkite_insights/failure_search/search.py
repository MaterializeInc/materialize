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

from materialize.buildkite_insights.buildkite_api.buildkite_config import MZ_PIPELINES
from materialize.buildkite_insights.buildkite_api.buildkite_constants import (
    BUILDKITE_COMPLETED_BUILD_STATES,
    BUILDKITE_FAILED_BUILD_STATES,
)
from materialize.buildkite_insights.cache import annotations_cache, builds_cache
from materialize.buildkite_insights.cache.cache_constants import (
    FETCH_MODE_CHOICES,
    FetchMode,
)
from materialize.buildkite_insights.failure_search.search_result_presentation import (
    print_before_search_results,
    print_match,
    print_summary,
)
from materialize.buildkite_insights.failure_search.search_utility import (
    _search_value_to_pattern,
)


def search_build(
    build: Any, search_value: str, use_regex: bool, fetch_mode: FetchMode
) -> int:
    build_number = build["number"]
    build_pipeline = build["pipeline"]["slug"]
    build_state = build["state"]
    web_url = build["web_url"]

    is_completed_build_state = build_state in BUILDKITE_COMPLETED_BUILD_STATES

    annotations = annotations_cache.get_or_query_annotations(
        fetch_mode=fetch_mode,
        pipeline_slug=build_pipeline,
        build_number=build_number,
        add_to_cache_if_not_present=is_completed_build_state,
    )

    matches_in_build = search_annotations(
        build_number,
        build_pipeline,
        web_url,
        annotations,
        search_value=search_value,
        use_regex=use_regex,
    )

    return matches_in_build


def search_annotations(
    build_number: str,
    build_pipeline: str,
    web_url: str,
    annotations: list[Any],
    search_value: str,
    use_regex: bool,
) -> int:
    count_matches = 0

    search_pattern = _search_value_to_pattern(search_value, use_regex)
    for annotation in annotations:
        annotation_html = annotation["body_html"]
        annotation_text = clean_annotation_text(annotation_html)

        if search_pattern.search(annotation_text) is not None:
            print_match(
                build_number,
                build_pipeline,
                web_url,
                annotation_text,
                search_value=search_value,
                use_regex=use_regex,
            )
            count_matches = count_matches + 1

    return count_matches


def clean_annotation_text(annotation_html: str) -> str:
    return re.sub(r"<[^>]+>", "", annotation_html)


def main(
    pipeline_slug: str,
    fetch_builds_mode: FetchMode,
    fetch_annotations_mode: FetchMode,
    max_build_fetches: int,
    only_failed_builds: bool,
    search_value: str,
    use_regex: bool,
) -> None:
    assert len(search_value) > 0

    if only_failed_builds:
        build_states = BUILDKITE_FAILED_BUILD_STATES
    else:
        build_states = []

    if pipeline_slug == "*":
        builds_data = builds_cache.get_or_query_builds_for_all_pipelines(
            fetch_builds_mode,
            max_build_fetches,
            build_states=build_states,
        )
    else:
        builds_data = builds_cache.get_or_query_builds(
            pipeline_slug,
            fetch_builds_mode,
            max_build_fetches,
            branch=None,
            build_states=build_states,
        )

    print_before_search_results()

    count_matches = 0

    for build in builds_data:
        matches_in_build = search_build(
            build, search_value, use_regex=use_regex, fetch_mode=fetch_annotations_mode
        )
        count_matches = count_matches + matches_in_build

    print_summary(pipeline_slug, builds_data, count_matches)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="buildkite-failure-search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--pipeline",
        choices=MZ_PIPELINES,
        type=str,
        required=True,
        help="Use * for all pipelines",
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
    parser.add_argument(
        "--only-failed-builds",
        default=True,
        action="store_true",
    )
    parser.add_argument("--value", required=True, type=str)
    parser.add_argument(
        "--use-regex",
        action="store_true",
    )
    args = parser.parse_args()

    main(
        args.pipeline,
        args.fetch_builds,
        args.fetch_annotations,
        args.max_build_fetches,
        args.only_failed_builds,
        args.value,
        args.use_regex,
    )
