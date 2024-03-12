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
    FETCH_MODE_AUTO,
    FETCH_MODE_NO,
    FETCH_MODE_YES,
)
from materialize.buildkite_insights.failure_search.search_result_presentation import (
    print_before_search_results,
    print_match,
    print_summary,
)


def search_build(build: Any, search_value: str, fetch_mode: str) -> int:
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
        build_number, build_pipeline, web_url, annotations, search_value
    )

    return matches_in_build


def search_annotations(
    build_number: str,
    build_pipeline: str,
    web_url: str,
    annotations: list[Any],
    search_value: str,
) -> int:
    count_matches = 0

    for annotation in annotations:
        annotation_html = annotation["body_html"]
        annotation_text = clean_annotation_text(annotation_html)

        if matches(annotation_text=annotation_text, search_value=search_value):
            print_match(
                build_number, build_pipeline, web_url, annotation_text, search_value
            )
            count_matches = count_matches + 1

    return count_matches


def clean_annotation_text(annotation_html: str) -> str:
    return re.sub(r"<[^>]+>", "", annotation_html)


def matches(annotation_text: str, search_value: str) -> bool:
    return search_value.lower() in annotation_text.lower()


def main(
    pipeline_slug: str,
    fetch_mode: str,
    max_build_fetches: int,
    only_failed_builds: bool,
    search_value: str,
) -> None:
    assert len(search_value) > 0

    if only_failed_builds:
        build_states = BUILDKITE_FAILED_BUILD_STATES
    else:
        build_states = []

    if pipeline_slug == "*":
        builds_data = builds_cache.get_or_query_builds_for_all_pipelines(
            fetch_mode,
            max_build_fetches,
            build_states=build_states,
        )
    else:
        builds_data = builds_cache.get_or_query_builds(
            pipeline_slug,
            fetch_mode,
            max_build_fetches,
            branch=None,
            build_states=build_states,
        )

    print_before_search_results()

    count_matches = 0

    for build in builds_data:
        matches_in_build = search_build(build, search_value, fetch_mode=fetch_mode)
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
        "--fetch",
        choices=[FETCH_MODE_AUTO, FETCH_MODE_NO, FETCH_MODE_YES],
        default=FETCH_MODE_AUTO,
        type=str,
        help="Whether to fetch new data from Buildkite or reuse previously fetched, matching data.",
    )
    parser.add_argument("--max-build-fetches", default=2, type=int)
    parser.add_argument(
        "--only-failed-builds",
        default=True,
        action="store_true",
    )
    parser.add_argument("--value", required=True, type=str)
    args = parser.parse_args()

    main(
        args.pipeline,
        args.fetch,
        args.max_build_fetches,
        args.only_failed_builds,
        args.value,
    )
