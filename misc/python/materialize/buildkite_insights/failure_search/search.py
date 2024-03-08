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
from materialize.terminal import (
    COLOR_CYAN,
    COLOR_GREEN,
    STYLE_BOLD,
    with_formatting,
    with_formattings,
)


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


def highlight_match(annotation_text: str, search_value: str) -> str:
    case_insensitive_pattern = re.compile(re.escape(search_value), re.IGNORECASE)
    highlighted_match = with_formattings(search_value, [COLOR_GREEN, STYLE_BOLD])
    return case_insensitive_pattern.sub(highlighted_match, annotation_text)


def trim_match(annotation_text: str, search_value: str) -> str:
    return annotation_text.strip()


def print_match(
    build_number: str,
    build_pipeline: str,
    web_url: str,
    annotation_text: str,
    search_value: str,
) -> None:
    match_snippet = trim_match(
        annotation_text=annotation_text, search_value=search_value
    )
    match_snippet = highlight_match(
        annotation_text=match_snippet,
        search_value=search_value,
    )

    print(
        with_formatting(
            f"Match in build #{build_number} (pipeline={build_pipeline}):", STYLE_BOLD
        )
    )
    print(f"URL: {with_formatting(web_url, COLOR_CYAN)}")
    print("----------")
    print(match_snippet)
    print(
        "-------------------------------------------------------------------------------------"
    )


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

    print()
    print(
        "-------------------------------------------------------------------------------------"
    )

    count_matches = 0

    for build in builds_data:
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
        count_matches = count_matches + matches_in_build

    if len(builds_data) == 0:
        print("Found no builds!")
    else:
        most_recent_build_number = builds_data[0]["number"]
        print(
            f"{count_matches} match(es) in {len(builds_data)} searched builds of pipeline '{pipeline_slug}'. "
            f"The most recent considered build was #{most_recent_build_number}."
        )


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
