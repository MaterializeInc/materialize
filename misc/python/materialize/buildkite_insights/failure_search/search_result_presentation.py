#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any

from materialize.buildkite_insights.failure_search.search_utility import (
    _search_value_to_pattern,
)
from materialize.terminal import (
    COLOR_CYAN,
    COLOR_GREEN,
    STYLE_BOLD,
    with_formatting,
    with_formattings,
)

SHORT_SEPARATOR = "----------"
LONG_SEPARATOR = "-------------------------------------------------------------------------------------"


def highlight_match(input: str, search_value: str, use_regex: bool) -> str:
    case_insensitive_pattern = _search_value_to_pattern(search_value, use_regex)
    match_replacement = with_formattings(r"\1", [COLOR_GREEN, STYLE_BOLD])
    return case_insensitive_pattern.sub(match_replacement, input)


def trim_match(input: str, search_value: str, use_regex: bool) -> str:
    # We do not care about multiple occurrences within an annotation and focus on the first one.

    input = input.strip()
    case_insensitive_pattern = _search_value_to_pattern(search_value, use_regex)

    max_chars_before_match = 300
    max_chars_after_match = 300

    match = case_insensitive_pattern.search(input)
    assert match is not None

    match_begin_index = match.start()
    match_end_index = match.end()

    # identify cut-off point before first match
    if match_begin_index > max_chars_before_match:
        cut_off_index_begin = input.find(
            " ", match_begin_index - max_chars_before_match, match_begin_index
        )

        if cut_off_index_begin == -1:
            cut_off_index_begin = match_begin_index - max_chars_before_match
    else:
        cut_off_index_begin = 0

    # identify cut-off point after first match
    if len(input) > match_end_index + 300:
        cut_off_index_end = input.rfind(
            " ", match_end_index, match_end_index + max_chars_after_match
        )

        if cut_off_index_end == -1:
            cut_off_index_end = match_end_index + max_chars_after_match
    else:
        cut_off_index_end = len(input)

    result = input[cut_off_index_begin:cut_off_index_end]
    result = result.strip()

    if cut_off_index_begin > 0:
        result = f"[...] {result}"

    if cut_off_index_end != len(input):
        result = f"{result} [...]"

    return result


def print_before_search_results() -> None:
    print()
    print(LONG_SEPARATOR)


def print_match(
    build_number: str,
    build_pipeline: str,
    branch: str,
    web_url: str,
    annotation_text: str,
    search_value: str,
    use_regex: bool,
) -> None:
    matched_snippet = trim_match(
        input=annotation_text, search_value=search_value, use_regex=use_regex
    )
    matched_snippet = highlight_match(
        input=matched_snippet,
        search_value=search_value,
        use_regex=use_regex,
    )

    print(
        with_formatting(
            f"Match in build #{build_number} (pipeline {build_pipeline} on {branch}):",
            STYLE_BOLD,
        )
    )
    print(f"URL: {with_formatting(web_url, COLOR_CYAN)}")
    print(SHORT_SEPARATOR)
    print(matched_snippet)
    print(LONG_SEPARATOR)


def print_summary(
    pipeline_slug: str, builds_data: list[Any], count_matches: int, max_results: int
) -> None:
    if len(builds_data) == 0:
        print("Found no builds!")
    else:
        most_recent_build_number = builds_data[0]["number"]
        suppressed_results_info = (
            f"Showing only the first {max_results} matches. "
            if count_matches > max_results
            else ""
        )
        print(
            f"{count_matches} match(es) in {len(builds_data)} searched builds of pipeline '{pipeline_slug}'. "
            f"{suppressed_results_info}"
            f"The most recent considered build was #{most_recent_build_number}."
        )
