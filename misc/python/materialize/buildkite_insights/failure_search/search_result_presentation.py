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

from materialize.terminal import (
    COLOR_CYAN,
    COLOR_GREEN,
    STYLE_BOLD,
    with_formatting,
    with_formattings,
)

SHORT_SEPARATOR = "----------"
LONG_SEPARATOR = "-------------------------------------------------------------------------------------"


def highlight_match(annotation_text: str, search_value: str) -> str:
    case_insensitive_pattern = re.compile(re.escape(search_value), re.IGNORECASE)
    highlighted_match = with_formattings(search_value, [COLOR_GREEN, STYLE_BOLD])
    return case_insensitive_pattern.sub(highlighted_match, annotation_text)


def trim_match(annotation_text: str, search_value: str) -> str:
    # We do not care about multiple occurrences within an annotation and focus on the first one.

    original_annotation_text = annotation_text.strip()
    annotation_text = original_annotation_text.lower()
    search_value = search_value.lower()

    max_chars_before_match = 300
    max_chars_after_match = 300

    match_begin_index = annotation_text.index(search_value)
    match_end_index = match_begin_index + len(search_value)

    # identify cut-off point before first match
    if match_begin_index > max_chars_before_match:
        cut_off_index_begin = annotation_text.find(
            " ", match_begin_index - max_chars_before_match
        )

        if cut_off_index_begin == -1:
            cut_off_index_begin = match_begin_index - max_chars_before_match
    else:
        cut_off_index_begin = 0

    # identify cut-off point after first match
    if len(annotation_text) - match_end_index > 300:
        cut_off_index_end = annotation_text.rfind(
            " ", match_end_index, match_end_index + max_chars_after_match
        )

        if cut_off_index_end == -1:
            cut_off_index_end = match_end_index + max_chars_after_match
    else:
        cut_off_index_end = len(annotation_text)

    cut_annotation_text = original_annotation_text[
        cut_off_index_begin:cut_off_index_end
    ]
    cut_annotation_text = cut_annotation_text.strip()

    if cut_off_index_begin > 0:
        cut_annotation_text = f"[...] {cut_annotation_text}"

    if cut_off_index_end != len(original_annotation_text):
        cut_annotation_text = f"{cut_annotation_text} [...]"

    return cut_annotation_text


def print_before_search_results() -> None:
    print()
    print(LONG_SEPARATOR)


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
    print(SHORT_SEPARATOR)
    print(match_snippet)
    print(LONG_SEPARATOR)


def print_summary(
    pipeline_slug: str, builds_data: list[Any], count_matches: int
) -> None:
    if len(builds_data) == 0:
        print("Found no builds!")
    else:
        most_recent_build_number = builds_data[0]["number"]
        print(
            f"{count_matches} match(es) in {len(builds_data)} searched builds of pipeline '{pipeline_slug}'. "
            f"The most recent considered build was #{most_recent_build_number}."
        )
