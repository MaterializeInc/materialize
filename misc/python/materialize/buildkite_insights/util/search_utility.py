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

from materialize.terminal import COLOR_GREEN, STYLE_BOLD, with_formattings


def _search_value_to_pattern(search_value: str, use_regex: bool) -> re.Pattern[str]:
    regex_pattern = _search_value_to_regex(search_value, use_regex)
    return re.compile(f"({regex_pattern})", re.IGNORECASE | re.DOTALL)


def _search_value_to_regex(search_value: str, use_regex: bool) -> str:
    if use_regex:
        return search_value

    return re.escape(search_value)


def highlight_match(
    input: str,
    search_value: str,
    use_regex: bool,
    style: list[str] = [COLOR_GREEN, STYLE_BOLD],
) -> str:
    case_insensitive_pattern = _search_value_to_pattern(search_value, use_regex)
    match_replacement = with_formattings(r"\1", style)
    return case_insensitive_pattern.sub(match_replacement, input)


def trim_match(
    input: str,
    search_value: str,
    use_regex: bool,
    max_chars_before_match: int = 300,
    max_chars_after_match: int = 300,
    search_offset: int = 0,
) -> str:
    input = input.strip()
    case_insensitive_pattern = _search_value_to_pattern(search_value, use_regex)

    match = case_insensitive_pattern.search(input, pos=search_offset)
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
    if len(input) > match_end_index + max_chars_after_match:
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
