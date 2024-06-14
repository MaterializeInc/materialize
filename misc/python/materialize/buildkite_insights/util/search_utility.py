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
    match_text: str,
    search_value: str,
    use_regex: bool,
    one_line_match_presentation: bool,
    max_chars_before_match: int = 300,
    max_chars_after_match: int = 300,
    search_offset: int = 0,
) -> str:
    match_text = match_text.strip()
    case_insensitive_pattern = _search_value_to_pattern(search_value, use_regex)

    match = case_insensitive_pattern.search(match_text, pos=search_offset)
    assert match is not None

    match_begin_index = match.start()
    match_end_index = match.end()

    if one_line_match_presentation:
        match_text, (match_begin_index, match_end_index) = _trim_match_to_one_line(
            match_text, match_begin_index, match_end_index
        )

    match_text = _trim_match_to_max_length(
        match_text,
        match_begin_index,
        match_end_index,
        max_chars_after_match,
        max_chars_before_match,
    )

    return match_text


def _trim_match_to_one_line(
    input: str, match_begin_index: int, match_end_index: int
) -> tuple[str, tuple[int, int]]:
    """
    :return: trimmed text, new match_begin_index, new match_end_index
    """
    cut_off_index_begin = input.rfind("\n", 0, match_begin_index)
    if cut_off_index_begin == -1:
        cut_off_index_begin = 0
    cut_off_index_end = input.find("\n", match_end_index)
    if cut_off_index_end == -1:
        cut_off_index_end = len(input)
    input = input[cut_off_index_begin:cut_off_index_end]
    return input, (
        match_begin_index - cut_off_index_begin,
        match_end_index - cut_off_index_begin,
    )


def _trim_match_to_max_length(
    input: str,
    match_begin_index: int,
    match_end_index: int,
    max_chars_after_match: int,
    max_chars_before_match: int,
) -> str:
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


def determine_line_number(input: str, position: int) -> int:
    return 1 + input[:position].count("\n")


def determine_position_in_line(input: str, position: int) -> int:
    position_line_start = input[:position].rfind("\n")
    return position - position_line_start
