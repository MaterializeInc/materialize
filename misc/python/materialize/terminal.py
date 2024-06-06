# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Terminal utilities."""


COLOR_GREEN = "\033[92m"
COLOR_RED = "\033[91m"
COLOR_BLUE = "\033[34m"
COLOR_CYAN = "\033[36m"
COLOR_OK = COLOR_GREEN
COLOR_ERROR = COLOR_RED
COLOR_GOOD = COLOR_GREEN
COLOR_BAD = COLOR_RED
STYLE_BOLD = "\033[1m"
_END_FORMATTING = "\033[0m"


def with_formatting(text: str, formatting: str) -> str:
    return with_formattings(text, [formatting])


def with_conditional_formatting(text: str, formatting: str, condition: bool) -> str:
    if condition:
        return with_formatting(text, formatting)

    return text


def with_formattings(text: str, formattings: list[str]) -> str:
    formatting = "".join(formattings)
    return f"{formatting}{text}{_END_FORMATTING}"
