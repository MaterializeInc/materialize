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


def _search_value_to_pattern(search_value: str, use_regex: bool) -> re.Pattern[str]:
    regex_pattern = _search_value_to_regex(search_value, use_regex)
    return re.compile(f"({regex_pattern})", re.IGNORECASE | re.DOTALL)


def _search_value_to_regex(search_value: str, use_regex: bool) -> str:
    if use_regex:
        return search_value

    return re.escape(search_value)
