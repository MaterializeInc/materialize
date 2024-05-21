# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum


class ExpressionCharacteristics(Enum):
    """Characteristics describing an `Expression`"""

    NULL = 0
    NON_EMPTY = 1
    """not NULL and not ZERO"""

    ZERO = 100
    ONE = 101
    TINY_VALUE = 102
    LARGE_VALUE = 103
    MAX_VALUE = 104
    NAN = 105

    OVERSIZE = 120
    NEGATIVE = 121
    DECIMAL = 122
    """value is not an integer"""

    INTERVAL_WITH_MONTHS = 130
    """time interval containing months or years"""

    TEXT_EMPTY = 140
    TEXT_WITH_SPECIAL_SPACE_CHARS = 141
    """Lines with tabulators, newlines, and further whitespace types"""
    TEXT_WITH_BACKSLASH_CHAR = 142
    TEXT_WITH_SPECIAL_NON_SPACE_CHARS = 143
    TEXT_WITH_ESZETT = 144

    JSON_EMPTY = 150
    JSON_ARRAY = 151

    COLLECTION_EMPTY = 160
    MAP_WITH_DUP_KEYS = 161
    LIST_WITH_DUP_ENTRIES = 162
    ARRAY_WITH_DUP_ENTRIES = 163
