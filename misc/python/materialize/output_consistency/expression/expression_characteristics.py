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

    OVERSIZE = 120
    NEGATIVE = 121
    DECIMAL = 122
    """value is not an integer"""

    DECIMAL_OR_FLOAT_TYPED = 150
    FLOAT_TYPED = 151
    UNSIGNED_TYPED = 152

    TYPE_LARGER_INT4_SIZED = 160
    """INT8 or UINT4 or UINT8"""
    TYPE_INT8_SIZED = 161
    """INT8 or UINT8"""
