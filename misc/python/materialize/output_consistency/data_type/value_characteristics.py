# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum


class ValueCharacteristics(Enum):
    NULL = 0
    ZERO = 1
    # not (NULL or ZERO)
    NON_EMPTY = 2

    ONE = 100
    TINY_VALUE = 101
    LARGE_VALUE = 102
    MAX_VALUE = 103
    OVERSIZE = 104
    NEGATIVE = 105
    # value is not an integer
    DECIMAL = 106

    DECIMAL_OR_FLOAT_TYPED = 150
    LARGER_THAN_INT4_TYPED = 151
    FLOAT_TYPED = 152
