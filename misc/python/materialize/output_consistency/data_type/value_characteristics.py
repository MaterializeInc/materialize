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
    ONE = 2
    LARGE_VALUE = 3
    MAX_VALUE = 4
    OVERSIZE = 5
    NEGATIVE = 6
    DECIMAL = 7
    NON_EMPTY = 8
