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
    MAX_VALUE = 3
    OVERSIZE = 4
    NEGATIVE = 5
    DECIMAL = 6
    NON_EMPTY = 7
