# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum


class JoinType(Enum):
    INNER = 1
    LEFT_OUTER = 2


class JoinColumn(Enum):
    SAME = 1
    RANDOM_SAME_TYPE = 2
    RANDOM_ANY = 3


class JoinColumnComparison(Enum):
    EQUAL = 1
    NOT_EQUAL = 2


class JoinSpecification:
    def __init__(
        self,
        join_type: JoinType,
        join_column: JoinColumn,
        join_column_comparison: JoinColumnComparison,
    ):
        self.join_type = join_type
        self.join_column = join_column
        self.join_column_comparison = join_column_comparison
