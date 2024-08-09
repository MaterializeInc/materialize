# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum


class JoinTarget(Enum):
    SAME_DATA_TYPE = 1
    SAME_DATA_TYPE_CATEGORY = 2
    ANY_COLUMN = 3
    RANDOM_COLUMN_IS_NOT_NULL = 4
    BOOLEAN_EXPRESSION = 5


JOIN_TARGET_WEIGHTS = [0.4, 0.2, 0.1, 0.2, 0.1]


class JoinOperator(Enum):
    INNER = "INNER JOIN"
    LEFT_OUTER = "LEFT OUTER JOIN"
    RIGHT_OUTER = "RIGHT OUTER JOIN"
    FULL_OUTER = "FULL OUTER JOIN"

    def to_sql(self) -> str:
        return self.value
