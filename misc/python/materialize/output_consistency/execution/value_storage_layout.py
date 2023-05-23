# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum


class ValueStorageLayout(Enum):
    HORIZONTAL = 1
    """Column per value: Each value has its own column, only one row exists"""
    VERTICAL = 2
    """Column per type: Each data type has its own column, rows contain the values"""


VERTICAL_LAYOUT_ROW_INDEX_COL_NAME = "row_index"
