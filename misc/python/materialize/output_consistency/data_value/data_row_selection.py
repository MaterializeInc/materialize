# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional, Set


class DataRowSelection:
    """A selection of table rows, useful when collecting involved characteristics in vertical storage layout"""

    def __init__(self, row_indices: Optional[Set[int]] = None):
        """
        :param row_indices: index of selected rows; all rows if not specified
        """
        self.row_indices = row_indices

    def includes_all(self) -> bool:
        return self.row_indices is None

    def is_included_row(self, row_index: int) -> bool:
        if self.row_indices is None:
            return True

        return row_index in self.row_indices
