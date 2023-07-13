# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Generic, Optional, Set, TypeVar

T = TypeVar("T")


class SelectionByKey(Generic[T]):
    def __init__(self, keys: Optional[Set[T]] = None):
        self.keys = keys

    def includes_all(self) -> bool:
        return self.keys is None

    def is_included(self, key: T) -> bool:
        if self.keys is None:
            return True

        return key in self.keys

    def __str__(self) -> str:
        filter_string = ""
        if self.keys is not None:
            filter_string = ", ".join(str(key) for key in self.keys)

        return f"{type(self).__name__}({filter_string})"


class DataRowSelection(SelectionByKey[int]):
    """A selection of table rows, useful when collecting involved characteristics in vertical storage layout"""

    def __init__(self, row_indices: Optional[Set[int]] = None):
        """
        :param row_indices: index of selected rows; all rows if not specified
        """
        super().__init__(row_indices)


class QueryColumnByIndexSelection(SelectionByKey[int]):
    def __init__(self, column_indices: Optional[Set[int]] = None):
        """
        :param column_indices: name of selected columns; all columns if not specified
        """
        super().__init__(column_indices)


class TableColumnByNameSelection(SelectionByKey[str]):
    def __init__(self, column_names: Optional[Set[str]] = None):
        """
        :param column_names: name of selected columns; all columns if not specified
        """
        super().__init__(column_names)


ALL_ROWS_SELECTION = DataRowSelection()
ALL_QUERY_COLUMNS_BY_INDEX_SELECTION = QueryColumnByIndexSelection()
ALL_TABLE_COLUMNS_BY_NAME_SELECTION = TableColumnByNameSelection()
