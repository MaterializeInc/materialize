# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Generic, TypeVar

from materialize.output_consistency.data_value.source_column_identifier import (
    SourceColumnIdentifier,
)
from materialize.output_consistency.query.data_source import DataSource

T = TypeVar("T")


class SelectionByKey(Generic[T]):
    def __init__(self, keys: set[T] | None = None):
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


class DataRowSelection:
    """A selection of table rows, useful when collecting involved characteristics in vertical storage layout"""

    def __init__(self):
        self.row_indices_per_data_source: dict[DataSource, set[int]] = dict()

    def includes_all_of_all_sources(self) -> bool:
        return len(self.row_indices_per_data_source) == 0

    def has_selection(self) -> bool:
        return not self.includes_all_of_all_sources()

    def set_row_indices(self, data_source: DataSource, row_indices: set[int]):
        self.row_indices_per_data_source[data_source] = row_indices

    def get_row_indices(self, data_source: DataSource) -> set[int]:
        assert not self.includes_all_of_source(data_source)
        return self.row_indices_per_data_source[data_source]

    def includes_all_of_source(self, data_source: DataSource) -> bool:
        return data_source not in self.row_indices_per_data_source.keys()

    def is_included_in_source(self, data_source: DataSource, index: int) -> bool:
        if self.includes_all_of_source(data_source):
            return True

        return index in self.get_row_indices(data_source)


class QueryColumnByIndexSelection(SelectionByKey[int]):
    def __init__(self, column_indices: set[int] | None = None):
        """
        :param column_indices: name of selected columns; all columns if not specified
        """
        super().__init__(column_indices)


class TableColumnByNameSelection(SelectionByKey[SourceColumnIdentifier]):
    def __init__(self, column_identifiers: set[SourceColumnIdentifier] | None = None):
        """
        :param column_identifiers: identifiers of selected columns; all columns if not specified
        """
        super().__init__(column_identifiers)

    def requires_data_source(self, data_source: DataSource) -> bool:
        if self.includes_all():
            return True

        assert self.keys is not None
        for column_identifier in self.keys:
            if data_source.alias() == column_identifier.data_source_alias:
                return True

        return False


ALL_ROWS_SELECTION = DataRowSelection()
ALL_QUERY_COLUMNS_BY_INDEX_SELECTION = QueryColumnByIndexSelection()
ALL_TABLE_COLUMNS_BY_NAME_SELECTION = TableColumnByNameSelection()
