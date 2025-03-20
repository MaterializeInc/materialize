# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.query.data_source import DataSource


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

    def trim_to_minimized_sources(self, data_sources: list[DataSource]):
        data_sources_to_remove_from_selection = []

        for data_source in self.row_indices_per_data_source.keys():
            if data_source not in data_sources:
                data_sources_to_remove_from_selection.append(data_source)

        for data_source in data_sources_to_remove_from_selection:
            del self.row_indices_per_data_source[data_source]


ALL_ROWS_SELECTION = DataRowSelection()
