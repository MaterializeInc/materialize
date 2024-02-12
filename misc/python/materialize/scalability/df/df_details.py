# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from collections.abc import Callable

import pandas as pd
from pandas import Series
from pandas.core.groupby.generic import SeriesGroupBy

from materialize.scalability.df import df_details_cols
from materialize.scalability.df.df_wrapper_base import (
    DfWrapperBase,
    concat_df_wrapper_data,
)


class DfDetails(DfWrapperBase):
    """Wrapper for details data frame. Columns are specified in df_details_cols."""

    def __init__(self, data: pd.DataFrame = pd.DataFrame()):
        super().__init__(data)

    def to_filtered_by_concurrency(self, concurrency: int) -> DfDetails:
        filtered_data = self.data.loc[
            self.data[df_details_cols.CONCURRENCY] == concurrency
        ]

        return DfDetails(filtered_data)

    def get_wallclock_values(self, group_by_transaction: bool = True) -> list[float]:
        aggregation = lambda groupby_series: groupby_series.sum()
        return self._get_column_values(
            df_details_cols.WALLCLOCK, group_by_transaction, aggregation
        ).tolist()

    def get_concurrency_values(self, group_by_transaction: bool = True) -> list[int]:
        aggregation = lambda groupby_series: groupby_series.unique()
        return self._get_column_values(
            df_details_cols.CONCURRENCY, group_by_transaction, aggregation
        ).tolist()

    def get_unique_concurrency_values(self) -> list[int]:
        return self.data[df_details_cols.CONCURRENCY].unique().tolist()

    def _get_column_values(
        self,
        column_name: str,
        group_by_transaction: bool,
        aggregation: Callable[[SeriesGroupBy], Series],
    ) -> Series:
        if group_by_transaction:
            groupby_series = self.data.groupby(by=[df_details_cols.TRANSACTION_INDEX])[
                column_name
            ]
            return aggregation(groupby_series)

        return self.data[column_name]


def concat_df_details(entries: list[DfDetails]) -> DfDetails:
    return DfDetails(concat_df_wrapper_data(entries))
