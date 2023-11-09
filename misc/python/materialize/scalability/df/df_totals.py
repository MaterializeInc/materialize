# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

import pandas as pd

from materialize.scalability.df import df_totals_cols, df_totals_ext_cols
from materialize.scalability.df.df_wrapper_base import (
    DfWrapperBase,
    concat_df_wrapper_data,
)
from materialize.scalability.endpoint import Endpoint
from materialize.scalability.regression import Regression


class DfTotalsBase(DfWrapperBase):
    """Wrapper base for totals data frame."""

    def __init__(self, data: pd.DataFrame = pd.DataFrame()):
        super().__init__(data)

    def get_max_concurrency(self) -> int:
        return self.data[df_totals_cols.CONCURRENCY].max()

    def get_concurrency_values(self) -> list[int]:
        return self.data[df_totals_cols.CONCURRENCY].tolist()


class DfTotals(DfTotalsBase):
    """
    Wrapper for totals data frame.
    Columns are specified in df_totals_cols.
    """

    def __init__(self, data: pd.DataFrame = pd.DataFrame()):
        super().__init__(data)

    def get_tps_values(self) -> list[int]:
        return self.data[df_totals_cols.TPS].tolist()

    def merge(self, other: DfTotals) -> DfTotalsMerged:
        merge_columns = [
            df_totals_cols.COUNT,
            df_totals_cols.CONCURRENCY,
            df_totals_cols.WORKLOAD,
        ]
        columns_to_keep = merge_columns + [df_totals_cols.TPS]
        tps_per_endpoint = self.data[columns_to_keep].merge(
            other.data[columns_to_keep], on=merge_columns
        )

        tps_per_endpoint.rename(
            columns={
                f"{df_totals_cols.TPS}_x": df_totals_ext_cols.TPS_BASELINE,
                f"{df_totals_cols.TPS}_y": df_totals_ext_cols.TPS_OTHER,
            },
            inplace=True,
        )
        return DfTotalsMerged(tps_per_endpoint)


def concat_df_totals(entries: list[DfTotals]) -> DfTotals:
    return DfTotals(concat_df_wrapper_data(entries))


class DfTotalsMerged(DfTotalsBase):
    """
    Wrapper for two totals data frame of different endpoints that were merged.
    It is an intermediate representation and not intended to be used in evaluations and plots.
    """

    def __init__(self, data: pd.DataFrame = pd.DataFrame()):
        super().__init__(data)

    def to_enriched_result_frame(
        self,
        baseline_version_name: str,
        other_version_name: str,
    ) -> DfTotalsExtended:
        tps_per_endpoint = self.data

        tps_per_endpoint[df_totals_ext_cols.TPS_DIFF] = (
            tps_per_endpoint[df_totals_ext_cols.TPS_OTHER]
            - tps_per_endpoint[df_totals_ext_cols.TPS_BASELINE]
        )
        tps_per_endpoint[df_totals_ext_cols.TPS_DIFF_PERC] = (
            tps_per_endpoint[df_totals_ext_cols.TPS_DIFF]
            / tps_per_endpoint[df_totals_ext_cols.TPS_BASELINE]
        )
        tps_per_endpoint[df_totals_ext_cols.INFO_BASELINE] = baseline_version_name
        tps_per_endpoint[df_totals_ext_cols.INFO_OTHER] = other_version_name

        return DfTotalsExtended(tps_per_endpoint)


class DfTotalsExtended(DfTotalsBase):
    """
    Wrapper for two totals data frame of different endpoints that were merged and enriched with further data.
    Columns are specified in df_totals_ext_cols.
    """

    def __init__(self, data: pd.DataFrame = pd.DataFrame()):
        super().__init__(data)

    def to_filtered_with_threshold(self, max_deviation: float) -> DfTotalsExtended:
        tps_per_endpoint = self.data
        filtered_data = tps_per_endpoint.loc[
            # keep entries x% worse than the baseline
            tps_per_endpoint[df_totals_ext_cols.TPS_DIFF_PERC] * (-1)
            > max_deviation
        ]

        return DfTotalsExtended(filtered_data)

    def to_regressions(
        self,
        workload_name: str,
        other_endpoint: Endpoint,
    ) -> list[Regression]:
        result = []
        for index, row in self.data.iterrows():
            regression = Regression(
                workload_name,
                concurrency=int(row[df_totals_ext_cols.CONCURRENCY]),
                count=int(row[df_totals_ext_cols.COUNT]),
                tps=row[df_totals_ext_cols.TPS_OTHER],
                tps_baseline=row[df_totals_ext_cols.TPS_BASELINE],
                tps_diff=row[df_totals_ext_cols.TPS_DIFF],
                tps_diff_percent=row[df_totals_ext_cols.TPS_DIFF_PERC],
                endpoint=other_endpoint,
            )
            result.append(regression)

        return result


def concat_df_totals_extended(entries: list[DfTotalsExtended]) -> DfTotalsExtended:
    return DfTotalsExtended(concat_df_wrapper_data(entries))
