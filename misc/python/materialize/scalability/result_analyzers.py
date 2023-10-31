# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import pandas as pd

from materialize.scalability.df import df_totals_cols, df_totals_ext_cols
from materialize.scalability.endpoint import Endpoint
from materialize.scalability.regression import Regression, RegressionOutcome
from materialize.scalability.result_analyzer import (
    ResultAnalyzer,
)
from materialize.scalability.workload_result import WorkloadResult


class DefaultResultAnalyzer(ResultAnalyzer):
    def __init__(self, max_deviation_as_percent_decimal: float):
        self.max_deviation_as_percent_decimal = max_deviation_as_percent_decimal

    def determine_regression_in_workload(
        self,
        workload_name: str,
        baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
        regression_baseline_result: WorkloadResult,
        other_result: WorkloadResult,
    ) -> RegressionOutcome:
        # tps = transactions per seconds (higher is better)

        tps_per_endpoint = self._merge_endpoint_result_frames(
            regression_baseline_result.df_totals, other_result.df_totals
        )
        self._enrich_result_frame(tps_per_endpoint, baseline_endpoint, other_endpoint)
        entries_exceeding_threshold = self._filter_entries_above_threshold(
            tps_per_endpoint
        )

        return self.collect_regressions(
            workload_name,
            baseline_endpoint,
            other_endpoint,
            entries_exceeding_threshold,
        )

    def _merge_endpoint_result_frames(
        self, regression_baseline_data: pd.DataFrame, other_data: pd.DataFrame
    ) -> pd.DataFrame:
        merge_columns = [
            df_totals_cols.COUNT,
            df_totals_cols.CONCURRENCY,
            df_totals_cols.WORKLOAD,
        ]
        columns_to_keep = merge_columns + [df_totals_cols.TPS]
        tps_per_endpoint = regression_baseline_data[columns_to_keep].merge(
            other_data[columns_to_keep], on=merge_columns
        )

        tps_per_endpoint.rename(
            columns={
                f"{df_totals_cols.TPS}_x": df_totals_ext_cols.TPS_BASELINE,
                f"{df_totals_cols.TPS}_y": df_totals_ext_cols.TPS_OTHER,
            },
            inplace=True,
        )
        return tps_per_endpoint

    def _enrich_result_frame(
        self,
        tps_per_endpoint: pd.DataFrame,
        baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
    ) -> None:
        tps_per_endpoint[df_totals_ext_cols.TPS_DIFF] = (
            tps_per_endpoint[df_totals_ext_cols.TPS_OTHER]
            - tps_per_endpoint[df_totals_ext_cols.TPS_BASELINE]
        )
        tps_per_endpoint[df_totals_ext_cols.TPS_DIFF_PERC] = (
            tps_per_endpoint[df_totals_ext_cols.TPS_DIFF]
            / tps_per_endpoint[df_totals_ext_cols.TPS_BASELINE]
        )
        tps_per_endpoint[
            df_totals_ext_cols.INFO_BASELINE
        ] = baseline_endpoint.try_load_version()
        tps_per_endpoint[
            df_totals_ext_cols.INFO_OTHER
        ] = other_endpoint.try_load_version()

    def _filter_entries_above_threshold(
        self, tps_per_endpoint: pd.DataFrame
    ) -> pd.DataFrame:
        return tps_per_endpoint.loc[
            # keep entries x% worse than the baseline
            tps_per_endpoint[df_totals_ext_cols.TPS_DIFF_PERC] * (-1)
            > self.max_deviation_as_percent_decimal
        ]

    def collect_regressions(
        self,
        workload_name: str,
        baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
        entries_exceeding_threshold: pd.DataFrame,
    ) -> RegressionOutcome:
        regression_outcome = RegressionOutcome()
        for index, row in entries_exceeding_threshold.iterrows():
            regression = Regression(
                row,
                workload_name,
                concurrency=int(row[df_totals_cols.CONCURRENCY]),
                count=int(row[df_totals_cols.COUNT]),
                tps=row[df_totals_ext_cols.TPS_OTHER],
                tps_baseline=row[df_totals_ext_cols.TPS_BASELINE],
                tps_diff=row[df_totals_ext_cols.TPS_DIFF],
                tps_diff_percent=row[df_totals_ext_cols.TPS_DIFF_PERC],
                endpoint=other_endpoint,
            )
            regression_outcome.regressions.append(regression)

        regression_outcome.append_raw_data(entries_exceeding_threshold)
        return regression_outcome
