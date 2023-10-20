# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import pandas as pd

from materialize.scalability.endpoint import Endpoint
from materialize.scalability.regression import Regression, RegressionOutcome
from materialize.scalability.result_analyzer import (
    ResultAnalyzer,
)
from materialize.scalability.workload_result import WorkloadResult

COL_CONCURRENCY = "concurrency"
COL_WORKLOAD = "workload"
COL_COUNT = "count"
COL_TPS = "tps"
COL_TPS_DIFF = "tps_diff"
COL_TPS_DIFF_PERC = "tps_diff_perc"
COL_TPS_BASELINE = "baseline_tps"
COL_TPS_OTHER = "other_tps"
COL_INFO_BASELINE = "baseline_info"
COL_INFO_OTHER = "other_info"


class DefaultResultAnalyzer(ResultAnalyzer):
    def __init__(self, max_deviation_in_percent: float):
        self.max_deviation_in_percent = max_deviation_in_percent

    def determine_regressions_in_workload(
        self,
        regression_outcome: RegressionOutcome,
        baseline_endpoint: Endpoint,
        workload_name: str,
        results_by_endpoint: dict[Endpoint, WorkloadResult],
    ) -> None:
        count_endpoints = len(results_by_endpoint)

        if count_endpoints <= 1:
            raise RuntimeError("Cannot compute regressions with a single target")

        if baseline_endpoint not in results_by_endpoint.keys():
            raise RuntimeError("Regression baseline endpoint not in results!")

        other_endpoints = list(results_by_endpoint.keys() - {baseline_endpoint})

        for other_endpoint in other_endpoints:
            self.determine_regression_in_workload(
                regression_outcome,
                workload_name,
                baseline_endpoint,
                other_endpoint,
                results_by_endpoint[baseline_endpoint],
                results_by_endpoint[other_endpoint],
            )

    def determine_regression_in_workload(
        self,
        regression_outcome: RegressionOutcome,
        workload_name: str,
        baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
        regression_baseline_result: WorkloadResult,
        other_result: WorkloadResult,
    ) -> None:
        # tps = transactions per seconds (higher is better)

        tps_per_endpoint = self._merge_endpoint_result_frames(
            regression_baseline_result.df_totals, other_result.df_totals
        )
        self._enrich_result_frame(tps_per_endpoint, baseline_endpoint, other_endpoint)
        entries_exceeding_threshold = self._filter_entries_above_threshold(
            tps_per_endpoint
        )

        self.collect_regressions(
            regression_outcome,
            workload_name,
            baseline_endpoint,
            other_endpoint,
            entries_exceeding_threshold,
        )

    def _merge_endpoint_result_frames(
        self, regression_baseline_data: pd.DataFrame, other_data: pd.DataFrame
    ) -> pd.DataFrame:
        merge_columns = [COL_COUNT, COL_CONCURRENCY, COL_WORKLOAD]
        columns_to_keep = merge_columns + [COL_TPS]
        tps_per_endpoint = regression_baseline_data[columns_to_keep].merge(
            other_data[columns_to_keep], on=merge_columns
        )

        tps_per_endpoint.rename(
            columns={f"{COL_TPS}_x": COL_TPS_BASELINE, f"{COL_TPS}_y": COL_TPS_OTHER},
            inplace=True,
        )
        return tps_per_endpoint

    def _enrich_result_frame(
        self,
        tps_per_endpoint: pd.DataFrame,
        baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
    ) -> None:
        tps_per_endpoint[COL_TPS_DIFF] = (
            tps_per_endpoint[COL_TPS_OTHER] - tps_per_endpoint[COL_TPS_BASELINE]
        )
        tps_per_endpoint[COL_TPS_DIFF_PERC] = (
            tps_per_endpoint[COL_TPS_DIFF] / tps_per_endpoint[COL_TPS_BASELINE]
        )
        tps_per_endpoint[COL_INFO_BASELINE] = baseline_endpoint.name()
        tps_per_endpoint[COL_INFO_OTHER] = other_endpoint.name()

    def _filter_entries_above_threshold(
        self, tps_per_endpoint: pd.DataFrame
    ) -> pd.DataFrame:
        return tps_per_endpoint.loc[
            # keep entries x% worse than the baseline
            tps_per_endpoint[COL_TPS_DIFF_PERC] * (-1)
            > self.max_deviation_in_percent
        ]

    def collect_regressions(
        self,
        regression_outcome: RegressionOutcome,
        workload_name: str,
        baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
        entries_exceeding_threshold: pd.DataFrame,
    ) -> None:
        for index, row in entries_exceeding_threshold.iterrows():
            regression = Regression(
                row,
                workload_name,
                concurrency=int(row[COL_CONCURRENCY]),
                count=int(row[COL_COUNT]),
                tps=row[COL_TPS_OTHER],
                tps_baseline=row[COL_TPS_BASELINE],
                tps_diff=row[COL_TPS_DIFF],
                tps_diff_percent=row[COL_TPS_DIFF_PERC],
                endpoint=other_endpoint,
            )
            regression_outcome.regressions.append(regression)

        regression_outcome.append_raw_data(entries_exceeding_threshold)
