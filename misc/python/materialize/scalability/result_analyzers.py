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
COL_COUNT = "count"
COL_TPS = "tps"
COL_TPS_DIFF = "tps_diff"
COL_TPS_DIFF_PERC = "tps_diff_perc"
COL_TPS_BASELINE = "tps_x"
COL_TPS_OTHER = "tps_y"


class DefaultResultAnalyzer(ResultAnalyzer):
    def __init__(self, max_deviation_in_percent: float):
        self.max_deviation_in_percent = max_deviation_in_percent

    def determine_regressions_in_workload(
        self,
        regression_outcome: RegressionOutcome,
        regression_baseline_endpoint: Endpoint,
        workload_name: str,
        endpoint_result_data_by_endpoint: dict[Endpoint, WorkloadResult],
    ) -> None:
        count_endpoints = len(endpoint_result_data_by_endpoint)

        if count_endpoints <= 1:
            raise RuntimeError("Cannot compute regressions with a single target")

        if count_endpoints > 2:
            raise RuntimeError(
                "Regressions for more than two targets are currently not supported"
            )

        if regression_baseline_endpoint not in endpoint_result_data_by_endpoint.keys():
            raise RuntimeError("Regression baseline endpoint not in results!")

        other_endpoint = next(
            iter(
                endpoint_result_data_by_endpoint.keys() - {regression_baseline_endpoint}
            )
        )

        self.determine_regression_in_workload(
            regression_outcome,
            workload_name,
            regression_baseline_endpoint,
            other_endpoint,
            endpoint_result_data_by_endpoint[regression_baseline_endpoint],
            endpoint_result_data_by_endpoint[other_endpoint],
        )

    def determine_regression_in_workload(
        self,
        regression_outcome: RegressionOutcome,
        workload_name: str,
        regression_baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
        regression_baseline_result: WorkloadResult,
        other_result: WorkloadResult,
    ) -> None:
        # tps = transactions per seconds (higher is better)

        columns_to_keep = [COL_COUNT, COL_CONCURRENCY, COL_TPS]
        tps_per_endpoint = regression_baseline_result.df_totals[columns_to_keep].merge(
            other_result.df_totals[columns_to_keep], on=[COL_COUNT, COL_CONCURRENCY]
        )

        tps_per_endpoint[COL_TPS_DIFF] = (
            tps_per_endpoint[COL_TPS_OTHER] - tps_per_endpoint[COL_TPS_BASELINE]
        )
        tps_per_endpoint[COL_TPS_DIFF_PERC] = (
            tps_per_endpoint[COL_TPS_DIFF] / tps_per_endpoint[COL_TPS_BASELINE]
        )

        entries_exceeding_threshold = tps_per_endpoint.loc[
            # keep entries x% worse than the baseline
            tps_per_endpoint[COL_TPS_DIFF_PERC] * (-1)
            > self.max_deviation_in_percent
        ]

        self.collect_regressions(
            regression_outcome,
            workload_name,
            regression_baseline_endpoint,
            other_endpoint,
            entries_exceeding_threshold,
        )

    def collect_regressions(
        self,
        regression_outcome: RegressionOutcome,
        workload_name: str,
        regression_baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
        entries_exceeding_threshold: pd.DataFrame,
    ) -> None:
        for index, row in entries_exceeding_threshold.iterrows():
            regression = Regression(
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


def row_count(data_frame: pd.DataFrame) -> int:
    return len(data_frame.index)
