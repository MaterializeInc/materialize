# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass

import pandas as pd

from materialize.scalability.regression import RegressionOutcome
from materialize.scalability.workload_result import WorkloadResult


@dataclass
class BenchmarkResult:
    overall_regression_outcome: RegressionOutcome
    df_total_by_endpoint_name_and_workload: dict[str, dict[str, pd.DataFrame]]

    def __init__(self):
        self.overall_regression_outcome = RegressionOutcome()
        self.df_total_by_endpoint_name_and_workload = dict()

    def add_regression(self, regression_outcome: RegressionOutcome | None) -> None:
        if regression_outcome is not None:
            self.overall_regression_outcome.merge(regression_outcome)

    def get_endpoint_names(self) -> list[str]:
        return list(self.df_total_by_endpoint_name_and_workload.keys())

    def append_workload_result(
        self, endpoint_version_info: str, result: WorkloadResult
    ) -> None:
        if (
            endpoint_version_info
            not in self.df_total_by_endpoint_name_and_workload.keys()
        ):
            self.df_total_by_endpoint_name_and_workload[endpoint_version_info] = dict()

        workload_name = result.workload.name()
        assert (
            workload_name
            not in self.df_total_by_endpoint_name_and_workload[
                endpoint_version_info
            ].keys()
        ), f"Results already contain an entry for this endpoint ({endpoint_version_info}) and workload {workload_name}"

        self.df_total_by_endpoint_name_and_workload[endpoint_version_info][
            workload_name
        ] = result.df_totals

    def get_df_total_by_endpoint_name(self, endpoint_name: str) -> pd.DataFrame:
        return pd.concat(
            self.df_total_by_endpoint_name_and_workload[endpoint_name].values(),
            ignore_index=True,
        )
