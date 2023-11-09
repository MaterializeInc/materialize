# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.scalability.endpoint import Endpoint
from materialize.scalability.regression_outcome import RegressionOutcome
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

        merged_data = regression_baseline_result.df_totals.merge(other_result.df_totals)
        tps_per_endpoint_data = merged_data.to_enriched_result_frame(
            baseline_endpoint.try_load_version(), other_endpoint.try_load_version()
        )
        entries_exceeding_threshold = tps_per_endpoint_data.to_filtered_with_threshold(
            self.max_deviation_as_percent_decimal
        )

        regression_outcome = RegressionOutcome()
        regressions = entries_exceeding_threshold.to_regressions(
            workload_name,
            other_endpoint,
        )
        regression_outcome.regressions.extend(regressions)
        regression_outcome.append_raw_data(entries_exceeding_threshold)
        return regression_outcome
