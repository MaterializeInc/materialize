# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.scalability.endpoint.endpoint import Endpoint
from materialize.scalability.result.comparison_outcome import ComparisonOutcome
from materialize.scalability.result.result_analyzer import ResultAnalyzer
from materialize.scalability.result.scalability_change import (
    Regression,
    ScalabilityImprovement,
)
from materialize.scalability.result.workload_result import WorkloadResult


class DefaultResultAnalyzer(ResultAnalyzer):
    def __init__(self, max_deviation_as_percent_decimal: float):
        self.max_deviation_as_percent_decimal = max_deviation_as_percent_decimal

    def perform_comparison_in_workload(
        self,
        workload_name: str,
        baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
        regression_baseline_result: WorkloadResult,
        other_result: WorkloadResult,
    ) -> ComparisonOutcome:
        # tps = transactions per seconds (higher is better)

        merged_data = regression_baseline_result.df_totals.merge(other_result.df_totals)
        tps_per_endpoint_data = merged_data.to_enriched_result_frame(
            baseline_endpoint.try_load_version(), other_endpoint.try_load_version()
        )
        entries_worse_than_threshold = tps_per_endpoint_data.to_filtered_with_threshold(
            self.max_deviation_as_percent_decimal,
            match_results_better_than_baseline=False,
        )
        entries_better_than_threshold = (
            tps_per_endpoint_data.to_filtered_with_threshold(
                self.max_deviation_as_percent_decimal,
                match_results_better_than_baseline=True,
            )
        )

        comparison_outcome = ComparisonOutcome()
        regressions = entries_worse_than_threshold.to_scalability_change(
            Regression,
            workload_name,
            other_endpoint,
        )
        improvements = entries_better_than_threshold.to_scalability_change(
            ScalabilityImprovement,
            workload_name,
            other_endpoint,
        )
        comparison_outcome.append_regressions(
            regressions,
            improvements,
            entries_worse_than_threshold,
            entries_better_than_threshold,
        )
        return comparison_outcome
