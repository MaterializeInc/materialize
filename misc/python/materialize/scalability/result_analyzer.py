# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from __future__ import annotations

from materialize.scalability.endpoint import Endpoint
from materialize.scalability.regression import RegressionOutcome
from materialize.scalability.workload_result import WorkloadResult


class ResultAnalyzer:
    def determine_regression(
        self,
        baseline_endpoint: Endpoint,
        results_by_workload_name: dict[str, dict[Endpoint, WorkloadResult]],
    ) -> RegressionOutcome:
        regression_outcome = RegressionOutcome()
        for workload_name in results_by_workload_name.keys():
            self.determine_regressions_in_workload(
                regression_outcome,
                baseline_endpoint,
                workload_name,
                results_by_workload_name[workload_name],
            )

        return regression_outcome

    def determine_regressions_in_workload(
        self,
        regression_outcome: RegressionOutcome,
        baseline_endpoint: Endpoint,
        workload_name: str,
        results_by_endpoint: dict[Endpoint, WorkloadResult],
    ) -> bool:
        raise NotImplementedError
