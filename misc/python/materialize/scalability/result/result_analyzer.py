# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from __future__ import annotations

from materialize.scalability.endpoint.endpoint import Endpoint
from materialize.scalability.result.comparison_outcome import ComparisonOutcome
from materialize.scalability.result.workload_result import WorkloadResult


class ResultAnalyzer:
    def perform_comparison_in_workload(
        self,
        workload_name: str,
        baseline_endpoint: Endpoint,
        other_endpoint: Endpoint,
        regression_baseline_result: WorkloadResult,
        other_result: WorkloadResult,
    ) -> ComparisonOutcome:
        raise NotImplementedError
