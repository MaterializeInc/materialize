# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from materialize.docker import (
    get_mz_version_from_image_tag,
    is_image_tag_of_release_version,
)
from materialize.mz_version import MzVersion
from materialize.mzcompose.test_result import TestFailureDetails
from materialize.scalability.endpoint.endpoint import Endpoint
from materialize.scalability.result.comparison_outcome import ComparisonOutcome
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS,
)
from materialize.version_list import (
    get_commits_of_accepted_regressions_between_versions,
)


class RegressionAssessment:
    def __init__(
        self,
        baseline_endpoint: Endpoint | None,
        comparison_outcome: ComparisonOutcome,
    ):
        self.baseline_endpoint = baseline_endpoint
        self.comparison_outcome = comparison_outcome
        self.endpoints_with_regressions_and_justifications: dict[
            Endpoint, str | None
        ] = {}
        self.determine_whether_regressions_are_justified()
        assert len(comparison_outcome.endpoints_with_regressions) == len(
            self.endpoints_with_regressions_and_justifications
        )

    def has_comparison_target(self) -> bool:
        return self.baseline_endpoint is not None

    def has_regressions(self) -> bool:
        return self.comparison_outcome.has_regressions()

    def has_unjustified_regressions(self):
        return any(
            justification is None
            for justification in self.endpoints_with_regressions_and_justifications.values()
        )

    def determine_whether_regressions_are_justified(self) -> None:
        if self.baseline_endpoint is None:
            return

        if not self.comparison_outcome.has_regressions():
            return

        if not self._endpoint_references_release_version(self.baseline_endpoint):
            # justified regressions require a version as comparison target
            self._mark_all_targets_with_regressions_as_unjustified()
            return

        baseline_version = get_mz_version_from_image_tag(
            self.baseline_endpoint.resolved_target()
        )

        for endpoint in self.comparison_outcome.endpoints_with_regressions:
            commits_with_accepted_regressions = (
                self.collect_accepted_regression_commits_of_endpoint(
                    endpoint, baseline_version
                )
            )

            if len(commits_with_accepted_regressions) > 0:
                self.endpoints_with_regressions_and_justifications[endpoint] = (
                    ", ".join(commits_with_accepted_regressions)
                )
            else:
                self.endpoints_with_regressions_and_justifications[endpoint] = None

    def collect_accepted_regression_commits_of_endpoint(
        self, endpoint: Endpoint, baseline_version: MzVersion
    ) -> list[str]:
        """
        Collect known regressions (in form of commits) between the endpoint version and baseline version.
        @returns list of commits representing known regressions
        """
        if not self._endpoint_references_release_version(endpoint):
            # no explicit version referenced: not supported
            return []

        endpoint_version = get_mz_version_from_image_tag(endpoint.resolved_target())

        if baseline_version >= endpoint_version:
            # baseline more recent than endpoint: not supported, should not be relevant
            return []

        return get_commits_of_accepted_regressions_between_versions(
            ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS,
            since_version_exclusive=baseline_version,
            to_version_inclusive=endpoint_version,
        )

    def _mark_all_targets_with_regressions_as_unjustified(self) -> None:
        for endpoint in self.comparison_outcome.endpoints_with_regressions:
            self.endpoints_with_regressions_and_justifications[endpoint] = None

    def _endpoint_references_release_version(self, endpoint: Endpoint) -> bool:
        target = endpoint.resolved_target()
        return is_image_tag_of_release_version(
            target
        ) and MzVersion.is_valid_version_string(target)

    def to_failure_details(self) -> list[TestFailureDetails]:
        failure_details = []

        assert self.baseline_endpoint is not None
        baseline_version = self.baseline_endpoint.try_load_version()
        for (
            endpoint_with_regression,
            justification,
        ) in self.endpoints_with_regressions_and_justifications.items():
            if justification is not None:
                continue

            regressions = self.comparison_outcome.get_regressions_by_endpoint(
                endpoint_with_regression
            )

            for regression in regressions:
                failure_details.append(
                    TestFailureDetails(
                        test_case_name_override=f"Workload '{regression.workload_name}'",
                        message=f"New regression against {baseline_version}",
                        details=str(regression),
                    )
                )

        return failure_details
