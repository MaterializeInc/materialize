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
    is_image_tag_of_version,
)
from materialize.mz_version import MzVersion
from materialize.scalability.comparison_outcome import ComparisonOutcome
from materialize.scalability.endpoint import Endpoint
from materialize.version_list import (
    ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS,
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
        self.check_targets()
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

    def check_targets(self) -> None:
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
            if not self._endpoint_references_release_version(endpoint):
                continue

            endpoint_version = get_mz_version_from_image_tag(endpoint.resolved_target())

            if baseline_version >= endpoint_version:
                # not supported, should not be a relevant case
                continue

            commits_with_accepted_regressions = (
                get_commits_of_accepted_regressions_between_versions(
                    ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS,
                    since_version_exclusive=baseline_version,
                    to_version_inclusive=endpoint_version,
                )
            )

            if len(commits_with_accepted_regressions) > 0:
                self.endpoints_with_regressions_and_justifications[
                    endpoint
                ] = ", ".join(commits_with_accepted_regressions)
            else:
                self.endpoints_with_regressions_and_justifications[endpoint] = None

    def _mark_all_targets_with_regressions_as_unjustified(self) -> None:
        for endpoint in self.comparison_outcome.endpoints_with_regressions:
            self.endpoints_with_regressions_and_justifications[endpoint] = None

    def _endpoint_references_release_version(self, endpoint: Endpoint) -> bool:
        target = endpoint.resolved_target()
        return is_image_tag_of_version(target) and MzVersion.is_valid_version_string(
            target
        )
