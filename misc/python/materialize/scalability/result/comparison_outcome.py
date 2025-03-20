# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from collections.abc import Sequence

from materialize.scalability.df.df_totals import (
    DfTotalsExtended,
    concat_df_totals_extended,
)
from materialize.scalability.endpoint.endpoint import Endpoint
from materialize.scalability.result.scalability_change import (
    Regression,
    ScalabilityChange,
    ScalabilityImprovement,
)


class ComparisonOutcome:
    def __init__(
        self,
    ):
        self.regressions: list[Regression] = []
        self.significant_improvements: list[ScalabilityImprovement] = []
        self.regression_df = DfTotalsExtended()
        self.significant_improvement_df = DfTotalsExtended()
        self.endpoints_with_regressions: set[Endpoint] = set()

    def has_regressions(self) -> bool:
        assert len(self.regressions) == self.regression_df.length()
        return len(self.regressions) > 0

    def has_significant_improvements(self) -> bool:
        assert (
            len(self.significant_improvements)
            == self.significant_improvement_df.length()
        )
        return len(self.significant_improvements) > 0

    def has_scalability_changes(self) -> bool:
        return self.has_regressions() or self.has_significant_improvements()

    def __str__(self) -> str:
        return f"{len(self.regressions)} regressions, {len(self.significant_improvements)} significant improvements"

    def to_description(self) -> str:
        regressions_description = (
            f"Regressions:\n{self._to_description(self.regressions)}"
        )
        improvements_description = (
            f"Improvements:\n{self._to_description(self.significant_improvements)}"
        )
        return "\n".join([regressions_description, improvements_description])

    def _to_description(self, entries: Sequence[ScalabilityChange]) -> str:
        if len(entries) == 0:
            return "* None"

        return "\n".join(f"* {x}" for x in entries)

    def merge(self, other: ComparisonOutcome) -> None:
        self.append_regressions(
            other.regressions,
            other.significant_improvements,
            other.regression_df,
            other.significant_improvement_df,
        )

    def append_regressions(
        self,
        regressions: list[Regression],
        significant_improvements: list[ScalabilityImprovement],
        regression_df: DfTotalsExtended,
        significant_improvement_df: DfTotalsExtended,
    ) -> None:
        self.regressions.extend(regressions)
        self.significant_improvements.extend(significant_improvements)
        self._append_regression_df(regression_df)
        self._append_significant_improvement_df(significant_improvement_df)

        for regression in regressions:
            self.endpoints_with_regressions.add(regression.endpoint)

    def _append_regression_df(self, regressions_data: DfTotalsExtended) -> None:
        self.regression_df = concat_df_totals_extended(
            [self.regression_df, regressions_data]
        )

    def _append_significant_improvement_df(
        self, significant_improvements_data: DfTotalsExtended
    ) -> None:
        self.significant_improvement_df = concat_df_totals_extended(
            [self.significant_improvement_df, significant_improvements_data]
        )

    def get_regressions_by_endpoint(self, endpoint: Endpoint) -> list[Regression]:
        return [
            regression
            for regression in self.regressions
            if regression.endpoint == endpoint
        ]
