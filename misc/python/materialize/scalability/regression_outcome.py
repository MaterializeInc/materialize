# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from materialize.scalability.df.df_totals import (
    DfTotalsExtended,
    concat_df_totals_extended,
)
from materialize.scalability.regression import Regression


class RegressionOutcome:
    def __init__(
        self,
    ):
        self.regressions: list[Regression] = []
        self.regression_data = DfTotalsExtended()

    def has_regressions(self) -> bool:
        assert len(self.regressions) == self.regression_data.length()
        return len(self.regressions) > 0

    def __str__(self) -> str:
        if not self.has_regressions():
            return "No regressions"

        return "\n".join(f"* {x}" for x in self.regressions)

    def merge(self, other: RegressionOutcome) -> None:
        self.regressions.extend(other.regressions)
        self.append_raw_data(other.regression_data)

    def append_raw_data(self, regressions_data: DfTotalsExtended) -> None:
        self.regression_data = concat_df_totals_extended(
            [self.regression_data, regressions_data]
        )
