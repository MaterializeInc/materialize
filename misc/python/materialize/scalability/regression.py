# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

import pandas as pd

from materialize.scalability.endpoint import Endpoint


class Regression:
    def __init__(
        self,
        row: pd.Series,
        workload_name: str,
        concurrency: int,
        count: int,
        tps: float,
        tps_baseline: float,
        tps_diff: float,
        tps_diff_percent: float,
        endpoint: Endpoint,
    ):
        self.row = row
        self.workload_name = workload_name
        self.concurrency = concurrency
        self.count = count
        self.tps = tps
        self.tps_baseline = tps_baseline
        assert tps_diff < 0, "Not a regression!"
        self.tps_diff = tps_diff
        self.tps_diff_percent = tps_diff_percent
        self.endpoint = endpoint

    def __str__(self) -> str:
        return (
            f"Regression in workload '{self.workload_name}' at concurrency {self.concurrency} with {self.endpoint}:"
            f" {round(self.tps, 2)} tps vs. {round(self.tps_baseline, 2)} tps"
            f" ({round(self.tps_diff, 2)} tps; {round(100 * self.tps_diff_percent, 2)}%)"
        )


class RegressionOutcome:
    def __init__(
        self,
    ):
        self.regressions: list[Regression] = []
        self.raw_regression_data = pd.DataFrame()

    def has_regressions(self) -> bool:
        assert len(self.regressions) == len(self.raw_regression_data.index)
        return len(self.regressions) > 0

    def __str__(self) -> str:
        if not self.has_regressions():
            return "No regressions"

        return "\n".join(f"* {x}" for x in self.regressions)

    def merge(self, other: RegressionOutcome) -> None:
        self.regressions.extend(other.regressions)
        self.append_raw_data(other.raw_regression_data)

    def append_raw_data(self, regressions_frame: pd.DataFrame) -> None:
        self.raw_regression_data = pd.concat(
            [self.raw_regression_data, regressions_frame], ignore_index=True
        )
