# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Model for a single measured performance movement."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum

from materialize.buildkite_insights.buildkite_api import builds_api


class Direction(Enum):
    """Which way a metric has to move to be an improvement."""

    LOWER_IS_BETTER = "lower_is_better"
    HIGHER_IS_BETTER = "higher_is_better"


class BaselineKind(Enum):
    """Where the value a movement is measured against came from."""

    JOB_COMPARISON = "job"
    """The CI step ran both sides itself and printed the comparison."""

    WINDOW_MEDIAN = "window"
    """We compared the newest build against the median of preceding builds."""


@dataclass(frozen=True)
class JobRef:
    """Identifies the build job a movement was read from."""

    pipeline_slug: str
    build_number: int
    job_id: str
    step_key: str
    commit_hash: str | None = None
    source: str | None = None
    """Set when the input came from a file rather than a Buildkite job, in which case
    `build_number` and `job_id` do not identify anything."""

    def describe(self) -> str:
        return self.source or f"{self.pipeline_slug}#{self.build_number}"

    def url(self) -> str:
        return builds_api.get_url_to_build(
            self.pipeline_slug, self.build_number, self.job_id
        )


@dataclass(frozen=True)
class Movement:
    """One metric of one scenario, measured against a baseline.

    `this` and `baseline` are in the metric's own unit and are never
    pre-signed: `direction` alone decides whether a change is good or bad.
    """

    job: JobRef
    scenario: str
    metric: str
    this: float
    baseline: float
    direction: Direction
    baseline_kind: BaselineKind
    unit: str | None = None
    threshold_percent: float | None = None
    reported_regression: bool | None = None
    """The step's own verdict, where it publishes one. Independent of `threshold_percent`,
    which some steps print without a verdict."""

    @property
    def change_percent(self) -> float:
        """Signed change of `this` against `baseline`, before applying `direction`."""
        if self.baseline == 0:
            # A metric that was zero and no longer is has no meaningful ratio, but it is
            # still a movement worth ranking above every finite one.
            return 0.0 if self.this == 0 else math.copysign(math.inf, self.this)
        return (self.this / self.baseline - 1.0) * 100.0

    @property
    def magnitude_percent(self) -> float:
        return abs(self.change_percent)

    @property
    def is_improvement(self) -> bool:
        change = self.change_percent
        if change == 0:
            return False
        if self.direction == Direction.LOWER_IS_BETTER:
            return change < 0
        return change > 0

    @property
    def is_deterioration(self) -> bool:
        return self.change_percent != 0 and not self.is_improvement

    def exceeds(self, threshold_percent: float) -> bool:
        """Whether the movement is large enough to report.

        `threshold_percent` is the caller's floor and is the only thing consulted. The
        step's own `threshold_percent` is a gate for failing a build, which is set well
        above the noise floor; deferring to it would hide every movement the gate
        tolerates, including the confirmation that an optimization landed.
        """
        return self.magnitude_percent >= threshold_percent
